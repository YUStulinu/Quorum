/*
 * Write-ahead log. The format and the reasoning behind it are in wal.h.
 */
#include "storage/wal.h"

#include <stdlib.h>
#include <time.h>

#include "util/common.h"
#include "util/u32map.h"

#define WAL_MAGIC 0x57535354u
#define WAL_VERSION 1u
#define RECOVERY_BATCH 64u /* frames read per I/O during recovery */

struct ts_wal {
    ts_vfs  *vfs;
    ts_file *file;

    uint32_t ckpt_seq;
    uint32_t salt1, salt2;
    uint32_t header_crc;

    u32map   index;        /* page number -> offset of its newest committed frame */
    uint64_t end;          /* where the next frame goes */
    uint32_t chain;        /* checksum of the last committed frame (or header) */
    uint32_t frames;       /* committed frames in the log */
    uint64_t checkpoints;
    uint64_t rng;
};

static uint32_t random_u32(ts_wal *w) {
    return (uint32_t)(ts_splitmix64(&w->rng) >> 16);
}

static void encode_header(const ts_wal *w, uint8_t *h, uint32_t *crc_out) {
    memset(h, 0, WAL_HEADER_SIZE);
    put_u32(h + 0, WAL_MAGIC);
    put_u32(h + 4, WAL_VERSION);
    put_u32(h + 8, TS_PAGE_SIZE);
    put_u32(h + 12, w->ckpt_seq);
    put_u32(h + 16, w->salt1);
    put_u32(h + 20, w->salt2);
    uint32_t crc = ts_crc32c(0, h, 28);
    put_u32(h + 28, crc);
    *crc_out = crc;
}

/* Starts a new generation of the log: new salts, empty file. */
static int reset_log(ts_wal *w, int sync) {
    w->ckpt_seq++;
    w->salt1++;
    w->salt2 = random_u32(w);

    uint8_t h[WAL_HEADER_SIZE];
    encode_header(w, h, &w->header_crc);
    /* Write the new header before truncating: if we crash in between, the new
       salts already invalidate every old frame. */
    TS_TRY(vfs_write(w->file, h, sizeof h, 0));
    TS_TRY(vfs_truncate(w->file, WAL_HEADER_SIZE));
    if (sync) TS_TRY(vfs_sync(w->file));

    u32map_clear(&w->index);
    w->end = WAL_HEADER_SIZE;
    w->chain = w->header_crc;
    w->frames = 0;
    return TS_OK;
}

int wal_open(ts_vfs *vfs, const char *path, ts_wal **out) {
    ts_wal *w = (ts_wal *)calloc(1, sizeof *w);
    if (!w) return TS_NOMEM;
    w->vfs = vfs;
    w->rng = (uint64_t)time(NULL) ^ ((uint64_t)clock() << 32) ^ (uint64_t)(uintptr_t)w;
    int rc = u32map_init(&w->index, 256);
    if (rc == TS_OK) rc = vfs->open(vfs, path, 1, &w->file);
    if (rc != TS_OK) {
        u32map_free(&w->index);
        free(w);
        return rc;
    }
    *out = w;
    return TS_OK;
}

void wal_close(ts_wal *w) {
    if (!w) return;
    if (w->file) vfs_close(w->file);
    u32map_free(&w->index);
    free(w);
}

/* Index entries of the transaction being read; applied only when its commit frame validates. */
typedef struct {
    uint32_t *pgno;
    uint64_t *off;
    size_t    n, cap;
} pending_list;

static int pending_add(pending_list *p, uint32_t pgno, uint64_t off) {
    if (p->n == p->cap) {
        size_t cap = p->cap ? p->cap * 2 : 64;
        uint32_t *a = (uint32_t *)realloc(p->pgno, cap * sizeof *a);
        if (!a) return TS_NOMEM;
        p->pgno = a;
        uint64_t *b = (uint64_t *)realloc(p->off, cap * sizeof *b);
        if (!b) return TS_NOMEM;
        p->off = b;
        p->cap = cap;
    }
    p->pgno[p->n] = pgno;
    p->off[p->n] = off;
    p->n++;
    return TS_OK;
}

int wal_recover(ts_wal *w, uint32_t *db_pages) {
    *db_pages = 0;
    uint64_t size = 0;
    TS_TRY(vfs_size(w->file, &size));

    uint8_t h[WAL_HEADER_SIZE];
    size_t got = 0;
    if (size >= WAL_HEADER_SIZE) TS_TRY(vfs_read(w->file, h, sizeof h, 0, &got));
    int header_ok = got == WAL_HEADER_SIZE && get_u32(h) == WAL_MAGIC && get_u32(h + 4) == WAL_VERSION &&
                    get_u32(h + 8) == TS_PAGE_SIZE && get_u32(h + 28) == ts_crc32c(0, h, 28);
    if (!header_ok) {
        /* No log, or a log whose header never made it to disk: nothing to recover. */
        w->salt1 = random_u32(w);
        return reset_log(w, 1);
    }
    w->ckpt_seq = get_u32(h + 12);
    w->salt1 = get_u32(h + 16);
    w->salt2 = get_u32(h + 20);
    w->header_crc = get_u32(h + 28);
    u32map_clear(&w->index);

    uint8_t *buf = (uint8_t *)malloc((size_t)RECOVERY_BATCH * WAL_FRAME_SIZE);
    if (!buf) return TS_NOMEM;
    pending_list pending = {0};
    int rc = TS_OK;

    uint32_t chain = w->header_crc;
    uint64_t pos = WAL_HEADER_SIZE;
    uint64_t committed_end = WAL_HEADER_SIZE;
    uint32_t committed_chain = chain;
    uint32_t committed_frames = 0, frames_seen = 0;

    int done = 0;
    while (!done && rc == TS_OK) {
        rc = vfs_read(w->file, buf, (size_t)RECOVERY_BATCH * WAL_FRAME_SIZE, pos, &got);
        if (rc != TS_OK) break;
        size_t nframes = got / WAL_FRAME_SIZE;
        if (nframes == 0) break;
        for (size_t i = 0; i < nframes; i++) {
            const uint8_t *f = buf + i * WAL_FRAME_SIZE;
            uint32_t pgno = get_u32(f);
            uint32_t commit = get_u32(f + 4);
            if (get_u32(f + 8) != w->salt1 || get_u32(f + 12) != w->salt2) {
                done = 1; /* a frame from an older generation: the log ends here */
                break;
            }
            uint32_t c = ts_crc32c(chain, f, 20);
            c = ts_crc32c(c, f + WAL_FRAME_HEADER, TS_PAGE_SIZE);
            if (c != get_u32(f + 20)) {
                done = 1; /* torn or never-completed write */
                break;
            }
            chain = c;
            frames_seen++;
            uint64_t frame_off = pos + (uint64_t)i * WAL_FRAME_SIZE;
            rc = pending_add(&pending, pgno, frame_off);
            if (rc != TS_OK) break;
            if (commit != 0) {
                for (size_t k = 0; k < pending.n && rc == TS_OK; k++)
                    rc = u32map_put(&w->index, pending.pgno[k], pending.off[k]);
                pending.n = 0;
                committed_end = frame_off + WAL_FRAME_SIZE;
                committed_chain = chain;
                committed_frames = frames_seen;
                *db_pages = commit;
            }
        }
        if (nframes < RECOVERY_BATCH) break;
        pos += (uint64_t)nframes * WAL_FRAME_SIZE;
    }

    free(pending.pgno);
    free(pending.off);
    free(buf);
    if (rc != TS_OK) return rc;

    w->end = committed_end;
    w->chain = committed_chain;
    w->frames = committed_frames;
    /* Cut off the uncommitted tail so new frames are written right after the
       last commit, then make that durable before anything else happens. */
    if (size > committed_end) {
        TS_TRY(vfs_truncate(w->file, committed_end));
        TS_TRY(vfs_sync(w->file));
    }
    return TS_OK;
}

int wal_find(const ts_wal *w, uint32_t pgno, uint64_t *frame_off) {
    return u32map_get(&w->index, pgno, frame_off);
}

int wal_read_page(ts_wal *w, uint64_t frame_off, uint8_t *page) {
    return vfs_read_exact(w->file, page, TS_PAGE_SIZE, frame_off + WAL_FRAME_HEADER);
}

int wal_commit(ts_wal *w, uint8_t *const *pages, const uint32_t *pgnos, size_t n, uint32_t db_pages, int sync) {
    if (n == 0) return TS_OK;
    uint8_t *buf = (uint8_t *)malloc(n * WAL_FRAME_SIZE);
    if (!buf) return TS_NOMEM;

    uint32_t chain = w->chain;
    for (size_t i = 0; i < n; i++) {
        uint8_t *f = buf + i * WAL_FRAME_SIZE;
        put_u32(f, pgnos[i]);
        put_u32(f + 4, i + 1 == n ? db_pages : 0);
        put_u32(f + 8, w->salt1);
        put_u32(f + 12, w->salt2);
        put_u32(f + 16, 0);
        memcpy(f + WAL_FRAME_HEADER, pages[i], TS_PAGE_SIZE);
        chain = ts_crc32c(chain, f, 20);
        chain = ts_crc32c(chain, f + WAL_FRAME_HEADER, TS_PAGE_SIZE);
        put_u32(f + 20, chain);
    }

    /* One write for the whole transaction, then one fsync: the commit is
       durable exactly when the sync returns. */
    int rc = vfs_write(w->file, buf, n * WAL_FRAME_SIZE, w->end);
    free(buf);
    if (rc == TS_OK && sync) rc = vfs_sync(w->file);
    if (rc != TS_OK) return rc;

    for (size_t i = 0; i < n; i++) TS_TRY(u32map_put(&w->index, pgnos[i], w->end + (uint64_t)i * WAL_FRAME_SIZE));
    w->end += (uint64_t)n * WAL_FRAME_SIZE;
    w->chain = chain;
    w->frames += (uint32_t)n;
    return TS_OK;
}

static int cmp_u32(const void *a, const void *b) {
    uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b;
    return (x > y) - (x < y);
}

int wal_checkpoint(ts_wal *w, ts_file *db, int sync) {
    if (w->index.count == 0) {
        if (w->end > WAL_HEADER_SIZE) return reset_log(w, sync);
        return TS_OK;
    }

    /* Ordering is what makes a checkpoint crash-safe:
       1. The log must be durable before any page reaches the database file;
          otherwise a crash could keep a page from a commit that the log then
          loses, mixing two versions of the tree (relevant with TS_SYNC_NORMAL).
       2. The database file must be durable before the log is emptied;
          until then, recovery simply copies the same pages again. */
    if (sync) TS_TRY(vfs_sync(w->file));

    uint32_t *pgnos = (uint32_t *)malloc(w->index.count * sizeof *pgnos);
    uint8_t *page = (uint8_t *)malloc(TS_PAGE_SIZE);
    if (!pgnos || !page) {
        free(pgnos);
        free(page);
        return TS_NOMEM;
    }
    size_t n = 0;
    for (size_t i = 0; i < w->index.cap; i++)
        if (w->index.keys[i]) pgnos[n++] = w->index.keys[i] - 1;
    qsort(pgnos, n, sizeof *pgnos, cmp_u32); /* ascending: sequential writes */

    int rc = TS_OK;
    for (size_t i = 0; i < n && rc == TS_OK; i++) {
        uint64_t off = 0;
        (void)u32map_get(&w->index, pgnos[i], &off);
        rc = wal_read_page(w, off, page);
        if (rc == TS_OK) rc = vfs_write(db, page, TS_PAGE_SIZE, (uint64_t)pgnos[i] * TS_PAGE_SIZE);
    }
    free(pgnos);
    free(page);
    if (rc == TS_OK && sync) rc = vfs_sync(db);
    if (rc == TS_OK) rc = reset_log(w, sync);
    if (rc == TS_OK) w->checkpoints++;
    return rc;
}

uint32_t wal_frame_count(const ts_wal *w) {
    return w->frames;
}

uint64_t wal_checkpoint_count(const ts_wal *w) {
    return w->checkpoints;
}
