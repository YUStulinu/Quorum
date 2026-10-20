/*
 * Pager: page cache, page I/O, transactions. Overview in pager.h.
 */
#include "storage/pager.h"

#include <stdlib.h>

#include "storage/wal.h"
#include "util/u32map.h"

#define DB_MAGIC "TESSERA"   /* 7 chars + NUL = 8 bytes */
#define DB_FORMAT_VERSION 1u
#define MAX_TREE_HEIGHT 40u
#define MAX_PAGE_COUNT 0xFFFFFF00u

struct ts_pager {
    ts_vfs      *vfs;
    ts_file     *db;
    ts_wal      *wal;
    char        *path;
    char        *wal_path;

    u32map       cache;      /* page number -> ts_page* */
    ts_page      lru;        /* sentinel; lru.next = most recently used */
    ts_page      dirty;      /* sentinel; pages modified by the open transaction */
    size_t       cached;
    size_t       limit;

    ts_meta      meta;
    ts_meta      saved;      /* meta at BEGIN, restored by ROLLBACK */
    bool         in_txn;
    bool         poisoned;
    uint64_t     db_pages;   /* whole pages present in the database file */

    ts_sync_mode sync;
    uint32_t     ckpt_threshold;
    uint64_t     version;
    uint64_t     hits, misses, commits;
};

/* ------------------------------------------------------------------------
 * Intrusive doubly linked lists with a sentinel
 * ---------------------------------------------------------------------- */
static void list_init(ts_page *s) {
    s->prev = s->next = s;
}
static void list_remove(ts_page *pg) {
    pg->prev->next = pg->next;
    pg->next->prev = pg->prev;
    pg->prev = pg->next = NULL;
}
static void list_push_front(ts_page *s, ts_page *pg) {
    pg->next = s->next;
    pg->prev = s;
    s->next->prev = pg;
    s->next = pg;
}

/* ------------------------------------------------------------------------
 * Header (page 0) encoding
 * ---------------------------------------------------------------------- */
static void encode_meta(const ts_meta *m, uint8_t *pg) {
    memset(pg, 0, TS_PAGE_USABLE);
    memcpy(pg, DB_MAGIC, 8);
    put_u32(pg + 8, DB_FORMAT_VERSION);
    put_u32(pg + 12, TS_PAGE_SIZE);
    put_u32(pg + 16, m->page_count);
    put_u32(pg + 20, m->root);
    put_u32(pg + 24, m->height);
    put_u32(pg + 28, m->freelist_head);
    put_u32(pg + 32, m->freelist_count);
    put_u64(pg + 40, m->txn_id);
    put_u64(pg + 48, m->key_count);
}

static int decode_meta(const uint8_t *pg, ts_meta *m) {
    if (memcmp(pg, DB_MAGIC, 8) != 0) return TS_NOTADB;
    if (!ts_page_verify(pg)) return TS_CORRUPT;
    if (get_u32(pg + 8) != DB_FORMAT_VERSION || get_u32(pg + 12) != TS_PAGE_SIZE) return TS_NOTADB;
    m->page_count = get_u32(pg + 16);
    m->root = get_u32(pg + 20);
    m->height = get_u32(pg + 24);
    m->freelist_head = get_u32(pg + 28);
    m->freelist_count = get_u32(pg + 32);
    m->txn_id = get_u64(pg + 40);
    m->key_count = get_u64(pg + 48);
    if (m->page_count < 2 || m->page_count > MAX_PAGE_COUNT || m->root == 0 || m->root >= m->page_count ||
        m->height == 0 || m->height > MAX_TREE_HEIGHT || m->freelist_head >= m->page_count ||
        m->freelist_count >= m->page_count)
        return TS_CORRUPT;
    return TS_OK;
}

/* ------------------------------------------------------------------------
 * Cache frames
 * ---------------------------------------------------------------------- */
static void free_page_struct(ts_page *pg) {
    free(pg->data);
    free(pg);
}

/* A frame for `pgno`, registered in the cache, unpinned, clean, contents undefined. */
static int new_frame(ts_pager *p, uint32_t pgno, ts_page **out) {
    ts_page *pg = NULL;
    if (p->cached >= p->limit && p->lru.prev != &p->lru) {
        pg = p->lru.prev; /* least recently used clean, unpinned page */
        list_remove(pg);
        u32map_del(&p->cache, pg->pgno);
    } else {
        pg = (ts_page *)calloc(1, sizeof *pg);
        if (!pg) return TS_NOMEM;
        pg->data = (uint8_t *)malloc(TS_PAGE_SIZE);
        if (!pg->data) {
            free(pg);
            return TS_NOMEM;
        }
        p->cached++;
    }
    pg->pgno = pgno;
    pg->pin = 0;
    pg->dirty = false;
    pg->checked = false;
    pg->prev = pg->next = NULL;
    int rc = u32map_put(&p->cache, pgno, (uint64_t)(uintptr_t)pg);
    if (rc != TS_OK) {
        free_page_struct(pg);
        p->cached--;
        return rc;
    }
    *out = pg;
    return TS_OK;
}

static void drop_frame(ts_pager *p, ts_page *pg) {
    u32map_del(&p->cache, pg->pgno);
    free_page_struct(pg);
    p->cached--;
}

/* Reads the committed version of a page: newest WAL frame, else the database file. */
static int load_page(ts_pager *p, uint32_t pgno, uint8_t *buf) {
    uint64_t off = 0;
    if (wal_find(p->wal, pgno, &off)) {
        TS_TRY(wal_read_page(p->wal, off, buf));
    } else if ((uint64_t)pgno < p->db_pages) {
        TS_TRY(vfs_read_exact(p->db, buf, TS_PAGE_SIZE, (uint64_t)pgno * TS_PAGE_SIZE));
    } else {
        return TS_CORRUPT; /* referenced, but stored nowhere */
    }
    return ts_page_verify(buf) ? TS_OK : TS_CORRUPT;
}

/* ------------------------------------------------------------------------
 * Page access
 * ---------------------------------------------------------------------- */
int pager_get(ts_pager *p, uint32_t pgno, ts_page **out) {
    if (p->poisoned) return TS_POISONED;
    if (pgno >= p->meta.page_count) return TS_CORRUPT; /* a bad pointer in some page */

    uint64_t v = 0;
    if (u32map_get(&p->cache, pgno, &v)) {
        ts_page *pg = (ts_page *)(uintptr_t)v;
        if (pg->pin == 0 && !pg->dirty) list_remove(pg);
        pg->pin++;
        p->hits++;
        *out = pg;
        return TS_OK;
    }

    p->misses++;
    ts_page *pg = NULL;
    TS_TRY(new_frame(p, pgno, &pg));
    int rc = load_page(p, pgno, pg->data);
    if (rc != TS_OK) {
        drop_frame(p, pg);
        return rc;
    }
    pg->pin = 1;
    *out = pg;
    return TS_OK;
}

void pager_release(ts_pager *p, ts_page *pg) {
    if (!pg) return;
    if (pg->pin == 0) return; /* defensive: double release */
    pg->pin--;
    if (pg->pin == 0 && !pg->dirty) list_push_front(&p->lru, pg);
}

int pager_write(ts_pager *p, ts_page *pg) {
    if (p->poisoned) return TS_POISONED;
    if (!p->in_txn) return TS_TXN;
    if (!pg->dirty) {
        pg->dirty = true;
        list_push_front(&p->dirty, pg); /* pinned pages are on no list, so this is safe */
    }
    p->version++;
    return TS_OK;
}

int pager_alloc(ts_pager *p, ts_page **out) {
    if (p->poisoned) return TS_POISONED;
    if (!p->in_txn) return TS_TXN;

    uint32_t pgno = 0;
    TS_TRY(freelist_pop(p, &pgno));
    if (pgno == 0) {
        if (p->meta.page_count >= MAX_PAGE_COUNT) return TS_TOOBIG;
        pgno = p->meta.page_count++;
    }

    ts_page *pg = NULL;
    uint64_t v = 0;
    if (u32map_get(&p->cache, pgno, &v)) {
        pg = (ts_page *)(uintptr_t)v;
        if (pg->pin == 0 && !pg->dirty) list_remove(pg);
    } else {
        TS_TRY(new_frame(p, pgno, &pg));
    }
    pg->pin++;
    pg->checked = false;
    memset(pg->data, 0, TS_PAGE_SIZE);
    int rc = pager_write(p, pg);
    if (rc != TS_OK) {
        pager_release(p, pg);
        return rc;
    }
    *out = pg;
    return TS_OK;
}

int pager_free(ts_pager *p, uint32_t pgno) {
    if (p->poisoned) return TS_POISONED;
    if (!p->in_txn) return TS_TXN;
    if (pgno == 0 || pgno >= p->meta.page_count) return TS_CORRUPT;
    return freelist_push(p, pgno);
}

/* ------------------------------------------------------------------------
 * Transactions
 * ---------------------------------------------------------------------- */
int pager_begin(ts_pager *p) {
    if (p->poisoned) return TS_POISONED;
    if (p->in_txn) return TS_TXN;
    p->saved = p->meta;
    p->in_txn = true;
    return TS_OK;
}

bool pager_in_txn(const ts_pager *p) {
    return p->in_txn;
}

static int cmp_page_ptr(const void *a, const void *b) {
    uint32_t x = (*(ts_page *const *)a)->pgno, y = (*(ts_page *const *)b)->pgno;
    return (x > y) - (x < y);
}

int pager_commit(ts_pager *p) {
    if (p->poisoned) return TS_POISONED;
    if (!p->in_txn) return TS_TXN;

    /* Field by field: memcmp on structs would also compare padding bytes. */
    const ts_meta *a = &p->meta, *b = &p->saved;
    bool meta_changed = a->page_count != b->page_count || a->root != b->root || a->height != b->height ||
                        a->freelist_head != b->freelist_head || a->freelist_count != b->freelist_count ||
                        a->txn_id != b->txn_id || a->key_count != b->key_count;
    if (p->dirty.next == &p->dirty && !meta_changed) {
        p->in_txn = false; /* read-only transaction: nothing to write */
        return TS_OK;
    }

    /* The header goes into every commit: it carries the new transaction id,
       page count and root, and its frame is the commit frame. */
    p->meta.txn_id++;
    ts_page *hdr = NULL;
    int rc = pager_get(p, 0, &hdr);
    if (rc == TS_OK) rc = pager_write(p, hdr);
    if (rc != TS_OK) {
        pager_release(p, hdr);
        p->meta.txn_id--;
        return rc;
    }
    encode_meta(&p->meta, hdr->data);
    pager_release(p, hdr);

    size_t n = 0;
    for (ts_page *pg = p->dirty.next; pg != &p->dirty; pg = pg->next) n++;
    ts_page **pages = (ts_page **)malloc(n * sizeof *pages);
    uint8_t **datas = (uint8_t **)malloc(n * sizeof *datas);
    uint32_t *pgnos = (uint32_t *)malloc(n * sizeof *pgnos);
    if (!pages || !datas || !pgnos) {
        free(pages);
        free(datas);
        free(pgnos);
        p->meta.txn_id--;
        return TS_NOMEM;
    }
    size_t i = 0;
    for (ts_page *pg = p->dirty.next; pg != &p->dirty; pg = pg->next) pages[i++] = pg;
    /* Ascending page order (sequential checkpoint I/O), header last: page 0
       sorts first, so rotate it to the end where it becomes the commit frame. */
    qsort(pages, n, sizeof *pages, cmp_page_ptr);
    for (i = 0; i + 1 < n; i++) {
        ts_page *t = pages[i];
        pages[i] = pages[i + 1];
        pages[i + 1] = t;
    }
    for (i = 0; i < n; i++) {
        ts_page_seal(pages[i]->data);
        datas[i] = pages[i]->data;
        pgnos[i] = pages[i]->pgno;
    }

    rc = wal_commit(p->wal, datas, pgnos, n, p->meta.page_count, p->sync == TS_SYNC_FULL);
    free(datas);
    free(pgnos);
    if (rc != TS_OK) {
        /* We cannot know how much of the commit reached the disk, and after a
           failed fsync the OS may have discarded the data. Stop here; reopening
           the database lets recovery decide from what is actually on disk. */
        free(pages);
        p->poisoned = true;
        return rc;
    }

    for (i = 0; i < n; i++) {
        ts_page *pg = pages[i];
        list_remove(pg);
        pg->dirty = false;
        if (pg->pin == 0) list_push_front(&p->lru, pg);
    }
    free(pages);
    p->in_txn = false;
    p->commits++;
    p->version++;

    if (p->ckpt_threshold && wal_frame_count(p->wal) >= p->ckpt_threshold) {
        /* The commit is already durable; a failed checkpoint only poisons the
           handle (reported by the next operation). */
        (void)pager_checkpoint(p);
    }
    return TS_OK;
}

int pager_rollback(ts_pager *p) {
    if (!p->in_txn) return TS_TXN;
    ts_page *pg = p->dirty.next;
    while (pg != &p->dirty) {
        ts_page *next = pg->next;
        list_remove(pg);
        pg->dirty = false;
        if (pg->pin == 0) {
            drop_frame(p, pg); /* the committed version will be re-read on demand */
        } else {
            /* Someone still holds the page: restore its committed bytes in place.
               A page created by this transaction has none; zeroes then. */
            pg->checked = false;
            if (load_page(p, pg->pgno, pg->data) != TS_OK) memset(pg->data, 0, TS_PAGE_SIZE);
        }
        pg = next;
    }
    p->meta = p->saved;
    p->in_txn = false;
    p->version++;
    return TS_OK;
}

int pager_checkpoint(ts_pager *p) {
    if (p->poisoned) return TS_POISONED;
    if (p->in_txn) return TS_TXN;
    int rc = wal_checkpoint(p->wal, p->db, p->sync != TS_SYNC_OFF);
    if (rc != TS_OK) {
        p->poisoned = true;
        return rc;
    }
    if (p->db_pages < p->meta.page_count) p->db_pages = p->meta.page_count;
    return TS_OK;
}

ts_meta *pager_meta(ts_pager *p) {
    return &p->meta;
}

uint64_t pager_version(const ts_pager *p) {
    return p->version;
}

bool pager_poisoned(const ts_pager *p) {
    return p->poisoned;
}

void pager_stats(const ts_pager *p, ts_pager_stats *out) {
    out->hits = p->hits;
    out->misses = p->misses;
    out->commits = p->commits;
    out->wal_frames = wal_frame_count(p->wal);
    out->checkpoints = wal_checkpoint_count(p->wal);
    out->cached_pages = p->cached;
}

/* ------------------------------------------------------------------------
 * Open / close
 * ---------------------------------------------------------------------- */
static void free_all(ts_pager *p) {
    if (p->cache.keys) {
        for (size_t i = 0; i < p->cache.cap; i++) {
            if (p->cache.keys[i]) free_page_struct((ts_page *)(uintptr_t)p->cache.vals[i]);
        }
        u32map_free(&p->cache);
    }
    if (p->wal) wal_close(p->wal);
    if (p->db) vfs_close(p->db);
    free(p->path);
    free(p->wal_path);
    free(p);
}

static int read_header(ts_pager *p, ts_meta *m) {
    uint8_t *buf = (uint8_t *)malloc(TS_PAGE_SIZE);
    if (!buf) return TS_NOMEM;
    uint64_t off = 0;
    int rc;
    if (wal_find(p->wal, 0, &off)) rc = wal_read_page(p->wal, off, buf);
    else rc = vfs_read_exact(p->db, buf, TS_PAGE_SIZE, 0);
    if (rc == TS_OK) rc = decode_meta(buf, m);
    free(buf);
    return rc;
}

int pager_open(ts_vfs *vfs, const char *path, const ts_options *opt, ts_pager **out, bool *is_new) {
    *out = NULL;
    *is_new = false;
    ts_pager *p = (ts_pager *)calloc(1, sizeof *p);
    if (!p) return TS_NOMEM;
    p->vfs = vfs;
    p->sync = opt->sync;
    p->ckpt_threshold = opt->checkpoint_pages;
    p->limit = opt->cache_pages < 16 ? 16 : opt->cache_pages;
    list_init(&p->lru);
    list_init(&p->dirty);

    size_t len = strlen(path);
    p->path = ts_strdup(path);
    p->wal_path = (char *)malloc(len + 5);
    int rc = (p->path && p->wal_path) ? TS_OK : TS_NOMEM;
    if (rc == TS_OK) {
        memcpy(p->wal_path, path, len);
        memcpy(p->wal_path + len, "-wal", 5);
        rc = u32map_init(&p->cache, p->limit * 2);
    }

    int db_existed = 0, wal_existed = 0;
    if (rc == TS_OK) rc = vfs->exists(vfs, p->path, &db_existed);
    if (rc == TS_OK) rc = vfs->exists(vfs, p->wal_path, &wal_existed);
    if (rc == TS_OK && !db_existed && !opt->create) rc = TS_NOTFOUND;
    if (rc == TS_OK) rc = vfs->open(vfs, p->path, 1, &p->db);
    if (rc == TS_OK) rc = p->db->vfs->lock(p->db);
    if (rc == TS_OK) rc = wal_open(vfs, p->wal_path, &p->wal);
    /* A newly created file is not durable until its directory entry is. */
    if (rc == TS_OK && (!db_existed || !wal_existed)) rc = vfs->sync_dir(vfs, p->path);

    uint32_t wal_pages = 0;
    if (rc == TS_OK) rc = wal_recover(p->wal, &wal_pages);
    uint64_t db_size = 0;
    if (rc == TS_OK) rc = vfs_size(p->db, &db_size);
    if (rc != TS_OK) {
        free_all(p);
        return rc;
    }
    p->db_pages = db_size / TS_PAGE_SIZE;

    if (p->db_pages == 0 && wal_pages == 0) {
        /* A brand-new database. Leave a transaction open with an empty header
           page; the caller adds the tree root and commits. */
        p->in_txn = true;
        memset(&p->saved, 0, sizeof p->saved);
        memset(&p->meta, 0, sizeof p->meta);
        p->meta.page_count = 1;
        /* Commit adds one, so the creation becomes transaction 0 and the
           first transaction a user makes is number 1. */
        p->meta.txn_id = UINT64_MAX;
        ts_page *hdr = NULL;
        rc = new_frame(p, 0, &hdr);
        if (rc != TS_OK) {
            free_all(p);
            return rc;
        }
        memset(hdr->data, 0, TS_PAGE_SIZE);
        hdr->dirty = true;
        list_push_front(&p->dirty, hdr);
        *is_new = true;
        *out = p;
        return TS_OK;
    }

    rc = read_header(p, &p->meta);
    if (rc == TS_OK && wal_pages != 0 && wal_pages != p->meta.page_count) rc = TS_CORRUPT;
    if (rc != TS_OK) {
        free_all(p);
        return rc;
    }
    p->saved = p->meta;
    *out = p;
    return TS_OK;
}

int pager_close(ts_pager *p) {
    if (!p) return TS_OK;
    int rc = TS_OK;
    if (p->in_txn) (void)pager_rollback(p);
    if (!p->poisoned) {
        rc = pager_checkpoint(p);
        if (rc == TS_OK) {
            /* After a clean checkpoint the log is empty: remove it, like SQLite does. */
            wal_close(p->wal);
            p->wal = NULL;
            rc = p->vfs->remove(p->vfs, p->wal_path);
        }
    } else {
        rc = TS_POISONED;
    }
    free_all(p);
    return rc;
}

void pager_abandon(ts_pager *p) {
    if (p) free_all(p);
}
