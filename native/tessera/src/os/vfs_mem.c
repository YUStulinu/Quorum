/*
 * In-memory file system with power-failure simulation. See vfs_mem.h.
 */
#include "os/vfs_mem.h"

#include <stdlib.h>
#include <string.h>

#include "os/vfs.h"
#include "util/common.h"

#define SECTOR 512u

typedef struct pending_op {
    int                is_truncate;
    uint64_t           off;  /* write offset, or new size for a truncate */
    size_t             len;
    uint8_t           *data;
    struct pending_op *next;
} pending_op;

typedef struct mem_node {
    char            *path;
    uint8_t         *cur;      /* current contents (what reads see) */
    size_t           cur_size;
    size_t           cur_cap;
    uint8_t         *dur;      /* contents as of the last sync */
    size_t           dur_size;
    pending_op      *ops_head; /* writes since the last sync, in order */
    pending_op      *ops_tail;
    int              locked;
    struct mem_node *next;
} mem_node;

typedef struct {
    ts_vfs    base;
    mem_node *files;
    int64_t   budget;          /* -1 = unlimited */
    int       fail_next_sync;
} mem_vfs;

typedef struct {
    ts_file   base;
    mem_node *node;
    int       has_lock;
} mem_handle;

/* ---------------------------------------------------------------------- */

static mem_node *find_node(mem_vfs *v, const char *path) {
    for (mem_node *n = v->files; n; n = n->next)
        if (strcmp(n->path, path) == 0) return n;
    return NULL;
}

static void free_ops(mem_node *n) {
    pending_op *op = n->ops_head;
    while (op) {
        pending_op *next = op->next;
        free(op->data);
        free(op);
        op = next;
    }
    n->ops_head = n->ops_tail = NULL;
}

static void free_node(mem_node *n) {
    free_ops(n);
    free(n->cur);
    free(n->dur);
    free(n->path);
    free(n);
}

static int reserve(uint8_t **buf, size_t *cap, size_t need) {
    if (need <= *cap) return TS_OK;
    size_t c = *cap ? *cap : 4096;
    while (c < need) c *= 2;
    uint8_t *p = (uint8_t *)realloc(*buf, c);
    if (!p) return TS_NOMEM;
    *buf = p;
    *cap = c;
    return TS_OK;
}

/* Resizes `buf` (logical size *size, capacity *cap) to new_size, zero-filling growth. */
static int resize_bytes(uint8_t **buf, size_t *size, size_t *cap, size_t new_size) {
    TS_TRY(reserve(buf, cap, new_size));
    if (new_size > *size) memset(*buf + *size, 0, new_size - *size);
    *size = new_size;
    return TS_OK;
}

static int write_bytes(uint8_t **buf, size_t *size, size_t *cap, const uint8_t *src, size_t len, uint64_t off) {
    size_t end = (size_t)off + len;
    if (end > *size) TS_TRY(resize_bytes(buf, size, cap, end));
    memcpy(*buf + off, src, len);
    return TS_OK;
}

static int record_op(mem_node *n, int is_truncate, uint64_t off, const void *data, size_t len) {
    pending_op *op = (pending_op *)calloc(1, sizeof *op);
    if (!op) return TS_NOMEM;
    op->is_truncate = is_truncate;
    op->off = off;
    op->len = len;
    if (len) {
        op->data = (uint8_t *)malloc(len);
        if (!op->data) {
            free(op);
            return TS_NOMEM;
        }
        memcpy(op->data, data, len);
    }
    if (n->ops_tail) n->ops_tail->next = op;
    else n->ops_head = op;
    n->ops_tail = op;
    return TS_OK;
}

static int consume_budget(mem_vfs *v) {
    if (v->budget < 0) return TS_OK;
    if (v->budget == 0) return TS_IOERR;
    v->budget--;
    return TS_OK;
}

/* ---------------------------------------------------------------------- */

static int m_close(ts_file *f) {
    mem_handle *h = (mem_handle *)f;
    if (h->has_lock && h->node) h->node->locked = 0;
    free(h);
    return TS_OK;
}

static int m_read(ts_file *f, void *buf, size_t n, uint64_t off, size_t *got) {
    mem_node *node = ((mem_handle *)f)->node;
    *got = 0;
    if (off >= node->cur_size) return TS_OK;
    size_t avail = node->cur_size - (size_t)off;
    size_t take = n < avail ? n : avail;
    memcpy(buf, node->cur + off, take);
    *got = take;
    return TS_OK;
}

static int m_write(ts_file *f, const void *buf, size_t n, uint64_t off) {
    mem_handle *h = (mem_handle *)f;
    mem_vfs *v = (mem_vfs *)f->vfs;
    TS_TRY(consume_budget(v));
    mem_node *node = h->node;
    TS_TRY(write_bytes(&node->cur, &node->cur_size, &node->cur_cap, (const uint8_t *)buf, n, off));
    return record_op(node, 0, off, buf, n);
}

static int m_sync(ts_file *f) {
    mem_vfs *v = (mem_vfs *)f->vfs;
    if (v->fail_next_sync) {
        v->fail_next_sync = 0;
        return TS_IOERR;
    }
    TS_TRY(consume_budget(v));
    mem_node *node = ((mem_handle *)f)->node;
    uint8_t *copy = NULL;
    if (node->cur_size) {
        copy = (uint8_t *)malloc(node->cur_size);
        if (!copy) return TS_NOMEM;
        memcpy(copy, node->cur, node->cur_size);
    }
    free(node->dur);
    node->dur = copy;
    node->dur_size = node->cur_size;
    free_ops(node);
    return TS_OK;
}

static int m_truncate(ts_file *f, uint64_t size) {
    mem_vfs *v = (mem_vfs *)f->vfs;
    TS_TRY(consume_budget(v));
    mem_node *node = ((mem_handle *)f)->node;
    TS_TRY(resize_bytes(&node->cur, &node->cur_size, &node->cur_cap, (size_t)size));
    return record_op(node, 1, size, NULL, 0);
}

static int m_size(ts_file *f, uint64_t *out) {
    *out = ((mem_handle *)f)->node->cur_size;
    return TS_OK;
}

static int m_lock(ts_file *f) {
    mem_handle *h = (mem_handle *)f;
    if (h->node->locked && !h->has_lock) return TS_BUSY;
    h->node->locked = 1;
    h->has_lock = 1;
    return TS_OK;
}

static int m_open(ts_vfs *vfs, const char *path, int create, ts_file **out) {
    mem_vfs *v = (mem_vfs *)vfs;
    mem_node *node = find_node(v, path);
    if (!node) {
        if (!create) return TS_IOERR;
        node = (mem_node *)calloc(1, sizeof *node);
        if (!node) return TS_NOMEM;
        node->path = ts_strdup(path);
        if (!node->path) {
            free(node);
            return TS_NOMEM;
        }
        node->next = v->files;
        v->files = node;
    }
    mem_handle *h = (mem_handle *)calloc(1, sizeof *h);
    if (!h) return TS_NOMEM;
    h->base.vfs = vfs;
    h->node = node;
    *out = &h->base;
    return TS_OK;
}

static int m_exists(ts_vfs *vfs, const char *path, int *out) {
    *out = find_node((mem_vfs *)vfs, path) != NULL;
    return TS_OK;
}

static int m_remove(ts_vfs *vfs, const char *path) {
    mem_vfs *v = (mem_vfs *)vfs;
    for (mem_node **pp = &v->files; *pp; pp = &(*pp)->next) {
        if (strcmp((*pp)->path, path) == 0) {
            mem_node *dead = *pp;
            *pp = dead->next;
            free_node(dead);
            return TS_OK;
        }
    }
    return TS_OK;
}

static int m_sync_dir(ts_vfs *vfs, const char *path) {
    TS_UNUSED(vfs);
    TS_UNUSED(path);
    return TS_OK;
}

/* ---------------------------------------------------------------------- */

ts_vfs *ts_memvfs_create(void) {
    mem_vfs *v = (mem_vfs *)calloc(1, sizeof *v);
    if (!v) return NULL;
    v->base.name = "memory";
    v->base.open = m_open;
    v->base.exists = m_exists;
    v->base.remove = m_remove;
    v->base.sync_dir = m_sync_dir;
    v->base.close = m_close;
    v->base.read = m_read;
    v->base.write = m_write;
    v->base.sync = m_sync;
    v->base.truncate = m_truncate;
    v->base.size = m_size;
    v->base.lock = m_lock;
    v->budget = -1;
    return &v->base;
}

void ts_memvfs_destroy(ts_vfs *vfs) {
    if (!vfs) return;
    mem_vfs *v = (mem_vfs *)vfs;
    mem_node *n = v->files;
    while (n) {
        mem_node *next = n->next;
        free_node(n);
        n = next;
    }
    free(v);
}

void ts_memvfs_set_io_budget(ts_vfs *vfs, int64_t ops) {
    ((mem_vfs *)vfs)->budget = ops;
}

void ts_memvfs_fail_next_sync(ts_vfs *vfs) {
    ((mem_vfs *)vfs)->fail_next_sync = 1;
}

uint8_t *ts_memvfs_file_data(ts_vfs *vfs, const char *path, uint64_t *size) {
    mem_node *n = find_node((mem_vfs *)vfs, path);
    if (!n) return NULL;
    *size = n->cur_size;
    return n->cur;
}

/* Applies a write that was torn: only some of its 512-byte sectors reach the disk. */
static void apply_torn(uint8_t **buf, size_t *size, size_t *cap, const pending_op *op, uint64_t *rng) {
    uint64_t first = op->off / SECTOR;
    uint64_t last = (op->off + op->len - 1) / SECTOR;
    for (uint64_t s = first; s <= last; s++) {
        if (ts_splitmix64(rng) & 1) continue;
        uint64_t lo = s * SECTOR > op->off ? s * SECTOR : op->off;
        uint64_t hi = (s + 1) * SECTOR < op->off + op->len ? (s + 1) * SECTOR : op->off + op->len;
        (void)write_bytes(buf, size, cap, op->data + (lo - op->off), (size_t)(hi - lo), lo);
    }
}

void ts_memvfs_crash(ts_vfs *vfs, ts_crash_mode mode, uint64_t seed) {
    mem_vfs *v = (mem_vfs *)vfs;
    uint64_t rng = seed ? seed : 1;
    for (mem_node *n = v->files; n; n = n->next) {
        uint8_t *buf = NULL;
        size_t size = 0, cap = 0;
        if (n->dur_size) (void)write_bytes(&buf, &size, &cap, n->dur, n->dur_size, 0);

        for (pending_op *op = n->ops_head; op; op = op->next) {
            int apply = 0, torn = 0;
            switch (mode) {
                case TS_CRASH_LOSE_UNSYNCED: apply = 0; break;
                case TS_CRASH_KEEP_ALL:      apply = 1; break;
                case TS_CRASH_RANDOM: {
                    uint64_t r = ts_splitmix64(&rng) % 4;
                    apply = r != 0;
                    torn = r == 3 && !op->is_truncate && op->len > 0;
                    break;
                }
            }
            if (!apply) continue;
            if (op->is_truncate) (void)resize_bytes(&buf, &size, &cap, (size_t)op->off);
            else if (torn) apply_torn(&buf, &size, &cap, op, &rng);
            else (void)write_bytes(&buf, &size, &cap, op->data, op->len, op->off);
        }

        free_ops(n);
        free(n->cur);
        free(n->dur);
        n->cur = buf;
        n->cur_size = size;
        n->cur_cap = cap;
        n->dur = NULL;
        n->dur_size = 0;
        if (size) {
            n->dur = (uint8_t *)malloc(size);
            if (n->dur) {
                memcpy(n->dur, buf, size);
                n->dur_size = size;
            }
        }
        n->locked = 0; /* every process that held the file is gone */
    }
    v->budget = -1;
    v->fail_next_sync = 0;
}
