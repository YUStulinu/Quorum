/*
 * Public API: handles, autocommit, transactions, iterators.
 */
#include <stdlib.h>

#include "btree/btree.h"
#include "engine/db_internal.h"
#include "os/vfs.h"

void ts_options_default(ts_options *opt) {
    opt->cache_pages = 2048;
    opt->checkpoint_pages = 2000;
    opt->sync = TS_SYNC_FULL;
    opt->create = 1;
    opt->vfs = NULL;
}

int ts_open(const char *path, const ts_options *opt, ts_db **out) {
    if (!path || !out) return TS_INVALID;
    *out = NULL;
    ts_db *db = (ts_db *)calloc(1, sizeof *db);
    if (!db) return TS_NOMEM;
    if (opt) db->opt = *opt;
    else ts_options_default(&db->opt);
    db->vfs = db->opt.vfs ? db->opt.vfs : ts_vfs_os();

    bool is_new = false;
    int rc = pager_open(db->vfs, path, &db->opt, &db->pager, &is_new);
    if (rc == TS_OK && is_new) {
        /* The creation is itself a transaction: a crash halfway leaves no database. */
        rc = bt_create(db->pager);
        if (rc == TS_OK) rc = pager_commit(db->pager);
        if (rc == TS_OK) rc = pager_checkpoint(db->pager);
    }
    if (rc != TS_OK) {
        pager_abandon(db->pager);
        free(db);
        return rc;
    }
    *out = db;
    return TS_OK;
}

int ts_close(ts_db *db) {
    if (!db) return TS_OK;
    int rc = pager_close(db->pager);
    free(db);
    return rc;
}

void ts_db_abandon(ts_db *db) {
    if (!db) return;
    pager_abandon(db->pager);
    free(db);
}

/* ------------------------------------------------------------------------
 * Transactions
 * ---------------------------------------------------------------------- */
static int usable(const ts_db *db) {
    if (!db) return TS_INVALID;
    if (pager_poisoned(db->pager)) return TS_POISONED;
    if (db->aborted) return TS_TXN;
    return TS_OK;
}

int ts_begin(ts_db *db) {
    TS_TRY(usable(db));
    if (db->explicit_txn) return TS_TXN;
    TS_TRY(pager_begin(db->pager));
    db->explicit_txn = true;
    return TS_OK;
}

int ts_commit(ts_db *db) {
    if (!db || !db->explicit_txn) return TS_TXN;
    db->explicit_txn = false;
    if (db->aborted) {
        db->aborted = false; /* it was already rolled back when the error happened */
        return TS_TXN;
    }
    return pager_commit(db->pager);
}

int ts_rollback(ts_db *db) {
    if (!db || !db->explicit_txn) return TS_TXN;
    db->explicit_txn = false;
    if (db->aborted) {
        db->aborted = false;
        return TS_OK;
    }
    return pager_rollback(db->pager);
}

int ts_in_transaction(const ts_db *db) {
    return db && db->explicit_txn;
}

/* Every write runs inside a transaction: the caller's, or one of its own. */
static int write_begin(ts_db *db, bool *autocommit) {
    TS_TRY(usable(db));
    *autocommit = !db->explicit_txn;
    if (*autocommit) TS_TRY(pager_begin(db->pager));
    return TS_OK;
}

static int write_end(ts_db *db, bool autocommit, int rc) {
    if (rc < 0) {
        /* A failure can leave the tree half-modified in the cache: undo the
           whole transaction, which is the only state known to be consistent. */
        if (pager_in_txn(db->pager)) (void)pager_rollback(db->pager);
        if (!autocommit) db->aborted = true;
        return rc;
    }
    if (autocommit) TS_TRY(pager_commit(db->pager));
    return rc;
}

/* ------------------------------------------------------------------------
 * Data
 * ---------------------------------------------------------------------- */
int ts_put(ts_db *db, const void *key, size_t klen, const void *val, size_t vlen) {
    if ((!key && klen) || (!val && vlen)) return TS_INVALID;
    if (klen > TS_MAX_KEY_SIZE) return TS_INVALID;
    if (vlen > TS_MAX_VALUE_SIZE) return TS_TOOBIG;
    bool autocommit = false;
    TS_TRY(write_begin(db, &autocommit));
    int rc = bt_put(db->pager, (const uint8_t *)key, klen, (const uint8_t *)val, vlen);
    return write_end(db, autocommit, rc);
}

int ts_delete(ts_db *db, const void *key, size_t klen) {
    if (!key && klen) return TS_INVALID;
    bool autocommit = false;
    TS_TRY(write_begin(db, &autocommit));
    int rc = bt_delete(db->pager, (const uint8_t *)key, klen);
    return write_end(db, autocommit, rc);
}

int ts_get(ts_db *db, const void *key, size_t klen, void **val, size_t *vlen) {
    if ((!key && klen) || !val || !vlen) return TS_INVALID;
    TS_TRY(usable(db));
    uint8_t *v = NULL;
    int rc = bt_get(db->pager, (const uint8_t *)key, klen, &v, vlen);
    if (rc == TS_OK) *val = v;
    return rc;
}

/* ------------------------------------------------------------------------
 * Iterators
 * ---------------------------------------------------------------------- */
struct ts_iter {
    ts_db    *db;
    bt_cursor cur;
    uint8_t  *lo, *hi;
    size_t    lolen, hilen;
    bool      has_lo, has_hi;
    bool      started, done;
    uint8_t  *val;
    size_t    vlen;
    bool      val_loaded;
};

static int copy_bound(const void *src, size_t len, uint8_t **dst) {
    *dst = (uint8_t *)malloc(len ? len : 1);
    if (!*dst) return TS_NOMEM;
    if (len) memcpy(*dst, src, len);
    return TS_OK;
}

int ts_iter_open(ts_db *db, const void *lo, size_t lolen, const void *hi, size_t hilen, ts_iter **out) {
    if (!out) return TS_INVALID;
    TS_TRY(usable(db));
    ts_iter *it = (ts_iter *)calloc(1, sizeof *it);
    if (!it) return TS_NOMEM;
    it->db = db;
    bt_cursor_init(&it->cur, db->pager);
    int rc = TS_OK;
    if (lo) {
        it->has_lo = true;
        it->lolen = lolen;
        rc = copy_bound(lo, lolen, &it->lo);
    }
    if (rc == TS_OK && hi) {
        it->has_hi = true;
        it->hilen = hilen;
        rc = copy_bound(hi, hilen, &it->hi);
    }
    if (rc != TS_OK) {
        ts_iter_close(it);
        return rc;
    }
    *out = it;
    return TS_OK;
}

int ts_iter_next(ts_iter *it) {
    if (!it) return TS_INVALID;
    if (it->done) return TS_DONE;
    if (pager_poisoned(it->db->pager)) return TS_POISONED;
    free(it->val);
    it->val = NULL;
    it->val_loaded = false;

    int rc;
    if (!it->started) {
        it->started = true;
        rc = bt_cursor_seek(&it->cur, it->has_lo ? it->lo : NULL, it->lolen);
    } else {
        rc = bt_cursor_next(&it->cur);
    }
    if (rc == TS_OK && it->has_hi && ts_keycmp(it->cur.key, it->cur.klen, it->hi, it->hilen) >= 0) rc = TS_DONE;
    if (rc != TS_OK) it->done = true;
    return rc;
}

const void *ts_iter_key(const ts_iter *it, size_t *len) {
    if (!it || it->done || !it->started) return NULL;
    if (len) *len = it->cur.klen;
    return it->cur.key ? (const void *)it->cur.key : (const void *)"";
}

const void *ts_iter_value(ts_iter *it, size_t *len, int *rc) {
    int dummy;
    if (!rc) rc = &dummy;
    *rc = TS_OK;
    if (!it || it->done || !it->started) {
        *rc = TS_INVALID;
        return NULL;
    }
    if (!it->val_loaded) {
        *rc = bt_cursor_value(&it->cur, &it->val, &it->vlen);
        if (*rc != TS_OK) return NULL;
        it->val_loaded = true;
    }
    if (len) *len = it->vlen;
    return it->val;
}

void ts_iter_close(ts_iter *it) {
    if (!it) return;
    bt_cursor_free(&it->cur);
    free(it->lo);
    free(it->hi);
    free(it->val);
    free(it);
}

/* ------------------------------------------------------------------------
 * Maintenance
 * ---------------------------------------------------------------------- */
int ts_checkpoint(ts_db *db) {
    TS_TRY(usable(db));
    if (db->explicit_txn) return TS_TXN;
    return pager_checkpoint(db->pager);
}

int ts_get_stats(ts_db *db, ts_stats *out) {
    if (!db || !out) return TS_INVALID;
    const ts_meta *m = pager_meta(db->pager);
    ts_pager_stats ps;
    pager_stats(db->pager, &ps);
    memset(out, 0, sizeof *out);
    out->keys = m->key_count;
    out->txn_id = m->txn_id;
    out->page_size = TS_PAGE_SIZE;
    out->pages = m->page_count;
    out->free_pages = m->freelist_count;
    out->tree_height = m->height;
    out->wal_frames = ps.wal_frames;
    out->cache_hits = ps.hits;
    out->cache_misses = ps.misses;
    out->checkpoints = ps.checkpoints;
    out->commits = ps.commits;
    return TS_OK;
}
