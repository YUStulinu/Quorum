/*
 * Cursor over the leaf chain.
 *
 * A cursor remembers (leaf page, slot) and a copy of the current key. It
 * holds no pins between calls, so it never blocks eviction. If the pager's
 * version changed since the cursor last looked, the remembered position may
 * point into a page that was split, merged or freed; the cursor then simply
 * searches again for the first key after the one it returned last.
 */
#include <stdlib.h>

#include "btree/btree.h"
#include "btree/node.h"

void bt_cursor_init(bt_cursor *c, ts_pager *p) {
    memset(c, 0, sizeof *c);
    c->p = p;
}

void bt_cursor_free(bt_cursor *c) {
    free(c->key);
    c->key = NULL;
    c->klen = c->kcap = 0;
}

static int remember_key(bt_cursor *c, const uint8_t *key, size_t klen) {
    if (klen > c->kcap) {
        size_t cap = klen < 64 ? 64 : klen;
        uint8_t *k = (uint8_t *)realloc(c->key, cap);
        if (!k) return TS_NOMEM;
        c->key = k;
        c->kcap = cap;
    }
    if (klen) memcpy(c->key, key, klen);
    c->klen = klen;
    return TS_OK;
}

static int get_leaf(ts_pager *p, uint32_t pgno, ts_page **out) {
    TS_TRY(pager_get(p, pgno, out));
    if (!(*out)->checked) {
        if (!node_sane((*out)->data)) {
            pager_release(p, *out);
            return TS_CORRUPT;
        }
        (*out)->checked = true;
    }
    if (node_type((*out)->data) != NODE_LEAF) {
        pager_release(p, *out);
        return TS_CORRUPT;
    }
    return TS_OK;
}

/* From (leaf, idx), skips forward over the end of leaves until an entry is
   found, and copies its key. */
static int settle(bt_cursor *c) {
    unsigned hops = 0;
    for (;;) {
        ts_page *pg = NULL;
        TS_TRY(get_leaf(c->p, c->leaf, &pg));
        if (c->idx < node_count(pg->data)) {
            size_t klen = 0;
            const uint8_t *key = cell_key(NODE_LEAF, node_cell_c(pg->data, c->idx), &klen);
            int rc = remember_key(c, key, klen);
            pager_release(c->p, pg);
            if (rc != TS_OK) return rc;
            c->state = 1;
            c->version = pager_version(c->p);
            return TS_OK;
        }
        uint32_t next = node_link(pg->data);
        pager_release(c->p, pg);
        if (next == 0) {
            c->state = 2;
            return TS_DONE;
        }
        if (++hops > pager_meta(c->p)->page_count) return TS_CORRUPT; /* a cycle in the leaf chain */
        c->leaf = next;
        c->idx = 0;
    }
}

/* Finds the leaf and slot of the first key >= key (or > key if `after`). */
static int position(bt_cursor *c, const uint8_t *key, size_t klen, int after) {
    const ts_meta *m = pager_meta(c->p);
    uint32_t pgno = m->root;
    for (unsigned depth = 0;; depth++) {
        if (depth >= BT_MAX_DEPTH || depth >= m->height) return TS_CORRUPT;
        ts_page *pg = NULL;
        TS_TRY(pager_get(c->p, pgno, &pg));
        if (!pg->checked) {
            if (!node_sane(pg->data)) {
                pager_release(c->p, pg);
                return TS_CORRUPT;
            }
            pg->checked = true;
        }
        if (node_type(pg->data) == NODE_LEAF) {
            unsigned idx = 0;
            if (key) {
                bool exact = false;
                idx = node_lower_bound(pg->data, key, klen, &exact);
                if (exact && after) idx++;
            }
            pager_release(c->p, pg);
            c->leaf = pgno;
            c->idx = idx;
            return settle(c);
        }
        unsigned i = key ? node_child_index(pg->data, key, klen) : 0;
        pgno = node_child_at(pg->data, i);
        pager_release(c->p, pg);
    }
}

int bt_cursor_seek(bt_cursor *c, const uint8_t *key, size_t klen) {
    return position(c, key, klen, 0);
}

int bt_cursor_next(bt_cursor *c) {
    if (c->state != 1) return TS_DONE;
    if (c->version != pager_version(c->p)) {
        /* The tree changed under us. The key copy is the cursor's real position.
           position() may reuse c->key's memory while searching, so search with a copy. */
        uint8_t *k = (uint8_t *)malloc(c->klen ? c->klen : 1);
        if (!k) return TS_NOMEM;
        if (c->klen) memcpy(k, c->key, c->klen);
        int rc = position(c, k, c->klen, 1);
        free(k);
        return rc;
    }
    c->idx++;
    return settle(c);
}

int bt_cursor_value(bt_cursor *c, uint8_t **val, size_t *vlen) {
    if (c->state != 1) return TS_NOTFOUND;
    if (c->version != pager_version(c->p)) return bt_get(c->p, c->key, c->klen, val, vlen);
    ts_page *pg = NULL;
    TS_TRY(get_leaf(c->p, c->leaf, &pg));
    int rc = c->idx < node_count(pg->data) ? bt_cell_value(c->p, node_cell_c(pg->data, c->idx), val, vlen)
                                           : TS_CORRUPT;
    pager_release(c->p, pg);
    return rc;
}
