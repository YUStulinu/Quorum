/*
 * B+tree operations. Overview in btree.h, page format in node.h.
 */
#include "btree/btree.h"

#include <stdlib.h>

#include "btree/node.h"

typedef struct {
    uint32_t pgno[BT_MAX_DEPTH];
    unsigned idx[BT_MAX_DEPTH]; /* child index taken at each internal level */
    unsigned depth;             /* number of levels; pgno[depth - 1] is the leaf */
} bt_path;

/* Fetches a node page and verifies its structure before anyone trusts it.
   The check runs once per page load, not on every access: pages this code
   modifies stay well-formed by construction. */
static int get_node(ts_pager *p, uint32_t pgno, ts_page **out) {
    TS_TRY(pager_get(p, pgno, out));
    if (!(*out)->checked) {
        if (!node_sane((*out)->data)) {
            pager_release(p, *out);
            *out = NULL;
            return TS_CORRUPT;
        }
        (*out)->checked = true;
    }
    return TS_OK;
}

/* Walks from the root to the leaf that would hold `key`, recording the path. */
static int descend(ts_pager *p, const uint8_t *key, size_t klen, bt_path *path) {
    const ts_meta *m = pager_meta(p);
    uint32_t pgno = m->root;
    for (unsigned depth = 0;; depth++) {
        if (depth >= BT_MAX_DEPTH || depth >= m->height) return TS_CORRUPT;
        ts_page *pg = NULL;
        TS_TRY(get_node(p, pgno, &pg));
        path->pgno[depth] = pgno;
        if (node_type(pg->data) == NODE_LEAF) {
            pager_release(p, pg);
            path->depth = depth + 1;
            return path->depth == m->height ? TS_OK : TS_CORRUPT;
        }
        unsigned i = node_child_index(pg->data, key, klen);
        path->idx[depth] = i;
        pgno = node_child_at(pg->data, i);
        pager_release(p, pg);
    }
}

int bt_create(ts_pager *p) {
    ts_page *root = NULL;
    TS_TRY(pager_alloc(p, &root));
    node_init(root->data, NODE_LEAF);
    ts_meta *m = pager_meta(p);
    m->root = root->pgno;
    m->height = 1;
    m->key_count = 0;
    pager_release(p, root);
    return TS_OK;
}

int bt_cell_value(ts_pager *p, const uint8_t *cell, uint8_t **val, size_t *vlen) {
    size_t n = leaf_vlen(cell);
    uint8_t *buf = (uint8_t *)malloc(n ? n : 1);
    if (!buf) return TS_NOMEM;
    int rc = TS_OK;
    if (leaf_has_overflow(cell)) rc = ovf_read(p, leaf_overflow_page(cell), n, buf);
    else if (n) memcpy(buf, leaf_value(cell), n);
    if (rc != TS_OK) {
        free(buf);
        return rc;
    }
    *val = buf;
    *vlen = n;
    return TS_OK;
}

int bt_get(ts_pager *p, const uint8_t *key, size_t klen, uint8_t **val, size_t *vlen) {
    if (klen > TS_MAX_KEY_SIZE) return TS_NOTFOUND;
    bt_path path;
    TS_TRY(descend(p, key, klen, &path));
    ts_page *leaf = NULL;
    TS_TRY(get_node(p, path.pgno[path.depth - 1], &leaf));
    bool exact = false;
    unsigned i = node_lower_bound(leaf->data, key, klen, &exact);
    int rc = exact ? bt_cell_value(p, node_cell_c(leaf->data, i), val, vlen) : TS_NOTFOUND;
    pager_release(p, leaf);
    return rc;
}

/* ------------------------------------------------------------------------
 * Splitting
 * ---------------------------------------------------------------------- */

/* For a leaf: the k in [1, n-1] that balances bytes(0,k) and bytes(k,n). */
static size_t leaf_split_point(const cell_list *l) {
    size_t total = cell_list_bytes(l, 0, l->n);
    size_t left = 0, best = 1, best_diff = SIZE_MAX;
    for (size_t k = 1; k < l->n; k++) {
        left += l->cells[k - 1].size + 2u;
        size_t right = total - left;
        size_t diff = left > right ? left - right : right - left;
        if (diff < best_diff) {
            best_diff = diff;
            best = k;
        }
    }
    return best;
}

/* For an internal node: the m in [1, n-2] whose cell moves up, balancing both sides. */
static size_t internal_split_point(const cell_list *l) {
    size_t total = cell_list_bytes(l, 0, l->n);
    size_t left = 0, best = 1, best_diff = SIZE_MAX;
    for (size_t m = 1; m + 1 < l->n; m++) {
        left += l->cells[m - 1].size + 2u;
        size_t right = total - left - (l->cells[m].size + 2u);
        size_t diff = left > right ? left - right : right - left;
        if (diff < best_diff) {
            best_diff = diff;
            best = m;
        }
    }
    return best;
}

static int write_node(ts_pager *p, bt_path *path, unsigned level, unsigned type, uint32_t link, cell_list *l);

/* After the node at `level` split into `left` and `right`, records `key` as
   the separator between them in the parent (growing a new root if needed). */
static int insert_separator(ts_pager *p, bt_path *path, unsigned level, uint32_t left, uint32_t right,
                            const uint8_t *key, size_t klen) {
    uint8_t cell[NODE_MAX_CELL];
    size_t csize = make_internal_cell(cell, left, key, klen);

    if (level == 0) {
        ts_page *root = NULL;
        TS_TRY(pager_alloc(p, &root));
        node_init(root->data, NODE_INTERNAL);
        node_set_link(root->data, right);
        bool ok = node_insert(root->data, 0, cell, csize);
        uint32_t root_pgno = root->pgno;
        pager_release(p, root);
        if (!ok) return TS_CORRUPT;
        ts_meta *m = pager_meta(p);
        m->root = root_pgno;
        m->height++;
        return m->height > BT_MAX_DEPTH ? TS_TOOBIG : TS_OK;
    }

    unsigned parent = level - 1;
    unsigned i = path->idx[parent];
    ts_page *pg = NULL;
    TS_TRY(get_node(p, path->pgno[parent], &pg));
    cell_list l;
    cell_list_init(&l);
    uint32_t link = node_link(pg->data);
    int rc = cell_list_load(&l, pg->data);
    pager_release(p, pg);
    if (rc == TS_OK) rc = cell_list_insert(&l, i, cell, csize);
    if (rc == TS_OK) {
        /* The pointer that used to reach `left` (slot i) now sits at slot i+1
           and must reach `right` instead. */
        if (i + 1 < l.n) int_set_child(l.cells[i + 1].data, right);
        else link = right;
        rc = write_node(p, path, parent, NODE_INTERNAL, link, &l);
    }
    cell_list_free(&l);
    return rc;
}

/* Rewrites the node at path[level] with the cells in `l`; splits it if they do not fit. */
static int write_node(ts_pager *p, bt_path *path, unsigned level, unsigned type, uint32_t link, cell_list *l) {
    ts_page *pg = NULL;
    TS_TRY(pager_get(p, path->pgno[level], &pg));
    int rc = pager_write(p, pg);
    if (rc != TS_OK) {
        pager_release(p, pg);
        return rc;
    }

    if (cell_list_bytes(l, 0, l->n) <= NODE_SPACE) {
        node_build(pg->data, type, link, l, 0, l->n);
        pager_release(p, pg);
        return TS_OK;
    }

    ts_page *right = NULL;
    rc = pager_alloc(p, &right);
    if (rc != TS_OK) {
        pager_release(p, pg);
        return rc;
    }
    uint32_t left_pgno = pg->pgno, right_pgno = right->pgno;
    const uint8_t *sep;
    size_t seplen;

    if (type == NODE_LEAF) {
        size_t k = leaf_split_point(l);
        /* Leaves stay chained: left -> right -> whatever followed left. */
        node_build(pg->data, NODE_LEAF, right_pgno, l, 0, k);
        node_build(right->data, NODE_LEAF, link, l, k, l->n);
        size_t llen = 0, rlen = 0;
        const uint8_t *lkey = cell_key(NODE_LEAF, l->cells[k - 1].data, &llen);
        const uint8_t *rkey = cell_key(NODE_LEAF, l->cells[k].data, &rlen);
        sep = rkey;
        seplen = make_separator(lkey, llen, rkey, rlen);
    } else {
        size_t m = internal_split_point(l);
        /* The middle cell moves up: its left child becomes the left node's rightmost child. */
        node_build(pg->data, NODE_INTERNAL, int_child(l->cells[m].data), l, 0, m);
        node_build(right->data, NODE_INTERNAL, link, l, m + 1, l->n);
        sep = int_key(l->cells[m].data);
        seplen = int_klen(l->cells[m].data);
    }
    pager_release(p, pg);
    pager_release(p, right);
    return insert_separator(p, path, level, left_pgno, right_pgno, sep, seplen);
}

/* ------------------------------------------------------------------------
 * Insert
 * ---------------------------------------------------------------------- */
int bt_put(ts_pager *p, const uint8_t *key, size_t klen, const uint8_t *val, size_t vlen) {
    if (klen > TS_MAX_KEY_SIZE) return TS_INVALID;
    if (vlen > TS_MAX_VALUE_SIZE) return TS_TOOBIG;

    uint8_t cell[NODE_MAX_CELL];
    put_u16(cell, (uint16_t)klen);
    put_u32(cell + 2, (uint32_t)vlen);
    if (klen) memcpy(cell + LEAF_CELL_HEADER, key, klen);
    size_t csize = LEAF_CELL_HEADER + klen;
    if (leaf_value_inline(klen, vlen)) {
        if (vlen) memcpy(cell + csize, val, vlen);
        csize += vlen;
    } else {
        uint32_t first = 0;
        TS_TRY(ovf_write(p, val, vlen, &first));
        put_u32(cell + csize, first);
        csize += 4;
    }

    bt_path path;
    TS_TRY(descend(p, key, klen, &path));
    unsigned level = path.depth - 1;
    ts_page *leaf = NULL;
    TS_TRY(get_node(p, path.pgno[level], &leaf));
    int rc = pager_write(p, leaf);
    if (rc != TS_OK) {
        pager_release(p, leaf);
        return rc;
    }

    bool exact = false;
    unsigned idx = node_lower_bound(leaf->data, key, klen, &exact);
    if (exact) {
        /* Replace: drop the old cell (and its overflow chain, if any). */
        const uint8_t *old = node_cell_c(leaf->data, idx);
        uint32_t old_first = leaf_has_overflow(old) ? leaf_overflow_page(old) : 0;
        size_t old_vlen = leaf_vlen(old);
        node_remove(leaf->data, idx);
        if (old_first) rc = ovf_free(p, old_first, old_vlen);
    } else {
        pager_meta(p)->key_count++;
    }

    if (rc == TS_OK && !node_insert(leaf->data, idx, cell, csize)) {
        /* Full: rebuild as two leaves. */
        cell_list l;
        cell_list_init(&l);
        uint32_t link = node_link(leaf->data);
        rc = cell_list_load(&l, leaf->data);
        if (rc == TS_OK) rc = cell_list_insert(&l, idx, cell, csize);
        pager_release(p, leaf);
        leaf = NULL;
        if (rc == TS_OK) rc = write_node(p, &path, level, NODE_LEAF, link, &l);
        cell_list_free(&l);
    }
    pager_release(p, leaf);
    return rc;
}

/* ------------------------------------------------------------------------
 * Delete and rebalance
 * ---------------------------------------------------------------------- */

/* Loads the parent's cells into `l`; returns its link. */
static int load_parent(ts_pager *p, uint32_t pgno, cell_list *l, uint32_t *link) {
    ts_page *pg = NULL;
    TS_TRY(get_node(p, pgno, &pg));
    *link = node_link(pg->data);
    int rc = node_type(pg->data) == NODE_INTERNAL ? cell_list_load(l, pg->data) : TS_CORRUPT;
    pager_release(p, pg);
    return rc;
}

static int rebalance(ts_pager *p, bt_path *path, unsigned level);

/* Rewrites a node that only shrank (always fits), then rebalances it if it underflowed. */
static int shrink_parent(ts_pager *p, bt_path *path, unsigned level, uint32_t link, cell_list *l) {
    ts_page *pg = NULL;
    TS_TRY(pager_get(p, path->pgno[level], &pg));
    int rc = pager_write(p, pg);
    if (rc == TS_OK) node_build(pg->data, NODE_INTERNAL, link, l, 0, l->n);
    size_t used = node_used(pg->data);
    pager_release(p, pg);
    if (rc == TS_OK && level > 0 && used < NODE_UNDERFLOW) rc = rebalance(p, path, level);
    return rc;
}

/* The node at path[level] (not the root) is under-full: merge it with a
   sibling, or move cells over from the sibling. */
static int rebalance(ts_pager *p, bt_path *path, unsigned level) {
    unsigned plevel = level - 1;
    uint32_t parent_pgno = path->pgno[plevel];
    unsigned i = path->idx[plevel];

    cell_list parent;
    cell_list_init(&parent);
    uint32_t plink = 0;
    int rc = load_parent(p, parent_pgno, &parent, &plink);
    if (rc == TS_OK && parent.n == 0) rc = TS_CORRUPT; /* an internal node always has two children */
    if (rc != TS_OK) {
        cell_list_free(&parent);
        return rc;
    }

    /* Pair the node with its left sibling if it has one, else its right one.
       `sep` is the parent cell that separates the pair. */
    unsigned sep = i > 0 ? i - 1 : 0;
    uint32_t left_pgno = i > 0 ? int_child(parent.cells[i - 1].data) : path->pgno[level];
    uint32_t right_pgno = i > 0 ? path->pgno[level]
                                : (parent.n > 1 ? int_child(parent.cells[1].data) : plink);

    ts_page *left = NULL, *right = NULL;
    rc = get_node(p, left_pgno, &left);
    if (rc == TS_OK) rc = get_node(p, right_pgno, &right);
    if (rc == TS_OK && node_type(left->data) != node_type(right->data)) rc = TS_CORRUPT;
    if (rc == TS_OK) rc = pager_write(p, left);
    if (rc == TS_OK) rc = pager_write(p, right);
    if (rc != TS_OK) {
        pager_release(p, left);
        pager_release(p, right);
        cell_list_free(&parent);
        return rc;
    }

    unsigned type = node_type(left->data);
    uint32_t right_link = node_link(right->data);
    cell_list both;
    cell_list_init(&both);
    rc = cell_list_load(&both, left->data);
    if (rc == TS_OK && type == NODE_INTERNAL) {
        /* For internal nodes the separator comes down between the two halves,
           pointing at the left node's former rightmost child. */
        uint8_t cell[NODE_MAX_CELL];
        size_t sz = make_internal_cell(cell, node_link(left->data), int_key(parent.cells[sep].data),
                                       int_klen(parent.cells[sep].data));
        rc = cell_list_push(&both, cell, sz);
    }
    if (rc == TS_OK) rc = cell_list_load(&both, right->data);

    if (rc == TS_OK && cell_list_bytes(&both, 0, both.n) <= NODE_SPACE) {
        /* Merge into the left node; the right node goes to the free list. */
        node_build(left->data, type, right_link, &both, 0, both.n);
        pager_release(p, left);
        pager_release(p, right);
        left = right = NULL;
        rc = pager_free(p, right_pgno);

        if (rc == TS_OK) {
            cell_list_remove(&parent, sep);
            /* The pointer that reached the right node now reaches the merged node. */
            if (sep < parent.n) int_set_child(parent.cells[sep].data, left_pgno);
            else plink = left_pgno;

            if (plevel == 0 && parent.n == 0) {
                /* The root has a single child left: that child becomes the root. */
                ts_meta *m = pager_meta(p);
                m->root = left_pgno;
                m->height--;
                rc = pager_free(p, parent_pgno);
            } else {
                rc = shrink_parent(p, path, plevel, plink, &parent);
            }
        }
    } else if (rc == TS_OK) {
        /* Too big for one page: split the combined cells evenly instead. */
        const uint8_t *sepkey;
        size_t seplen;
        if (type == NODE_LEAF) {
            size_t k = leaf_split_point(&both);
            node_build(left->data, NODE_LEAF, right_pgno, &both, 0, k);
            node_build(right->data, NODE_LEAF, right_link, &both, k, both.n);
            size_t llen = 0, rlen = 0;
            const uint8_t *lkey = cell_key(NODE_LEAF, both.cells[k - 1].data, &llen);
            sepkey = cell_key(NODE_LEAF, both.cells[k].data, &rlen);
            seplen = make_separator(lkey, llen, sepkey, rlen);
        } else {
            size_t m = internal_split_point(&both);
            node_build(left->data, NODE_INTERNAL, int_child(both.cells[m].data), &both, 0, m);
            node_build(right->data, NODE_INTERNAL, right_link, &both, m + 1, both.n);
            sepkey = int_key(both.cells[m].data);
            seplen = int_klen(both.cells[m].data);
        }
        pager_release(p, left);
        pager_release(p, right);
        left = right = NULL;

        /* Replace the separator in the parent. A longer separator can make the
           parent overflow, so this goes through the general write-or-split path. */
        uint8_t cell[NODE_MAX_CELL];
        size_t sz = make_internal_cell(cell, left_pgno, sepkey, seplen);
        cell_list_remove(&parent, sep);
        rc = cell_list_insert(&parent, sep, cell, sz);
        if (rc == TS_OK) rc = write_node(p, path, plevel, NODE_INTERNAL, plink, &parent);
    }

    pager_release(p, left);
    pager_release(p, right);
    cell_list_free(&both);
    cell_list_free(&parent);
    return rc;
}

int bt_delete(ts_pager *p, const uint8_t *key, size_t klen) {
    if (klen > TS_MAX_KEY_SIZE) return TS_NOTFOUND;
    bt_path path;
    TS_TRY(descend(p, key, klen, &path));
    unsigned level = path.depth - 1;
    ts_page *leaf = NULL;
    TS_TRY(get_node(p, path.pgno[level], &leaf));

    bool exact = false;
    unsigned idx = node_lower_bound(leaf->data, key, klen, &exact);
    if (!exact) {
        pager_release(p, leaf);
        return TS_NOTFOUND;
    }
    int rc = pager_write(p, leaf);
    if (rc != TS_OK) {
        pager_release(p, leaf);
        return rc;
    }
    const uint8_t *cell = node_cell_c(leaf->data, idx);
    uint32_t first = leaf_has_overflow(cell) ? leaf_overflow_page(cell) : 0;
    size_t vlen = leaf_vlen(cell);
    node_remove(leaf->data, idx);
    size_t used = node_used(leaf->data);
    pager_release(p, leaf);
    pager_meta(p)->key_count--;

    if (first) TS_TRY(ovf_free(p, first, vlen));
    if (level > 0 && used < NODE_UNDERFLOW) rc = rebalance(p, &path, level);
    return rc;
}
