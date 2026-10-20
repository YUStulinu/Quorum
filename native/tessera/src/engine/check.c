/*
 * Integrity checker: everything that must be true of a healthy file.
 *
 *  - every page that is read has a valid checksum and a well-formed layout;
 *  - keys are strictly increasing inside every node, and every key lies
 *    within the bounds set by the separators above it;
 *  - all leaves are at the same depth, which matches the header's height;
 *  - no node except the root is empty;
 *  - the leaf chain visits exactly the leaves of the tree, in key order;
 *  - overflow chains have the right length and page types;
 *  - the key count and free-page count in the header are right;
 *  - every page is accounted for exactly once: header, tree node, overflow
 *    page, free-list trunk or free page. Nothing leaks, nothing is shared.
 */
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include "btree/btree.h"
#include "btree/node.h"
#include "engine/db_internal.h"

typedef struct {
    ts_pager *p;
    const ts_meta *m;
    uint8_t *owner;          /* per page: 0 = unseen, else the first owner kind */
    uint32_t *leaves;        /* leaves in tree order */
    size_t nleaves, leaves_cap;
    uint64_t keys;
    uint64_t problems;
    void (*report)(void *ctx, const char *msg);
    void *ctx;
} checker;

enum { OWN_HEADER = 1, OWN_NODE, OWN_OVERFLOW, OWN_TRUNK, OWN_FREE };
static const char *owner_name[] = {"", "header", "tree node", "overflow page", "free-list trunk", "free page"};

TS_PRINTF(2, 3) static void problem(checker *ck, const char *fmt, ...) {
    ck->problems++;
    if (!ck->report) return;
    char msg[256];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof msg, fmt, ap);
    va_end(ap);
    ck->report(ck->ctx, msg);
}

/* Claims a page for `kind`; false if it is out of range or already owned. */
static bool claim(checker *ck, uint32_t pgno, int kind) {
    if (pgno >= ck->m->page_count) {
        problem(ck, "page %u (%s) is beyond the end of the database (%u pages)", (unsigned)pgno,
                owner_name[kind], (unsigned)ck->m->page_count);
        return false;
    }
    if (ck->owner[pgno]) {
        problem(ck, "page %u is used twice: as %s and as %s", (unsigned)pgno, owner_name[ck->owner[pgno]],
                owner_name[kind]);
        return false;
    }
    ck->owner[pgno] = (uint8_t)kind;
    return true;
}

static void check_overflow(checker *ck, uint32_t first, size_t vlen, uint32_t leaf) {
    uint32_t pgno = first;
    size_t remaining = vlen;
    while (remaining > 0) {
        if (pgno == 0) {
            problem(ck, "leaf %u: overflow chain ends %zu bytes early", (unsigned)leaf, remaining);
            return;
        }
        if (!claim(ck, pgno, OWN_OVERFLOW)) return;
        ts_page *pg = NULL;
        int rc = pager_get(ck->p, pgno, &pg);
        if (rc != TS_OK) {
            problem(ck, "overflow page %u: unreadable (%s)", (unsigned)pgno, ts_errstr(rc));
            return;
        }
        size_t want = remaining < OVERFLOW_CAPACITY ? remaining : OVERFLOW_CAPACITY;
        uint32_t used = get_u32(pg->data + 8);
        uint32_t next = get_u32(pg->data + 4);
        if (pg->data[0] != NODE_OVERFLOW) problem(ck, "page %u: expected an overflow page", (unsigned)pgno);
        if (used != want) problem(ck, "overflow page %u: holds %u bytes, expected %zu", (unsigned)pgno, (unsigned)used, want);
        pager_release(ck->p, pg);
        remaining -= want;
        if (remaining == 0 && next != 0) problem(ck, "overflow page %u: chain continues past the value", (unsigned)pgno);
        pgno = next;
    }
}

static void add_leaf(checker *ck, uint32_t pgno) {
    if (ck->nleaves == ck->leaves_cap) {
        size_t cap = ck->leaves_cap ? ck->leaves_cap * 2 : 256;
        uint32_t *l = (uint32_t *)realloc(ck->leaves, cap * sizeof *l);
        if (!l) return;
        ck->leaves = l;
        ck->leaves_cap = cap;
    }
    ck->leaves[ck->nleaves++] = pgno;
}

/* Keys in this subtree must satisfy lo <= key < hi (each bound optional). */
static void check_node(checker *ck, uint32_t pgno, unsigned depth, const uint8_t *lo, size_t lolen,
                       const uint8_t *hi, size_t hilen) {
    if (depth >= BT_MAX_DEPTH) {
        problem(ck, "tree deeper than %d levels", BT_MAX_DEPTH);
        return;
    }
    if (!claim(ck, pgno, OWN_NODE)) return;
    ts_page *pg = NULL;
    int rc = pager_get(ck->p, pgno, &pg);
    if (rc != TS_OK) {
        problem(ck, "node %u: unreadable (%s)", (unsigned)pgno, ts_errstr(rc));
        return;
    }
    /* Work on a private copy: recursion below may evict this page from the cache. */
    uint8_t *d = (uint8_t *)malloc(TS_PAGE_SIZE);
    if (!d) {
        pager_release(ck->p, pg);
        problem(ck, "out of memory");
        return;
    }
    memcpy(d, pg->data, TS_PAGE_SIZE);
    pager_release(ck->p, pg);

    if (!node_sane(d)) {
        problem(ck, "node %u: malformed page layout", (unsigned)pgno);
        free(d);
        return;
    }
    unsigned type = node_type(d);
    unsigned n = node_count(d);
    bool is_root = depth == 0;
    if (type == NODE_LEAF && depth + 1 != ck->m->height)
        problem(ck, "leaf %u is at depth %u, but the tree height is %u", (unsigned)pgno, depth + 1, (unsigned)ck->m->height);
    if (type == NODE_INTERNAL && depth + 1 >= ck->m->height)
        problem(ck, "internal node %u is at leaf depth %u", (unsigned)pgno, depth + 1);
    if (!is_root && n == 0) problem(ck, "node %u is empty but is not the root", (unsigned)pgno);

    const uint8_t *prev = NULL;
    size_t prevlen = 0;
    for (unsigned i = 0; i < n; i++) {
        const uint8_t *c = node_cell_c(d, i);
        size_t klen = 0;
        const uint8_t *key = cell_key(type, c, &klen);
        if (prev && ts_keycmp(prev, prevlen, key, klen) >= 0)
            problem(ck, "node %u: keys out of order at slot %u", (unsigned)pgno, i);
        if (lo && ts_keycmp(key, klen, lo, lolen) < 0)
            problem(ck, "node %u: key at slot %u is below its separator", (unsigned)pgno, i);
        if (hi && ts_keycmp(key, klen, hi, hilen) >= 0)
            problem(ck, "node %u: key at slot %u is not below the next separator", (unsigned)pgno, i);
        prev = key;
        prevlen = klen;
    }

    if (type == NODE_LEAF) {
        add_leaf(ck, pgno);
        ck->keys += n;
        for (unsigned i = 0; i < n; i++) {
            const uint8_t *c = node_cell_c(d, i);
            if (leaf_has_overflow(c)) check_overflow(ck, leaf_overflow_page(c), leaf_vlen(c), pgno);
        }
    } else {
        /* Child i covers [key(i-1), key(i)); the rightmost child covers [key(n-1), hi). */
        for (unsigned i = 0; i <= n; i++) {
            const uint8_t *clo = lo, *chi = hi;
            size_t clolen = lolen, chilen = hilen;
            if (i > 0) {
                clo = int_key(node_cell_c(d, i - 1));
                clolen = int_klen(node_cell_c(d, i - 1));
            }
            if (i < n) {
                chi = int_key(node_cell_c(d, i));
                chilen = int_klen(node_cell_c(d, i));
            }
            check_node(ck, node_child_at(d, i), depth + 1, clo, clolen, chi, chilen);
        }
    }
    free(d);
}

static void check_leaf_chain(checker *ck) {
    for (size_t i = 0; i < ck->nleaves; i++) {
        ts_page *pg = NULL;
        if (pager_get(ck->p, ck->leaves[i], &pg) != TS_OK) continue; /* already reported */
        uint32_t link = node_link(pg->data);
        pager_release(ck->p, pg);
        uint32_t expected = i + 1 < ck->nleaves ? ck->leaves[i + 1] : 0;
        if (link != expected)
            problem(ck, "leaf %u links to %u, but the next leaf in key order is %u", (unsigned)ck->leaves[i],
                    (unsigned)link, (unsigned)expected);
    }
}

static void check_freelist(checker *ck) {
    uint32_t trunk = ck->m->freelist_head;
    uint64_t counted = 0;
    while (trunk != 0) {
        if (!claim(ck, trunk, OWN_TRUNK)) return;
        counted++;
        ts_page *pg = NULL;
        int rc = pager_get(ck->p, trunk, &pg);
        if (rc != TS_OK) {
            problem(ck, "free-list trunk %u: unreadable (%s)", (unsigned)trunk, ts_errstr(rc));
            return;
        }
        uint32_t count = get_u32(pg->data + 8);
        uint32_t next = get_u32(pg->data + 4);
        if (pg->data[0] != FREELIST_TRUNK_TYPE || count > FREELIST_CAPACITY) {
            problem(ck, "page %u: not a valid free-list trunk", (unsigned)trunk);
            pager_release(ck->p, pg);
            return;
        }
        for (uint32_t i = 0; i < count; i++) {
            if (claim(ck, get_u32(pg->data + 12 + 4 * i), OWN_FREE)) counted++;
        }
        pager_release(ck->p, pg);
        trunk = next;
    }
    if (counted != ck->m->freelist_count)
        problem(ck, "free list holds %llu pages, the header says %u", (unsigned long long)counted,
                (unsigned)ck->m->freelist_count);
}

int ts_check(ts_db *db, void (*report)(void *ctx, const char *msg), void *ctx, uint64_t *problems) {
    if (!db) return TS_INVALID;
    if (pager_poisoned(db->pager)) return TS_POISONED;
    checker ck;
    memset(&ck, 0, sizeof ck);
    ck.p = db->pager;
    ck.m = pager_meta(db->pager);
    ck.report = report;
    ck.ctx = ctx;
    ck.owner = (uint8_t *)calloc(ck.m->page_count, 1);
    if (!ck.owner) return TS_NOMEM;

    claim(&ck, 0, OWN_HEADER);
    check_node(&ck, ck.m->root, 0, NULL, 0, NULL, 0);
    check_leaf_chain(&ck);
    check_freelist(&ck);

    if (ck.keys != ck.m->key_count)
        problem(&ck, "the tree holds %llu keys, the header says %llu", (unsigned long long)ck.keys,
                (unsigned long long)ck.m->key_count);
    uint32_t leaked = 0, first_leak = 0;
    for (uint32_t i = 0; i < ck.m->page_count; i++) {
        if (!ck.owner[i]) {
            if (leaked++ == 0) first_leak = i;
        }
    }
    if (leaked) problem(&ck, "%u pages belong to nothing (first: page %u)", (unsigned)leaked, (unsigned)first_leak);

    free(ck.owner);
    free(ck.leaves);
    if (problems) *problems = ck.problems;
    return TS_OK;
}
