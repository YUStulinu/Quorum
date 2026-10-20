/*
 * Slotted-page node operations. Format described in node.h.
 */
#include "btree/node.h"

#include <stdlib.h>

#define SLOT(pg, i) ((pg) + NODE_HEADER + 2u * (i))

static inline unsigned content_start(const uint8_t *pg) { return get_u16(pg + 4); }
static inline unsigned fragmented(const uint8_t *pg) { return get_u16(pg + 6); }

size_t cell_size(unsigned type, const uint8_t *c) {
    if (type == NODE_LEAF) {
        size_t klen = leaf_klen(c);
        size_t vlen = leaf_vlen(c);
        return LEAF_CELL_HEADER + klen + (leaf_value_inline(klen, vlen) ? vlen : 4u);
    }
    return INTERNAL_CELL_HEADER + int_klen(c);
}

const uint8_t *cell_key(unsigned type, const uint8_t *c, size_t *klen) {
    if (type == NODE_LEAF) {
        *klen = leaf_klen(c);
        return leaf_key(c);
    }
    *klen = int_klen(c);
    return int_key(c);
}

void node_init(uint8_t *pg, unsigned type) {
    memset(pg, 0, TS_PAGE_USABLE);
    pg[0] = (uint8_t)type;
    put_u16(pg + 2, 0);
    put_u16(pg + 4, (uint16_t)TS_PAGE_USABLE);
    put_u16(pg + 6, 0);
}

size_t node_free(const uint8_t *pg) {
    size_t gap = content_start(pg) - (NODE_HEADER + 2u * node_count(pg));
    return gap + fragmented(pg);
}

size_t node_used(const uint8_t *pg) {
    return NODE_SPACE - node_free(pg);
}

/* Rewrites the cell area so that all free space is one contiguous gap. */
void node_compact(uint8_t *pg) {
    unsigned n = node_count(pg);
    unsigned type = node_type(pg);
    uint8_t *tmp = (uint8_t *)malloc(TS_PAGE_SIZE);
    if (!tmp) return; /* compaction is an optimization; callers re-check the space */
    memcpy(tmp, pg, TS_PAGE_SIZE);
    unsigned top = TS_PAGE_USABLE;
    for (unsigned i = 0; i < n; i++) {
        const uint8_t *c = tmp + get_u16(SLOT(tmp, i));
        size_t sz = cell_size(type, c);
        top -= (unsigned)sz;
        memcpy(pg + top, c, sz);
        put_u16(SLOT(pg, i), (uint16_t)top);
    }
    put_u16(pg + 4, (uint16_t)top);
    put_u16(pg + 6, 0);
    free(tmp);
}

bool node_insert(uint8_t *pg, unsigned idx, const uint8_t *cell, size_t size) {
    unsigned n = node_count(pg);
    if (node_free(pg) < size + 2u) return false;
    size_t gap = content_start(pg) - (NODE_HEADER + 2u * n);
    if (gap < size + 2u) {
        node_compact(pg);
        gap = content_start(pg) - (NODE_HEADER + 2u * n);
        if (gap < size + 2u) return false;
    }
    unsigned top = content_start(pg) - (unsigned)size;
    memcpy(pg + top, cell, size);
    memmove(SLOT(pg, idx + 1), SLOT(pg, idx), 2u * (n - idx));
    put_u16(SLOT(pg, idx), (uint16_t)top);
    put_u16(pg + 2, (uint16_t)(n + 1));
    put_u16(pg + 4, (uint16_t)top);
    return true;
}

void node_remove(uint8_t *pg, unsigned idx) {
    unsigned n = node_count(pg);
    unsigned off = get_u16(SLOT(pg, idx));
    size_t sz = cell_size(node_type(pg), pg + off);
    memmove(SLOT(pg, idx), SLOT(pg, idx + 1), 2u * (n - idx - 1));
    put_u16(pg + 2, (uint16_t)(n - 1));
    if (off == content_start(pg)) {
        put_u16(pg + 4, (uint16_t)(off + sz)); /* the cell was at the edge of the gap */
    } else {
        put_u16(pg + 6, (uint16_t)(fragmented(pg) + sz));
    }
}

unsigned node_lower_bound(const uint8_t *pg, const uint8_t *key, size_t klen, bool *exact) {
    unsigned type = node_type(pg);
    unsigned lo = 0, hi = node_count(pg);
    *exact = false;
    while (lo < hi) {
        unsigned mid = lo + (hi - lo) / 2;
        size_t mlen = 0;
        const uint8_t *mkey = cell_key(type, node_cell_c(pg, mid), &mlen);
        int c = ts_keycmp(mkey, mlen, key, klen);
        if (c < 0) {
            lo = mid + 1;
        } else {
            if (c == 0) *exact = true;
            hi = mid;
        }
    }
    if (*exact && lo < node_count(pg)) {
        size_t mlen = 0;
        const uint8_t *mkey = cell_key(type, node_cell_c(pg, lo), &mlen);
        *exact = ts_keycmp(mkey, mlen, key, klen) == 0;
    }
    return lo;
}

unsigned node_child_index(const uint8_t *pg, const uint8_t *key, size_t klen) {
    /* Child i holds keys in [key(i-1), key(i)): follow the first cell whose key
       is strictly greater than the search key. */
    bool exact = false;
    unsigned i = node_lower_bound(pg, key, klen, &exact);
    return exact ? i + 1 : i;
}

uint32_t node_child_at(const uint8_t *pg, unsigned idx) {
    return idx < node_count(pg) ? int_child(node_cell_c(pg, idx)) : node_link(pg);
}

bool node_sane(const uint8_t *pg) {
    unsigned type = node_type(pg);
    if (type != NODE_LEAF && type != NODE_INTERNAL) return false;
    unsigned n = node_count(pg);
    unsigned start = content_start(pg);
    if (NODE_HEADER + 2u * n > start || start > TS_PAGE_USABLE) return false;
    size_t total = 0;
    for (unsigned i = 0; i < n; i++) {
        unsigned off = get_u16(SLOT(pg, i));
        if (off < start || off + LEAF_CELL_HEADER > TS_PAGE_USABLE) return false;
        size_t sz = cell_size(type, pg + off);
        if (off + sz > TS_PAGE_USABLE || sz > NODE_MAX_CELL) return false;
        size_t klen = 0;
        (void)cell_key(type, pg + off, &klen);
        if (klen > TS_MAX_KEY_SIZE) return false;
        total += sz;
    }
    /* Cells plus fragments must exactly fill the content area. */
    return total + fragmented(pg) == TS_PAGE_USABLE - start;
}

/* ------------------------------------------------------------------------ */

void cell_list_init(cell_list *l) {
    memset(l, 0, sizeof *l);
}

void cell_list_free(cell_list *l) {
    free(l->cells);
    free(l->arena);
    memset(l, 0, sizeof *l);
}

static int arena_copy(cell_list *l, const uint8_t *cell, size_t size, uint8_t **out) {
    /* Size the arena generously up front (four pages covers every split and
       merge), so growing it - and rebasing every cell - is rare. */
    if (l->arena_used + size > l->arena_cap) {
        size_t cap = l->arena_cap ? l->arena_cap : 4 * TS_PAGE_SIZE;
        while (cap < l->arena_used + size) cap *= 2;
        /* Grow by copying rather than realloc, so the old block is still
           valid while the cells are rebased onto the new one. */
        uint8_t *a = (uint8_t *)malloc(cap);
        if (!a) return TS_NOMEM;
        if (l->arena) {
            memcpy(a, l->arena, l->arena_used);
            for (size_t i = 0; i < l->n; i++) l->cells[i].data = a + (l->cells[i].data - l->arena);
            free(l->arena);
        }
        l->arena = a;
        l->arena_cap = cap;
    }
    uint8_t *dst = l->arena + l->arena_used;
    memcpy(dst, cell, size);
    l->arena_used += size;
    *out = dst;
    return TS_OK;
}

int cell_list_insert(cell_list *l, size_t i, const uint8_t *cell, size_t size) {
    if (l->n == l->cap) {
        size_t cap = l->cap ? l->cap * 2 : 64;
        cellref *c = (cellref *)realloc(l->cells, cap * sizeof *c);
        if (!c) return TS_NOMEM;
        l->cells = c;
        l->cap = cap;
    }
    uint8_t *copy = NULL;
    TS_TRY(arena_copy(l, cell, size, &copy));
    memmove(&l->cells[i + 1], &l->cells[i], (l->n - i) * sizeof *l->cells);
    l->cells[i].data = copy;
    l->cells[i].size = (uint16_t)size;
    l->n++;
    return TS_OK;
}

int cell_list_push(cell_list *l, const uint8_t *cell, size_t size) {
    return cell_list_insert(l, l->n, cell, size);
}

void cell_list_remove(cell_list *l, size_t i) {
    memmove(&l->cells[i], &l->cells[i + 1], (l->n - i - 1) * sizeof *l->cells);
    l->n--;
}

int cell_list_load(cell_list *l, const uint8_t *pg) {
    unsigned type = node_type(pg);
    unsigned n = node_count(pg);
    for (unsigned i = 0; i < n; i++) {
        const uint8_t *c = node_cell_c(pg, i);
        TS_TRY(cell_list_push(l, c, cell_size(type, c)));
    }
    return TS_OK;
}

size_t cell_list_bytes(const cell_list *l, size_t from, size_t to) {
    size_t total = 0;
    for (size_t i = from; i < to; i++) total += l->cells[i].size + 2u;
    return total;
}

void node_build(uint8_t *pg, unsigned type, uint32_t link, const cell_list *l, size_t from, size_t to) {
    node_init(pg, type);
    node_set_link(pg, link);
    unsigned top = TS_PAGE_USABLE;
    unsigned n = 0;
    for (size_t i = from; i < to; i++) {
        top -= l->cells[i].size;
        memcpy(pg + top, l->cells[i].data, l->cells[i].size);
        put_u16(SLOT(pg, n), (uint16_t)top);
        n++;
    }
    put_u16(pg + 2, (uint16_t)n);
    put_u16(pg + 4, (uint16_t)top);
}

size_t make_internal_cell(uint8_t *buf, uint32_t child, const uint8_t *key, size_t klen) {
    put_u32(buf, child);
    put_u16(buf + 4, (uint16_t)klen);
    if (klen) memcpy(buf + INTERNAL_CELL_HEADER, key, klen);
    return INTERNAL_CELL_HEADER + klen;
}

size_t make_separator(const uint8_t *left, size_t llen, const uint8_t *right, size_t rlen) {
    size_t common = 0;
    while (common < llen && common < rlen && left[common] == right[common]) common++;
    /* right > left, so right has a byte at `common` (if left were longer and
       equal up to rlen, right would be a prefix of left and therefore smaller). */
    return common + 1 <= rlen ? common + 1 : rlen;
}
