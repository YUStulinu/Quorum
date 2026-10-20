/*
 * B+tree node format: a "slotted page".
 *
 *   0   u8  type        1 = leaf, 2 = internal
 *   1   u8  reserved
 *   2   u16 cell count
 *   4   u16 content start   cells occupy [content start, 4092)
 *   6   u16 fragmented      bytes inside the content area freed by deletes
 *   8   u32 link            leaf: next leaf (0 = last); internal: rightmost child
 *  12   u32 reserved
 *  16   u16 slots[count]    offset of each cell, in key order
 *       ... free space ...
 *       cells, packed from the end of the page towards the slots
 *  4092 u32 page checksum
 *
 * Cells are variable-length:
 *   leaf      u16 key length | u32 value length | key | value
 *             or, when the value does not fit (see below), the same header
 *             with the value replaced by a u32: the first overflow page.
 *   internal  u32 left child | u16 key length | key
 *             The left child holds keys < key; the next pointer (the next
 *             cell's left child, or the link) holds keys >= key.
 *
 * Inserting in key order only moves 2-byte slots; the cell bytes stay put.
 * Deleting leaves a hole that is reclaimed by compacting the page when needed.
 *
 * Sizes are capped so that at least four cells always fit in a page; that
 * guarantees a split always produces two valid nodes and a node can always
 * absorb one more separator.
 */
#ifndef TS_NODE_H
#define TS_NODE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "util/common.h"

#define NODE_LEAF 1u
#define NODE_INTERNAL 2u
#define NODE_OVERFLOW 3u

#define NODE_HEADER 16u
#define NODE_SPACE (TS_PAGE_USABLE - NODE_HEADER) /* bytes for slots + cells: 4076 */
#define NODE_MAX_CELL 1016u                      /* (NODE_SPACE / 4) - slot size */
#define LEAF_CELL_HEADER 6u
#define INTERNAL_CELL_HEADER 6u
#define NODE_UNDERFLOW (NODE_SPACE / 4u)         /* less than this many bytes used: rebalance */

/* Header access */
static inline unsigned node_type(const uint8_t *pg) { return pg[0]; }
static inline unsigned node_count(const uint8_t *pg) { return get_u16(pg + 2); }
static inline uint32_t node_link(const uint8_t *pg) { return get_u32(pg + 8); }
static inline void node_set_link(uint8_t *pg, uint32_t v) { put_u32(pg + 8, v); }
static inline uint8_t *node_cell(uint8_t *pg, unsigned i) { return pg + get_u16(pg + NODE_HEADER + 2 * i); }
static inline const uint8_t *node_cell_c(const uint8_t *pg, unsigned i) {
    return pg + get_u16(pg + NODE_HEADER + 2 * i);
}

/* Leaf cells */
static inline uint16_t leaf_klen(const uint8_t *c) { return get_u16(c); }
static inline uint32_t leaf_vlen(const uint8_t *c) { return get_u32(c + 2); }
static inline const uint8_t *leaf_key(const uint8_t *c) { return c + LEAF_CELL_HEADER; }
/* Does a value of this length, with this key, live inside the cell? */
static inline bool leaf_value_inline(size_t klen, size_t vlen) {
    return LEAF_CELL_HEADER + klen + vlen <= NODE_MAX_CELL;
}
static inline bool leaf_has_overflow(const uint8_t *c) { return !leaf_value_inline(leaf_klen(c), leaf_vlen(c)); }
static inline const uint8_t *leaf_value(const uint8_t *c) { return c + LEAF_CELL_HEADER + leaf_klen(c); }
static inline uint32_t leaf_overflow_page(const uint8_t *c) { return get_u32(c + LEAF_CELL_HEADER + leaf_klen(c)); }

/* Internal cells */
static inline uint32_t int_child(const uint8_t *c) { return get_u32(c); }
static inline void int_set_child(uint8_t *c, uint32_t v) { put_u32(c, v); }
static inline uint16_t int_klen(const uint8_t *c) { return get_u16(c + 4); }
static inline const uint8_t *int_key(const uint8_t *c) { return c + INTERNAL_CELL_HEADER; }

/* Size of the cell at `c` in a node of type `type`. */
size_t cell_size(unsigned type, const uint8_t *c);
/* Key of a cell (leaf or internal). */
const uint8_t *cell_key(unsigned type, const uint8_t *c, size_t *klen);

void node_init(uint8_t *pg, unsigned type);
/* Bytes still free for a new cell, counting fragments (i.e. after compaction). */
size_t node_free(const uint8_t *pg);
/* Bytes taken by slots and cells. */
size_t node_used(const uint8_t *pg);

/* Inserts a cell at slot `idx`, compacting the page if that makes room.
   Returns false (page unchanged) if it does not fit. */
bool node_insert(uint8_t *pg, unsigned idx, const uint8_t *cell, size_t size);
void node_remove(uint8_t *pg, unsigned idx);
void node_compact(uint8_t *pg);

/* Index of the first cell whose key is >= key; *exact tells whether it is equal. */
unsigned node_lower_bound(const uint8_t *pg, const uint8_t *key, size_t klen, bool *exact);
/* In an internal node: index of the child pointer to follow for `key`
   (0..count, where count means the link / rightmost child). */
unsigned node_child_index(const uint8_t *pg, const uint8_t *key, size_t klen);
uint32_t node_child_at(const uint8_t *pg, unsigned idx);

/* Structural checks for one node, used before trusting its contents. */
bool node_sane(const uint8_t *pg);

/* ------------------------------------------------------------------------
 * Cell lists: splits, merges and redistribution work on copies of the cells,
 * then rebuild whole pages from them.
 * ---------------------------------------------------------------------- */
typedef struct {
    uint8_t *data;
    uint16_t size;
} cellref;

typedef struct {
    cellref *cells;
    size_t   n, cap;
    uint8_t *arena;      /* owns the bytes of every cell in the list */
    size_t   arena_used, arena_cap;
} cell_list;

void cell_list_init(cell_list *l);
void cell_list_free(cell_list *l);
/* Appends a copy of a cell. */
int  cell_list_push(cell_list *l, const uint8_t *cell, size_t size);
/* Inserts a copy at position i. */
int  cell_list_insert(cell_list *l, size_t i, const uint8_t *cell, size_t size);
void cell_list_remove(cell_list *l, size_t i);
/* Appends copies of every cell of a page. */
int  cell_list_load(cell_list *l, const uint8_t *pg);
/* Bytes the cells [from, to) need in a page (cells + slots). */
size_t cell_list_bytes(const cell_list *l, size_t from, size_t to);
/* Rewrites `pg` as a node holding cells [from, to). The caller checks it fits. */
void node_build(uint8_t *pg, unsigned type, uint32_t link, const cell_list *l, size_t from, size_t to);

/* Builds an internal cell into buf (at least INTERNAL_CELL_HEADER + klen bytes). */
size_t make_internal_cell(uint8_t *buf, uint32_t child, const uint8_t *key, size_t klen);

/* The shortest key s with left < s <= right: the prefix of `right` one byte
   longer than its common prefix with `left`. Shorter separators mean more
   keys per internal node and therefore a shallower tree ("suffix truncation"). */
size_t make_separator(const uint8_t *left, size_t llen, const uint8_t *right, size_t rlen);

#endif
