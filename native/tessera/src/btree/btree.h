/*
 * B+tree over the pager.
 *
 * All entries live in leaves, sorted by key; leaves are chained left to right
 * for range scans. Internal nodes hold separator keys only. Every leaf is at
 * the same depth (the height stored in the header).
 *
 * Nodes keep no parent pointers: an operation remembers the path it took from
 * the root, so a split or merge never has to update the children it moves.
 *
 * Insert: split a full node in two by bytes and push a separator up.
 * Delete: a node that drops below a quarter full is merged with a sibling if
 * the two fit in one page, or else shares cells with it; the separator in the
 * parent is updated, which can cascade up to the root, and an internal root
 * left with a single child is replaced by that child.
 *
 * All functions must run inside a pager transaction when they modify the tree.
 */
#ifndef TS_BTREE_H
#define TS_BTREE_H

#include <stddef.h>
#include <stdint.h>

#include "storage/pager.h"

#define BT_MAX_DEPTH 40

/* Creates an empty tree (one empty leaf) and records it in the header. */
int bt_create(ts_pager *p);

int bt_get(ts_pager *p, const uint8_t *key, size_t klen, uint8_t **val, size_t *vlen);
int bt_put(ts_pager *p, const uint8_t *key, size_t klen, const uint8_t *val, size_t vlen);
int bt_delete(ts_pager *p, const uint8_t *key, size_t klen);

/* Overflow chains for values that do not fit in a leaf cell (overflow.c).
   Page layout: u8 type (3) | u32 next page at 4 | u32 bytes used at 8 | data from 12. */
#define OVERFLOW_HEADER 12u
#define OVERFLOW_CAPACITY (TS_PAGE_USABLE - OVERFLOW_HEADER)
int ovf_write(ts_pager *p, const uint8_t *val, size_t vlen, uint32_t *first);
int ovf_read(ts_pager *p, uint32_t first, size_t vlen, uint8_t *out);
int ovf_free(ts_pager *p, uint32_t first, size_t vlen);

/* Reads the value of a leaf cell into a malloc'd buffer. */
int bt_cell_value(ts_pager *p, const uint8_t *cell, uint8_t **val, size_t *vlen);

/* ------------------------------------------------------------------------
 * Cursor (cursor.c). Holds no page pins between calls; if the tree changes
 * between calls (detected through pager_version), it repositions itself on
 * the first key after the one it last returned.
 * ---------------------------------------------------------------------- */
typedef struct {
    ts_pager *p;
    uint32_t  leaf;
    unsigned  idx;
    uint64_t  version;
    int       state;      /* 0 = not positioned, 1 = on an entry, 2 = past the end */
    uint8_t  *key;        /* copy of the current key */
    size_t    klen, kcap;
} bt_cursor;

void bt_cursor_init(bt_cursor *c, ts_pager *p);
void bt_cursor_free(bt_cursor *c);
/* Positions on the first entry with key >= `key` (NULL: the first entry). TS_OK or TS_DONE. */
int  bt_cursor_seek(bt_cursor *c, const uint8_t *key, size_t klen);
/* Moves to the next entry. TS_OK or TS_DONE. */
int  bt_cursor_next(bt_cursor *c);
/* Loads the current entry's value (malloc'd). TS_NOTFOUND if it was deleted meanwhile. */
int  bt_cursor_value(bt_cursor *c, uint8_t **val, size_t *vlen);

#endif
