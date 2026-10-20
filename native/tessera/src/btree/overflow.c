/*
 * Overflow chains: a value too large for a leaf cell is stored in a linked
 * list of pages, and the cell keeps only the first page number.
 */
#include <stdlib.h>

#include "btree/btree.h"
#include "btree/node.h"

static uint32_t pages_for(size_t vlen) {
    return (uint32_t)((vlen + OVERFLOW_CAPACITY - 1) / OVERFLOW_CAPACITY);
}

int ovf_write(ts_pager *p, const uint8_t *val, size_t vlen, uint32_t *first) {
    *first = 0;
    ts_page *prev = NULL;
    size_t off = 0;
    while (off < vlen) {
        ts_page *pg = NULL;
        int rc = pager_alloc(p, &pg);
        if (rc != TS_OK) {
            pager_release(p, prev);
            return rc; /* the transaction is rolled back by the caller */
        }
        size_t chunk = vlen - off < OVERFLOW_CAPACITY ? vlen - off : OVERFLOW_CAPACITY;
        pg->data[0] = NODE_OVERFLOW;
        put_u32(pg->data + 4, 0);
        put_u32(pg->data + 8, (uint32_t)chunk);
        memcpy(pg->data + OVERFLOW_HEADER, val + off, chunk);
        if (prev) {
            put_u32(prev->data + 4, pg->pgno); /* prev is still writable: allocated in this txn */
            pager_release(p, prev);
        } else {
            *first = pg->pgno;
        }
        prev = pg;
        off += chunk;
    }
    pager_release(p, prev);
    return TS_OK;
}

/* Walks a chain, validating every page; copies the bytes into `out` if not NULL
   and frees the pages if `free_pages`. */
static int walk(ts_pager *p, uint32_t first, size_t vlen, uint8_t *out, int free_pages) {
    uint32_t expected = pages_for(vlen);
    uint32_t pgno = first;
    size_t off = 0;
    for (uint32_t i = 0; i < expected; i++) {
        if (pgno == 0) return TS_CORRUPT;
        ts_page *pg = NULL;
        TS_TRY(pager_get(p, pgno, &pg));
        const uint8_t *d = pg->data;
        uint32_t used = get_u32(d + 8);
        uint32_t next = get_u32(d + 4);
        size_t want = vlen - off < OVERFLOW_CAPACITY ? vlen - off : OVERFLOW_CAPACITY;
        if (d[0] != NODE_OVERFLOW || used != want || (i + 1 == expected) != (next == 0)) {
            pager_release(p, pg);
            return TS_CORRUPT;
        }
        if (out) memcpy(out + off, d + OVERFLOW_HEADER, used);
        pager_release(p, pg);
        if (free_pages) TS_TRY(pager_free(p, pgno));
        off += used;
        pgno = next;
    }
    return TS_OK;
}

int ovf_read(ts_pager *p, uint32_t first, size_t vlen, uint8_t *out) {
    return walk(p, first, vlen, out, 0);
}

int ovf_free(ts_pager *p, uint32_t first, size_t vlen) {
    return walk(p, first, vlen, NULL, 1);
}
