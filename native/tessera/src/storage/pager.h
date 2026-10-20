/*
 * Pager: pages in, pages out, and transactions.
 *
 * Everything above the pager sees the database as an array of 4 KiB pages,
 * numbered from 0. The pager
 *   - caches pages in a fixed-size pool with LRU eviction,
 *   - finds the current version of a page (the cache, then the WAL, then the
 *     database file) and verifies its checksum on every read from storage,
 *   - tracks which pages a transaction dirtied and, on commit, writes them to
 *     the WAL as one atomic unit; on rollback it simply forgets them,
 *   - owns the database header (page 0) and the free-page list.
 *
 * Policy: "no-steal". A dirty page is never written anywhere before its
 * transaction commits, so rollback never has to undo anything on disk. The
 * cost is that a transaction's dirty pages must fit in memory; the cache
 * grows past its limit rather than evict them.
 *
 * Page 0 layout (the database header):
 *    0  "TESSERA\0"          magic
 *    8  u32 format version (1)
 *   12  u32 page size
 *   16  u32 page count       pages in the database, including free ones
 *   20  u32 root page        root of the B+tree
 *   24  u32 tree height      1 = the root is a leaf
 *   28  u32 free list head   first trunk page of the free list, 0 = none
 *   32  u32 free page count
 *   40  u64 transaction id   of the last committed transaction
 *   48  u64 key count
 *   4092 u32 CRC32C of bytes 0..4091 (like every page)
 */
#ifndef TS_PAGER_H
#define TS_PAGER_H

#include <stdbool.h>
#include <stdint.h>

#include "os/vfs.h"
#include "util/common.h"

typedef struct ts_page {
    uint32_t        pgno;
    uint32_t        pin;     /* references held by callers; pinned pages are never evicted */
    bool            dirty;   /* modified by the open transaction */
    bool            checked; /* structure validated since it was read (see btree get_node) */
    uint8_t        *data;    /* TS_PAGE_SIZE bytes */
    struct ts_page *prev;    /* links in the LRU list (clean, unpinned) or the dirty list */
    struct ts_page *next;
} ts_page;

typedef struct {
    uint32_t page_count;
    uint32_t root;
    uint32_t height;
    uint32_t freelist_head;
    uint32_t freelist_count;
    uint64_t txn_id;
    uint64_t key_count;
} ts_meta;

typedef struct ts_pager ts_pager;

/* *is_new is set when the database did not exist: the pager then leaves a
   transaction open, with page 0 allocated, for the caller to finish creating
   the database (the B+tree root) and commit. */
int  pager_open(ts_vfs *vfs, const char *path, const ts_options *opt, ts_pager **out, bool *is_new);
/* Rolls back, checkpoints (unless poisoned) and frees. */
int  pager_close(ts_pager *p);
/* Frees everything without any further I/O: what a crash looks like to the files. */
void pager_abandon(ts_pager *p);

int  pager_get(ts_pager *p, uint32_t pgno, ts_page **out);
void pager_release(ts_pager *p, ts_page *page);
/* Must be called before modifying a page's bytes. */
int  pager_write(ts_pager *p, ts_page *page);
/* A new zeroed page, pinned and writable: from the free list, or appended. */
int  pager_alloc(ts_pager *p, ts_page **out);
/* Returns a page to the free list. The caller must not hold a pin on it. */
int  pager_free(ts_pager *p, uint32_t pgno);

int  pager_begin(ts_pager *p);
int  pager_commit(ts_pager *p);
int  pager_rollback(ts_pager *p);
bool pager_in_txn(const ts_pager *p);
int  pager_checkpoint(ts_pager *p);

/* The header fields; modify only inside a transaction. */
ts_meta *pager_meta(ts_pager *p);
/* Changes on every modification, commit and rollback; cursors use it to detect writes. */
uint64_t pager_version(const ts_pager *p);
/* Non-zero once a write to storage has failed: the in-memory state can no
   longer be trusted to match the files, so all further operations fail. */
bool pager_poisoned(const ts_pager *p);

typedef struct {
    uint64_t hits, misses, commits;
    uint32_t wal_frames;
    uint64_t checkpoints;
    size_t   cached_pages;
} ts_pager_stats;
void pager_stats(const ts_pager *p, ts_pager_stats *out);

/* Free list (freelist.c), used by pager_alloc / pager_free. */
#define FREELIST_TRUNK_TYPE 4u
#define FREELIST_CAPACITY ((TS_PAGE_USABLE - 12u) / 4u)
int freelist_pop(ts_pager *p, uint32_t *pgno);   /* *pgno = 0 when the list is empty */
int freelist_push(ts_pager *p, uint32_t pgno);

#endif
