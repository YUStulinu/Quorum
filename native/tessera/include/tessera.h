/*
 * Tessera — an embedded key-value store.
 *
 * Keys and values are arbitrary byte strings, kept sorted by key in a B+tree
 * stored in fixed-size pages on disk. Changes are made durable through a
 * write-ahead log (WAL); after a crash, opening the database replays the
 * committed part of the log and discards the rest, so the data is always
 * exactly as of some committed transaction.
 *
 * Threading: a ts_db handle must be used by one thread at a time. One process
 * at a time may open a database file (opening it twice returns TS_BUSY).
 */
#ifndef TESSERA_H
#define TESSERA_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TESSERA_VERSION "1.0.0"

/* ------------------------------------------------------------------------
 * Result codes. Zero and positive values are not errors.
 * ---------------------------------------------------------------------- */
enum {
    TS_OK        =  0,
    TS_NOTFOUND  =  1,  /* the key does not exist */
    TS_DONE      =  2,  /* an iterator has no more entries */

    TS_ERROR     = -1,  /* unspecified failure */
    TS_NOMEM     = -2,  /* out of memory */
    TS_IOERR     = -3,  /* the operating system reported an I/O error */
    TS_CORRUPT   = -4,  /* the file contents fail a consistency check */
    TS_BUSY      = -5,  /* the database is open in another process or handle */
    TS_INVALID   = -6,  /* bad argument (e.g. key too long) */
    TS_TOOBIG    = -7,  /* value or database larger than supported */
    TS_TXN       = -8,  /* transaction misuse (e.g. COMMIT without BEGIN) */
    TS_POISONED  = -9,  /* an earlier write failed; close and reopen the database */
    TS_NOTADB    = -10  /* the file is not a Tessera database */
};

#define TS_MAX_KEY_SIZE    1000u               /* bytes */
#define TS_MAX_VALUE_SIZE  (64u * 1024u * 1024u) /* bytes */

/* A short, static, human-readable description of a result code. */
const char *ts_errstr(int rc);

/* ------------------------------------------------------------------------
 * Opening and closing
 * ---------------------------------------------------------------------- */
typedef struct ts_db ts_db;
typedef struct ts_vfs ts_vfs;

typedef enum {
    /* fsync the log on every commit: a committed transaction survives power loss. */
    TS_SYNC_FULL = 0,
    /* fsync only at checkpoints: a crash may lose the last few commits, but the
       database is still consistent (it rolls back to an earlier commit). */
    TS_SYNC_NORMAL = 1,
    /* never fsync: for tests and throw-away data. */
    TS_SYNC_OFF = 2
} ts_sync_mode;

typedef struct {
    size_t       cache_pages;      /* page cache size, in 4 KiB pages (default 2048 = 8 MiB) */
    uint32_t     checkpoint_pages; /* checkpoint when the WAL holds this many pages (default 2000; 0 = only on close) */
    ts_sync_mode sync;             /* default TS_SYNC_FULL */
    int          create;           /* create the database if it does not exist (default 1) */
    ts_vfs      *vfs;              /* file system to use; NULL = the operating system's */
} ts_options;

void ts_options_default(ts_options *opt);

/* Opens (and if needed creates) the database at `path`. The write-ahead log
   lives next to it, in `path` + "-wal". `opt` may be NULL for defaults. */
int ts_open(const char *path, const ts_options *opt, ts_db **out);

/* Checkpoints the log into the database file and closes the handle. Any open
   transaction is rolled back. Always frees the handle, even on error. */
int ts_close(ts_db *db);

/* ------------------------------------------------------------------------
 * Transactions
 *
 * Without ts_begin, every ts_put / ts_delete is its own transaction.
 * If an operation fails inside an explicit transaction, the transaction is
 * rolled back at once and further operations return TS_TXN until the caller
 * calls ts_rollback (or ts_commit, which then also reports TS_TXN).
 * ---------------------------------------------------------------------- */
int ts_begin(ts_db *db);
int ts_commit(ts_db *db);
int ts_rollback(ts_db *db);
int ts_in_transaction(const ts_db *db);

/* ------------------------------------------------------------------------
 * Data
 * ---------------------------------------------------------------------- */
int ts_put(ts_db *db, const void *key, size_t klen, const void *val, size_t vlen);

/* On TS_OK, *val is a malloc'd copy of the value (free it with ts_free). */
int ts_get(ts_db *db, const void *key, size_t klen, void **val, size_t *vlen);

/* TS_NOTFOUND if the key did not exist. */
int ts_delete(ts_db *db, const void *key, size_t klen);

void ts_free(void *p);

/* ------------------------------------------------------------------------
 * Ordered iteration over [lo, hi). A NULL bound means unbounded.
 * An iterator stays valid across writes to the database: it continues with
 * the first key after the last one it returned.
 * ---------------------------------------------------------------------- */
typedef struct ts_iter ts_iter;

int ts_iter_open(ts_db *db, const void *lo, size_t lolen, const void *hi, size_t hilen, ts_iter **out);
/* TS_OK when positioned on the next entry, TS_DONE at the end, < 0 on error. */
int ts_iter_next(ts_iter *it);
const void *ts_iter_key(const ts_iter *it, size_t *len);
/* Loads the value on first use; NULL with *rc set on error. */
const void *ts_iter_value(ts_iter *it, size_t *len, int *rc);
void ts_iter_close(ts_iter *it);

/* ------------------------------------------------------------------------
 * Maintenance and introspection
 * ---------------------------------------------------------------------- */

/* Copies the log into the database file and empties the log. */
int ts_checkpoint(ts_db *db);

typedef struct {
    uint64_t keys;           /* number of keys */
    uint64_t txn_id;         /* id of the last committed transaction */
    uint32_t page_size;
    uint32_t pages;          /* pages in the database, including free ones */
    uint32_t free_pages;
    uint32_t tree_height;    /* 1 = the root is a leaf */
    uint32_t wal_frames;     /* pages waiting in the log */
    uint64_t cache_hits;
    uint64_t cache_misses;
    uint64_t checkpoints;
    uint64_t commits;
} ts_stats;

int ts_get_stats(ts_db *db, ts_stats *out);

/* Walks the whole file and verifies every structural invariant: page
   checksums, key order, separator bounds, equal leaf depth, sibling links,
   overflow chains, the free list, and that every page is used exactly once.
   Each problem is passed to `report` (may be NULL). *problems receives the
   count. Returns TS_OK if the check ran (look at *problems for the verdict). */
int ts_check(ts_db *db, void (*report)(void *ctx, const char *msg), void *ctx, uint64_t *problems);

/* ------------------------------------------------------------------------
 * In-memory file system, with power-failure simulation (used by the tests;
 * also handy for a purely in-memory database).
 * ---------------------------------------------------------------------- */
ts_vfs *ts_memvfs_create(void);
void    ts_memvfs_destroy(ts_vfs *vfs);

#ifdef __cplusplus
}
#endif

#endif /* TESSERA_H */
