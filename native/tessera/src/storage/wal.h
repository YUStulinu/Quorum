/*
 * Write-ahead log.
 *
 * A commit appends the new image of every page the transaction changed to
 * the log, the last one marked as the commit frame, and fsyncs the log. The
 * database file itself is not touched: it is brought up to date later, in
 * bulk, by a checkpoint. Readers find the newest committed copy of a page
 * through an in-memory index (page number -> frame offset).
 *
 * File format (all integers little-endian):
 *
 *   header, 32 bytes
 *     0  u32 magic 0x57535354 ("TSSW")
 *     4  u32 format version (1)
 *     8  u32 page size
 *    12  u32 checkpoint sequence number
 *    16  u32 salt1        changes on every reset, so frames left over from
 *    20  u32 salt2        an earlier generation of the log can never validate
 *    24  u32 reserved (0)
 *    28  u32 CRC32C of bytes 0..27
 *
 *   frames, 24 + page size bytes each
 *     0  u32 page number
 *     4  u32 commit marker: database size in pages if this frame ends a
 *            transaction, 0 otherwise
 *     8  u32 salt1, 12 u32 salt2   copied from the header
 *    16  u32 reserved (0)
 *    20  u32 checksum = CRC32C(previous checksum, bytes 0..19, page)
 *            The chain starts from the header checksum. A frame is valid only
 *            if its salts match and the whole chain up to it checks out, so a
 *            torn or stale frame ends the valid log, and every frame after it
 *            is ignored.
 *     24 page image
 */
#ifndef TS_WAL_H
#define TS_WAL_H

#include <stddef.h>
#include <stdint.h>

#include "os/vfs.h"
#include "util/common.h"

#define WAL_HEADER_SIZE 32u
#define WAL_FRAME_HEADER 24u
#define WAL_FRAME_SIZE (WAL_FRAME_HEADER + TS_PAGE_SIZE)

typedef struct ts_wal ts_wal;

int  wal_open(ts_vfs *vfs, const char *path, ts_wal **out);
void wal_close(ts_wal *w);

/* Scans the log, keeps every fully committed transaction and drops the rest
   (truncating the file after the last commit). *db_pages receives the database
   size recorded by the last commit, or 0 if the log holds no commit. */
int wal_recover(ts_wal *w, uint32_t *db_pages);

/* Offset of the newest committed copy of `pgno`; returns 1 if there is one. */
int wal_find(const ts_wal *w, uint32_t pgno, uint64_t *frame_off);
int wal_read_page(ts_wal *w, uint64_t frame_off, uint8_t *page);

/* Appends one transaction: pages[i] is the image of page pgnos[i]. The last
   page becomes the commit frame. Fsyncs the log if `sync`. */
int wal_commit(ts_wal *w, uint8_t *const *pages, const uint32_t *pgnos, size_t n, uint32_t db_pages, int sync);

/* Copies the newest committed version of every page into `db`, makes it
   durable, then resets the log to empty. */
int wal_checkpoint(ts_wal *w, ts_file *db, int sync);

uint32_t wal_frame_count(const ts_wal *w);
uint64_t wal_checkpoint_count(const ts_wal *w);

#endif
