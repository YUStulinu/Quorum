/*
 * Power-failure simulation for the in-memory file system.
 *
 * Every file keeps two images: what is durable (as of the last sync) and
 * the current contents, plus the list of writes made since the last sync.
 * A simulated crash rebuilds each file from its durable image and then
 * replays the unsynced writes according to the chosen mode — none of them,
 * all of them, or a random subset with some writes torn at 512-byte sector
 * boundaries — which is what a real disk with a volatile cache may do.
 */
#ifndef TS_VFS_MEM_H
#define TS_VFS_MEM_H

#include <stdint.h>

#include "tessera.h"

typedef enum {
    TS_CRASH_LOSE_UNSYNCED, /* only synced data survives */
    TS_CRASH_KEEP_ALL,      /* everything survives: like a killed process */
    TS_CRASH_RANDOM         /* random subset of unsynced writes, some torn */
} ts_crash_mode;

/* Applies the crash to every file. Handles opened before the crash must not be used afterwards. */
void ts_memvfs_crash(ts_vfs *vfs, ts_crash_mode mode, uint64_t seed);

/* After `ops` more successful writes/syncs/truncates, every further one fails
   with TS_IOERR, as if the device disappeared. -1 disables the limit. */
void ts_memvfs_set_io_budget(ts_vfs *vfs, int64_t ops);

/* Fails only the next sync (fsync) call with TS_IOERR. */
void ts_memvfs_fail_next_sync(ts_vfs *vfs);

/* Direct access to a file's current bytes, for corruption tests. NULL if absent. */
uint8_t *ts_memvfs_file_data(ts_vfs *vfs, const char *path, uint64_t *size);

#endif
