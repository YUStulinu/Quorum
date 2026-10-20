/*
 * VFS: the only way the engine touches storage.
 *
 * Everything above this layer reads and writes byte ranges at explicit
 * offsets and calls sync() when it needs data to be durable. Three
 * implementations exist:
 *   - vfs_posix.c  Linux / macOS (pread, pwrite, fdatasync / F_FULLFSYNC, flock)
 *   - vfs_win32.c  Windows (ReadFile/WriteFile with offsets, FlushFileBuffers, LockFileEx)
 *   - vfs_mem.c    an in-memory disk that remembers which writes were synced and
 *                  can simulate a power cut, dropping or tearing the rest.
 */
#ifndef TS_VFS_H
#define TS_VFS_H

#include <stddef.h>
#include <stdint.h>

#include "tessera.h"

typedef struct ts_file ts_file;

struct ts_vfs {
    const char *name;
    int (*open)(ts_vfs *vfs, const char *path, int create, ts_file **out);
    int (*exists)(ts_vfs *vfs, const char *path, int *out);
    int (*remove)(ts_vfs *vfs, const char *path);
    /* Makes the existence of `path` durable (fsync of the parent directory). */
    int (*sync_dir)(ts_vfs *vfs, const char *path);

    int (*close)(ts_file *f);
    /* Reads up to n bytes; *got < n only at end of file. */
    int (*read)(ts_file *f, void *buf, size_t n, uint64_t off, size_t *got);
    int (*write)(ts_file *f, const void *buf, size_t n, uint64_t off);
    int (*sync)(ts_file *f);
    int (*truncate)(ts_file *f, uint64_t size);
    int (*size)(ts_file *f, uint64_t *out);
    /* Exclusive lock for the lifetime of the handle; TS_BUSY if already held. */
    int (*lock)(ts_file *f);
};

/* Every implementation's file object starts with this. */
struct ts_file {
    ts_vfs *vfs;
};

/* The platform's real file system. */
ts_vfs *ts_vfs_os(void);

/* Convenience wrappers. */
static inline int vfs_read(ts_file *f, void *buf, size_t n, uint64_t off, size_t *got) {
    return f->vfs->read(f, buf, n, off, got);
}
static inline int vfs_write(ts_file *f, const void *buf, size_t n, uint64_t off) {
    return f->vfs->write(f, buf, n, off);
}
static inline int vfs_sync(ts_file *f) { return f->vfs->sync(f); }
static inline int vfs_truncate(ts_file *f, uint64_t size) { return f->vfs->truncate(f, size); }
static inline int vfs_size(ts_file *f, uint64_t *out) { return f->vfs->size(f, out); }
static inline int vfs_close(ts_file *f) { return f->vfs->close(f); }

/* Reads exactly n bytes or fails with TS_CORRUPT on a short read. */
static inline int vfs_read_exact(ts_file *f, void *buf, size_t n, uint64_t off) {
    size_t got = 0;
    int rc = vfs_read(f, buf, n, off, &got);
    if (rc < 0) return rc;
    return got == n ? TS_OK : TS_CORRUPT;
}

#endif
