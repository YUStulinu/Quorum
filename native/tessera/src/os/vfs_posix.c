/*
 * VFS for Linux, macOS and other POSIX systems.
 *
 * Durability notes, because this is where databases usually get it wrong:
 *  - fsync is what makes data durable; write() only hands it to the OS cache.
 *  - On macOS, fsync() does not flush the drive's own cache. F_FULLFSYNC does.
 *  - Creating a file is a change to its directory: the directory must be
 *    fsynced too, or the new file can vanish after a power cut.
 *  - If fsync fails, the kernel may already have dropped the dirty pages, so
 *    retrying it can report success for data that is gone. The engine never
 *    retries: a failed sync poisons the handle (see pager.c).
 */
#if !defined(_WIN32)

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include "os/vfs.h"
#include "util/common.h"

typedef struct {
    ts_file base;
    int     fd;
} posix_file;

static int p_close(ts_file *f) {
    posix_file *pf = (posix_file *)f;
    int rc = close(pf->fd) == 0 ? TS_OK : TS_IOERR;
    free(pf);
    return rc;
}

static int p_read(ts_file *f, void *buf, size_t n, uint64_t off, size_t *got) {
    posix_file *pf = (posix_file *)f;
    size_t done = 0;
    while (done < n) {
        ssize_t r = pread(pf->fd, (char *)buf + done, n - done, (off_t)(off + done));
        if (r < 0) {
            if (errno == EINTR) continue;
            return TS_IOERR;
        }
        if (r == 0) break; /* end of file */
        done += (size_t)r;
    }
    *got = done;
    return TS_OK;
}

static int p_write(ts_file *f, const void *buf, size_t n, uint64_t off) {
    posix_file *pf = (posix_file *)f;
    size_t done = 0;
    while (done < n) {
        ssize_t r = pwrite(pf->fd, (const char *)buf + done, n - done, (off_t)(off + done));
        if (r < 0) {
            if (errno == EINTR) continue;
            return TS_IOERR;
        }
        done += (size_t)r;
    }
    return TS_OK;
}

static int p_sync(ts_file *f) {
    posix_file *pf = (posix_file *)f;
#if defined(__APPLE__)
    if (fcntl(pf->fd, F_FULLFSYNC) == 0) return TS_OK;
    /* Some file systems (e.g. network shares) do not support F_FULLFSYNC. */
    return fsync(pf->fd) == 0 ? TS_OK : TS_IOERR;
#elif defined(__linux__)
    /* fdatasync skips metadata that is not needed to read the data back
       (like mtime), but does flush a changed file size. */
    return fdatasync(pf->fd) == 0 ? TS_OK : TS_IOERR;
#else
    return fsync(pf->fd) == 0 ? TS_OK : TS_IOERR;
#endif
}

static int p_truncate(ts_file *f, uint64_t size) {
    posix_file *pf = (posix_file *)f;
    while (ftruncate(pf->fd, (off_t)size) != 0) {
        if (errno != EINTR) return TS_IOERR;
    }
    return TS_OK;
}

static int p_size(ts_file *f, uint64_t *out) {
    posix_file *pf = (posix_file *)f;
    struct stat st;
    if (fstat(pf->fd, &st) != 0) return TS_IOERR;
    *out = (uint64_t)st.st_size;
    return TS_OK;
}

static int p_lock(ts_file *f) {
    posix_file *pf = (posix_file *)f;
    /* flock locks belong to the open file description, so a second open() of
       the same file - even in this process - conflicts, which is what we want. */
    if (flock(pf->fd, LOCK_EX | LOCK_NB) == 0) return TS_OK;
    return errno == EWOULDBLOCK ? TS_BUSY : TS_IOERR;
}

static int p_open(ts_vfs *vfs, const char *path, int create, ts_file **out) {
    int flags = O_RDWR;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    if (create) flags |= O_CREAT;
    int fd;
    do {
        fd = open(path, flags, 0644);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) return errno == ENOENT ? TS_NOTFOUND : TS_IOERR;

    posix_file *pf = (posix_file *)calloc(1, sizeof *pf);
    if (!pf) {
        close(fd);
        return TS_NOMEM;
    }
    pf->base.vfs = vfs;
    pf->fd = fd;
    *out = &pf->base;
    return TS_OK;
}

static int p_exists(ts_vfs *vfs, const char *path, int *out) {
    TS_UNUSED(vfs);
    *out = access(path, F_OK) == 0;
    return TS_OK;
}

static int p_remove(ts_vfs *vfs, const char *path) {
    TS_UNUSED(vfs);
    if (unlink(path) == 0 || errno == ENOENT) return TS_OK;
    return TS_IOERR;
}

static int p_sync_dir(ts_vfs *vfs, const char *path) {
    TS_UNUSED(vfs);
    char *dir = ts_strdup(path);
    if (!dir) return TS_NOMEM;
    char *slash = strrchr(dir, '/');
    if (slash == dir) slash[1] = '\0';         /* "/file" -> "/" */
    else if (slash) *slash = '\0';             /* "a/b/file" -> "a/b" */
    else strcpy(dir, ".");                     /* "file" -> "." */

    int fd = open(dir, O_RDONLY);
    free(dir);
    if (fd < 0) return TS_IOERR;
    int rc = fsync(fd) == 0 ? TS_OK : TS_IOERR;
    /* Some file systems refuse fsync on directories; that is not an error we can act on. */
    if (rc != TS_OK && (errno == EINVAL || errno == EBADF)) rc = TS_OK;
    close(fd);
    return rc;
}

static ts_vfs posix_vfs = {
    "posix", p_open, p_exists, p_remove, p_sync_dir,
    p_close, p_read, p_write, p_sync, p_truncate, p_size, p_lock,
};

ts_vfs *ts_vfs_os(void) {
    return &posix_vfs;
}

#else
/* ISO C forbids an empty translation unit. */
typedef int ts_vfs_posix_unused;
#endif
