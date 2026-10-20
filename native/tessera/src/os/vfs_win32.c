/*
 * VFS for Windows.
 *
 *  - Paths are UTF-8 and converted to UTF-16 for the wide-character API, so
 *    non-ASCII folder names (e.g. with Romanian diacritics) work.
 *  - Reads and writes pass the offset in an OVERLAPPED structure: the
 *    equivalent of pread/pwrite, with no shared file pointer to race on.
 *  - FlushFileBuffers is the fsync: it also asks the drive to flush its cache.
 *  - The single-writer lock is a byte-range lock far beyond the end of the
 *    file. Windows locks are mandatory, so locking real data would block
 *    reads from other tools; a lock on an unused offset only blocks other
 *    writers that try to take the same lock.
 */
#if defined(_WIN32)

#include <stdlib.h>
#include <windows.h>

#include "os/vfs.h"
#include "util/common.h"

#define LOCK_OFFSET_HIGH 0x7FFFFFFFu /* lock byte at ~2^62: never real data */

typedef struct {
    ts_file base;
    HANDLE  h;
    int     locked;
} win_file;

static wchar_t *to_wide(const char *utf8) {
    int n = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, utf8, -1, NULL, 0);
    if (n <= 0) return NULL;
    wchar_t *w = (wchar_t *)malloc((size_t)n * sizeof(wchar_t));
    if (!w) return NULL;
    if (MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, utf8, -1, w, n) <= 0) {
        free(w);
        return NULL;
    }
    return w;
}

static OVERLAPPED at_offset(uint64_t off) {
    OVERLAPPED ov;
    memset(&ov, 0, sizeof ov);
    ov.Offset = (DWORD)(off & 0xFFFFFFFFu);
    ov.OffsetHigh = (DWORD)(off >> 32);
    return ov;
}

static int w_close(ts_file *f) {
    win_file *wf = (win_file *)f;
    if (wf->locked) {
        OVERLAPPED ov = at_offset((uint64_t)LOCK_OFFSET_HIGH << 32);
        UnlockFileEx(wf->h, 0, 1, 0, &ov);
    }
    int rc = CloseHandle(wf->h) ? TS_OK : TS_IOERR;
    free(wf);
    return rc;
}

static int w_read(ts_file *f, void *buf, size_t n, uint64_t off, size_t *got) {
    win_file *wf = (win_file *)f;
    size_t done = 0;
    while (done < n) {
        size_t chunk = n - done;
        DWORD want = chunk > 0x40000000u ? 0x40000000u : (DWORD)chunk;
        DWORD read = 0;
        OVERLAPPED ov = at_offset(off + done);
        if (!ReadFile(wf->h, (char *)buf + done, want, &read, &ov)) {
            if (GetLastError() == ERROR_HANDLE_EOF) break;
            return TS_IOERR;
        }
        if (read == 0) break;
        done += read;
    }
    *got = done;
    return TS_OK;
}

static int w_write(ts_file *f, const void *buf, size_t n, uint64_t off) {
    win_file *wf = (win_file *)f;
    size_t done = 0;
    while (done < n) {
        size_t chunk = n - done;
        DWORD want = chunk > 0x40000000u ? 0x40000000u : (DWORD)chunk;
        DWORD written = 0;
        OVERLAPPED ov = at_offset(off + done);
        if (!WriteFile(wf->h, (const char *)buf + done, want, &written, &ov) || written == 0) return TS_IOERR;
        done += written;
    }
    return TS_OK;
}

static int w_sync(ts_file *f) {
    return FlushFileBuffers(((win_file *)f)->h) ? TS_OK : TS_IOERR;
}

static int w_truncate(ts_file *f, uint64_t size) {
    win_file *wf = (win_file *)f;
    FILE_END_OF_FILE_INFO info;
    info.EndOfFile.QuadPart = (LONGLONG)size;
    return SetFileInformationByHandle(wf->h, FileEndOfFileInfo, &info, sizeof info) ? TS_OK : TS_IOERR;
}

static int w_size(ts_file *f, uint64_t *out) {
    LARGE_INTEGER sz;
    if (!GetFileSizeEx(((win_file *)f)->h, &sz)) return TS_IOERR;
    *out = (uint64_t)sz.QuadPart;
    return TS_OK;
}

static int w_lock(ts_file *f) {
    win_file *wf = (win_file *)f;
    OVERLAPPED ov = at_offset((uint64_t)LOCK_OFFSET_HIGH << 32);
    if (LockFileEx(wf->h, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &ov)) {
        wf->locked = 1;
        return TS_OK;
    }
    DWORD err = GetLastError();
    return (err == ERROR_LOCK_VIOLATION || err == ERROR_IO_PENDING) ? TS_BUSY : TS_IOERR;
}

static int w_open(ts_vfs *vfs, const char *path, int create, ts_file **out) {
    wchar_t *wpath = to_wide(path);
    if (!wpath) return TS_INVALID;
    HANDLE h = CreateFileW(wpath, GENERIC_READ | GENERIC_WRITE,
                           FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
                           create ? OPEN_ALWAYS : OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    free(wpath);
    if (h == INVALID_HANDLE_VALUE) {
        DWORD err = GetLastError();
        return (err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND) ? TS_NOTFOUND : TS_IOERR;
    }
    win_file *wf = (win_file *)calloc(1, sizeof *wf);
    if (!wf) {
        CloseHandle(h);
        return TS_NOMEM;
    }
    wf->base.vfs = vfs;
    wf->h = h;
    *out = &wf->base;
    return TS_OK;
}

static int w_exists(ts_vfs *vfs, const char *path, int *out) {
    TS_UNUSED(vfs);
    wchar_t *wpath = to_wide(path);
    if (!wpath) return TS_INVALID;
    *out = GetFileAttributesW(wpath) != INVALID_FILE_ATTRIBUTES;
    free(wpath);
    return TS_OK;
}

static int w_remove(ts_vfs *vfs, const char *path) {
    TS_UNUSED(vfs);
    wchar_t *wpath = to_wide(path);
    if (!wpath) return TS_INVALID;
    BOOL ok = DeleteFileW(wpath);
    DWORD err = ok ? 0 : GetLastError();
    free(wpath);
    return (ok || err == ERROR_FILE_NOT_FOUND) ? TS_OK : TS_IOERR;
}

static int w_sync_dir(ts_vfs *vfs, const char *path) {
    /* NTFS journals directory changes itself; there is no directory fsync. */
    TS_UNUSED(vfs);
    TS_UNUSED(path);
    return TS_OK;
}

static ts_vfs win32_vfs = {
    "win32", w_open, w_exists, w_remove, w_sync_dir,
    w_close, w_read, w_write, w_sync, w_truncate, w_size, w_lock,
};

ts_vfs *ts_vfs_os(void) {
    return &win32_vfs;
}

#else
typedef int ts_vfs_win32_unused;
#endif
