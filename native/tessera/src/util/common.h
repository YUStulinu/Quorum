/*
 * Definitions shared by every part of the engine.
 *
 * All multi-byte integers on disk are little-endian and are read and written
 * byte by byte through the helpers below. That keeps the file format the same
 * on every CPU and avoids unaligned-access traps: a page buffer is just bytes.
 */
#ifndef TS_COMMON_H
#define TS_COMMON_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "tessera.h"

/* Page geometry. The last 4 bytes of every page hold a CRC32C of the rest. */
#define TS_PAGE_SIZE    4096u
#define TS_PAGE_USABLE  (TS_PAGE_SIZE - 4u)

#define TS_UNUSED(x) ((void)(x))

/* Lets GCC/Clang check printf-style calls against their format string. */
#if defined(__GNUC__) || defined(__clang__)
#define TS_PRINTF(fmt_index, first_arg) __attribute__((format(printf, fmt_index, first_arg)))
#else
#define TS_PRINTF(fmt_index, first_arg)
#endif

/* Propagate errors (negative codes) but let TS_OK / TS_NOTFOUND / TS_DONE through. */
#define TS_TRY(expr)              \
    do {                          \
        int ts_rc_ = (expr);      \
        if (ts_rc_ < 0) return ts_rc_; \
    } while (0)

static inline uint16_t get_u16(const uint8_t *p) {
    return (uint16_t)(p[0] | (p[1] << 8));
}
static inline uint32_t get_u32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static inline uint64_t get_u64(const uint8_t *p) {
    return (uint64_t)get_u32(p) | ((uint64_t)get_u32(p + 4) << 32);
}
static inline void put_u16(uint8_t *p, uint16_t v) {
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
}
static inline void put_u32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16);
    p[3] = (uint8_t)(v >> 24);
}
static inline void put_u64(uint8_t *p, uint64_t v) {
    put_u32(p, (uint32_t)v);
    put_u32(p + 4, (uint32_t)(v >> 32));
}

/* Lexicographic byte-string order; a proper prefix sorts first. */
static inline int ts_keycmp(const uint8_t *a, size_t alen, const uint8_t *b, size_t blen) {
    size_t n = alen < blen ? alen : blen;
    int c = n ? memcmp(a, b, n) : 0;
    if (c != 0) return c;
    return (alen > blen) - (alen < blen);
}

/* SplitMix64: a tiny, high-quality PRNG for salts, tests and workloads. */
static inline uint64_t ts_splitmix64(uint64_t *state) {
    uint64_t z = (*state += 0x9E3779B97F4A7C15ull);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    return z ^ (z >> 31);
}

uint32_t ts_crc32c(uint32_t crc, const void *data, size_t len);

/* Page checksums cover bytes [0, TS_PAGE_USABLE) and live in the last 4 bytes. */
static inline void ts_page_seal(uint8_t *page) {
    put_u32(page + TS_PAGE_USABLE, ts_crc32c(0, page, TS_PAGE_USABLE));
}
static inline bool ts_page_verify(const uint8_t *page) {
    return get_u32(page + TS_PAGE_USABLE) == ts_crc32c(0, page, TS_PAGE_USABLE);
}

char *ts_strdup(const char *s);

#endif /* TS_COMMON_H */
