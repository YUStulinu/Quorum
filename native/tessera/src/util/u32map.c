#include "util/u32map.h"

#include <stdlib.h>
#include <string.h>

#include "util/common.h"

static size_t slot_of(uint32_t key, size_t cap) {
    /* Fibonacci hashing spreads sequential page numbers across the table. */
    uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ull;
    return (size_t)(h >> 32) & (cap - 1);
}

int u32map_init(u32map *m, size_t initial_cap) {
    size_t cap = 16;
    while (cap < initial_cap) cap <<= 1;
    m->keys = (uint32_t *)calloc(cap, sizeof(uint32_t));
    m->vals = (uint64_t *)malloc(cap * sizeof(uint64_t));
    if (!m->keys || !m->vals) {
        free(m->keys);
        free(m->vals);
        m->keys = NULL;
        m->vals = NULL;
        return TS_NOMEM;
    }
    m->cap = cap;
    m->count = 0;
    return TS_OK;
}

void u32map_free(u32map *m) {
    free(m->keys);
    free(m->vals);
    m->keys = NULL;
    m->vals = NULL;
    m->cap = m->count = 0;
}

void u32map_clear(u32map *m) {
    memset(m->keys, 0, m->cap * sizeof(uint32_t));
    m->count = 0;
}

int u32map_get(const u32map *m, uint32_t key, uint64_t *val) {
    uint32_t stored = key + 1;
    size_t i = slot_of(key, m->cap);
    for (;;) {
        uint32_t k = m->keys[i];
        if (k == 0) return 0;
        if (k == stored) {
            *val = m->vals[i];
            return 1;
        }
        i = (i + 1) & (m->cap - 1);
    }
}

static int grow(u32map *m) {
    u32map bigger;
    if (u32map_init(&bigger, m->cap * 2) != TS_OK) return TS_NOMEM;
    for (size_t i = 0; i < m->cap; i++) {
        if (m->keys[i]) {
            size_t j = slot_of(m->keys[i] - 1, bigger.cap);
            while (bigger.keys[j]) j = (j + 1) & (bigger.cap - 1);
            bigger.keys[j] = m->keys[i];
            bigger.vals[j] = m->vals[i];
        }
    }
    bigger.count = m->count;
    u32map_free(m);
    *m = bigger;
    return TS_OK;
}

int u32map_put(u32map *m, uint32_t key, uint64_t val) {
    /* Keep the load factor under 1/2: linear probing degrades quickly above it. */
    if ((m->count + 1) * 2 > m->cap) TS_TRY(grow(m));
    uint32_t stored = key + 1;
    size_t i = slot_of(key, m->cap);
    while (m->keys[i] && m->keys[i] != stored) i = (i + 1) & (m->cap - 1);
    if (!m->keys[i]) {
        m->keys[i] = stored;
        m->count++;
    }
    m->vals[i] = val;
    return TS_OK;
}

int u32map_del(u32map *m, uint32_t key) {
    uint32_t stored = key + 1;
    size_t mask = m->cap - 1;
    size_t i = slot_of(key, m->cap);
    while (m->keys[i] != stored) {
        if (m->keys[i] == 0) return 0;
        i = (i + 1) & mask;
    }
    /* Backward-shift: move later entries of the probe run into the hole, so
       every remaining key stays reachable from its home slot. */
    size_t hole = i;
    size_t j = i;
    for (;;) {
        j = (j + 1) & mask;
        if (m->keys[j] == 0) break;
        size_t home = slot_of(m->keys[j] - 1, m->cap);
        /* Can entry j move to the hole? Only if its home is not inside (hole, j]. */
        int home_in_range = (hole <= j) ? (home > hole && home <= j) : (home > hole || home <= j);
        if (!home_in_range) {
            m->keys[hole] = m->keys[j];
            m->vals[hole] = m->vals[j];
            hole = j;
        }
    }
    m->keys[hole] = 0;
    m->count--;
    return 1;
}
