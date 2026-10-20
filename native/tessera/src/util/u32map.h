/*
 * A hash map from 32-bit keys (page numbers) to 64-bit values (pointers or
 * file offsets). Open addressing with linear probing and backward-shift
 * deletion: no tombstones, so lookups never slow down after many deletes.
 * The page cache and the WAL index both use it.
 */
#ifndef TS_U32MAP_H
#define TS_U32MAP_H

#include <stddef.h>
#include <stdint.h>

typedef struct {
    uint32_t *keys;   /* stored as key + 1; 0 marks an empty slot */
    uint64_t *vals;
    size_t    cap;    /* power of two */
    size_t    count;
} u32map;

int  u32map_init(u32map *m, size_t initial_cap);
void u32map_free(u32map *m);
void u32map_clear(u32map *m);

/* Returns 1 and sets *val if found, 0 otherwise. */
int u32map_get(const u32map *m, uint32_t key, uint64_t *val);
/* Inserts or replaces. TS_OK or TS_NOMEM. */
int u32map_put(u32map *m, uint32_t key, uint64_t val);
/* Returns 1 if the key was present. */
int u32map_del(u32map *m, uint32_t key);

/* Iteration: for (size_t i = 0; i < m->cap; i++) if (m->keys[i]) ... key = keys[i] - 1 */

#endif
