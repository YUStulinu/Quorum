/*
 * CRC-32C (Castagnoli), the checksum used by iSCSI, ext4, Btrfs and many
 * databases. Software "slicing-by-8": eight lookup tables let the loop
 * consume 8 bytes per iteration instead of 1, which matters because every
 * page read and every log frame goes through here.
 *
 * ts_crc32c(crc, ...) continues a running checksum, so
 *   ts_crc32c(ts_crc32c(0, a, n), b, m) == ts_crc32c(0, a||b, n+m).
 */
#include "util/common.h"

#define CRC32C_POLY 0x82F63B78u /* reflected 0x1EDC6F41 */

static uint32_t table[8][256];
static int table_ready = 0;

static void build_tables(void) {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++) c = (c & 1) ? (c >> 1) ^ CRC32C_POLY : c >> 1;
        table[0][i] = c;
    }
    for (uint32_t i = 0; i < 256; i++) {
        for (int t = 1; t < 8; t++) {
            table[t][i] = (table[t - 1][i] >> 8) ^ table[0][table[t - 1][i] & 0xFF];
        }
    }
    table_ready = 1;
}

uint32_t ts_crc32c(uint32_t crc, const void *data, size_t len) {
    /* Building the tables is idempotent, so a race between two threads that
       both see table_ready == 0 only does the same work twice. */
    if (!table_ready) build_tables();

    const uint8_t *p = (const uint8_t *)data;
    crc = ~crc;
    while (len >= 8) {
        uint32_t lo = crc ^ get_u32(p);
        uint32_t hi = get_u32(p + 4);
        crc = table[7][lo & 0xFF] ^ table[6][(lo >> 8) & 0xFF] ^
              table[5][(lo >> 16) & 0xFF] ^ table[4][lo >> 24] ^
              table[3][hi & 0xFF] ^ table[2][(hi >> 8) & 0xFF] ^
              table[1][(hi >> 16) & 0xFF] ^ table[0][hi >> 24];
        p += 8;
        len -= 8;
    }
    while (len--) crc = (crc >> 8) ^ table[0][(crc ^ *p++) & 0xFF];
    return ~crc;
}
