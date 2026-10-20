/*
 * Free-page list.
 *
 * Pages freed by deletes are reused before the file grows. The list is a
 * chain of "trunk" pages, each holding up to FREELIST_CAPACITY page numbers:
 *
 *    0  u8  type (4)
 *    4  u32 next trunk page (0 = end)
 *    8  u32 number of entries
 *   12  u32 entries[...]
 *
 * Pop takes the last entry of the head trunk; when the trunk is empty, the
 * trunk page itself is handed out. Push appends to the head trunk, or turns
 * the freed page into the new head trunk when the head is full. Both touch
 * only the head trunk, so they cost one page each.
 */
#include "storage/pager.h"

int freelist_pop(ts_pager *p, uint32_t *pgno) {
    ts_meta *m = pager_meta(p);
    *pgno = 0;
    if (m->freelist_head == 0) return TS_OK;

    ts_page *trunk = NULL;
    TS_TRY(pager_get(p, m->freelist_head, &trunk));
    uint8_t *d = trunk->data;
    uint32_t count = get_u32(d + 8);
    if (d[0] != FREELIST_TRUNK_TYPE || count > FREELIST_CAPACITY || m->freelist_count == 0) {
        pager_release(p, trunk);
        return TS_CORRUPT;
    }
    int rc = TS_OK;
    if (count > 0) {
        uint32_t page = get_u32(d + 12 + 4 * (count - 1));
        if (page == 0 || page >= m->page_count) {
            rc = TS_CORRUPT;
        } else {
            rc = pager_write(p, trunk);
            if (rc == TS_OK) {
                put_u32(d + 8, count - 1);
                *pgno = page;
            }
        }
    } else {
        *pgno = m->freelist_head;
        m->freelist_head = get_u32(d + 4);
    }
    pager_release(p, trunk);
    if (rc == TS_OK) m->freelist_count--;
    return rc;
}

int freelist_push(ts_pager *p, uint32_t pgno) {
    ts_meta *m = pager_meta(p);
    if (m->freelist_head != 0) {
        ts_page *trunk = NULL;
        TS_TRY(pager_get(p, m->freelist_head, &trunk));
        uint8_t *d = trunk->data;
        uint32_t count = get_u32(d + 8);
        if (d[0] != FREELIST_TRUNK_TYPE || count > FREELIST_CAPACITY) {
            pager_release(p, trunk);
            return TS_CORRUPT;
        }
        if (count < FREELIST_CAPACITY) {
            int rc = pager_write(p, trunk);
            if (rc == TS_OK) {
                put_u32(d + 12 + 4 * count, pgno);
                put_u32(d + 8, count + 1);
                m->freelist_count++;
            }
            pager_release(p, trunk);
            return rc;
        }
        pager_release(p, trunk);
    }

    /* No trunk, or the head trunk is full: the freed page becomes the new head. */
    ts_page *page = NULL;
    TS_TRY(pager_get(p, pgno, &page));
    int rc = pager_write(p, page);
    if (rc == TS_OK) {
        memset(page->data, 0, TS_PAGE_SIZE);
        page->data[0] = FREELIST_TRUNK_TYPE;
        put_u32(page->data + 4, m->freelist_head);
        put_u32(page->data + 8, 0);
        m->freelist_head = pgno;
        m->freelist_count++;
    }
    pager_release(p, page);
    return rc;
}
