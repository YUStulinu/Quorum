#include "query/exec.h"

#include <errno.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "util/common.h"

static void out(ts_writer *w, const char *s) {
    w->write(w->ctx, s, strlen(s));
}

TS_PRINTF(2, 3) static void outf(ts_writer *w, const char *fmt, ...) {
    char buf[512];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    if (n > 0) w->write(w->ctx, buf, (size_t)n < sizeof buf ? (size_t)n : sizeof buf - 1);
}

/* Length of a valid UTF-8 sequence starting at s (2..4), or 0. */
static size_t utf8_seq(const unsigned char *s, size_t n) {
    size_t len;
    uint32_t cp;
    if (s[0] >= 0xC2 && s[0] <= 0xDF) {
        len = 2;
        cp = s[0] & 0x1Fu;
    } else if (s[0] >= 0xE0 && s[0] <= 0xEF) {
        len = 3;
        cp = s[0] & 0x0Fu;
    } else if (s[0] >= 0xF0 && s[0] <= 0xF4) {
        len = 4;
        cp = s[0] & 0x07u;
    } else {
        return 0;
    }
    if (len > n) return 0;
    for (size_t i = 1; i < len; i++) {
        if ((s[i] & 0xC0) != 0x80) return 0;
        cp = (cp << 6) | (s[i] & 0x3Fu);
    }
    if ((len == 3 && cp < 0x800) || (len == 4 && (cp < 0x10000 || cp > 0x10FFFF)) || (cp >= 0xD800 && cp <= 0xDFFF))
        return 0; /* overlong encodings and surrogates */
    return len;
}

/* A literal can be written bare when the lexer would read it back as the
   same single word and it does not look like a keyword or a comment. */
static int bare_ok(const unsigned char *d, size_t n) {
    if (n == 0) return 0;
    if (n >= 2 && d[0] == '-' && d[1] == '-') return 0;
    for (size_t i = 0; i < n;) {
        unsigned char c = d[i];
        if (c >= 0x80) {
            size_t k = utf8_seq(d + i, n - i);
            if (!k) return 0;
            i += k;
            continue;
        }
        if (c <= ' ' || c == ';' || c == '"' || c == '\'' || c == '#' || c == 127) return 0;
        i++;
    }
    return 1;
}

void exec_write_literal(ts_writer *w, const unsigned char *d, size_t n) {
    if (bare_ok(d, n)) {
        w->write(w->ctx, (const char *)d, n);
        return;
    }
    out(w, "\"");
    for (size_t i = 0; i < n;) {
        unsigned char c = d[i];
        char esc[8];
        if (c >= 0x80) {
            size_t k = utf8_seq(d + i, n - i);
            if (k) {
                w->write(w->ctx, (const char *)d + i, k);
                i += k;
                continue;
            }
        }
        switch (c) {
            case '"':  out(w, "\\\""); break;
            case '\\': out(w, "\\\\"); break;
            case '\n': out(w, "\\n"); break;
            case '\r': out(w, "\\r"); break;
            case '\t': out(w, "\\t"); break;
            default:
                if (c >= 0x20 && c < 0x7F) {
                    w->write(w->ctx, (const char *)&d[i], 1);
                } else {
                    snprintf(esc, sizeof esc, "\\x%02x", c);
                    out(w, esc);
                }
        }
        i++;
    }
    out(w, "\"");
}

static int report(ts_writer *w, int rc) {
    outf(w, "error: %s\n", ts_errstr(rc));
    return rc;
}

/* The key range of a statement: [lo, hi), either bound possibly absent.
   PREFIX p becomes [p, successor(p)), where the successor is p with its last
   non-0xFF byte incremented (and anything after it dropped). */
typedef struct {
    const uint8_t *lo, *hi;
    size_t lolen, hilen;
    uint8_t *owned;
} key_range;

static int make_range(const stmt *s, key_range *r) {
    memset(r, 0, sizeof *r);
    if (s->has_prefix) {
        r->lo = s->prefix.data;
        r->lolen = s->prefix.len;
        size_t n = s->prefix.len;
        while (n > 0 && s->prefix.data[n - 1] == 0xFF) n--;
        if (n > 0) {
            r->owned = (uint8_t *)malloc(n);
            if (!r->owned) return TS_NOMEM;
            memcpy(r->owned, s->prefix.data, n);
            r->owned[n - 1]++;
            r->hi = r->owned;
            r->hilen = n;
        }
        return TS_OK;
    }
    if (s->has_from) {
        r->lo = s->from.data;
        r->lolen = s->from.len;
    }
    if (s->has_to) {
        r->hi = s->to.data;
        r->hilen = s->to.len;
    }
    return TS_OK;
}

static int do_scan(ts_db *db, const stmt *s, ts_writer *w, int count_only) {
    key_range r;
    TS_TRY(make_range(s, &r));

    if (count_only && !r.lo && !r.hi) {
        ts_stats st;
        int rc = ts_get_stats(db, &st);
        free(r.owned);
        if (rc != TS_OK) return report(w, rc);
        outf(w, "%" PRIu64 "\n", st.keys);
        return TS_OK;
    }

    ts_iter *it = NULL;
    int rc = ts_iter_open(db, r.lo, r.lolen, r.hi, r.hilen, &it);
    if (rc != TS_OK) {
        free(r.owned);
        return report(w, rc);
    }
    uint64_t limit = s->has_limit ? s->limit : (count_only ? UINT64_MAX : EXEC_DEFAULT_SCAN_LIMIT);
    uint64_t n = 0;
    int more = 0;
    while ((rc = ts_iter_next(it)) == TS_OK) {
        if (n == limit) {
            more = 1;
            break;
        }
        n++;
        if (count_only) continue;
        size_t klen = 0;
        const unsigned char *k = (const unsigned char *)ts_iter_key(it, &klen);
        exec_write_literal(w, k, klen);
        if (!s->keys_only) {
            size_t vlen = 0;
            int vrc = TS_OK;
            const unsigned char *v = (const unsigned char *)ts_iter_value(it, &vlen, &vrc);
            if (vrc != TS_OK) {
                rc = vrc;
                out(w, "\n");
                break;
            }
            out(w, " = ");
            exec_write_literal(w, v, vlen);
        }
        out(w, "\n");
    }
    ts_iter_close(it);
    free(r.owned);
    if (rc < 0) return report(w, rc);
    if (count_only) {
        outf(w, "%" PRIu64 "\n", n);
    } else if (more) {
        outf(w, "(%" PRIu64 " shown; more follow, use LIMIT to see more)\n", n);
    } else {
        outf(w, "(%" PRIu64 " %s)\n", n, n == 1 ? "entry" : "entries");
    }
    return TS_OK;
}

static int parse_stored_int(const uint8_t *v, size_t n, int64_t *out) {
    if (n == 0 || n > 20) return 0;
    char buf[24];
    memcpy(buf, v, n);
    buf[n] = '\0';
    char *end = NULL;
    errno = 0;
    long long x = strtoll(buf, &end, 10);
    if (errno == ERANGE || *end != '\0' || (buf[0] != '-' && (buf[0] < '0' || buf[0] > '9'))) return 0;
    *out = (int64_t)x;
    return 1;
}

static int do_incr(ts_db *db, const stmt *s, ts_writer *w) {
    /* Read-modify-write must be atomic: run it in a transaction of its own
       unless the caller already opened one. */
    int own = !ts_in_transaction(db);
    if (own) {
        int rc = ts_begin(db);
        if (rc != TS_OK) return report(w, rc);
    }
    void *v = NULL;
    size_t vlen = 0;
    int64_t cur = 0;
    int rc = ts_get(db, s->key.data, s->key.len, &v, &vlen);
    if (rc == TS_OK) {
        if (!parse_stored_int((const uint8_t *)v, vlen, &cur)) rc = TS_INVALID;
        ts_free(v);
    } else if (rc == TS_NOTFOUND) {
        rc = TS_OK;
    }
    if (rc == TS_OK && ((s->delta > 0 && cur > INT64_MAX - s->delta) || (s->delta < 0 && cur < INT64_MIN - s->delta)))
        rc = TS_TOOBIG;
    char buf[32];
    if (rc == TS_OK) {
        cur += s->delta;
        snprintf(buf, sizeof buf, "%" PRId64, cur);
        rc = ts_put(db, s->key.data, s->key.len, buf, strlen(buf));
    }
    if (own) {
        if (rc == TS_OK) rc = ts_commit(db);
        else (void)ts_rollback(db);
    }
    if (rc == TS_INVALID) {
        out(w, "error: the stored value is not an integer\n");
        return rc;
    }
    if (rc != TS_OK) return report(w, rc);
    outf(w, "%s\n", buf);
    return TS_OK;
}

static void check_report(void *ctx, const char *msg) {
    ts_writer *w = (ts_writer *)ctx;
    outf(w, "  problem: %s\n", msg);
}

static const char *help_text =
    "Commands (keywords are case-insensitive; separate commands with ; or new lines):\n"
    "  PUT <key> <value>            insert or replace\n"
    "  GET <key>                    print the value\n"
    "  DEL <key>                    delete\n"
    "  EXISTS <key>                 true or false\n"
    "  INCR <key> [<n>]             add n (default 1) to an integer value\n"
    "  SCAN [range] [LIMIT n] [KEYS] list entries in key order (default limit 100)\n"
    "  COUNT [range]                count keys\n"
    "      range: FROM <key> [TO <key>] | TO <key> | PREFIX <bytes>   (TO is exclusive)\n"
    "  BEGIN | COMMIT | ROLLBACK    transactions (otherwise every write commits on its own)\n"
    "  CHECK                        verify the whole file\n"
    "  STATS                        sizes and counters\n"
    "  CHECKPOINT                   copy the write-ahead log into the database file\n"
    "Keys and values with spaces or special bytes go in quotes: \"a b\", \"tab\\there\", \"\\x00\\xff\".\n";

int exec_statement(ts_db *db, const stmt *s, ts_writer *w) {
    int rc = TS_OK;
    switch (s->kind) {
        case ST_PUT:
            rc = ts_put(db, s->key.data, s->key.len, s->value.data, s->value.len);
            if (rc != TS_OK) return report(w, rc);
            out(w, "OK\n");
            return TS_OK;
        case ST_GET: {
            void *v = NULL;
            size_t vlen = 0;
            rc = ts_get(db, s->key.data, s->key.len, &v, &vlen);
            if (rc == TS_NOTFOUND) {
                out(w, "(nil)\n");
                return TS_OK;
            }
            if (rc != TS_OK) return report(w, rc);
            exec_write_literal(w, (const unsigned char *)v, vlen);
            out(w, "\n");
            ts_free(v);
            return TS_OK;
        }
        case ST_DEL:
            rc = ts_delete(db, s->key.data, s->key.len);
            if (rc == TS_NOTFOUND) {
                out(w, "(not found)\n");
                return TS_OK;
            }
            if (rc != TS_OK) return report(w, rc);
            out(w, "OK\n");
            return TS_OK;
        case ST_EXISTS: {
            void *v = NULL;
            size_t vlen = 0;
            rc = ts_get(db, s->key.data, s->key.len, &v, &vlen);
            if (rc < 0) return report(w, rc);
            ts_free(v);
            out(w, rc == TS_OK ? "true\n" : "false\n");
            return TS_OK;
        }
        case ST_INCR: return do_incr(db, s, w);
        case ST_SCAN: return do_scan(db, s, w, 0);
        case ST_COUNT: return do_scan(db, s, w, 1);
        case ST_BEGIN:
            rc = ts_begin(db);
            break;
        case ST_COMMIT:
            rc = ts_commit(db);
            if (rc == TS_TXN) {
                out(w, "error: no transaction to commit, or it was aborted by an error and rolled back\n");
                return rc;
            }
            break;
        case ST_ROLLBACK:
            rc = ts_rollback(db);
            if (rc == TS_TXN) {
                out(w, "error: no transaction to roll back\n");
                return rc;
            }
            break;
        case ST_CHECKPOINT:
            rc = ts_checkpoint(db);
            break;
        case ST_CHECK: {
            uint64_t problems = 0;
            rc = ts_check(db, check_report, w, &problems);
            if (rc != TS_OK) return report(w, rc);
            ts_stats st;
            (void)ts_get_stats(db, &st);
            if (problems == 0)
                outf(w, "ok: %u pages, %" PRIu64 " keys, tree height %u, no problems found\n", st.pages, st.keys,
                     st.tree_height);
            else
                outf(w, "FAILED: %" PRIu64 " problem(s)\n", problems);
            return problems ? TS_CORRUPT : TS_OK;
        }
        case ST_STATS: {
            ts_stats st;
            rc = ts_get_stats(db, &st);
            if (rc != TS_OK) return report(w, rc);
            outf(w, "keys:             %" PRIu64 "\n", st.keys);
            outf(w, "pages:            %u (%u free), %u bytes each\n", st.pages, st.free_pages, st.page_size);
            outf(w, "file size:        %.1f KiB\n", (double)st.pages * st.page_size / 1024.0);
            outf(w, "tree height:      %u\n", st.tree_height);
            outf(w, "last transaction: %" PRIu64 "\n", st.txn_id);
            outf(w, "log (WAL) pages:  %u\n", st.wal_frames);
            outf(w, "cache:            %" PRIu64 " hits, %" PRIu64 " misses\n", st.cache_hits, st.cache_misses);
            outf(w, "this session:     %" PRIu64 " commit%s, %" PRIu64 " checkpoint%s\n", st.commits,
                 st.commits == 1 ? "" : "s", st.checkpoints, st.checkpoints == 1 ? "" : "s");
            return TS_OK;
        }
        case ST_HELP:
            out(w, help_text);
            return TS_OK;
    }
    if (rc != TS_OK) return report(w, rc);
    out(w, "OK\n");
    return TS_OK;
}

int exec_script(ts_db *db, const char *src, size_t len, ts_writer *w, int stop_on_error) {
    parser p;
    parser_init(&p, src, len);
    int failures = 0;
    for (;;) {
        stmt s;
        int rc = parser_next(&p, &s);
        if (rc == 0) break;
        if (rc == TS_INVALID) {
            outf(w, "syntax error, %s\n", p.error);
            failures++;
            if (stop_on_error) break;
            continue;
        }
        if (rc < 0) {
            failures++;
            (void)report(w, rc);
            break;
        }
        if (exec_statement(db, &s, w) != TS_OK) failures++;
        stmt_free(&s);
        if (failures && stop_on_error) break;
    }
    parser_free(&p);
    return failures;
}
