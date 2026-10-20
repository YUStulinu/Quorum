#include "query/parser.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "util/common.h"

void parser_init(parser *p, const char *src, size_t len) {
    memset(p, 0, sizeof *p);
    lexer_init(&p->lx, src, len);
}

void parser_free(parser *p) {
    if (p->has_look) token_free(&p->look);
    p->has_look = 0;
}

void stmt_free(stmt *s) {
    free(s->key.data);
    free(s->value.data);
    free(s->from.data);
    free(s->to.data);
    free(s->prefix.data);
    memset(s, 0, sizeof *s);
}

static int peek_tok(parser *p, token **out) {
    if (!p->has_look) {
        TS_TRY(lexer_next(&p->lx, &p->look));
        p->has_look = 1;
    }
    *out = &p->look;
    return TS_OK;
}

/* Hands the lookahead token to the caller, who must token_free it. */
static int take_tok(parser *p, token *out) {
    token *t = NULL;
    TS_TRY(peek_tok(p, &t));
    *out = *t;
    p->has_look = 0;
    return TS_OK;
}

static int is_keyword(const token *t, const char *kw) {
    if (t->kind != TK_WORD) return 0;
    size_t n = strlen(kw);
    if (t->len != n) return 0;
    for (size_t i = 0; i < n; i++) {
        int c = t->bytes[i];
        if (c >= 'a' && c <= 'z') c -= 32;
        if (c != kw[i]) return 0;
    }
    return 1;
}

static int fail(parser *p, const token *t, const char *msg) {
    snprintf(p->error, sizeof p->error, "line %d, column %d: %s", t->line, t->col, msg);
    return TS_INVALID;
}

/* A key or value: a word or a string, copied into a fresh buffer. */
static int expect_bytes(parser *p, qbytes *out, const char *what) {
    token *t = NULL;
    TS_TRY(peek_tok(p, &t));
    if (t->kind == TK_ERROR) return fail(p, t, t->error);
    if (t->kind != TK_WORD && t->kind != TK_STRING) {
        char msg[96];
        snprintf(msg, sizeof msg, "expected %s", what);
        return fail(p, t, msg);
    }
    out->data = (uint8_t *)malloc(t->len ? t->len : 1);
    if (!out->data) return TS_NOMEM;
    if (t->len) memcpy(out->data, t->bytes, t->len);
    out->len = t->len;
    token_free(t);
    p->has_look = 0;
    return TS_OK;
}

static int parse_int(const uint8_t *s, size_t n, int allow_sign, int64_t *out) {
    size_t i = 0;
    int neg = 0;
    if (n && allow_sign && (s[0] == '-' || s[0] == '+')) {
        neg = s[0] == '-';
        i = 1;
    }
    if (i == n) return 0;
    uint64_t v = 0;
    for (; i < n; i++) {
        if (s[i] < '0' || s[i] > '9') return 0;
        uint64_t d = (uint64_t)(s[i] - '0');
        if (v > (UINT64_MAX - d) / 10) return 0;
        v = v * 10 + d;
    }
    if (neg) {
        if (v > (uint64_t)INT64_MAX + 1) return 0;
        *out = v == (uint64_t)INT64_MAX + 1 ? INT64_MIN : -(int64_t)v;
    } else {
        if (v > (uint64_t)INT64_MAX) return 0;
        *out = (int64_t)v;
    }
    return 1;
}

static int expect_int(parser *p, int allow_sign, int64_t *out, const char *what) {
    token *t = NULL;
    TS_TRY(peek_tok(p, &t));
    if (t->kind == TK_ERROR) return fail(p, t, t->error);
    if ((t->kind != TK_WORD && t->kind != TK_STRING) || !parse_int(t->bytes, t->len, allow_sign, out)) {
        char msg[96];
        snprintf(msg, sizeof msg, "expected %s", what);
        return fail(p, t, msg);
    }
    token_free(t);
    p->has_look = 0;
    return TS_OK;
}

static int parse_range(parser *p, stmt *s, int allow_limit) {
    for (;;) {
        token *t = NULL;
        TS_TRY(peek_tok(p, &t));
        if (t->kind == TK_END || t->kind == TK_EOF) return TS_OK;
        if (is_keyword(t, "FROM") && !s->has_from && !s->has_prefix) {
            p->has_look = 0;
            TS_TRY(expect_bytes(p, &s->from, "a key after FROM"));
            s->has_from = 1;
        } else if (is_keyword(t, "TO") && !s->has_to && !s->has_prefix) {
            p->has_look = 0;
            TS_TRY(expect_bytes(p, &s->to, "a key after TO"));
            s->has_to = 1;
        } else if (is_keyword(t, "PREFIX") && !s->has_prefix && !s->has_from && !s->has_to) {
            p->has_look = 0;
            TS_TRY(expect_bytes(p, &s->prefix, "bytes after PREFIX"));
            s->has_prefix = 1;
        } else if (allow_limit && is_keyword(t, "LIMIT") && !s->has_limit) {
            p->has_look = 0;
            int64_t n = 0;
            TS_TRY(expect_int(p, 0, &n, "a number after LIMIT"));
            s->has_limit = 1;
            s->limit = (uint64_t)n;
        } else if (allow_limit && is_keyword(t, "KEYS") && !s->keys_only) {
            p->has_look = 0;
            s->keys_only = 1;
        } else {
            return fail(p, t, allow_limit ? "expected FROM, TO, PREFIX, LIMIT or KEYS (each at most once)"
                                          : "expected FROM, TO or PREFIX (each at most once)");
        }
    }
}

static int parse_statement(parser *p, stmt *s) {
    token head;
    TS_TRY(take_tok(p, &head));
    s->line = head.line;
    s->col = head.col;
    int rc = TS_OK;

    struct {
        const char *kw;
        stmt_kind kind;
        int args; /* 0 none, 1 key, 2 key value, 3 key [delta], 4 range+limit, 5 range */
    } table[] = {
        {"PUT", ST_PUT, 2}, {"GET", ST_GET, 1}, {"DEL", ST_DEL, 1}, {"DELETE", ST_DEL, 1},
        {"EXISTS", ST_EXISTS, 1}, {"INCR", ST_INCR, 3}, {"SCAN", ST_SCAN, 4}, {"COUNT", ST_COUNT, 5},
        {"BEGIN", ST_BEGIN, 0}, {"COMMIT", ST_COMMIT, 0}, {"ROLLBACK", ST_ROLLBACK, 0},
        {"CHECK", ST_CHECK, 0}, {"STATS", ST_STATS, 0}, {"CHECKPOINT", ST_CHECKPOINT, 0}, {"HELP", ST_HELP, 0},
    };
    int found = -1;
    for (size_t i = 0; i < sizeof table / sizeof table[0]; i++) {
        if (is_keyword(&head, table[i].kw)) {
            found = (int)i;
            break;
        }
    }
    if (head.kind == TK_ERROR) rc = fail(p, &head, head.error);
    else if (found < 0) rc = fail(p, &head, "unknown command (try HELP)");
    token_free(&head);
    if (rc != TS_OK) return rc;

    s->kind = table[found].kind;
    switch (table[found].args) {
        case 1:
            rc = expect_bytes(p, &s->key, "a key");
            break;
        case 2:
            rc = expect_bytes(p, &s->key, "a key");
            if (rc == TS_OK) rc = expect_bytes(p, &s->value, "a value");
            break;
        case 3: {
            rc = expect_bytes(p, &s->key, "a key");
            s->delta = 1;
            token *t = NULL;
            if (rc == TS_OK) rc = peek_tok(p, &t);
            if (rc == TS_OK && t->kind != TK_END && t->kind != TK_EOF)
                rc = expect_int(p, 1, &s->delta, "an integer amount");
            break;
        }
        case 4: rc = parse_range(p, s, 1); break;
        case 5: rc = parse_range(p, s, 0); break;
        default: break;
    }
    if (rc != TS_OK) return rc;

    token *t = NULL;
    TS_TRY(peek_tok(p, &t));
    if (t->kind != TK_END && t->kind != TK_EOF) return fail(p, t, "unexpected text after the command");
    return TS_OK;
}

int parser_next(parser *p, stmt *s) {
    memset(s, 0, sizeof *s);
    for (;;) {
        token *t = NULL;
        TS_TRY(peek_tok(p, &t));
        if (t->kind == TK_EOF) return 0;
        if (t->kind != TK_END) break;
        p->has_look = 0; /* skip empty statements */
    }
    int rc = parse_statement(p, s);
    if (rc != TS_OK) {
        stmt_free(s);
        if (rc == TS_NOMEM) return rc;
        /* Skip the rest of the broken statement so parsing can resume after it. */
        for (;;) {
            token *t = NULL;
            TS_TRY(peek_tok(p, &t));
            if (t->kind == TK_EOF) break;
            int end = t->kind == TK_END;
            token_free(t);
            p->has_look = 0;
            if (end) break;
        }
        return TS_INVALID;
    }
    return 1;
}
