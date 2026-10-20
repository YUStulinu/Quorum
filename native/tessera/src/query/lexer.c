#include "query/lexer.h"

#include <stdlib.h>
#include <string.h>

#include "util/common.h"

void lexer_init(lexer *lx, const char *src, size_t len) {
    lx->src = src;
    lx->len = len;
    lx->pos = 0;
    lx->line = 1;
    lx->col = 1;
}

void token_free(token *t) {
    if (t->owned) free(t->bytes);
    t->bytes = NULL;
    t->owned = 0;
}

static int peek(const lexer *lx, size_t ahead) {
    return lx->pos + ahead < lx->len ? (unsigned char)lx->src[lx->pos + ahead] : -1;
}

static void advance(lexer *lx) {
    if (lx->src[lx->pos] == '\n') {
        lx->line++;
        lx->col = 1;
    } else {
        lx->col++;
    }
    lx->pos++;
}

static int hexval(int c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

static int is_word_char(int c) {
    return c > ' ' && c != ';' && c != '"' && c != '\'' && c != '#' && c != 127;
}

static int lex_string(lexer *lx, token *t) {
    int quote = peek(lx, 0);
    advance(lx);
    /* The decoded string is never longer than the source text. */
    uint8_t *buf = (uint8_t *)malloc(lx->len - lx->pos + 1);
    if (!buf) return TS_NOMEM;
    size_t n = 0;
    for (;;) {
        int c = peek(lx, 0);
        if (c < 0 || c == '\n') {
            free(buf);
            t->kind = TK_ERROR;
            t->error = "unterminated string";
            return TS_OK;
        }
        if (c == quote) {
            advance(lx);
            break;
        }
        if (c != '\\') {
            buf[n++] = (uint8_t)c;
            advance(lx);
            continue;
        }
        advance(lx);
        int e = peek(lx, 0);
        int out;
        switch (e) {
            case '\\': out = '\\'; break;
            case '"':  out = '"'; break;
            case '\'': out = '\''; break;
            case 'n':  out = '\n'; break;
            case 'r':  out = '\r'; break;
            case 't':  out = '\t'; break;
            case '0':  out = 0; break;
            case 'x': {
                int h1 = hexval(peek(lx, 1)), h2 = hexval(peek(lx, 2));
                if (h1 < 0 || h2 < 0) {
                    free(buf);
                    t->kind = TK_ERROR;
                    t->error = "\\x must be followed by two hex digits";
                    return TS_OK;
                }
                advance(lx);
                advance(lx);
                out = h1 * 16 + h2;
                break;
            }
            default:
                free(buf);
                t->kind = TK_ERROR;
                t->error = "unknown escape sequence";
                return TS_OK;
        }
        advance(lx);
        buf[n++] = (uint8_t)out;
    }
    t->kind = TK_STRING;
    t->bytes = buf;
    t->len = n;
    t->owned = 1;
    return TS_OK;
}

int lexer_next(lexer *lx, token *t) {
    memset(t, 0, sizeof *t);
    for (;;) {
        int c = peek(lx, 0);
        if (c == ' ' || c == '\t' || c == '\r' || c == '\f' || c == '\v') {
            advance(lx);
        } else if (c == '#' || (c == '-' && peek(lx, 1) == '-')) {
            while (peek(lx, 0) >= 0 && peek(lx, 0) != '\n') advance(lx);
        } else {
            break;
        }
    }
    t->line = lx->line;
    t->col = lx->col;
    int c = peek(lx, 0);
    if (c < 0) {
        t->kind = TK_EOF;
        return TS_OK;
    }
    if (c == ';' || c == '\n') {
        advance(lx);
        t->kind = TK_END;
        return TS_OK;
    }
    if (c == '"' || c == '\'') return lex_string(lx, t);
    if (!is_word_char(c)) {
        advance(lx);
        t->kind = TK_ERROR;
        t->error = "unexpected control character";
        return TS_OK;
    }
    size_t start = lx->pos;
    while (is_word_char(peek(lx, 0))) advance(lx);
    t->kind = TK_WORD;
    t->bytes = (uint8_t *)(uintptr_t)(lx->src + start); /* points into the source; not owned */
    t->len = lx->pos - start;
    return TS_OK;
}
