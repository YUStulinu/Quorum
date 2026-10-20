/*
 * Lexer for the Tessera query language.
 *
 * Tokens:
 *   word     a run of characters other than whitespace, ';', '"', '\'' and '#'
 *            (keywords such as PUT are words too; the parser decides)
 *   string   "..." or '...' with escapes \\ \" \' \n \r \t \0 \xHH; any byte
 *            can be written, so keys and values are binary-safe
 *   end      ';' or a newline: ends a statement
 *   eof
 * Comments run from '#' or "--" to the end of the line.
 */
#ifndef TS_LEXER_H
#define TS_LEXER_H

#include <stddef.h>
#include <stdint.h>

typedef enum { TK_WORD, TK_STRING, TK_END, TK_EOF, TK_ERROR } tk_kind;

typedef struct {
    tk_kind     kind;
    int         line, col;      /* 1-based position of the first character */
    uint8_t    *bytes;          /* the token's value: decoded for strings */
    size_t      len;
    int         owned;          /* bytes was malloc'd (strings) */
    const char *error;          /* for TK_ERROR */
} token;

typedef struct {
    const char *src;
    size_t      len, pos;
    int         line, col;
} lexer;

void lexer_init(lexer *lx, const char *src, size_t len);
/* Fills *t. Returns TS_OK, or TS_NOMEM. Lexical errors come back as TK_ERROR. */
int  lexer_next(lexer *lx, token *t);
void token_free(token *t);

#endif
