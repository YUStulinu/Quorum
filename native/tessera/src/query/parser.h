/*
 * Parser for the Tessera query language.
 *
 *   PUT <key> <value>          insert or replace
 *   GET <key>                  print the value
 *   DEL <key>                  delete
 *   EXISTS <key>               true / false
 *   INCR <key> [<delta>]       add to a decimal integer value (missing = 0)
 *   SCAN [range] [LIMIT n] [KEYS]   list entries in key order
 *   COUNT [range]              number of keys
 *     range:  FROM <key> [TO <key>]   keys in [from, to)
 *             TO <key>
 *             PREFIX <bytes>          keys starting with <bytes>
 *   BEGIN | COMMIT | ROLLBACK
 *   CHECK                      verify every on-disk invariant
 *   STATS                      sizes and counters
 *   CHECKPOINT                 copy the log into the database file
 *   HELP
 *
 * Keywords are case-insensitive. A key or value that looks like a keyword,
 * or contains spaces or special bytes, is written as a quoted string.
 */
#ifndef TS_PARSER_H
#define TS_PARSER_H

#include <stddef.h>
#include <stdint.h>

#include "query/lexer.h"

typedef enum {
    ST_PUT, ST_GET, ST_DEL, ST_EXISTS, ST_INCR, ST_SCAN, ST_COUNT,
    ST_BEGIN, ST_COMMIT, ST_ROLLBACK, ST_CHECK, ST_STATS, ST_CHECKPOINT, ST_HELP
} stmt_kind;

typedef struct {
    uint8_t *data;
    size_t   len;
} qbytes;

typedef struct {
    stmt_kind kind;
    int       line, col;
    qbytes    key, value;
    int       has_from, has_to, has_prefix;
    qbytes    from, to, prefix;
    int       has_limit;
    uint64_t  limit;
    int       keys_only;
    int64_t   delta;
} stmt;

typedef struct {
    lexer lx;
    token look;
    int   has_look;
    char  error[160];
} parser;

void parser_init(parser *p, const char *src, size_t len);
/* 1: a statement was parsed into *s; 0: end of input; TS_INVALID: syntax error
   (message in p->error, and the rest of that statement is skipped);
   TS_NOMEM. */
int  parser_next(parser *p, stmt *s);
void stmt_free(stmt *s);
void parser_free(parser *p);

#endif
