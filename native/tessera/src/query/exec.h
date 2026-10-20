/*
 * Executes query-language statements against a database and writes the
 * results as text. Output goes through a small writer interface, so the
 * command-line tool prints to stdout and the tests capture it in memory.
 */
#ifndef TS_EXEC_H
#define TS_EXEC_H

#include <stddef.h>

#include "query/parser.h"
#include "tessera.h"

typedef struct {
    void (*write)(void *ctx, const char *s, size_t n);
    void *ctx;
} ts_writer;

#define EXEC_DEFAULT_SCAN_LIMIT 100u

/* Runs one statement. Returns TS_OK or an error code (already printed). */
int exec_statement(ts_db *db, const stmt *s, ts_writer *w);

/* Parses and runs every statement in `src`. Errors are printed as they happen;
   returns the number of statements that failed. If stop_on_error, stops at the first. */
int exec_script(ts_db *db, const char *src, size_t len, ts_writer *w, int stop_on_error);

/* Writes bytes in the query language's literal form: as-is when that is
   unambiguous, otherwise quoted with escapes. */
void exec_write_literal(ts_writer *w, const unsigned char *data, size_t len);

#endif
