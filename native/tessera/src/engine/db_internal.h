/*
 * The database handle, shared by db.c and check.c, plus test-only hooks.
 */
#ifndef TS_DB_INTERNAL_H
#define TS_DB_INTERNAL_H

#include <stdbool.h>

#include "storage/pager.h"

struct ts_db {
    ts_pager  *pager;
    ts_vfs    *vfs;
    ts_options opt;
    bool       explicit_txn; /* inside BEGIN ... COMMIT/ROLLBACK */
    bool       aborted;      /* an operation failed inside it: only ROLLBACK is accepted */
};

/* Frees the handle without checkpointing or writing anything: to the files,
   this looks exactly like the process dying. Used by crash tests. */
void ts_db_abandon(ts_db *db);

#endif
