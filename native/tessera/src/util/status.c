#include <stdlib.h>
#include <string.h>

#include "util/common.h"

const char *ts_errstr(int rc) {
    switch (rc) {
        case TS_OK:       return "ok";
        case TS_NOTFOUND: return "not found";
        case TS_DONE:     return "no more entries";
        case TS_ERROR:    return "error";
        case TS_NOMEM:    return "out of memory";
        case TS_IOERR:    return "I/O error";
        case TS_CORRUPT:  return "database file is corrupt";
        case TS_BUSY:     return "database is in use by another handle or process";
        case TS_INVALID:  return "invalid argument";
        case TS_TOOBIG:   return "too big";
        case TS_TXN:      return "transaction misuse or aborted transaction";
        case TS_POISONED: return "an earlier write failed; close and reopen the database";
        case TS_NOTADB:   return "file is not a Tessera database";
        default:          return "unknown error";
    }
}

char *ts_strdup(const char *s) {
    size_t n = strlen(s) + 1;
    char *p = (char *)malloc(n);
    if (p) memcpy(p, s, n);
    return p;
}

void ts_free(void *p) {
    free(p);
}
