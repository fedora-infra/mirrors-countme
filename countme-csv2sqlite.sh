#!/bin/bash

if [ $# -lt 2 ]; then
    echo "usage: $0 COUNTME_CSV COUNTME_SQLITE [TABLENAME]"
    echo "TABLENAME defaults to 'countme_totals'."
    exit 1
fi

COUNTME_CSV="$1"
COUNTME_SQLITE="$2"
COUNTME_TABLE="${3:-countme_totals}"

if [ ! -r "$COUNTME_CSV" ]; then
    echo "$0: error: can't read CSV file '$COUNTME_CSV'" >&2
    exit 2
fi

# TODO: CLI switch for weeknum or week_start+week_end
# TODO: maybe other schemas (schemata?) too?

sqlite3 "$COUNTME_SQLITE" <<__SQLITE__
.bail on
CREATE TABLE $COUNTME_TABLE (
    week_start DATETIME NOT NULL,
    week_end   DATETIME NOT NULL,
    hits       INTEGER NOT NULL,
    os_name    TEXT NOT NULL,
    os_version TEXT NOT NULL,
    os_variant TEXT NOT NULL,
    os_arch    TEXT NOT NULL,
    sys_age    INTEGER NOT NULL,
    repo_tag   TEXT NOT NULL,
    repo_arch  TEXT NOT NULL
);
CREATE INDEX week_start_idx ON $COUNTME_TABLE(week_start);
.import --csv --skip 1 $COUNTME_CSV $COUNTME_TABLE
__SQLITE__
