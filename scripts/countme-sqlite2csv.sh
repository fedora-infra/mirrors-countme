#!/bin/sh

die() { echo "sqlite2csv.sh: $@" >&2; exit 1; }

if [ $# = 0 ] || [ $# -gt 2 ]; then
    echo "usage: sqlite2csv.sh DATABASE [TABLENAME]"
    echo "TABLENAME is required if DATABASE contains multiple tables."
    exit 2
fi

DATABASE="$1"
TABLENAME="$2"

if [ ! -f "$DATABASE" ]; then
    die "'$DATABASE' not found"
fi

if [ ! -n "$TABLENAME" ]; then
    set -- $(sqlite3 "$DATABASE" ".tables")
    case $# in
        0) die "$DATABASE: no tables found" ;;
        1) TABLENAME="$*" ;;
        *) die "need table name (one of: $*)" ;;
    esac
fi

sqlite3 -csv -header "$DATABASE" "SELECT * FROM $TABLENAME"

