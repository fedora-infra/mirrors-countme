#!/bin/bash
# countme-update-totals.sh - update (local) countme totals from rawdb.

###
### A few constants / default values.
###

# Constants and paths
COUNTCMD="countme-totals"
OUTPUT_DIR="/var/lib/countme"

DEFAULT_RAWDB="$OUTPUT_DIR/raw.db"
DEFAULT_TOTALSDB="$OUTPUT_DIR/totals.db"
DEFAULT_TOTALSCSV="$OUTPUT_DIR/totals.csv"

###
### Option defaults and CLI parsing code
###

# Basename of the script
PROGNAME="${0##*/}"

# die msg...
# print msg to stderr and exit.
die() { echo "$PROGNAME: $@" >&2; exit 2; }

usage() {
    cat <<__USAGE__
usage: $PROGNAME [OPTIONS..]
Read "countme-raw" database, count hits, and write SQLite + CSV output.
Only writes data for complete weeks that are not in the output database.
See '$COUNTCMD' for more details.

Options:
  -h,--help               Show this help
  -n,--dryrun             Don't do anything, just show the command
  --progress              Show progress bars while counting
  --rawdb RAWDB           "countme-raw" database to read from
  --totals-db TOTALSDB    SQLite "countme-totals" database to update
  --totals-csv TOTALSCSV  Write CSV-formatted totals to this file

Defaults:
  RAWDB      $DEFAULT_RAWDB
  TOTALSDB   $DEFAULT_TOTALSDB
  TOTALSCSV  $DEFAULT_TOTALSCSV
__USAGE__
}

RAWDB="$DEFAULT_RAWDB"
TOTALSDB="$DEFAULT_TOTALSDB"
TOTALSCSV="$DEFAULT_TOTALSCSV"
PROGRESS=""
DRYRUN=""
COUNTCMD_ARGS=()

options=$(getopt --name "$PROGNAME" \
    --options hn \
    --long help,dryrun,progress,rawdb:,totals-db:,totals-csv: \
    -- "$@" \
) || exit 1
eval set -- "$options"

while true; do
    arg="$1"; shift
    case "$arg" in
        --rawdb) RAWDB="$1"; shift ;;
        --totals-db) TOTALSDB="$1"; shift ;;
        --totals-csv) TOTALSCSV="$1"; shift ;;
        --progress) PROGRESS=1 ;;
        -n|--dryrun) DRYRUN=1 ;;
        -h|--help) usage; exit 0 ;;
        --) break ;;
    esac
done

[ -f "$RAWDB" ] || die "can't find rawdb '$RAWDB'"

COUNTCMD_ARGS=("$TOTALSDB" --update-from "$RAWDB" --csv-dump "$TOTALSCSV")
[ -n "$DRYRUN" ] && COUNTCMD="echo $COUNTCMD"
[ -n "$PROGRESS" ] && COUNTCMD_ARGS+=(--progress)

###
### CLI parsing finished - run the counter!
###

$COUNTCMD "${COUNTCMD_ARGS[@]}"
