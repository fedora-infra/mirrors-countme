#!/bin/bash
# countme-update-rawdb.sh - update raw `countme` database from new log files

###
### A few constants / default values.
###

# Constants and paths
F32_BRANCH="2020-02-11"      # Fedora 32 is where we turned 'countme' on
MIN_LOG_TIMESTAMP=1581379200 # Timestamp for F32_BRANCH
LOG_JITTER_WINDOW=600        # A safe window for out-of-order log entries

STATS_MOUNT="/mnt/fedora_stats"
PARSECMD="./parse-access-log.py"
DEFAULT_RAWDB="$STATS_MOUNT/data/countme/raw.db"
DEFAULT_LOGDIR="$STATS_MOUNT/combined-http"
DEFAULT_LOGFMT="%Y/%m/%d/mirrors.fedoraproject.org-access.log"


###
### Helper functions for dealing with dates and timestamps
###

# date_parse DATESTR [TIMEFMT:%s]
# Convert a string accepted by date(1) to a timestamp (or any TIMEFMT)
date_parse() { date --date="$1" +"${2:-%s}" 2>/dev/null; }

# strftime TIMEFMT TIMESTAMP
# bash 4.x printf has a builtin strftime to format timestamps. Neat!
# Use that if it's available, otherwise use date(1).
if [ "${BASH_VERSINFO[0]}" -ge 4 ]; then
    strftime() { printf "%($1)T\\n" "$2" 2>/dev/null; }
else
    strftime() { date --date="@$2" +"$1" 2>/dev/null; }
fi

# date_seq STARTDATE ENDDATE [TIMEFMT:%Y-%m-%d]
# print TIMEFMT for each day between STARTDATE and ENDDATE (inclusive)
date_seq() {
    local start=$(date_parse "$1") || die "couldn't parse '$1'"
    local end=$(date_parse "$2") || die "couldn't parse '$2'"
    local fmt="${3:-%Y-%m-%d}"
    local ts n numdays=$(( (end-start) / 60 / 60 / 24 ))
    for (( ts=start, n=0 ; n <= numdays ; ts+=86400, n++ )); do
        strftime "$fmt" "$ts"
    done
}

# rawdb_maxtime RAWDB
# print the maximum 'timestamp' value from the rawdb
rawdb_maxtime() {
    sqlite3 "$1" "SELECT MAX(timestamp) FROM countme_raw"
}

# rawdb_lastlog_date RAWDB
# print an ISO-formatted date for the last log file that was parsed into rawdb.
rawdb_lastlog_date() {
    local maxtime=$(rawdb_maxtime "$1") fudge="$LOG_JITTER_WINDOW"
    if [ $((maxtime+0)) -ge $MIN_LOG_TIMESTAMP ]; then
        date --utc --date="@$((maxtime+fudge))" +"%Y-%m-%d"
    else
        echo "$F32_BRANCH"
    fi
}
# NOTE:
# logs are named for the day they get rotated, so (say) 2020/06/07/foo.log
# will contain data for 2020/06/06, usually starting just after 00:00:00 and
# ending just after midnight the next day. So the highest timestamp _usually_
# lands on the day corresponding to the date of the file - but not always.
# We add a fudge factor to the timestamp so it's more likely to land on the
# proper day, but if that fails, parse-access-log.py also checks if the first
# item in each log is already in the database, and skips that log if so.


###
### Here's the CLI options and CLI parsing stuff.
###

# die msg...
# print msg to stderr and exit.
die() { echo "${0##*/}: $@" >&2; exit 2; }

LOGDIR="$DEFAULT_LOGDIR"
LOGFMT="$DEFAULT_LOGFMT"
RAWDB="$DEFAULT_RAWDB"
STARTDATE=""
DRYRUN=""
PROGRESS=""
PARSECMD_ARGS=()

options=$(getopt -o hn \
    --long help,dryrun,progress,logdir:,logfmt:,rawdb:,newer: \
    -- "$@" \
)
eval set -- "$options"

while true; do
    arg="$1"; shift
    case "$arg" in
        --logdir) LOGDIR="$1"; shift ;;
        --logfmt) LOGFMT="$1"; shift ;;
        --rawdb) RAWDB="$1"; shift ;;
        --newer) STARTDATE="$1"; shift ;;
        --all) STARTDATE="$F32_BRANCH" ;;
        --progress) PROGRESS=1 ;;
        -n|--dryrun) DRYRUN=1 ;;
        -h|--help) usage; exit 0 ;;
        --) break ;;
    esac
done

[ -n "$STARTDATE" ] && STARTDATE=$(rawdb_lastlog_date $RAWDB)
[ -n "$DRYRUN" ] && PARSECMD="echo $PARSECMD"
[ -n "$PROGRESS" ] && PARSECMD_ARGS+=(--progress)
PARSECMD_ARGS+=(--sqlite $RAWDB)


###
### Okay, we've handled the CLI options, let's actually find & parse logs!
###

# Find logs to parse
COUNTME_LOGS=()
for log in $(date_seq $start_date today $LOGDIR/$LOGFMT); do
    if [ -f "$log" ]; then COUNTME_LOGS+=($log); fi
done

if [ -z "$DRYRUN" ]; then
    [ "${#COUNTME_LOGS[@]}" == 0 ] && die "nothing matching '$LOGFMT' under '$LOGDIR'"
fi

$PARSECMD "${PARSECMD_ARGS[@]}" "${COUNTME_LOGS[@]}"
