#!/bin/bash

date_parse() { date --date="$1" +"${2:-%s}" 2>/dev/null; }
die() { echo "${0##*/}: $@" >&2; exit 2; }
if [ "${BASH_VERSINFO[0]}" -ge 4 ]; then
    strftime() { printf ${3:+-v "$3"} "%($1)T\\n" "$2" 2>/dev/null; }
else
    strftime() { date --date="@$2" +"$1" 2>/dev/null; }
fi
# date_seq STARTDATE ENDDATE [DATEFMT:%Y-%m-%d]
date_seq() {
    local start=$(date_parse "$1") || die "couldn't parse '$1'"
    local end=$(date_parse "$2") || die "couldn't parse '$2'"
    local fmt="${3:-%Y-%m-%d}"
    local ts n numdays=$(( (end-start) / 60 / 60 / 24 ))
    for (( ts=start, n=0 ; n <= numdays ; ts+=86400, n++ )); do
        strftime "$fmt" "$ts"
    done
}

F32_BRANCH="2020-02-11"
LOGDIR="${LOGDIR:-/mnt/fedora_stats/combined-http}"
LOGFMT="%Y/%m/%d/mirrors.fedoraproject.org-access.log"

COUNTME_LOGS=()
for log in $(date_seq $F32_BRANCH today $LOGDIR/$LOGFMT); do
    if [ -f "$log" ]; then COUNTME_LOGS+=($log); fi
done
[ "${#COUNTME_LOGS[@]}" == 0 ] && die "nothing matching '$LOGFMT' under '$LOGDIR'"

./parse-access-log.py --progress --sqlite countme-raw.db "${COUNTME_LOGS[@]}"
