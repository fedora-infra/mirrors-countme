#!/bin/bash
#
# Rough idea:
# 1. Parse each rotated log file to a work-in-progress db table (or its own csv)
# 2. Once we have all the log entries for a week, total those up for the weekly totals
#    (at this point the in-progress data for that week can be deleted)

# fancy way of ensuring the log parser creates its output atomically.
# probably should just fix that in parse-access-log instead, but for now..
parse_log() {
    local log="$1" out="$2"
    local outdir="$(dirname $out)" outfn="$(basename $out)"
    local partial="$outdir/.$outfn.part"
    trap "rm -f $partial" EXIT
    $PARSE_ACCESS_LOG -o $partial $log
    mv $partial $out
    trap - EXIT
}

# TODO: if we have all 7 days of a week (plus the bookends) then we should have
# all its log entries. Tally up the week's data and write it out.

# TODO: need something to track the countme "epoch"
# TODO: find the "cursor" - the latest fully-processed week
# TODO: each week's data is like. ~40kb? one big file, split by year, or both?
