#!/usr/bin/python3

import sys
import sqlite3
import argparse
import datetime
from collections import Counter
from typing import NamedTuple
from countme import CountmeItem, weeknum, SQLiteWriter

# NOTE: log timestamps do not move monotonically forward, but they don't
# seem to ever jump backwards more than 241 seconds. I assume this is
# some timeout that's set to 4 minutes, and the log entry shows up after
# expiry, or something. Anyway, what this means is that once we've seen
# a timestamp that's 241 seconds past the end of a week, we can assume that
# there will be no further entries whose timestamps belong to the previous
# week.
# We could probably watch the max jitter between log lines and adjust
# this if needed, but for now I'm just gonna pad it to 600 seconds.
# The difference between 241 and 600 is kind of irrelevant - since we get logs
# in 24-hour chunks, any window that extends into the next day means we have to
# wait 24 hours until we can be sure we have all the data for the previous
# week, so the effect would be the same if this was 3600 or 43200 or whatever.
# TODO: this should probably move into the module somewhere..
LOG_JITTER_WINDOW = 600

# Feb 11 2020 was the date that we branched F32 from Rawhide, so we've decided
# to use that as the starting week for countme data.
COUNTME_START_TIME=1581292800 # =Mon Feb 10 00:00:00 2020 (UTC)
COUNTME_START_WEEKNUM=2614

DAY_LEN = 24*60*60
WEEK_LEN = 7*DAY_LEN
COUNTME_EPOCH = 345600  # =00:00:00 Mon Jan 5 00:00:00 1970 (UTC)

# And here's how you convert a weeknum to a human-readable date
COUNTME_EPOCH_ORDINAL=719167
def weekdate(weeknum, weekday=0):
    if weekday < 0 or weekday > 6:
        raise ValueError("weekday must be between 0 (Mon) and 6 (Sun)")
    d = datetime.date.fromordinal(COUNTME_EPOCH_ORDINAL + 7*weeknum + weekday)
    return d.isoformat()

# ===========================================================================
# ====== CLI parser & main() ================================================
# ===========================================================================

def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description = "Aggregate 'countme' log records to weekly totals.",
    )
    p.add_argument("-V", "--version", action='version',
        version='%(prog)s 0.0.1')

    p.add_argument("countme_raw",
        help="Database to read (from parse-access-log.py)")

    p.add_argument("countme_totals",
        help="Database to write")

    p.add_argument("--recount", action="store_true",
        help="Redo counts for existing data")

    p.add_argument("--progress", choices=("auto", "basic", "tqdm", "none"),
        nargs='?', const="auto",
        help="progress meter. auto if stdout is tty; use tqdm if installed.")

    args = p.parse_args(argv)

    return args

class CountItem(NamedTuple):
    count: int
    weeknum: int
    os_name: str
    os_version: str
    os_variant: str
    os_arch: str
    countme: int
    repo_tag: str
    repo_arch: str

CountItemSelect = CountItem(
    count = "COUNT(*) AS count",
    weeknum = f"((timestamp-{COUNTME_EPOCH})/{WEEK_LEN}) as weeknum",
    os_name = "os_name",
    os_version = "os_version",
    os_variant = "os_variant",
    os_arch = "os_arch",
    countme = "countme",
    repo_tag = "repo_tag",
    repo_arch = "repo_arch"
)

class RawDB:
    def __init__(self, filename, mode='ro', tablename='countme_raw', timefield='timestamp'):
        self.name = filename
        self._tablename = tablename
        self._con = sqlite3.connect(f"file:{filename}?mode={mode}", uri=True)
        self._timefield = timefield
        self._tsmin = None
        self._tsmax = None

    def _minmax(self, column):
        cur = self._con.execute(f"SELECT min({column}),max({column}) FROM {self._tablename}")
        return cur.fetchone()

    def _tsminmax(self):
        if not (self._tsmin and self._tsmax):
            self._tsmin, self._tsmax = self._minmax(column=self._timefield)
        return self._tsmin, self._tsmax

    def set_progress_handler(self, callback, interval=1):
        self._con.set_progress_handler(callback, interval)

    @property
    def tsmin(self):
        return self._tsmin if self._tsmin else self._tsminmax()[0]

    @property
    def tsmax(self):
        return self._tsmax if self._tsmax else self._tsminmax()[1]

    def complete_weeks(self):
        '''Return a range(startweek, provweek) that covers (valid + complete)
        weeknums contained in this database. The database may contain some
        data for `provweek`, but since it's provisional/incomplete it's
        outside the range.'''
        # startweek can't be earlier than the first week of data
        startweek = max(weeknum(self.tsmin), COUNTME_START_WEEKNUM)
        # A week is provisional until the LOG_JITTER_WINDOW expires, so once
        # tsmax minus LOG_JITTER_WINDOW ticks over into a new weeknum, that
        # weeknum is the provisional one. So...
        provweek = weeknum(self.tsmax - LOG_JITTER_WINDOW)
        return range(startweek, provweek)

    def iter_week_counts(self, startweek=None, endweek=None):
        weeks = self.complete_weeks()
        if startweek is None:
            startweek = weeks.start
        if endweek is None:
            endweek = weeks.stop
        start_ts = startweek*WEEK_LEN+COUNTME_EPOCH
        end_ts = endweek*WEEK_LEN+COUNTME_EPOCH
        item_select = ','.join(CountItemSelect)
        group_fields = ','.join(f for f in CountItem._fields if f != 'count')
        return self._con.execute(
            f"SELECT {item_select}"
            f" FROM {self._tablename}"
            f" WHERE timestamp >= {start_ts} AND timestamp < {end_ts}"
            f" GROUP BY {group_fields}")

def main():
    args = parse_args()

    rawdb = RawDB(args.countme_raw)

    # Initialize the writer (better to fail early..)
    totals = SQLiteWriter(open(args.countme_totals, 'ab+'),
                          CountItem,
                          timefield='weeknum',
                          tablename='countme_totals')
    totals.write_header()

    # Find which weeks aren't already in the totals
    did_weeks = set(totals._distinct('weeknum'))
    new_weeks = set(rawdb.complete_weeks()) ^ did_weeks
    startweek = min(new_weeks)
    endweek = max(new_weeks)+1

    # SQL POWER GO! Do some counting!
    print(f"Counting items for {weekdate(startweek)} - {weekdate(endweek-1, 6)}")
    counts = list(rawdb.iter_week_counts(startweek, endweek))

    # And write them into the output.
    print(f"Writing {len(counts)} totals to {args.countme_totals}")
    totals.write_items(counts)
    totals.write_index()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(3) # You know, 3, like 'C', like Ctrl-C!
