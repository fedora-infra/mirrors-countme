import datetime
from collections import Counter
from typing import NamedTuple

from . import CountmeItem, CSVWriter, SQLiteReader, SQLiteWriter, weeknum
from .constants import (
    COUNTME_EPOCH,
    COUNTME_EPOCH_ORDINAL,
    COUNTME_START_WEEKNUM,
    LOG_JITTER_WINDOW,
    WEEK_LEN,
)
from .progress import diyprog as Progress

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


def weekdate(weeknum, weekday=0):
    if weekday < 0 or weekday > 6:
        raise ValueError("weekday must be between 0 (Mon) and 6 (Sun)")
    ordinal = COUNTME_EPOCH_ORDINAL + 7 * int(weeknum) + weekday
    return datetime.date.fromordinal(ordinal)


def daterange(weeknum):
    return weekdate(weeknum, 0), weekdate(weeknum, 6)


# ===========================================================================
# ====== Count Buckets & Items ==============================================
# ===========================================================================


class CountBucket(NamedTuple):
    weeknum: str  # this is a query
    os_name: str
    os_version: str
    os_variant: str
    os_arch: str
    sys_age: str  # this is a key
    repo_tag: str
    repo_arch: str


BucketSelect = CountBucket(
    weeknum=f"((timestamp-{COUNTME_EPOCH})/{WEEK_LEN}) as weeknum",
    os_name="os_name",
    os_version="os_version",
    os_variant="os_variant",
    os_arch="os_arch",
    sys_age="sys_age",
    repo_tag="repo_tag",
    repo_arch="repo_arch",
)


class TotalsItem(NamedTuple):
    """TotalsItem is CountBucket with a "hits" count on the front."""

    hits: int
    weeknum: str  # this is a query
    os_name: str
    os_version: str
    os_variant: str
    os_arch: str
    sys_age: str  # this is a key
    repo_tag: str
    repo_arch: str


class CSVCountItem(NamedTuple):
    """
    Represents one row in a countme_totals.csv file.
    In the interest of human-readability, we replace 'weeknum' with the
    start and end dates of that week.
    """

    week_start: str
    week_end: str
    hits: int
    os_name: str
    os_version: str
    os_variant: str
    os_arch: str
    sys_age: int
    repo_tag: str
    repo_arch: str

    @classmethod
    def from_totalitem(cls, item):
        """Use this method to convert a CountItem to a CSVCountItem."""
        hits, weeknum, *rest = item
        week_start, week_end = daterange(weeknum)
        return cls._make([week_start, week_end, hits] + rest)


# ===========================================================================
# ====== SQL + Progress helpers =============================================
# ===========================================================================


class RawDB(SQLiteReader):
    def __init__(self, fp, **kwargs):
        super().__init__(fp, CountmeItem, tablename="countme_raw", **kwargs)

    def complete_weeks(self):
        """Return a range(startweek, provweek) that covers (valid + complete)
        weeknums contained in this database. The database may contain some
        data for `provweek`, but since it's provisional/incomplete it's
        outside the range."""
        # startweek can't be earlier than the first week of data
        startweek = max(weeknum(self.mintime), COUNTME_START_WEEKNUM)
        # A week is provisional until the LOG_JITTER_WINDOW expires, so once
        # tsmax minus LOG_JITTER_WINDOW ticks over into a new weeknum, that
        # weeknum is the provisional one. So...
        provweek = weeknum(self.maxtime - LOG_JITTER_WINDOW)
        return range(startweek, provweek)

    def week_count(self, weeknum):
        start_ts = weeknum * WEEK_LEN + COUNTME_EPOCH
        end_ts = start_ts + WEEK_LEN
        cursor = self._connection.execute(
            f"SELECT COUNT(*)"
            f" FROM {self._tablename}"
            f" WHERE timestamp >= {start_ts} AND timestamp < {end_ts}"
        )
        return cursor.fetchone()[0]

    def week_iter(self, weeknum, select="*"):
        if isinstance(select, (tuple, list)):
            item_select = ",".join(select)
        elif isinstance(select, str):
            item_select = select
        else:
            raise ValueError(f"select should be a string or tuple, not {select.__class__.__name__}")
        start_ts = weeknum * WEEK_LEN + COUNTME_EPOCH
        end_ts = start_ts + WEEK_LEN
        return self._connection.execute(
            f"SELECT {item_select}"
            f" FROM {self._tablename}"
            f" WHERE timestamp >= {start_ts} AND timestamp < {end_ts}"
        )


def totals(args):
    # Initialize the writer (better to fail early..)
    totals = SQLiteWriter(
        args.countme_totals, TotalsItem, timefield="weeknum", tablename="countme_totals"
    )
    totals.write_header()

    # Are we doing an update?
    if args.countme_raw:
        rawdb = RawDB(args.countme_raw)

        # Check to see if there's any new weeks to get data for
        complete_weeks = sorted(rawdb.complete_weeks())
        newest_totals = totals.maxtime or -1
        new_weeks = [w for w in complete_weeks if w > int(newest_totals)]

        # Count week by week
        for week in new_weeks:
            # Set up a progress meter and counter
            mon, sun = daterange(week)
            desc = f"week {week} ({mon} -- {sun})"
            total = rawdb.week_count(week)
            prog = Progress(
                total=total,
                desc=desc,
                disable=True if not args.progress else None,
                unit="row",
                unit_scale=False,
            )
            hitcount = Counter()

            # Select raw items into their buckets and count 'em up
            for bucket in rawdb.week_iter(week, select=BucketSelect):
                hitcount[bucket] += 1
                prog.update()

            # Write the resulting totals into countme_totals
            totals.write_items((hits,) + bucket for bucket, hits in hitcount.items())
            prog.close()

        # Oh and make sure we index them by time.
        totals.write_index()

    # Was a CSV dump requested?
    if args.csv_dump:
        totalreader = SQLiteReader(
            args.countme_totals,
            TotalsItem,
            timefield="weeknum",
            tablename="countme_totals",
        )
        writer = CSVWriter(args.csv_dump, CSVCountItem, timefield="week_start")
        writer.write_header()
        for item in totalreader:
            writer.write_item(CSVCountItem.from_totalitem(item))
