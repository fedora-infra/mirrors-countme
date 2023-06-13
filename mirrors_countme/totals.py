import datetime
from collections import Counter
from typing import NamedTuple

from .constants import (
    COUNTME_EPOCH,
    COUNTME_EPOCH_ORDINAL,
    COUNTME_START_WEEKNUM,
    LOG_JITTER_WINDOW,
    WEEK_LEN,
)
from .output_items import CountmeItem
from .progress import DIYProgress
from .readers import SQLiteReader
from .util import weeknum
from .writers import CSVWriter, SQLiteWriter

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


# Same as BucketSelect, but ignore sys_age
BucketSelectUniqueIP = CountBucket(
    weeknum=f"((timestamp-{COUNTME_EPOCH})/{WEEK_LEN}) as weeknum",
    os_name="os_name",
    os_version="os_version",
    os_variant="os_variant",
    os_arch="os_arch",
    sys_age="-1",  # roll all sys_age values into one for totals
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
    def from_totalsitem(cls, item):
        """Use this method to convert a TotalsItem to a CSVCountItem."""
        hits, weeknum, *rest = item
        week_start, week_end = daterange(weeknum)
        return cls._make([week_start, week_end, hits] + rest)


# ===========================================================================
# ====== SQL + Progress helpers =============================================
# ===========================================================================


class RawDB(SQLiteReader):
    def __init__(self, filename, **kwargs):
        super().__init__(filename, CountmeItem, tablename="countme_raw", **kwargs)

    @property
    def mintime(self):
        return self.mintime_countme

    @property
    def maxtime(self):
        return self.maxtime_countme

    def complete_weeks(self):
        """Compute range of complete weeks in database.

        Return a range(startweek, provweek) that covers (valid + complete)
        weeknums contained in this database. The database may contain some
        data for `provweek`, but since it's provisional/incomplete it's
        outside the range."""
        if self.mintime is None:
            return []
        # startweek can't be earlier than the first week of data
        startweek = max(weeknum(self.mintime), COUNTME_START_WEEKNUM)
        # A week is provisional until the LOG_JITTER_WINDOW expires, so once
        # tsmax minus LOG_JITTER_WINDOW ticks over into a new weeknum, that
        # weeknum is the provisional one. So...
        provweek = max(weeknum(self.maxtime - LOG_JITTER_WINDOW), COUNTME_START_WEEKNUM)
        return range(startweek, provweek)

    def week_iter(self, weeknum, select: tuple | list):
        item_select = ",".join(select)
        start_ts = weeknum * WEEK_LEN + COUNTME_EPOCH
        end_ts = start_ts + WEEK_LEN
        return self._connection.execute(
            f"SELECT {item_select}"
            f" FROM {self._tablename}"
            f" WHERE timestamp >= {start_ts} AND timestamp < {end_ts} AND sys_age >= 0"
        )

    def week_count(self, weeknum):
        cursor = self.week_iter(weeknum, ("COUNT(*)",))
        return cursor.fetchone()[0]


class RawDBU(RawDB):
    def __init__(self, fp, **kwargs):
        super().__init__(fp, **kwargs)

    # As the comment in week_count() says, although we look at both countme
    # and unique IP data ... when we need to know where "the data" starts we
    # only look for unique IP data, otherwise we could process a week that just
    # has countme data as also having unique IP data (and get very low numbers).
    @property
    def mintime(self):
        return self.mintime_unique

    @property
    def maxtime(self):
        return self.maxtime_unique

    def week_iter(self, weeknum, select: tuple | list):
        start_ts = weeknum * WEEK_LEN + COUNTME_EPOCH
        return SplitWeekDays(self, start_ts, select)

    def week_count(self, weeknum):
        start_ts = weeknum * WEEK_LEN + COUNTME_EPOCH
        end_ts = start_ts + WEEK_LEN

        # So this is a "problem" ... we could do a GROUP BY as we do in
        # SplitWeekDays(), but that is basically doing all the same work.
        # Just counting the rows gets us to roughly 8:1 ... so go with that.
        # Also note that we do sys_age < 0, so that we find weeks of data that
        # have unique IP data in it ... even though we'll look at both.
        cursor = self._connection.execute(
            f"SELECT COUNT(*)"
            f" FROM {self._tablename}"
            f" WHERE timestamp >= {start_ts} AND timestamp < {end_ts} AND sys_age < 0"
        )
        return int(cursor.fetchone()[0] / 8)


class SplitWeekDays(object):
    """Pretend to be a SQLite select object. But actually we are 7 of them combined."""

    def __init__(self, rawdb, start_ts, select: tuple | list):
        self.rawdb = rawdb
        self.start_ts = start_ts
        self.select = select

    def __iter__(self):
        return self.fetchall()

    def fetchone(self):
        """Probably don't use this."""
        for x in self.fetchall():
            return x
        else:
            pass  # pragma: no cover

    def fetchall(self):
        """Get a weeks data of unique IPs, by getting group'd data for each day ... for 7 days."""
        item_select = ",".join(self.select)
        start_ts = self.start_ts

        for d in range(7):
            end_ts = start_ts + (WEEK_LEN / 7)
            # Can add "COUNT(*) as nip" to test, but need to remove it for writes
            # Note that we look at _both_ unique/countme data here, so no sys_age
            # checks.
            cursor = self.rawdb._connection.execute(
                f"SELECT {item_select}"
                f" FROM {self.rawdb._tablename}"
                f" WHERE timestamp >= {start_ts} AND timestamp < {end_ts}"
                f" GROUP BY host, os_name, os_version, os_variant, os_arch, repo_tag, repo_arch"
            )
            for row in cursor.fetchall():
                yield row
            start_ts = end_ts


def totals(*, countme_totals, countme_raw=None, progress=False, csv_dump=None):
    # Initialize the writer (better to fail early..)
    totals = SQLiteWriter(
        countme_totals, TotalsItem, timefield="weeknum", tablename="countme_totals"
    )
    totals.write_header()

    # Are we doing an update?
    if countme_raw:
        rawdb = RawDB(countme_raw)

        # Make sure we index them by time.
        totals.write_index()

        # Check to see if there's any new weeks to get data for
        complete_weeks = sorted(rawdb.complete_weeks())
        latest_week_in_totals = totals.maxtime_countme or -1
        new_weeks = [w for w in complete_weeks if w > int(latest_week_in_totals)]

        # Count week by week
        for week in new_weeks:
            # Set up a progress meter and counter
            mon, sun = daterange(week)
            desc = f"week {week} ({mon} -- {sun})"
            if progress:
                total = rawdb.week_count(week)
            else:
                total = 1
            prog = DIYProgress(
                total=total, desc=desc, disable=not progress, unit="row", unit_scale=False
            )
            hitcount = Counter()

            # Select raw items into their buckets and count 'em up
            for bucket in rawdb.week_iter(week, select=BucketSelect):
                hitcount[bucket] += 1
                prog.update()

            # Write the resulting totals into countme_totals
            totals.write_items((hits,) + bucket for bucket, hits in hitcount.items())
            prog.close()

        # Now do roughly the same thing, but for Unique IPs...
        rawdb = RawDBU(countme_raw)

        # Check to see if there's any new weeks to get data for
        complete_weeks = sorted(rawdb.complete_weeks())
        latest_week_in_totals = totals.maxtime_unique or -1
        new_weeks = [w for w in complete_weeks if w > int(latest_week_in_totals)]

        # Count week by week
        for week in new_weeks:
            # Set up a progress meter and counter
            mon, sun = daterange(week)
            desc = f"weeku {week} ({mon} -- {sun})"
            if progress:
                total = rawdb.week_count(week)
            else:
                total = 1
            prog = DIYProgress(
                total=total, desc=desc, disable=not progress, unit="~ip", unit_scale=False
            )
            hitcount = Counter()

            # Select raw items into their buckets and count 'em up
            for bucket in rawdb.week_iter(week, select=BucketSelectUniqueIP):
                hitcount[bucket] += 1
                prog.update()

            # Write the resulting totals into countme_totals
            totals.write_items((hits,) + bucket for bucket, hits in hitcount.items())
            prog.close()
    else:  # pragma: no cover
        pass

    # Was a CSV dump requested?
    if csv_dump:
        totalreader = SQLiteReader(
            countme_totals,
            TotalsItem,
            timefield="weeknum",
            tablename="countme_totals",
        )
        writer = CSVWriter(csv_dump, CSVCountItem, timefield="week_start")
        writer.write_header()
        for item in totalreader:
            writer.write_item(CSVCountItem.from_totalsitem(item))
    else:  # pragma: no cover
        pass
