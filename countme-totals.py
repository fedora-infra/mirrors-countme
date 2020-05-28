#!/usr/bin/python3

import sys
import argparse
import datetime
from collections import Counter
from typing import NamedTuple

COUNTME_OFFSET = 345600       # 00:00:00 Mon Jan 5 00:00:00 1970
COUNTME_WINDOW = 7*24*60*60   # Exactly 7 days

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
LOG_JITTER_WINDOW = 600

def weektuple(ts):
    '''Return (week_num, week_secs) for a given timestamp'''
    return divmod(int(ts)-COUNTME_OFFSET, COUNTME_WINDOW)

def week_start_ts(ts):
    '''Return the timestamp of the start of the week containing ts'''
    weeksecs = (ts-COUNTME_OFFSET) % COUNTME_WINDOW
    return ts - weeksecs

def week_start(ts):
    '''Return an ISO-formatted date string of the Monday that starts the week
    that contains the given timestamp.'''
    ts = int(ts)
    weeksecs = (ts-COUNTME_OFFSET) % COUNTME_WINDOW
    weekstart = datetime.datetime.utcfromtimestamp(ts - weeksecs)
    return weekstart.date().isoformat()

# Here's the items we expect to be reading from our input file.
# TODO: we should be importing this from a 'countme' module or something
# rather than duplicating it between parse-access-log.py and here
class CountmeItem(NamedTuple):
    '''
    A "countme" match item.
    Includes the countme value and libdnf User-Agent fields.
    '''
    timestamp: int
    host: str
    os_name: str
    os_version: str
    os_variant: str
    os_arch: str
    countme: int
    repo_tag: str
    repo_arch: str

# And here's the "bucket" we sort each item into.
class CountmeBucket(NamedTuple):
    '''
    This defines the fields that we use to group/aggregate CountmeItems.
    '''
    week_start: str
    os_name: str
    os_version: str
    os_variant: str
    os_arch: str
    countme: int
    repo_tag: str
    repo_arch: str

    @classmethod
    def from_item(cls, item):
        return cls._make((week_start(item.timestamp),) + item[2:])


# ===========================================================================
# ====== ItemReader classes =================================================
# ===========================================================================

class ReaderError(RuntimeError):
    pass

class ItemReader:
    def __init__(self, fp, itemtuple, **kwargs):
        self._fp = fp
        self._itemtuple = itemtuple
        self._itemfields = itemtuple._fields
        self._itemfactory = itemtuple._make
        self._filefields = None
        self._get_reader(**kwargs)
        if not self._filefields:
            raise ReaderError("no field names found")
        if self._filefields != self._itemfields:
            raise ReaderError(f"field mismatch: expected {self._itemfields}, got {self._filefields}")
    def _get_reader(self):
        '''Set up the ItemReader.
        Should set self._filefields to a tuple of the fields found in fp.'''
        raise NotImplementedError
    def _iter_rows(self):
        '''Return an iterator/generator that produces a row for each item.'''
        raise NotImplementedError
    def __iter__(self):
        for item in self._iter_rows():
            yield self._itemfactory(item)

class CSVReader(ItemReader):
    def _get_reader(self, **kwargs):
        import csv
        self._reader = csv.reader(self._fp)
        self._filefields = tuple(next(self._reader))
        # If we have numbers in our fieldnames, probably there was no header
        if any(name.isnumeric() for name in self._filefields):
            header = ','.join(fields)
            raise ReaderError(f"header bad/missing, got: {header}")
    def _iter_rows(self):
        return self._reader

# TODO: AWKReader, JSONReader

class SQLiteReader(ItemReader):
    def _get_reader(self, tablename='countme_raw', **kwargs):
        import sqlite3
        self._con = sqlite3.connect(self._fp.name)
        # TODO: self._con.set_progress_handler(handler, call_interval)
        self._cur = self._con.cursor()
        self._tablename = tablename
        if False and sqlite3.sqlite_version_info >= (3,16,0):
            fields_sql = f"SELECT name FROM pragma_table_info(?)"
            self._filefields = tuple(r[0] for r in self._cur.execute(fields_sql, (tablename,)))
        else:
            fields_sql = f"PRAGMA table_info('{tablename}')"
            self._filefields = tuple(r[1] for r in self._cur.execute(fields_sql))
    def _iter_rows(self):
        fields = ",".join(self._itemfields)
        return self._cur.execute(f"SELECT {fields} FROM {self._tablename}")

# BUCKET COUNTER YOOOOOOO
# TODO: finish/clean this up
# TODO: If we're doing sqlite->sqlite we can probably do the count in pure SQL,
#       which is probably much faster? Complicated tho.
class BucketCounterBase:
    itemtuple = NotImplemented
    buckettuple = NotImplemented
    def __init__(self, item_filter=None, **kwargs):
        self._count = Counter()
        self.item_filter = item_filter
    @classmethod
    def item_bucket(cls, item):
        raise NotImplementedError
    def bucket_count(self, reader):
        if reader._itemtuple != self.itemtuple:
            raise ValueError(f"Reader item {reader._itemtuple!r}"
                             f" does not match expected item {self.itemtuple!r}")
        # Iterate through (maybe filtered) items and count 'em up
        itemiter = filter(self.item_filter, reader) if callable(self.item_filter) else iter(reader)
        count = Counter(self.item_bucket(item) for item in itemiter)
        # Add the new counts to the total
        self._count += count
        # Return the new counts
        return count

class CountmeBucketCounter:
    itemtuple = CountmeItem
    buckettuple = CountmeBucket
    @classmethod
    def item_bucket(cls, item):
        return cls.buckettuple._make((week_start(item.timestamp),) + item[2:])

# ===========================================================================
# ====== CLI parser & main() ================================================
# ===========================================================================

def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description = "Aggregate 'countme' log records to weekly totals.",
    )
    p.add_argument("-V", "--version", action='version',
        version='%(prog)s 0.0.1')

    p.add_argument("infiles", metavar="COUNTMEDATA",
        type=argparse.FileType('rt', encoding='utf-8'), nargs='+',
        help="Data to parse (from parse-access-log.py)")

    # TODO: atomic creation of output file
    p.add_argument("-o", "--output",
        type=argparse.FileType('at', encoding='utf-8'),
        help="output file (default: stdout)",
        default=sys.stdout)

    # TODO: refuse to overwrite existing files, unless..
    #p.add_argument("--force"
    # Or perhaps..
    #p.add_argument("--update")

    p.add_argument("-f", "--format",
        choices=("csv", "json", "awk"),
        help="output format (default: csv)",
        default="csv")

    # TODO: sqlite output, wheeee.
    # SQLite counting could probably all be done in pure SQL, tbh, so maybe
    # that's a totally different script?
    #p.add_argument("--sqlite", metavar="DB",
    #    help="sqlite database to write to")

    # TODO: use this..
    p.add_argument("--progress", action="store_true",
        help="print some progress info while counting")

    p.add_argument("--input-format", choices=("csv", "sqlite", "auto"),
        help="input file format (default: guess from extension)",
        default="auto")

    # TODO: allow specifying cutoff times so we don't double-count?
    # Also: cutoff time/date for "preliminary" data?

    args = p.parse_args(argv)

    # Pick the right reader factory
    if args.input_format == "csv":
        args.reader = CSVReader
    elif args.input_format == "sqlite":
        args.reader = SQLiteReader
    elif args.input_format == "auto":
        args.reader = autoreader
        # Check that we can figure out the right reader(s) before we start..
        for fp in args.infiles:
            if guessreader(fp) is None:
                raise argparse.ArgumentTypeError(
                    "Can't guess input format for {fp.name!r}. "
                    "Try '--input-format=FMT'.")
    else:
        raise argparse.ArgumentTypeError("unknown input format {args.input_format!r}")

    return args

# Guess the right reader based on the filename.
def guessreader(fp):
    if fp.name.endswith(".csv"):
        reader = CSVReader
    elif fp.name.endswith(".db"):
        reader = SQLiteReader
    else:
        # FIXME: better format detection!!
        # TODO: if fp is seekable, peek and figure out filetype
        reader = None
    return reader

def autoreader(fp, itemtuple, **kwargs):
    '''Convenience function to guess & instantiate the right writer'''
    reader = guessreader(fp)
    return reader(fp, itemtuple, **kwargs)

# FIXME: probably want the ItemWriters from parse-access-logs.py here
class CountWriter:
    def __init__(self, fp):
        import csv
        self._fp = fp
        self._writer = csv.writer(fp)
    def write(self, bucket, count):
        self._writer.writerow((count,)+bucket)
    @staticmethod
    def sortkey(bucketcount):
        bucket, count = bucketcount
        # Sort by date (old->new), then count (high->low), then other fields.
        return (bucket.week_start, -count) + bucket
    def writecounts(self, counts):
        for bucket, count in sorted(counts.items(), key=self.sortkey):
            self.write(bucket, count)


def main():
    args = parse_args()

    count = Counter()

    # Set up our counter and bucket-maker.
    # TODO: CountmeBucketCounter is half-assed; either full-ass it or just
    # go with a simple item_bucket function.
    #counter = CountmeBucketCounter()
    # Here's the function that finds the bucket for a given item.
    item_bucket = CountmeBucket.from_item

    # Set up writer.
    # FIXME: proper writers, append/update mode, etc.
    countwriter = CountWriter(args.output)

    for inf in args.infiles:
        for item in args.reader(inf, CountmeItem):
            bucket = item_bucket(item)
            count[bucket] += 1

    # TODO: how do we tell preliminary counts from final ones?
    # if bucket.week_start in prelim_weeks: ...
    countwriter.writecounts(count)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(3) # You know, 3, like 'C', like Ctrl-C!
