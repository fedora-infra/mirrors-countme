#!/usr/bin/python3

import sys
import argparse
from collections import Counter
from typing import NamedTuple
from countme import CountmeItem, weeknum, guessreader, autoreader, make_writer

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

# Here's the "bucket" we sort each item into.
class CountmeBucket(NamedTuple):
    '''
    This defines the fields that we use to group/aggregate CountmeItems.
    '''
    weeknum: int
    os_name: str
    os_version: str
    os_variant: str
    os_arch: str
    countme: int
    repo_tag: str
    repo_arch: str

    @classmethod
    def from_item(cls, item):
        return cls._make((weeknum(item.timestamp),) + item[2:])

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

    # TODO: atomic creation/update of output file?
    p.add_argument("-o", "--output",
        type=argparse.FileType('at', encoding='utf-8'),
        help="output file (default: stdout)",
        default=sys.stdout)

    # TODO: flag to write prelim data to a different file/table; otherwise,
    # don't include prelim data

    p.add_argument("-f", "--format",
        choices=("csv", "json", "awk", "sqlite"),
        help="output format (default: csv)",
        default="csv")

    p.add_argument("--input-format", choices=("csv", "sqlite", "auto"),
        help="input file format (default: guess from extension)",
        default="auto")

    args = p.parse_args(argv)

    # Pick the right reader factory
    if args.input_format == "csv":
        args.reader = CSVReader
    elif args.input_format == "sqlite":
        args.reader = SQLiteReader
    elif args.input_format == "auto":
        # Check that we can figure out the right reader(s) before we start..
        for fp in args.infiles:
            if guessreader(fp) is None:
                raise argparse.ArgumentTypeError(
                    "Can't guess input format for {fp.name!r}. "
                    "Try '--input-format=FMT'.")
        args.reader = autoreader
    else:
        raise argparse.ArgumentTypeError("unknown input format {args.input_format!r}")

    # TODO: if writing to existing file, check & bail out if field mismatch

    return args

class CountWriter:
    '''Like ItemWriter, but for count buckets'''
    def __init__(self, outformat, fp, bucketclass):
        self._fp = fp
        self._bucketclass = bucketclass
        self._countclass = NamedTuple(bucketclass.__name__ + "Count",
                                      [("count", int)] + list(bucketclass.__annotations__.items()))
        # TODO: countme_prelim "table" for prelim output
        self._writer = make_writer(outformat, self._fp, self._countclass, timefield='weeknum', tablename='countme_totals')
    @staticmethod
    def sortkey(bucketcount):
        bucket, count = bucketcount
        # Sort by date (old->new), then count (high->low), then other fields.
        return (bucket.weeknum, -count) + bucket
    def writecounts(self, counts):
        self._writer.write_header()
        for bucket, count in sorted(counts.items(), key=self.sortkey):
            countitem = self._countclass._make((count,)+bucket)
            self._writer.write_item(countitem)
        self._writer.write_footer()


def main():
    args = parse_args()

    # Just a plain ol' Counter
    count = Counter()

    # Here's the function that finds the bucket for a given item.
    item_bucket = CountmeBucket.from_item

    # Initialize the writer (better to fail early than after all the counting)
    writer = CountWriter(args.format, args.output, CountmeBucket)

    # Okay, start reading and counting!
    for inf in args.infiles:
        for item in args.reader(inf, CountmeItem):
            bucket = item_bucket(item)
            count[bucket] += 1

    # TODO: how do we split preliminary counts from final ones?

    writer.writecounts(count)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(3) # You know, 3, like 'C', like Ctrl-C!
