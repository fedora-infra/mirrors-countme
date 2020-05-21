#!/usr/bin/python

import argparse
import datetime
from collections import namedtuple, Counter

COUNTME_OFFSET = 345600       # 00:00:00 Mon Jan 5 00:00:00 1970
COUNTME_WINDOW = 7*24*60*60   # Exactly 7 days

def weektuple(ts):
    '''Return (week_num, week_secs) for a given timestamp'''
    return divmod(int(ts)-COUNTME_OFFSET, COUNTME_WINDOW)

COUNTME_SCHEMA = """
    CREATE TABLE week_total_item (
        weeknum    INTEGER   NOT NULL,
        countme    INTEGER   NOT NULL,
        os_name    TEXT      NOT NULL,
        os_version TEXT      NOT NULL,
        os_variant TEXT      NOT NULL,
        os_arch    TEXT      NOT NULL,
        repo_tag   TEXT      NOT NULL,
        repo_arch  TEXT      NOT NULL,
        count      INTEGER   NOT NULL,
        UNIQUE (weeknum, countme, os_name, os_version, os_variant, os_arch, repo_tag, repo_arch, count)
    );
"""

class ReaderError(RuntimeError):
    pass

class ItemReader:
    def __init__(self, fp, **kwargs):
        self._fp = fp
        self._fields = None
        self._get_reader(**kwargs)
        if not self._fields:
            raise ReaderError("no field names found")
        self._itemclass = namedtuple(self.__class__.__name__+"Item", self._fields)
        self._itemfactory = self._itemclass._make
    def __iter__(self):
        for item in self._iter_rows():
            yield self._itemfactory(item)
    def _get_reader(self):
        raise NotImplementedError
    def _iter_rows(self):
        raise NotImplementedError

class CSVReader(ItemReader):
    def _get_reader(self, **kwargs):
        import csv
        self._reader = csv.reader(self._fp)
        self._fields = tuple(next(self._reader))
        # If we have numbers in our fieldnames, probably there was no header
        if any(name.isnumeric() for name in self._fields):
            header = ','.join(fields)
            raise ReaderError(f"header bad/missing, got: {header}")
    def _iter_rows(self):
        header = next(self._rowreader)
        return self._rowreader

class SQLiteReader(ItemReader):
    def _get_reader(self, table_name, **kwargs):
        import sqlite3
        self._con = sqlite3.connect(self._fp)
        self._cur = self._con.cursor()
        self._table_name = table_name
        fields_sql = f"SELECT name FROM pragma_table_info({self._table_name})"
        self._fields = tuple(r[0] for r in self._cur.execute(fields_sql))
    def _iter_rows(self):
        fields = ",".join(self._fields)
        return self._cur.execute(f"SELECT {fields} FROM {self._table_name}")


class BucketCounter:
    def __init__(self, itemfields, bucketfields):
        self._itemfields = itemfields
        self._bucketfields = bucketfields
        self._count = Counter()

    def ingest(self, iterable):
        pass


# TODO: generalize this to work with whatever fields we get out of the reader
COUNTME_FIELDS = (
    "timestamp",
    "countme", "os_name", "os_version", "os_variant", "os_arch",
    "repo_tag", "repo_arch"
)

class CountmeItem(namedtuple("CountmeItem", COUNTME_FIELDS)):
    pass

class CountmeBucket(namedtuple("CountmeBucket", ("weeknum",)+CountmeItem._fields[1:])):
    @classmethod
    def from_item(cls, item):
        weeknum, _ = weektuple(item.timestamp)
        return cls._make((weeknum,)+item[1:])

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
        type=argparse.FileType('wt', encoding='utf-8'),
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

    # TODO: sqlite output, wheeee
    #p.add_argument("--sqlite", metavar="DB",
    #    help="sqlite database to write to")

    # TODO: use this
    p.add_argument("--progress", action="store_true",
        help="print some progress info while counting")

    args = p.parse_args(argv)

    # TODO: set args.reader based on

    return args

# Guess the right reader based on the filename.
# TODO: we could probably use better heuristics than this.
# Like looking for the sqlite file magic, for instance...
def itemreader(fp):
    # TODO: if fp is seekable (or peekable?) peek and figure out filetype
    if fp.name.endswith(".csv"):
        return CSVReader(fp)
    elif fp.name.endswith(".db"):
        return SQLiteReader(fp)

def main():
    args = parse_args()

    get_bucket = CountmeBucket.from_item

    count = Counter()

    for inf in infiles:
        reader = args.reader(inf)
        # TODO: make sure reader._fields doesn't change on us
        for item in args.reader(inf):
            bucket = get_bucket(item)
            count[bucket] += 1



if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(3) # You know, 3, like 'C', like Ctrl-C!
