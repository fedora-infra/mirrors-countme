#!/usr/bin/python3
# countme-trim-raw.py - Trim the raw.db file to the next week start.
#
# Copyright Red Hat
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
# Author: James Antill <jantill@redhat.com>
#
# The main point of this script is to remove all the data from raw.db
# upto the next week start. If you then run it again it'll remove the next week.
# Uses mindate() but then runs direct SQL.

import argparse
import datetime as dt
import locale
import math
import sqlite3
import time

from ..constants import COUNTME_EPOCH, WEEK_LEN
from ..version import __version__

CONF_NON_RECENT_DURATION_WEEKS = 13
WARN_SECONDS = 5

locale.setlocale(locale.LC_ALL, "")

# ===========================================================================
# ====== CLI parser & main() ================================================
# ===========================================================================


def positive_int(value):
    value = int(value)
    if value < 1:
        raise ValueError("Value must be > 1")
    return value


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Trim data from raw.db files.",
    )
    p.add_argument("-V", "--version", action="version", version=f"%(prog)s {__version__}")

    p.add_argument(
        "sqlite",
        metavar="DBFILE",
        help="Write to a sqlite database.",
    )
    p.add_argument(
        "--rw",
        "--read-write",
        dest="rw",
        default=False,
        action="store_true",
        help="Actually delete the entries.",
    )
    p.add_argument(
        "--noop",
        dest="rw",
        default=False,
        action="store_false",
        help="Don’t actually delete the entries. (default)",
    )

    p.add_argument(
        "--oldest-week", action="store_true", help="Trim the oldest (full) week of data."
    )
    p.add_argument(
        "keep",
        metavar="KEEP",
        type=positive_int,
        nargs="?",
        default=CONF_NON_RECENT_DURATION_WEEKS,
        help=(
            f"Only trim data older than KEEP weeks ago. (default: {CONF_NON_RECENT_DURATION_WEEKS})"
        ),
    )

    args = p.parse_args(argv)

    return p, args


# Mostly borrowed from mirrors_countme/__init__
def get_mintime(connection: sqlite3.Connection):
    cursor = connection.execute("SELECT MIN(timestamp) FROM countme_raw")
    return cursor.fetchone()[0]


def get_maxtime(connection: sqlite3.Connection):
    cursor = connection.execute("SELECT MAX(timestamp) FROM countme_raw")
    return cursor.fetchone()[0]


# Find the next week to trim, given the earliest timestamp.
def next_week(mintime: int | float) -> int:
    week_num = math.floor((mintime - COUNTME_EPOCH) / WEEK_LEN) + 1
    return COUNTME_EPOCH + week_num * WEEK_LEN


def _num_entries(connection: sqlite3.Connection, trim_begin: int | float, trim_end: int | float):
    cursor = connection.execute(
        "SELECT COUNT(*) FROM countme_raw WHERE timestamp >= ? AND timestamp < ?",
        (trim_begin, trim_end),
    )
    return cursor.fetchone()[0]


def _del_entries(connection: sqlite3.Connection, trim_begin: int | float, trim_end: int | float):
    connection.execute(
        "DELETE FROM countme_raw WHERE timestamp >= ? AND timestamp < ?", (trim_begin, trim_end)
    )
    connection.commit()


def tm2ui(timestamp):
    date = dt.datetime.fromtimestamp(timestamp, tz=dt.UTC).date()
    return date.isoformat()


def trim_data(
    *, connection: sqlite3.Connection, trim_begin: int | float, trim_end: int | float, rw: bool
):
    num_affected = _num_entries(connection, trim_begin, trim_end)
    if rw:
        print(f" ** About to DELETE data from {tm2ui(trim_begin)} to {tm2ui(trim_end)}. **")
        print(f" ** This will affect {num_affected} entries. **")
        print(f" ** Interrupt within {WARN_SECONDS} seconds to prevent that. **")
        time.sleep(WARN_SECONDS)
        print(" ** DELETING data … **")
        _del_entries(connection, trim_begin, trim_end)
        print(" ** Done. **")
    else:
        print(f" ** Not deleting data from {tm2ui(trim_begin)} to {tm2ui(trim_end)}. **")
        print(f" ** This would affect {num_affected} entries. **")


# Real main...
def _main():
    parser, args = parse_args()

    sqlite_uri = f"file:{args.sqlite}?mode=rwc"
    connection = sqlite3.connect(sqlite_uri, uri=True)

    # Find out what timespan is covered by data in database.
    min_time = get_mintime(connection)
    max_time = get_maxtime(connection)

    # Calculate last monday midnight UTC in data, to be used for calculating whole week boundaries.
    last = dt.datetime.fromtimestamp(max_time, tz=dt.UTC)
    last_midnight = last.replace(hour=0, minute=0, second=0, microsecond=0)
    last_monday_midnight = last_midnight - dt.timedelta(days=last_midnight.weekday())
    # Add 1 week because the last week contained likely isn’t complete.
    cutoff_monday_midnight = last_monday_midnight - dt.timedelta(weeks=args.keep + 1)

    trim_begin = min_time
    trim_end = max(cutoff_monday_midnight.timestamp(), trim_begin)
    if args.oldest_week:
        trim_end = min(next_week(trim_begin), trim_end)

    trim_data(connection=connection, trim_begin=trim_begin, trim_end=trim_end, rw=args.rw)


def cli():
    try:
        _main()
    except KeyboardInterrupt:
        raise SystemExit(3)  # sure, 3 is good, why not
