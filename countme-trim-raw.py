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
import locale
import sqlite3
import time

import mirrors_countme
from mirrors_countme.version import __version__

locale.setlocale(locale.LC_ALL, "")

# ===========================================================================
# ====== CLI parser & main() ================================================
# ===========================================================================


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Parse Fedora access.log files.",
    )
    p.add_argument("-V", "--version", action="version", version=f"%(prog)s {__version__}")

    p.add_argument(
        "--sqlite",
        metavar="DBFILE",
        help="write to a sqlite database",
    )
    p.add_argument(
        "--noop",
        dest="rw",
        default=True,
        action="store_false",
        help="Skip deleting the entries.",
    )

    args = p.parse_args(argv)

    return args


# Mostly borrowed from mirrors_countme/__init__
def get_mintime(connection):
    cursor = connection.execute("SELECT MIN(timestamp) FROM countme_raw")
    return cursor.fetchone()[0]


# Find the next week to trim, given the earliest timestamp.
def next_week(mintime):
    begin = mirrors_countme.COUNTME_EPOCH
    while begin <= mintime:
        begin += mirrors_countme.WEEK_LEN
    # Now begin is the first week _after_ the mintime.
    return begin


def _num_entries_before(connection, timestamp):
    cursor = connection.execute(
        "SELECT COUNT(*) FROM countme_raw WHERE timestamp < ?", (timestamp,)
    )
    return cursor.fetchone()[0]


def _num_entries(connection):
    cursor = connection.execute("SELECT COUNT(*) FROM countme_raw")
    return cursor.fetchone()[0]


def _del_entries_before(connection, timestamp):
    connection.execute("DELETE FROM countme_raw WHERE timestamp < ?", (timestamp,))
    connection.commit()


def tm2ui(timestamp):
    tm = time.gmtime(timestamp)
    return time.strftime("%Y-%m-%d %H:%M:%S", tm)


def num2ui(num):
    ret = locale.format_string("%d", num, grouping=True)
    mlen = len("100,000,000")
    if len(ret) < mlen:
        ret = " " * (mlen - len(ret)) + ret
    return ret


def get_trim_data(sqlite_filename):
    connection = sqlite3.connect(f"file:{sqlite_filename}?mode=rwc", uri=True)
    mintime = get_mintime(connection)
    week = next_week(mintime)

    print("First timestamp:", tm2ui(mintime))
    print("Next week      :", tm2ui(week))
    print("Entries        :", num2ui(_num_entries(connection)))
    print("Entries to trim:", num2ui(_num_entries_before(connection, week)))

    return connection, week


def trim_data(connection, week):
    print(" ** About to DELETE data. **")
    time.sleep(5)
    _del_entries_before(connection, week)


if __name__ == "__main__":
    try:
        args = parse_args()
        connection, week = get_trim_data(args.sqlite)
        if args.rw:
            trim_data(connection, week)
    except KeyboardInterrupt:
        raise SystemExit(3)  # sure, 3 is good, why not
