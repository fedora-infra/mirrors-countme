#!/usr/bin/python3
# countme-trim-raw.py - Trim the raw.db file to the next week start.
#
# Copyright Red Hat Inc.
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

import sys
import time
import argparse

import sqlite3

import mirrors_countme

# ===========================================================================
# ====== CLI parser & main() ================================================
# ===========================================================================


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Parse Fedora access.log files.",
    )
    p.add_argument("-V", "--version", action="version", version="%(prog)s 0.0.1")

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
def mintime(cur):
    cur = cur.execute("SELECT MIN(timestamp) FROM countme_raw")
    return cur.fetchone()[0]

# Find the next week to trim, given the earliest timestamp.
def next_week(mintime):
    beg = mirrors_countme.COUNTME_EPOCH
    while beg <= mintime:
        beg += mirrors_countme.WEEK_LEN
    # Now beg is the first week _after_ the mintime.
    return beg

def _num_entries_before(cur, timestamp):
    cur = cur.execute("SELECT COUNT(*) FROM countme_raw WHERE timestamp < ?", (timestamp,))
    return cur.fetchone()[0]

def _num_entries(cur):
    cur = cur.execute("SELECT COUNT(*) FROM countme_raw")
    return cur.fetchone()[0]

def _del_entries_before(con, timestamp):
    con.execute("DELETE FROM countme_raw WHERE timestamp < ?", (timestamp,))
    con.commit()

def tm2ui(timestamp):
    tm = time.gmtime(timestamp)
    return time.strftime("%Y-%m-%d %H:%M:%S", tm)

import locale
locale.setlocale(locale.LC_ALL, '')
def num2ui(num):
    ret = locale.format_string('%d', num, grouping=True)
    mlen = len("100,000,000")
    if len(ret) < mlen:
        ret = " " * (mlen-len(ret)) + ret
    return ret


def get_trim_data(sqlite_filename):
    connection = sqlite3.connect(f"file:{sqlite_filename}?mode=rwc", uri=True)
    mintime = mintime(connection)
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
