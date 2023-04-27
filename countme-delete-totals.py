#!/usr/bin/python3
# countme-delete-totals.py - Delete the last week from the totals.db file.
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
# The main point of this script is to remove the last weeknum of data from
# totals.db. If you then run it again it'll remove the next week.

import sys
import time
import argparse

import sqlite3

import mirrors_countme
import mirrors_countme.totals

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

def last_week(connection):
    cursor = connection.execute("SELECT MAX(weeknum) FROM countme_totals")
    return cursor.fetchone()[0]

def _num_entries_for(connection, weeknum):
    cursor = connection.execute("SELECT COUNT(*) FROM countme_totals WHERE weeknum = ?", (weeknum,))
    return cursor.fetchone()[0]

def _num_entries(connection):
    cursor = connection.execute("SELECT COUNT(*) FROM countme_totals")
    return cursor.fetchone()[0]

def _del_entries_for(connection, weeknum):
    connection.execute("DELETE FROM countme_totals WHERE weeknum = ?", (weeknum,))
    connection.commit()

def tm2ui(timestamp):
    tm = time.gmtime(timestamp)
    return time.strftime("%Y-%m-%d %H:%M:%S", tm)

def weeknum2tm(weeknum):
    ret = mirrors_countme.COUNTME_EPOCH
    return ret + int(weeknum) * mirrors_countme.WEEK_LEN

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
    week = last_week(connection)

    print("Next week     :", week, tm2ui(weeknum2tm(week)))
    print("Entries       :", num2ui(_num_entries(connection)))
    print("Entries to del:", num2ui(_num_entries_for(connection, week)))

    return connection, week

def trim_data(connection, week):
    print(" ** About to DELETE data. **")
    time.sleep(5)
    _del_entries_for(connection, week)

if __name__ == "__main__":
    try:
        args = parse_args()
        connection, week = get_trim_data(args.sqlite)
        if args.rw:
            trim_data(connection, week)
    except KeyboardInterrupt:
        raise SystemExit(3)  # sure, 3 is good, why not
