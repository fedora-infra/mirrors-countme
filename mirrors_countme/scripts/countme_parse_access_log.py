#!/usr/bin/python3
# parse-access-log.py - parse Fedora httpd access_log files to structured data.
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
# Author: Will Woods <wwoods@redhat.com>
#
# The main point of this script, as it says above, is parsing access_log to
# structured data. I'm trying to avoid packing Fedora-specific data-massaging
# into this; tools further down the pipeline can be responsible for figuring
# out how to group "updates-released-f32" and "fedora-modular-source-32".

import argparse
import sys

from ..matchers import CountmeMatcher, MirrorMatcher
from ..parse import parse
from ..version import __version__
from ..writers import make_writer

# ===========================================================================
# ====== CLI parser & main() ================================================
# ===========================================================================


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Parse Fedora access.log files.",
    )
    p.add_argument("-V", "--version", action="version", version=f"%(prog)s {__version__}")

    p.add_argument("logs", metavar="LOG", nargs="+", help="access_log file(s) to parse")

    p.add_argument("--progress", action="store_true", help="print some progress info while parsing")

    p.add_argument(
        "--matchmode",
        choices=("countme", "mirrors"),
        help="match 'countme' lines (default) or all mirrors",
        default="countme",
    )

    fmt = p.add_mutually_exclusive_group(required=True)

    fmt.add_argument(
        "--sqlite",
        metavar="DBFILE",
        type=argparse.FileType("ab+"),
        help="write to a sqlite database",
    )

    fmt.add_argument(
        "-f",
        "--format",
        choices=("csv", "json", "awk"),
        help="write to stdout in text format",
    )

    p.add_argument(
        "--no-header",
        dest="header",
        default=True,
        action="store_false",
        help="No header at the start of (csv,awk) output",
    )
    p.add_argument(
        "--no-index",
        dest="index",
        default=True,
        action="store_false",
        help="Do not add an index to the sqlite database",
    )
    p.add_argument(
        "--no-dup-check",
        dest="dupcheck",
        default=True,
        action="store_false",
        help="Skip check for already-parsed log data (sqlite)",
    )

    args = p.parse_args(argv)

    # Get matcher class for the requested matchmode
    if args.matchmode == "countme":
        args.matcher = CountmeMatcher
    else:  # args.matchmode == "mirrors":
        args.matcher = MirrorMatcher

    # Make a writer object
    if args.sqlite:
        args.writer = make_writer("sqlite", args.sqlite, args.matcher.itemtuple)
    else:
        args.writer = make_writer(args.format, sys.stdout, args.matcher.itemtuple)
        args.dupcheck = False

    return args


def cli():
    try:
        args = parse_args()
        parse(
            matchmode=args.matchmode,
            matcher=args.matcher,
            sqlite=args.sqlite,
            header=args.header,
            index=args.index,
            dupcheck=args.dupcheck,
            writer=args.writer,
            logs=args.logs,
            progress=args.progress,
        )
    except KeyboardInterrupt:  # pragma: no cover
        raise SystemExit(3)  # sure, 3 is good, why not
