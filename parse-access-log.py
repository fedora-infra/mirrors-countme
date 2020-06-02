#!/usr/bin/python3
# parse-access-log.py - parse Fedora httpd access_log files to structured data.
#
# Copyright (C) 2020, Red Hat Inc.
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

import sys
import argparse

from countme import CountmeMatcher, MirrorMatcher, make_writer

from countme.progress import ReadProgress

# ===========================================================================
# ====== CLI parser & main() ================================================
# ===========================================================================

def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description = "Parse Fedora access.log files.",
    )
    p.add_argument("-V", "--version", action='version',
        version='%(prog)s 0.0.1')

    # TODO: test whether reading as binary is faster?
    p.add_argument("logs", metavar="LOG",
        type=argparse.FileType('rt', encoding='utf-8'), nargs='+',
        help="access_log file to parse")

    p.add_argument("--matchmode",
        choices=("countme", "mirrors"),
        help="match 'countme' lines only, or 'mirrors' to match all mirrors",
        default="countme")

    # TODO: write to .OUTPUT.part and move it into place when finished
    p.add_argument("-o", "--output",
        type=argparse.FileType('at', encoding='utf-8'),
        help="output file (default: stdout)",
        default=sys.stdout)

    p.add_argument("-f", "--format",
        choices=("csv", "json", "awk", "sqlite"),
        help="output format (default: csv)",
        default="csv")

    p.add_argument("--progress", action="store_true",
        help="print some progress info while parsing")

    args = p.parse_args(argv)

    # Get matcher class for the requested matchmode
    if args.matchmode == "countme":
        args.matcher = CountmeMatcher
    elif args.matchmode == "mirrors":
        args.matcher = MirrorMatcher

    # Make a writer object
    args.writer = make_writer(args.format, args.output, args.matcher.itemtuple)

    return args


def main():
    args = parse_args()

    # TODO: If we're appending to an existing file, check_header() instead?
    args.writer.write_header()

    for logf in ReadProgress(args.logs, display=args.progress):
        for item in args.matcher(logf):
            args.writer.write_item(item)

    # TODO: more like writer.finish()?
    args.writer.write_footer()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(3)  # sure, 3 is good, why not
