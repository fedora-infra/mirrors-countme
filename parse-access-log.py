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

import os
import re
import sys
import argparse
import datetime
from urllib.parse import urlparse, parse_qsl
from collections import OrderedDict

# ===========================================================================
# ====== Regexes! Get your regexes here! ====================================
# ===========================================================================

# Log format, according to ansible/roles/httpd/proxy/templates/httpd.conf.j2:
#   LogFormat "%a %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\""
# That's the standard Combined Log Format, with numeric IPs (%a).
#
# Example log line:
#   240.159.140.173 - - [29/Mar/2020:16:04:28 +0000] "GET /metalink?repo=fedora-modular-32&arch=x86_64&countme=1 HTTP/2.0" 200 18336 "-" "libdnf (Fedora 32; workstation; Linux.x86_64)"
#
# Here it is as a Python regex, with a format placeholder for the actual field
# contents. Default field regexes are in LOG_PATTERN_FIELDS, below, and
# compile_log_regex() lets you construct more interesting regexes that only
# match the lines you care about.
# The request target is split into 'path' and 'query'; 'path' is always
# present but 'query' may be absent, depending on the value of 'query_match'.
# 'query_match' should be '?' (optional), '' (required), or '{0}' (absent).
LOG_PATTERN_FORMAT = (
    r'^'
    r'(?P<host>{host})\s'
    r'(?P<identity>{identity})\s'
    r'(?P<user>{user})\s'
    r'\[(?P<time>{time})\]\s'
    r'"(?P<method>{method})\s'
    r'(?P<path>{path})(?:\?(?P<query>{query})){query_match}'
    r'\s(?P<protocol>{protocol})"\s'
    r'(?P<status>{status})\s'
    r'(?P<nbytes>{nbytes})\s'
    r'"(?P<referrer>{referrer})"\s'
    r'"(?P<user_agent>{user_agent})"\s*'
    r'$'
)

# Pattern for a HTTP header token, as per RFC7230.
# Basically: all printable ASCII chars except '"(),/:;<=>?@[\]{}'
# (see https://tools.ietf.org/html/rfc7230#section-3.2.6)
HTTP_TOKEN_PATTERN=r"[\w\#$%^!&'*+.`|~-]+"

# Here's the default/fallback patterns for each field.
# Note that all fields are non-zero width except query, which is optional,
# and query_match, which should be '?', '', or '{0}', as described above.
LOG_PATTERN_FIELDS = {
    'host':       '\S+',
    'identity':   '\S+',
    'user':       '\S+',
    'time':       '.+?',
    'method':     HTTP_TOKEN_PATTERN,
    'path':       '[^\s\?]+',
    'query':      '\S*',
    'query_match':'?',
    'protocol':   'HTTP/\d\.\d',
    'status':     '\d+',
    'nbytes':     '\d+|-',
    'referrer':   '[^"]+',
    'user_agent': '.+?',
}

# A regex for libdnf user-agent strings.
# Examples:
#   "libdnf/0.35.5 (Fedora 32; workstation; Linux.x86_64)"
#   "libdnf (Fedora 32; generic; Linux.x86_64)"
#
# The format, according to libdnf/utils/os-release.cpp:getUserAgent():
#   f"{USER_AGENT} ({os_name} {os_version}; {os_variant}; {os_canon}.{os_arch})"
# where:
#   USER_AGENT = "libdnf" or "libdnf/{LIBDNF_VERSION}"
#   os_name    = os-release NAME
#   os_version = os-release VERSION_ID
#   os_variant = os-release VARIANT_ID
#   os_canon   = rpm %_os (via libdnf getCanonOS())
#   os_arch    = rpm %_arch (via libdnf getBaseArch())
#
# (libdnf before 0.37.2 used "libdnf/{LIBDNF_VERSION}" as USER_AGENT, but the
# version number was dropped in commit d8d0984 due to privacy concerns.)
#
# For more info on the User-Agent header, see RFC7231, Section 5.5.3:
#   https://tools.ietf.org/html/rfc7231#section-5.5.3)
LIBDNF_USER_AGENT_PATTERN = (
    r'(?P<product>libdnf(?:/(?P<product_version>\S+))?)\s+'
    r'\('
      r'(?P<os_name>.*)\s'
      r'(?P<os_version>[0-9a-z._-]*?);\s'
      r'(?P<os_variant>[0-9a-z._-]*);\s'
      r'(?P<os_canon>[\w./]+)\.'
      r'(?P<os_arch>\w+)'
    r'\)'
)
LIBDNF_USER_AGENT_RE = re.compile(LIBDNF_USER_AGENT_PATTERN)

# Helper function for making compiled log-matching regexes.
def compile_log_regex(flags=0, ascii=True, query_present=None, **kwargs):
    '''
    Return a compiled re.Pattern object that should match lines in access_log,
    capturing each field (as listed in LOG_PATTERN_FIELDS) in its own group.

    The default regex to match each field is in LOG_PATTERN_FIELDS but you
    can supply your own custom regexes as keyword arguments, like so:

        mirror_request_pattern = compile_log_regex(path='/foo.*?')

    The `flags` argument is passed to `re.compile()`. Since access_log contents
    should (according to the httpd docs) be ASCII-only, that flag is added by
    default, but you can turn that off by adding 'ascii=False'.

    If `query_present` is True, then the regex only matches lines where the
    target resource has a query string - i.e. query is required.
    If False, it only matches lines *without* a query string.
    If None (the default), the query string is optional.
    '''
    if ascii:
        flags |= re.ASCII

    fields      = LOG_PATTERN_FIELDS.copy()
    fields.update(kwargs)

    if query_present is not None:
        fields['query_match'] = '' if query_present else '{0}'

    pattern = LOG_PATTERN_FORMAT.format(**fields)

    return re.compile(pattern, flags=flags)

# Compiled pattern to match all mirrorlist/metalink hits, like mirrorlist.py
MIRRORS_LOG_RE = compile_log_regex(path=r'/metalink|/mirrorlist')

# Compiled pattern for countme lines.
# We only count:
#   * GET requests for /metalink or /mirrorlist,
#   * that have a query string containing "&countme=\d+",
#   * with libdnf's User-Agent string (see above).
COUNTME_LOG_RE = compile_log_regex(
    method        = "GET",
    query_present = True,
    path          = r'/metalink|/mirrorlist',
    query         = r'\S+&countme=\d+\S*',
    status        = r'200|302',
    user_agent    = LIBDNF_USER_AGENT_PATTERN,
)


# ===========================================================================
# ====== Helper functions for massaging matched data ========================
# ===========================================================================

# A couple little helpers for filling out important details of matched items.
def parse_logtime(logtime):
    '''Parse the log's 'time' string to a `datetime` object.'''
    return datetime.datetime.strptime(logtime, "%d/%b/%Y:%H:%M:%S %z")

def repo_proj(repostr):
    '''
    Parse a repository string and identify which project it belongs to
    ('fedora', 'epel', etc.)
    '''
    if not repostr:
        return 'none'
    words = repostr.split('-')
    if words[0] == 'fedora':
        return 'fedora'
    elif words[0] == 'updates':
        return 'fedora'
    elif words[0] == 'rawhide':
        return 'fedora'
    elif words[-1] == 'rawhide':
        return 'fedora'
    elif words[0] == 'epel':
        return 'epel'
    elif words[-1].startswith('epel'):
        return 'epel'
    else:
        return 'unknown'

def parse_countme_match(match):
    '''Make a complete log item by parsing the time and query string.'''
    item = match.groupdict()

    dt = parse_logtime(item['time'])
    item['datetime']  = dt
    item['timestamp'] = int(dt.timestamp())

    qd = dict(parse_qsl(item['query']))
    repo = qd.get('repo')
    item['querydict'] = qd
    item['repo']      = repo
    item['repo_proj'] = repo_proj(repo)
    item['repo_arch'] = qd.get('arch')
    item['countme']   = qd.get('countme')

    return item

def parse_mirrors_match(match):
    '''Make a complete log item from a MIRRORS_LOG_RE match.'''
    # Do the usual parsing of the timestamp, query, etc.
    item = parse_countme_match(match)

    # If we have a libdnf User-Agent, parse it
    ua_match = LIBDNF_USER_AGENT_RE.match(item['user_agent'])
    user_agent_dict = ua_match.groupdict() if ua_match else {}
    # Add the user_agent keys & parsed values (or Nones)
    for key in LIBDNF_USER_AGENT_RE.groupindex.keys():
        item[key] = user_agent_dict.get(key)

    return item

# ===========================================================================
# ====== Output formatting helpers  =========================================
# ===========================================================================

class ItemWriter:
    def __init__(self, fp, fields, **kwargs):
        self._fp = fp
        self._fields = fields
        self._get_writer(**kwargs)
    def _get_writer(self):
        raise NotImplementedError
    def _write_item(self, item):
        raise NotImplementedError
    def _filter_item(self, item):
        return OrderedDict((f, item.get(f)) for f in self._fields)
    def write_header(self):
        pass
    def write_footer(self):
        pass
    def write_item(self, item):
        self._write_item(self._filter_item(item))

class JSONWriter(ItemWriter):
    def _get_writer(self):
        import json
        self._dump = json.dump
    def _write_item(self, item):
        self._dump(item, self._fp)

class CSVWriter(ItemWriter):
    def _get_writer(self):
        import csv
        self._writer = csv.DictWriter(self._fp, fieldnames=self._fields, extrasaction='ignore', lineterminator='\n')
    def write_header(self):
        self._writer.writeheader()
    def _write_item(self, item):
        self._writer.writerow(item)

class AWKWriter(ItemWriter):
    def _get_writer(self, field_separator='\t'):
        self._fieldsep = field_separator
    def _write_row(self, vals):
        self._fp.write(self._fieldsep.join(str(v) for v in vals) + '\n')
    def write_header(self):
        self._write_row(self._fields)
    def _write_item(self, item):
        self._write_row(item.values())

class SQLiteWriter(ItemWriter):
    def _get_writer(self):
        import sqlite3
        self._con = sqlite3.connect(self._fp)
        self._cur = self._con.cursor()
        # We override _fields here because otherwise we'd have to dynamically
        # construct table schema and.. that's complicated, and this is a simple
        # tool. If you want to get fancier than this with your SQL, consider
        # using CSV output and writing your own importer.
        self._fields = ("timestamp", "os_name", "os_version", "os_variant",
                        "os_arch", "countme", "repo", "repo_proj", "repo_arch")
    def write_header(self):
        self._cur.execute("""
            CREATE TABLE IF NOT EXISTS countme_raw (
                timestamp  timestamp NOT NULL,
                os_name    TEXT      NOT NULL,
                os_version TEXT      NOT NULL,
                os_variant TEXT      NOT NULL,
                os_arch    TEXT      NOT NULL,
                countme    INTEGER   NOT NULL,
                repo       TEXT      NOT NULL,
                repo_proj  TEXT      NOT NULL,
                repo_arch  TEXT      NOT NULL)
        """)
        self._cur.execute("""
            CREATE INDEX IF NOT EXISTS timestamp ON countme_raw (timestamp)
        """)
    def _write_item(self, item):
        self._cur.execute("""
            INSERT INTO countme_raw VALUES (
                :timestamp,
                :os_name,
                :os_version,
                :os_variant,
                :os_arch,
                :countme,
                :repo,
                :repo_proj,
                :repo_arch)
        """, item)
    def write_footer(self):
        self._con.commit()
        self._cur.close()
        self._con.close()

# Little formatting helper for human-readable data sizes
def hrsize(nbytes):
    for suffix in ("bytes", "kb", "mb", "gb"):
        if nbytes <= 1024:
            break
        nbytes /= 1024
    return f"{nbytes:.1f}{suffix}"

# Little formatting helper for human-readable time intervals
def hrtime(nsecs):
    m, s = divmod(int(nsecs), 60)
    if m > 60:
        h, m = divmod(m, 60)
        return f"{h:02d}h{m:02d}m{s:02d}s"
    elif m:
        return f"{m:02d}m{s:02d}s"
    else:
        return f"{s:02d}s"

# Convenience function to get total size of a set of file objects
def total_data(logs):
    return sum(os.stat(f.name).st_size for f in logs)


# Here's where we parse the commandline arguments!
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

    p.add_argument("-o", "--output",
        type=argparse.FileType('wt', encoding='utf-8'),
        help="output file (default: stdout)",
        default=sys.stdout)

    p.add_argument("-f", "--format",
        choices=("csv", "json", "awk"),
        help="output format (default: csv)",
        default="csv")

    # TODO: maybe we should just pipe CSV into sqlite3..
    p.add_argument("--sqlite", metavar="DB",
        help="sqlite database to write to")

    p.add_argument("--progress", action="store_true",
        help="print some progress info while parsing")

    args = p.parse_args(argv)


    # Get matcher, item parser, and output fields for the requested matchmode.
    if args.matchmode == "countme":
        args.matcher = COUNTME_LOG_RE
        args.parse_match = parse_countme_match
        args.output_fields = (
            "timestamp",
            "os_name", "os_version", "os_variant", "os_arch",
            "countme", "repo", "repo_proj", "repo_arch",
        )
    elif args.matchmode == "mirrors":
        args.matcher = MIRRORS_LOG_RE
        args.parse_match = parse_mirrors_match
        args.output_fields = (
            "timestamp", "host",
            "os_name", "os_version", "os_variant", "os_arch",
            "countme", "repo", "repo_proj", "repo_arch",
        )

    # Make a writer object to format/write the matched items
    if args.format == "csv":
        args.writer = CSVWriter(args.output, fields=args.output_fields)
    elif args.format == "json":
        args.writer = JSONWriter(args.output, fields=args.output_fields)
    elif args.format == "awk":
        args.writer = AWKWriter(args.output, fields=args.output_fields)

    # Add the SQLiteWriter, if requested.
    # NOTE: this is kinda janky and should maybe just be a separate script
    # that imports csv...
    args.writers = [args.writer]
    if args.sqlite:
        # SQLiteWriter ignores fields, so we don't pass it here
        args.writers.append(SQLiteWriter(args.sqlite, fields=None))

    return args

# And here's our main() function, hooray
def main():
    args = parse_args()

    # Initialize some stat counters
    from time import perf_counter
    lines_read = 0
    bytes_read = 0
    bytes_total = total_data(args.logs)
    start = perf_counter()
    prog_check = 30000
    last_prog = 0.0

    # Okay, let's start parsing some stuff!
    for w in args.writers:
        w.write_header()

    for logf in args.logs:
        for line in logf:
            match = args.matcher.match(line)
            if match:
                item = args.parse_match(match)
                for w in args.writers:
                    w.write_item(item)
            # Update stats, maybe output progress info.
            # Interestingly, refactoring this out into a separate class made
            # it run like 10% slower, so I'm leaving it here.
            lines_read += 1
            bytes_read += len(line)
            if args.progress and lines_read == prog_check:
                elapsed = perf_counter() - start
                lines_per_sec = lines_read / elapsed
                if elapsed - last_prog < 1.0:
                    # oop, we're a bit early - check again in like, .1 second
                    prog_check += int(lines_per_sec)//10
                else:
                    # okay! update "next check" value and "last prog" time
                    prog_check += int(lines_per_sec)
                    last_prog = elapsed
                    # and actually print a progress update!
                    bytes_left = bytes_total - bytes_read
                    bytes_per_sec = bytes_read / elapsed
                    print(f"[{bytes_read/bytes_total:6.1%}]"
                          f" elapsed:{hrtime(elapsed):>6s}"
                          f" read:{hrsize(bytes_read):8s}"
                          f" ({lines_per_sec:.0f} lines/s,"
                          f" {hrsize(bytes_read/elapsed)}/s)"
                          f" left:~{hrtime(bytes_left/bytes_per_sec):6}")

    for w in args.writers:
        w.write_footer()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(3)  # sure, 3 is good, why not
