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
from datetime import datetime
from urllib.parse import urlparse, parse_qsl
from typing import NamedTuple, Optional

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

# Default matcher that should match any access.log line
LOG_RE = compile_log_regex()

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
# ====== Output item definitions and helpers ================================
# ===========================================================================

DAY_LEN = 24*60*60
WEEK_LEN = 7*DAY_LEN
COUNTME_EPOCH = 345600          # =00:00:00 Mon Jan 5 00:00:00 1970 (UTC)
COUNTME_EPOCH_ORDINAL = 719167  # same, as an ordinal day number

class CountmeWeek(NamedTuple):
    '''
    A `datetime`-style object representing a point in time in a
    countme-defined "week".  Week 0 started Mon Jan 5 00:00:00 1970 and each
    week is exactly 7*24*60*60 seconds long.

    Times are stored as (weeknum, weeksec) tuples representing the week number
    and the elapsed number of seconds since the start of that week.
    '''
    weeknum: int
    weeksec: int = 0 # Log times only have integer-second precision

    @classmethod
    def fromtimestamp(cls, ts):
        return cls._make(divmod(int(ts) - COUNTME_EPOCH, WEEK_LEN))
    @classmethod
    def fromordinal(cls, day):
        weeknum, weekday = divmod(day - COUNTME_EPOCH_ORDINAL, 7)
        return cls(weeknum, weekday * WEEK_LEN)
    @classmethod
    def fromlogtime(cls, logtime):
        dt = datetime.strptime(logtime, "%d/%b/%Y:%H:%M:%S %z")
        return cls._make(divmod(int(dt.timestamp()) - COUNTME_EPOCH, WEEK_LEN))
    @classmethod
    def now(cls):
        return cls.fromtimestamp(datetime.utcnow().timestamp())
    @classmethod
    def today(cls):
        return cls.fromordinal(datetime.utcnow().toordinal())

    def toordinal(self):
        return COUNTME_EPOCH_ORDINAL + (self.weeknum * 7)
    def timestamp(self):
        return COUNTME_EPOCH + (self.weeknum * WEEK_LEN) + self.weeksec
    def start_ts(self):
        return COUNTME_EPOCH + (self.weeknum * WEEK_LEN)
    def time_range(self):
        '''Return [start, end) timestamps. Like range(), 'end' is not included.'''
        start = COUNTME_EPOCH + (self.weeknum * WEEK_LEN)
        return (start, start+WEEK_LEN)
    def time_between(self):
        '''Return [start, last] timestamps. 'last' is included in the range.'''
        start = COUNTME_EPOCH + (self.weeknum * WEEK_LEN)
        return (start, start+WEEK_LEN-1)

def parse_logtime(logtime):
    '''Parse the log's 'time' string to a `datetime` object.'''
    return datetime.strptime(logtime, "%d/%b/%Y:%H:%M:%S %z")

def parse_querydict(querystr):
    '''Parse request query the way mirrormanager does (last value wins)'''
    return dict(parse_qsl(querystr))


class MirrorItem(NamedTuple):
    '''
    A basic mirrorlist/metalink metadata item.
    Each item has a timestamp, IP, and the requested repo= and arch= values.
    '''
    timestamp: int
    host: str
    repo_tag: Optional[str]
    repo_arch: Optional[str]

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

class LogMatcher:
    '''Base class for a LogMatcher, which iterates through a log file'''
    regex = NotImplemented
    itemtuple = NotImplemented
    def __init__(self, fileobj):
        self.fileobj = fileobj
    def iteritems(self):
        for line in self.fileobj:
            match = self.regex.match(line)
            if match:
                yield self.make_item(match)
    __iter__ = iteritems
    @classmethod
    def make_item(cls, match):
        raise NotImplementedError

class MirrorMatcher(LogMatcher):
    '''Match all mirrorlist/metalink items, like mirrorlist.py does.'''
    regex = MIRRORS_LOG_RE
    itemtuple = MirrorItem
    @classmethod
    def make_item(cls, match):
        timestamp = parse_logtime(match['time']).timestamp()
        query = parse_querydict(match['query'])
        return cls.itemtuple(timestamp = int(timestamp),
                             host      = match['host'],
                             repo_tag  = query.get('repo'),
                             repo_arch = query.get('arch'))

class CountmeMatcher(LogMatcher):
    '''Match the libdnf-style "countme" requests.'''
    regex = COUNTME_LOG_RE
    itemtuple = CountmeItem
    @classmethod
    def make_item(cls, match):
        timestamp = parse_logtime(match['time']).timestamp()
        query = parse_querydict(match['query'])
        return cls.itemtuple(timestamp  = int(timestamp),
                             host       = match['host'],
                             os_name    = match['os_name'],
                             os_version = match['os_version'],
                             os_variant = match['os_variant'],
                             os_arch    = match['os_arch'],
                             countme    = int(query.get('countme')),
                             repo_tag   = query.get('repo'),
                             repo_arch  = query.get('arch'))

# ===========================================================================
# ====== Output formatting classes ==========================================
# ===========================================================================

class ItemWriter:
    def __init__(self, fp, itemtuple, **kwargs):
        self._fp = fp
        self._itemtuple = itemtuple
        self._fields = itemtuple._fields
        assert "timestamp" in self._fields, f"{itemtuple.__class__.__name__!r} has no 'timestamp' field"
        self._get_writer(**kwargs)
    def _get_writer(self):
        raise NotImplementedError
    def write_item(self, item):
        raise NotImplementedError
    def write_header(self):
        pass
    def write_footer(self):
        pass

class JSONWriter(ItemWriter):
    def _get_writer(self):
        import json
        self._dump = json.dump
    def write_item(self, item):
        self._dump(item._asdict(), self._fp)

class CSVWriter(ItemWriter):
    def _get_writer(self):
        import csv
        self._writer = csv.writer(self._fp)
    def write_header(self):
        self._writer.writerow(self._fields)
    def write_item(self, item):
        self._writer.writerow(item)

class AWKWriter(ItemWriter):
    def _get_writer(self, field_separator='\t'):
        self._fieldsep = field_separator
    def _write_row(self, vals):
        self._fp.write(self._fieldsep.join(str(v) for v in vals) + '\n')
    def write_header(self):
        self._write_row(self._fields)
    def write_item(self, item):
        self._write_row(item)

class SQLiteWriter(ItemWriter):
    '''Write each item as a new row in a SQLite database table.'''
    # We have to get a little fancier with types here since SQL tables expect
    # typed values. Good thing Python has types now, eh?
    SQL_TYPE = {
        int: "INTEGER NOT NULL",
        str: "TEXT NOT NULL",
        float: "REAL NOT NULL",
        bytes: "BLOB NOT NULL",
        Optional[int]: "INTEGER",
        Optional[str]: "TEXT",
        Optional[float]: "REAL",
        Optional[bytes]: "BLOB",
    }
    def _sqltype(self, fieldname):
        typehint = self._itemtuple.__annotations__[fieldname]
        return self.SQL_TYPE.get(typehint, "TEXT")
    def _get_writer(self, tablename='countme_raw'):
        self._tablename = tablename
        import sqlite3
        self._con = sqlite3.connect(self._fp.name)
        self._cur = self._con.cursor()
        # Generate SQL commands so we can use them later.
        # self._create_table creates the table, with column names and types
        # matching the names and types of the fields in self._itemtuple.
        self._create_table = (
            "CREATE TABLE IF NOT EXISTS {table} ({coldefs})".format(
                table=tablename,
                coldefs=",".join(f"{f} {self._sqltype(f)}" for f in self._fields),
            )
        )
        # self._insert_item is an "INSERT" command with '?' placeholders.
        self._insert_item = (
            "INSERT INTO {table} ({colnames}) VALUES ({colvals})".format(
                table=tablename,
                colnames=",".join(self._fields),
                colvals=",".join("?" for f in self._fields),
            )
        )
        # self._create_time_index creates an index on 'timestamp'.
        self._create_time_index = (
            "CREATE INDEX IF NOT EXISTS timestamp_idx on {table} (timestamp)".format(
                table=tablename,
            )
        )
    def write_header(self):
        self._cur.execute(self._create_table)
    def write_item(self, item):
        self._cur.execute(self._insert_item, item)
    def write_footer(self):
        self._cur.execute(self._create_time_index)
        self._con.commit()

def make_writer(name, fp, itemtuple):
    '''Convenience function to grab/instantiate the right writer'''
    if name == "csv":
        writer = CSVWriter
    elif name == "json":
        writer = JSONWriter
    elif name == "awk":
        writer = AWKWriter
    elif name == "sqlite":
        writer = SQLiteWriter
    else:
        raise ValueError(f"Unknown writer '{name}'")
    return writer(fp, itemtuple)

# ===========================================================================
# ====== Progress meters & helpers ==========================================
# ===========================================================================

LOG_DATE_RE = compile_log_regex(time=r'(?P<date>[^:]+):.*?')
def log_date(line):
    match = LOG_DATE_RE.match(line)
    if match:
        return match['date']
    return "??/??/????"


# If we have the tqdm module available then hooray, they can do the work
class TQDMLogProgress:
    def __init__(self, logs, display=True):
        from tqdm import tqdm
        self.logs = logs
        self.disable = True if not display else None

    def __iter__(self):
        for n, logf in enumerate(self.logs):
            yield self._iter_and_count_bytes(logf, n)

    def _iter_and_count_bytes(self, logf, lognum):
        # Make a progress meter for this file
        prog = tqdm(unit="B", unit_scale=True, unit_divisor=1024,
                    total=os.stat(logf.name).st_size,
                    disable=self.disable,
                    desc=f"log {lognum+1}/{len(self.logs)}")
        # Get the first line manually so we can get logdate
        line = next(logf)
        prog.set_description(f"{prog.desc}, date={log_date(line)}")
        # Update bar and yield the first line
        prog.update(len(line))
        yield line
        # And now we do the rest of the file
        for line in logf:
            prog.update(len(line))
            yield line
        prog.close()


class DIYLogProgress:
    '''A very basic progress meter to be used when tqdm isn't available.'''
    def __init__(self, logs, display=True):
        self.logs = logs
        self.display = display
        self.desc = ''
        self._file_size = {f.name:os.stat(f.name).st_size for f in logs}
        self._total_size = sum(os.stat(f.name).st_size for f in logs)
        self._prev_read = 0
        self._total_read = 0
        self._cur_name = None
        self._cur_size = 0
        self._pct_vals = []
        self._cur_read = 0
        self._last_pct = None
        self._next_show = 0

    def __iter__(self):
        for n, logf in enumerate(self.logs):
            self.set_file(logf.name)
            yield self.iter_and_count_bytes(logf, n)
            self.end_file()

    def iter_and_count_bytes(self, logf, lognum):
        line = next(logf)
        self.desc = f"log {lognum+1}/{len(self.logs)}, date={log_date(line)}"
        self.update_bytes(len(line))
        yield line
        for line in logf:
            self.update_bytes(len(line))
            yield line

    def set_file(self, name):
        self._cur_size = self._file_size[name]
        self._pct_vals = [self._cur_size*n//100 for n in range(101)]
        self._cur_read = 0
        self._last_pct = 0
        self._next_show = self._pct_vals[1]

    def update_bytes(self, size):
        self._cur_read += size
        if self._cur_read >= self._next_show:
            self.show()

    def end_file(self):
        self._prev_read += self._cur_read
        if self._last_pct < 100: # rounding error or something...
            self.show()

    def show(self):
        cur_read = self._cur_read
        cur_size = self._cur_size
        cur_pct = 100*cur_read // cur_size
        total_size = self._total_size
        total_read = self._prev_read + cur_read
        total_pct = 100*total_read // total_size
        if self.display:
            if len(self.logs) > 1:
                print(f"{self.desc}:{cur_pct:3}%"
                      f" ({hrsize(cur_read)}/{hrsize(cur_size)}),"
                      f" total:{total_pct:3}%"
                      f" ({hrsize(total_read)}/{hrsize(total_size)})")
            else:
                print(f"{self.desc}:{cur_pct:3}%"
                      f" ({hrsize(cur_read)}/{hrsize(cur_size)})")
        self._last_pct = cur_pct
        if self._last_pct < 100:
            self._next_show = self._pct_vals[self._last_pct+1]
        self._total_read = total_read

# Formatting helper for human-readable data sizes
def hrsize(nbytes):
    for suffix in ("b", "kb", "mb", "gb"):
        if nbytes < 1000:
            break
        nbytes /= 1000
    return f"{nbytes:.1f}{suffix}"

# Formatting helper for human-readable time intervals
def hrtime(nsecs):
    m, s = divmod(int(nsecs), 60)
    if m > 60:
        h, m = divmod(m, 60)
        return f"{h:02d}h{m:02d}m{s:02d}s"
    elif m:
        return f"{m:02d}m{s:02d}s"
    else:
        return f"{s:02d}s"

# Set up LogProgress so it falls back to our own code if tqdm isn't here
try:
    from tqdm import tqdm
    LogProgress = TQDMLogProgress
except ImportError:
    LogProgress = DIYLogProgress

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

    prog = LogProgress(args.logs, display=args.progress)

    # TODO: If we're appending to an existing file, check_header() instead?
    args.writer.write_header()

    for logf in prog:
        for item in args.matcher(logf):
            args.writer.write_item(item)

    # TODO: more like writer.finish()?
    args.writer.write_footer()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(3)  # sure, 3 is good, why not
