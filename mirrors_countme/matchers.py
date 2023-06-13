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

import sys
from typing import Type

from .output_items import CountmeItem, MirrorItem
from .regex import COUNTME_LOG_RE, MIRRORS_LOG_RE
from .util import parse_logtime, parse_querydict


class LogMatcher:
    """Base class for a LogMatcher, which iterates through a log file"""

    regex = NotImplemented
    itemtuple: Type[MirrorItem] | Type[CountmeItem]

    def __init__(self, fileobj):
        self.fileobj = fileobj

    def iteritems(self):
        # TODO: at this point we're single-threaded and CPU-bound;
        # multithreading would speed things up here.
        for line in self.fileobj:
            match = self.regex.match(line)
            if match:
                try:
                    yield self.make_item(match)
                except Exception:
                    # Paper over any conversion errors
                    print(f"IGNORING MALFORMED LINE: {line!r}", file=sys.stderr)

    __iter__ = iteritems

    @classmethod
    def make_item(cls, match):
        raise NotImplementedError


class MirrorMatcher(LogMatcher):
    """Match all mirrorlist/metalink items, like mirrorlist.py does."""

    regex = MIRRORS_LOG_RE
    itemtuple = MirrorItem

    @classmethod
    def make_item(cls, match):
        timestamp = parse_logtime(match["time"]).timestamp()
        query = parse_querydict(match["query"])
        return cls.itemtuple(
            timestamp=int(timestamp),
            host=match["host"],
            repo_tag=query.get("repo"),
            repo_arch=query.get("arch"),
        )


class CountmeMatcher(LogMatcher):
    """Match the libdnf-style "countme" requests."""

    regex = COUNTME_LOG_RE
    itemtuple = CountmeItem

    @classmethod
    def make_item(cls, match):
        timestamp = parse_logtime(match["time"]).timestamp()
        query = parse_querydict(match["query"])
        return cls.itemtuple(
            timestamp=int(timestamp),
            host=match["host"],
            os_name=match["os_name"],
            os_version=match["os_version"],
            os_variant=match["os_variant"],
            os_arch=match["os_arch"],
            sys_age=int(query.get("countme", -1)),
            repo_tag=query.get("repo"),
            repo_arch=query.get("arch"),
        )
