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

from typing import NamedTuple

from .util import parse_logtime

# ===========================================================================
# ====== Output items =======================================================
# ===========================================================================


class LogItem(NamedTuple):
    """
    Generic access.log data holder.
    """

    host: str
    identity: str
    time: str
    method: str
    path: str
    query: str | None
    protocol: str
    status: int
    nbytes: int | None
    referrer: str
    user_agent: str

    def datetime(self):
        return parse_logtime(self.time)

    def timestamp(self):
        return parse_logtime(self.time).timestamp()


# TODO: would be kinda nice if there was a clear subclass / translation
# between item classes... or if compile_log_regex made the class for you?
# Or something? It feels like these things should be more closely bound.


class MirrorItem(NamedTuple):
    """
    A basic mirrorlist/metalink metadata item.
    Each item has a timestamp, IP, and the requested repo= and arch= values.
    """

    timestamp: int
    host: str
    repo_tag: str | None
    repo_arch: str | None


class CountmeItem(NamedTuple):
    """
    A "countme" match item.
    Includes the countme value and libdnf User-Agent fields.
    """

    timestamp: int
    host: str
    os_name: str
    os_version: str
    os_variant: str
    os_arch: str
    sys_age: int
    repo_tag: str
    repo_arch: str
