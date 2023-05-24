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

from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qsl

from .constants import COUNTME_EPOCH, MONTHIDX, WEEK_LEN

# ===========================================================================
# ====== Output item definitions and helpers ================================
# ===========================================================================


def weeknum(timestamp):
    return (int(timestamp) - COUNTME_EPOCH) // WEEK_LEN


def offset_to_timezone(offset):
    """Convert a UTC offset like -0400 to a datetime.timezone instance"""
    offmin = 60 * int(offset[1:3]) + int(offset[3:5])
    if offset[0] == "-":
        offmin = -offmin
    return timezone(timedelta(minutes=offmin))


def parse_logtime(logtime):
    # Equivalent to - but faster than - strptime_logtime.
    # It's like ~1.5usec vs 11usec, which might seem trivial but in my tests
    # the regex parser can handle like ~200k lines/sec - or 5usec/line - so
    # an extra ~10usec to parse the time field isn't totally insignificant.
    # (btw, slicing logtime by hand and using re.split are both marginally
    # slower. datetime.fromisoformat is slightly faster but not available
    # in Python 3.6 or earlier.)
    dt, off = logtime.split(" ", 1)
    date, hour, minute, second = dt.split(":", 3)
    day, month, year = date.split("/", 2)
    tz = timezone.utc if off in {"+0000", "-0000"} else offset_to_timezone(off)
    return datetime(
        int(year), MONTHIDX[month], int(day), int(hour), int(minute), int(second), 0, tz
    )


def parse_querydict(querystr):
    """Parse request query the way mirrormanager does (last value wins)"""
    return dict(parse_qsl(querystr, separator="&"))
