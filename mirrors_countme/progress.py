# mirrors_countme.progress: progress meters for CLI output
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

import gzip
import lzma
import os
import subprocess
import sys

from .regex import LOG_DATE_RE

__all__ = (
    "DIYProgress",
    "ReadProgress",
)

# ===========================================================================
# ====== Progress meters & helpers ==========================================
# ===========================================================================


def log_date(line):
    match = LOG_DATE_RE.match(line)
    if match:
        return match["date"]
    return "??/??/????"


def log_reader(logfn):
    if logfn.endswith(".xz"):
        return lzma.open(logfn, mode="rt", errors="replace")
    elif logfn.endswith(".gz"):
        return gzip.open(logfn, mode="rt", errors="replace")
    else:
        return open(logfn, mode="rt", errors="replace")


def xz_log_size(xz_filename):
    cmd = ["xz", "--list", "--robot", xz_filename]
    r = subprocess.run(cmd, stdout=subprocess.PIPE)
    if r.returncode != 0:
        return None
    for line in r.stdout.split(b"\n"):
        f = line.split(b"\t")
        if f[0] == b"totals":
            return int(f[4])
    return None  # pragma: no cover


def gz_log_size(gz_filename):
    cmd = ["gzip", "--quiet", "--list", gz_filename]
    r = subprocess.run(cmd, stdout=subprocess.PIPE)
    if r.returncode != 0:
        return None
    csize, uncsize, ratio, name = r.stdout.split()
    return int(uncsize)


def log_total_size(logfn):
    if logfn.endswith(".xz"):
        return xz_log_size(logfn)
    elif logfn.endswith(".gz"):
        return gz_log_size(logfn)
    else:
        try:
            return os.stat(logfn).st_size
        except Exception:
            return None


class DIYProgress:
    def __init__(
        self,
        desc=None,
        total=None,
        file=None,
        disable=False,
        unit="b",
        unit_scale=True,
        barchar="_-=#",
    ):
        self.desc = desc
        self.total = total
        self.file = sys.stderr if file is None else file
        self.disable = disable
        self.unit = unit
        self.unit_scale = unit_scale
        self.count = 0
        self.showat = 0
        self.barchar = barchar

    def set_description(self, desc=None, refresh=True):
        self.desc = desc
        if refresh and not self.disable:
            self.display()

    def update(self, n=1):
        if self.disable:
            return
        self.count += n
        if self.count >= self.showat:
            self.showat = min(self.total, self.showat + self.total // 100)
            self.display()

    def iter(self, iterable):
        for i in iterable:
            yield i
            self.update()

    @staticmethod
    def hrsize(n):
        for suffix in " kmgtp":
            if n < 1000:
                break
            if suffix != "p":
                n /= 1000
        if suffix == " ":
            suffix = ""
        return f"{n:.1f}{suffix}"

    def display(self):
        unit = self.unit
        desc = self.desc
        if self.unit_scale:
            count = self.hrsize(self.count) + unit
            total = self.hrsize(self.total) + unit
        else:
            count = str(self.count) + unit
            total = str(self.total) + unit
        pct = (self.count * 100) // self.total
        bar = (min(pct, 100) // 4) * self.barchar[-1]
        if pct < 100:
            bar += self.barchar[pct % 4]
        print(
            f"{desc}: {pct:>3}% [{bar:<25}] {count:>7}/{total:<7}",
            flush=True,
            file=self.file,
            end="\r",
        )

    def close(self):
        if self.disable:
            return
        print(flush=True, file=self.file)


class ReadProgress:
    def __init__(self, logs, display=True):
        """logs should be a sequence of line-iterable file-like objects.
        if display is False, no progress output will be printed."""
        self.logs = logs
        self.display = display

    def __iter__(self):
        """Iterator for ReadProgress; yields a sequence of line-iterable
        file-like objects (one for each log in logs)."""
        for num, logfn in enumerate(self.logs):
            logf = log_reader(logfn)
            total = log_total_size(logfn)
            yield self._iter_log_lines(logf, num, total)

    def _progress_obj(self, *args, **kwargs):
        return DIYProgress(*args, **kwargs)

    def _iter_log_lines(self, logf, num, total):
        # Make a progress meter for this file
        prog = self._progress_obj(
            unit="b",
            unit_scale=True,
            total=total,
            disable=not self.display or not total,
        )

        for i, line in enumerate(logf):
            if i == 0:
                # Set log date from first processed line
                desc = f"log {num+1}/{len(self.logs)}, date={log_date(line)}"
                prog.set_description(desc)
            prog.update(len(line))
            yield line
        prog.close()
