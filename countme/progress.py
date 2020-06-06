# countme.progress: progress meters for CLI output
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

import os
from .regex import compile_log_regex, LOG_DATE_RE

__all__ = (
    'ReadProgress', 'TQDMReadProgress', 'DIYReadProgress',
)

# ===========================================================================
# ====== Progress meters & helpers ==========================================
# ===========================================================================

def log_date(line):
    match = LOG_DATE_RE.match(line)
    if match:
        return match['date']
    return "??/??/????"

class ReadProgressBase:
    def __init__(self, logs, display=True):
        '''logs should be a sequence of line-iterable file-like objects.
        if display is False, no progress output will be printed.'''
        self.logs = logs
        self.display = display

    def __iter__(self):
        '''Iterator for ReadProgress; yields a sequence of line-iterable
        file-like objects (one for each log in logs).'''
        for num, logf in enumerate(self.logs):
            self._prog_setup(logf, num)
            yield self._iter_log_lines(logf, num)
            self._prog_close(logf, num)

    def _iter_log_lines(self, logf, lognum):
        raise NotImplementedError


# Here's how we use the tqdm progress module to show read progress.
class TQDMReadProgress(ReadProgressBase):
    def _prog_setup(self, logf, num):
        # Make a progress meter for this file
        self.prog = tqdm(unit="B", unit_scale=True, unit_divisor=1024,
                         total=os.stat(logf.name).st_size,
                         disable=True if not self.display else None)

    def _iter_log_lines(self, logf, num):
        # Make a progress meter for this file
        prog = self.prog
        # Get the first line manually so we can get logdate
        line = next(logf)
        desc = f"log {num+1}/{len(self.logs)}, date={log_date(line)}"
        prog.set_description(desc)
        # Update bar and yield the first line
        prog.update(len(line))
        yield line
        # And now we do the rest of the file
        for line in logf:
            prog.update(len(line))
            yield line

    def _prog_close(self, logf, num):
        self.prog.close()

# No TQDM? Use our little do-it-yourself knockoff version.
class DIYReadProgress(TQDMReadProgress):
    def _prog_setup(self, logf, num):
        self.prog = diyprog(total=os.stat(logf.name).st_size,
                            disable=True if not self.display else None)

class diyprog:
    def __init__(self, desc=None, total=None, file=None, disable=False,
                 unit='b', unit_scale=True, barchar='_-=#'):
        # COMPAT NOTE: tqdm objects with disable=True have no .desc attribute
        self.desc = desc
        self.total = total
        self.file = file
        self.disable = disable
        self.unit = unit
        self.unit_scale = unit_scale
        self.count = 0
        self.showat = 0
        self.barchar = barchar

    def set_description(self, desc=None, refresh=True):
        self.desc = desc
        if refresh:
            self.display()

    def update(self, n=1):
        if self.disable: return
        self.count += n
        if self.count >= self.showat:
            self.showat += self.total // 100
            self.display()

    @staticmethod
    def hrsize(n):
        for suffix in 'kmgtp':
            n /= 1000
            if n < 1000:
                break
        return f"{n:.1f}{suffix}"

    @staticmethod
    def hrtime(nsecs):
        m, s = divmod(int(nsecs), 60)
        if m > 60:
            h, m = divmod(m, 60)
            return f"{h:02d}h{m:02d}m{s:02d}s"
        elif m:
            return f"{m:02d}m{s:02d}s"
        else:
            return f"{s:02d}s"

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
        bar = (pct // 4) * self.barchar[-1]
        if pct < 100:
            bar += self.barchar[pct % 4]
        print(f"{desc}: {pct:>3}% [{bar:<25}] {count:>7}/{total:<7}",
              flush=True, file=self.file, end='\r')

    def close(self):
        if self.disable: return
        print(flush=True, file=self.file)

# Default ReadProgress: use tqdm if possible, else use the DIY one
try:
    # TODO: make this work with a local tqdm checkout/git submodule
    from tqdm import tqdm
    ReadProgress = TQDMReadProgress
except ImportError:
    ReadProgress = DIYReadProgress

