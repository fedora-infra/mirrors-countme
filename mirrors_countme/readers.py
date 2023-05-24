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

import csv
import sqlite3

# ===========================================================================
# ====== ItemReaders - counterpart to ItemWriter ============================
# ===========================================================================


class ReaderError(RuntimeError):
    pass


class ItemReader:
    def __init__(self, fp, itemtuple, **kwargs):
        self._fp = fp
        self._itemtuple = itemtuple
        self._itemfields = itemtuple._fields
        self._itemfactory = itemtuple._make
        self._get_reader(**kwargs)
        filefields = self._get_fields()
        if not filefields:
            raise ReaderError("no field names found")
        if filefields != self._itemfields:
            raise ReaderError(f"field mismatch: expected {self._itemfields}, got {filefields}")

    @property
    def fields(self):
        return self._itemfields

    def _get_reader(self):
        """Set up the ItemReader."""
        raise NotImplementedError

    def _get_fields(self):
        """Called immediately after _get_reader().
        Should return a tuple of the fieldnames found in self._fp."""
        raise NotImplementedError

    def _iter_rows(self):
        """Return an iterator/generator that produces a row for each item."""
        raise NotImplementedError

    def _find_item(self, item):
        """Return True if the given item is in this file"""
        raise NotImplementedError

    def __iter__(self):
        for item in self._iter_rows():
            yield self._itemfactory(item)

    def __contains__(self, item):
        return self._find_item(item)


class CSVReader(ItemReader):
    def _get_reader(self, **kwargs):
        self._reader = csv.reader(self._fp)

    def _get_fields(self):
        filefields = tuple(next(self._reader))
        # Sanity check: if any fieldname is a number... this isn't a header
        if any(name.isnumeric() for name in filefields):
            header = ",".join(filefields)
            raise ReaderError(f"header bad/missing: expected {self._itemfields}, got {header!r}")
        return filefields

    def _iter_rows(self):
        return self._reader

    def _dup(self):
        # This is pretty gross, but then, so's CSV
        return self.__class__(open(self._fp.name, "rt"), self._itemtuple)

    def _find_item(self, item):
        stritem = self._itemfactory(str(v) for v in item)
        return stritem in self._dup()  # O(n) worst case. Again: gross.


class SQLiteReader(ItemReader):
    def _get_reader(self, tablename="countme_raw", timefield="timestamp", **kwargs):
        if hasattr(self._fp, "name"):
            filename = self._fp.name
        else:
            filename = self._fp
        # self._connection = sqlite3.connect(f"file:{filename}?mode=ro", uri=True)
        self._connection = sqlite3.connect(filename)
        self._cursor = self._connection.cursor()
        self._tablename = tablename
        self._timefield = timefield
        self._filename = filename

    def _get_fields(self):
        fields_sql = f"PRAGMA table_info('{self._tablename}')"
        filefields = tuple(r[1] for r in self._cursor.execute(fields_sql))
        return filefields

    def _find_item(self, item):
        condition = " AND ".join(f"{field}=?" for field in self.fields)
        self._cursor.execute(f"SELECT COUNT(*) FROM {self._tablename} WHERE {condition}", item)
        return bool(self._cursor.fetchone()[0])

    def _iter_rows(self):
        fields = ",".join(self._itemfields)
        return self._cursor.execute(f"SELECT {fields} FROM {self._tablename}")

    @property
    def mintime(self):
        cursor = self._cursor.execute(f"SELECT MIN({self._timefield}) FROM {self._tablename}")
        return cursor.fetchone()[0]

    @property
    def maxtime(self):
        cursor = self._cursor.execute(f"SELECT MAX({self._timefield}) FROM {self._tablename}")
        return cursor.fetchone()[0]


# Guess the right reader based on the filename.
def guessreader(fp):
    if fp.name.endswith(".csv"):
        reader = CSVReader
    elif fp.name.endswith(".db"):
        reader = SQLiteReader
    else:
        # FIXME: better format detection!!
        # TODO: if fp is seekable, peek and figure out filetype
        reader = None
    return reader


# TODO: should have name/args more like make_writer...
def autoreader(fp, itemtuple, **kwargs):
    """Convenience function to guess & instantiate the right writer"""
    reader = guessreader(fp)
    return reader(fp, itemtuple, **kwargs)
