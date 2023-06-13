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
import json
import sqlite3

from .util import MinMaxPropMixin

# ===========================================================================
# ====== ItemWriters - output formatting classes ============================
# ===========================================================================


class ItemWriter:
    def __init__(self, fp, itemtuple, timefield="timestamp", **kwargs):
        self._fp = fp
        self._itemtuple = itemtuple
        self._fields = itemtuple._fields
        if timefield not in self._fields:
            raise ValueError(f"{itemtuple.__name__!r} has no time field {timefield!r}")
        self._timefield = timefield
        self._get_writer(**kwargs)

    def _get_writer(self, **kwargs):
        raise NotImplementedError

    def write_item(self, item):
        raise NotImplementedError

    def write_items(self, items):
        for item in items:
            self.write_item(item)

    def write_header(self):
        pass

    def commit(self):
        pass

    def write_index(self):
        pass


class JSONWriter(ItemWriter):
    def _get_writer(self, **kwargs):
        self._dump = json.dump

    def write_item(self, item):
        self._dump(item._asdict(), self._fp)


class CSVWriter(ItemWriter):
    def _get_writer(self, **kwargs):
        self._writer = csv.writer(self._fp)

    def write_header(self):
        self._writer.writerow(self._fields)

    def write_item(self, item):
        self._writer.writerow(item)


class AWKWriter(ItemWriter):
    def _get_writer(self, field_separator="\t", **kwargs):
        self._fieldsep = field_separator

    def _write_row(self, vals):
        self._fp.write(self._fieldsep.join(str(v) for v in vals) + "\n")

    def write_header(self):
        self._write_row(self._fields)

    def write_item(self, item):
        self._write_row(item)


class SQLiteWriter(ItemWriter, MinMaxPropMixin):
    """Write each item as a new row in a SQLite database table."""

    # We have to get a little fancier with types here since SQL tables expect
    # typed values. Good thing Python has types now, eh?
    SQL_TYPE = {
        int: "INTEGER NOT NULL",
        str: "TEXT NOT NULL",
        float: "REAL NOT NULL",
        bytes: "BLOB NOT NULL",
        int | None: "INTEGER",
        str | None: "TEXT",
        float | None: "REAL",
        bytes | None: "BLOB",
    }

    def _sqltype(self, fieldname):
        typehint = self._itemtuple.__annotations__[fieldname]
        return self.SQL_TYPE.get(typehint, "TEXT")

    def _get_writer(self, tablename="countme_raw", **kwargs):
        if hasattr(self._fp, "name"):
            filename = self._fp.name
        else:
            filename = self._fp
        self._connection = sqlite3.connect(f"file:{filename}?mode=rwc", uri=True)
        self._cursor = self._connection.cursor()
        self._tablename = tablename
        self._filename = filename
        # Generate SQL commands so we can use them later.
        # self._create_table creates the table, with column names and types
        # matching the names and types of the fields in self._itemtuple.
        self._create_table = "CREATE TABLE IF NOT EXISTS {table} ({coldefs})".format(
            table=tablename,
            coldefs=",".join(f"{f} {self._sqltype(f)}" for f in self._fields),
        )
        # self._insert_item is an "INSERT" command with '?' placeholders.
        self._insert_item = "INSERT INTO {table} ({colnames}) VALUES ({colvals})".format(
            table=tablename,
            colnames=",".join(self._fields),
            colvals=",".join("?" for f in self._fields),
        )
        # self._create_time_index creates an index on 'timestamp' or whatever
        # the time-series field is.
        self._create_time_index = (
            "CREATE INDEX IF NOT EXISTS {timefield}_idx ON {table} ({timefield})".format(
                table=tablename, timefield=self._timefield
            )
        )

    def write_header(self):
        self._cursor.execute(self._create_table)

    def write_item(self, item):
        self._cursor.execute(self._insert_item, item)

    def write_items(self, items):
        with self._connection:
            self._connection.executemany(self._insert_item, items)

    def commit(self):
        self._connection.commit()

    def write_index(self):
        self._cursor.execute(self._create_time_index)
        self.commit()

    def has_item(self, item):
        """Return True if a row matching `item` exists in this database."""
        condition = " AND ".join(f"{field}=?" for field in self._fields)
        cursor = self._cursor.execute(
            f"SELECT COUNT(*) FROM {self._tablename} WHERE {condition}", item
        )
        return bool(cursor.fetchone()[0])


def make_writer(name, *args, **kwargs):
    """Convenience function to grab/instantiate the right writer"""
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
    return writer(*args, **kwargs)
