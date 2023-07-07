import json
from contextlib import nullcontext
from io import StringIO
from typing import NamedTuple
from unittest import mock

import pytest

from mirrors_countme import writers


class Boo(NamedTuple):
    timestamp: int


@pytest.fixture
def item_writer_file(request, tmp_path):
    if issubclass(request.instance.writer_cls, writers.SQLiteWriter):
        # SQLiteWriter._get_writer() doesn’t cope well with StringIO and drops empty garbage files
        # into the current directory, give it a file path instead.
        return str(tmp_path / "test.db")
    else:
        return StringIO()


@pytest.fixture
def item_writer(item_writer_file, request):
    writer_cls = request.instance.writer_cls

    if writer_cls is writers.ItemWriter:
        mock_ctx = mock.patch.object(writers.ItemWriter, "_get_writer")
    else:
        mock_ctx = nullcontext()

    with mock_ctx:
        writer = writer_cls(item_writer_file, Boo)

    return writer


class TestItemWriter:
    writer_cls = writers.ItemWriter

    @pytest.mark.parametrize("testcase", ("happy-path", "unknown-timefield"))
    def test___init__(self, testcase):
        fp = mock.Mock()
        if "unknown-timefield" in testcase:

            class ItemTuple(NamedTuple):
                a_different_timestamp: int

            expectation = pytest.raises(ValueError)
        else:

            class ItemTuple(NamedTuple):
                timestamp: int

            expectation = nullcontext()

        with mock.patch.object(writers.ItemWriter, "_get_writer") as _get_writer, expectation:
            writer = writers.ItemWriter(fp, ItemTuple, foo="bar")

        if "unknown-timefield" not in testcase:
            _get_writer.assert_called_with(foo="bar")
            assert writer._fp == fp
            assert writer._itemtuple == ItemTuple
            assert writer._fields == ItemTuple._fields
            assert writer._timefield == "timestamp"

    @pytest.mark.parametrize("method, args", (("_get_writer", ()), ("write_item", (None,))))
    def test_not_implemented_methods(self, method, args, item_writer):
        with pytest.raises(NotImplementedError):
            getattr(item_writer, method)(*args)

    def test_write_items(self, item_writer):
        with mock.patch.object(item_writer, "write_item") as write_item:
            item_writer.write_items([1, 2, 3])
        assert write_item.call_args_list == [mock.call(1), mock.call(2), mock.call(3)]

    @pytest.mark.parametrize("method", ("write_header", "commit", "write_index"))
    def test_passing_methods(self, method, item_writer):
        getattr(item_writer, method)()


class TestJSONWriter:
    writer_cls = writers.JSONWriter

    def test__get_writer(self, item_writer):
        # The _get_writer() method is called from __init__().
        assert item_writer._dump is json.dump

    def test_write_item(self, item_writer):
        item = mock.Mock()
        with mock.patch.object(item_writer, "_dump") as _dump:
            item_writer.write_item(item)
        _dump.assert_called_once_with(item._asdict(), item_writer._fp)


class TestCSVWriter:
    writer_cls = writers.CSVWriter

    def test__get_writer(self, item_writer):
        # The _get_writer() method is called from __init__(). Unfortunately, csv.writer isn’t a real
        # type, so isinstance() fails, check for the presence of methods instead.
        assert callable(item_writer._writer.writerow)
        assert callable(item_writer._writer.writerows)

    def test_write_header(self, item_writer):
        # Can’t mock the methods on the writer, instead mock the whole writer.
        with mock.patch.object(item_writer, "_writer") as _writer:
            item_writer.write_header()
        _writer.writerow.assert_called_once_with(item_writer._fields)

    def test_write_item(self, item_writer):
        item = object()
        # Can’t mock the methods on the writer, instead mock the whole writer.
        with mock.patch.object(item_writer, "_writer") as _writer:
            item_writer.write_item(item)
        _writer.writerow.assert_called_once_with(item)


class TestAWKWriter:
    writer_cls = writers.AWKWriter

    def test__get_writer(self, item_writer):
        # The _get_writer() method is called from __init__().
        assert item_writer._fieldsep == "\t"

    def test__write_row(self, item_writer, item_writer_file):
        with mock.patch.object(item_writer_file, "write") as write:
            item_writer._write_row([1, 2, "three"])
        write.assert_called_once_with("1\t2\tthree\n")

    def test_write_header(self, item_writer):
        with mock.patch.object(item_writer, "_write_row") as _write_row:
            item_writer.write_header()
        _write_row.assert_called_once_with(item_writer._fields)

    def test_write_item(self, item_writer):
        item = object()
        with mock.patch.object(item_writer, "_write_row") as _write_row:
            item_writer.write_item(item)
        _write_row.assert_called_once_with(item)


class TestSQLiteWriter:
    writer_cls = writers.SQLiteWriter

    def test__sqltype(self, item_writer):
        # Have a named tuple having attributes with SQL types as their names (spaces replaced).
        fieldname_typehints = {
            sqltype.replace(" ", "_"): typehint
            for typehint, sqltype in item_writer.SQL_TYPE.items()
        }
        AllTypesItemTuple = NamedTuple("AllTypesItemTuple", **fieldname_typehints)
        with mock.patch.object(item_writer, "_itemtuple", new=AllTypesItemTuple):
            # Can’t use item_writer._fields because it’s initialited with another type.
            for fieldname in AllTypesItemTuple._fields:
                # This looks up field name -> field type -> SQL type, the first and last happen to
                # be the same (modulo spaces <-> underscores), as per the above.
                assert item_writer._sqltype(fieldname) == fieldname.replace("_", " ")

    @pytest.mark.parametrize("testcase", ("fileobj", "filename"))
    @mock.patch("mirrors_countme.writers.sqlite3")
    def test__get_writer(self, sqlite3, testcase, tmp_path, item_writer):
        db_path = tmp_path / "test.db"
        if "fileobj" in testcase:
            item_writer._fp = db_path.open("w")
        else:
            item_writer._fp = str(db_path)

        sqlite3.connect.return_value = connection = mock.Mock()
        connection.cursor.return_value = cursor = mock.Mock()

        item_writer._get_writer(tablename="tablename")

        sqlite3.connect.assert_called_once_with(f"file:{db_path}?mode=rwc", uri=True)
        connection.cursor.assert_called_once_with()

        assert item_writer._connection is connection
        assert item_writer._cursor is cursor
        assert item_writer._tablename == "tablename"

        assert item_writer._create_table.startswith("CREATE TABLE IF NOT EXISTS tablename (")
        assert item_writer._insert_item.startswith("INSERT INTO tablename (")
        timefield = item_writer._timefield
        assert item_writer._create_time_index == (
            f"CREATE INDEX IF NOT EXISTS {timefield}_idx ON tablename ({timefield})"
        )

    def test_write_header(self, item_writer):
        with mock.patch.object(item_writer, "_cursor") as _cursor:
            item_writer.write_header()

        _cursor.execute.assert_called_once_with(item_writer._create_table)

    def test_write_item(self, item_writer):
        item = object()

        with mock.patch.object(item_writer, "_cursor") as _cursor:
            item_writer.write_item(item)

        _cursor.execute.assert_called_once_with(item_writer._insert_item, item)

    def test_write_items(self, item_writer):
        items = object()

        with mock.patch.object(item_writer, "_connection") as _connection:
            item_writer.write_items(items)

        _connection.__enter__.assert_called_once()
        _connection.__exit__.assert_called_once()
        _connection.executemany.assert_called_once_with(item_writer._insert_item, items)

    def test_commit(self, item_writer):
        with mock.patch.object(item_writer, "_connection") as _connection:
            item_writer.commit()

        _connection.commit.assert_called_once_with()

    def test_write_index(self, item_writer):
        with mock.patch.object(item_writer, "_cursor") as _cursor, mock.patch.object(
            item_writer, "commit"
        ) as commit:
            item_writer.write_index()

        _cursor.execute.assert_called_once_with(item_writer._create_time_index)
        commit.assert_called_once_with()

    def test_has_item(self, item_writer):
        item_writer._fields = ("foo", "bar")
        item = ("sna", "fu")

        with mock.patch.object(item_writer, "_cursor") as _cursor:
            _cursor.execute.return_value.fetchone.result_value = (1,)
            result = item_writer.has_item(item)

        assert result is True
        _cursor.execute.assert_called_once_with(
            f"SELECT COUNT(*) FROM {item_writer._tablename} WHERE foo=? AND bar=?", item
        )

    @pytest.mark.parametrize("minmax", ("min", "max"))
    def test_mintime_maxtime(self, minmax, item_writer):
        expected = object()
        with mock.patch.object(item_writer, "_cursor") as _cursor:
            _cursor.execute.return_value.fetchone.return_value = (expected,)
            result = getattr(item_writer, f"{minmax}time")

        assert result is expected
        _cursor.execute.assert_called_once_with(
            f"SELECT {minmax.upper()}({item_writer._timefield}) FROM {item_writer._tablename}"
        )


@pytest.mark.parametrize("writer", ("CSV", "JSON", "AWK", "SQLite", "illegal"))
@mock.patch("mirrors_countme.writers.SQLiteWriter")
@mock.patch("mirrors_countme.writers.AWKWriter")
@mock.patch("mirrors_countme.writers.JSONWriter")
@mock.patch("mirrors_countme.writers.CSVWriter")
def test_make_writer(CSVWriter, JSONWriter, AWKWriter, SQLiteWriter, writer):
    writer_classes = (CSVWriter, JSONWriter, AWKWriter, SQLiteWriter)
    if writer == "illegal":
        expectation = pytest.raises(ValueError)
        writer_cls = None
    else:
        expectation = nullcontext()
        writer_cls = locals()[f"{writer}Writer"]
        writer_cls.return_value = expected = object()

    with expectation:
        result = writers.make_writer(writer.lower(), "boo", bar=5)

    if writer != "illegal":
        assert result is expected
        for cls in writer_classes:
            if cls is writer_cls:
                cls.assert_called_once_with("boo", bar=5)
            else:
                cls.assert_not_called()
