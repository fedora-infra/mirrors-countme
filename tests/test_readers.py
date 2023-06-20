from contextlib import nullcontext
from typing import NamedTuple
from unittest import mock

import pytest

from mirrors_countme import readers


class ItemTuple(NamedTuple):
    itemtuple: int


@pytest.fixture
def item_reader_file(tmp_path):
    return str(tmp_path / "test.db")


@pytest.fixture
def item_reader(item_reader_file):
    with mock.patch.object(readers.SQLiteReader, "_get_fields") as _get_fields:
        _get_fields.return_value = ItemTuple._fields
        item_reader = readers.SQLiteReader(item_reader_file, ItemTuple)

    return item_reader


class TestSQLiteReader:
    @pytest.mark.parametrize("testcase", ("happy-path", "no-filefields", "wrong-filefields"))
    def test__init__(self, testcase, item_reader_file):
        if testcase not in ("happy-path", "attr_name"):
            expectation = pytest.raises(readers.ReaderError)
        else:
            expectation = nullcontext()

        with expectation as exc_info, mock.patch.object(
            readers.SQLiteReader, "_get_reader"
        ) as _get_reader, mock.patch.object(readers.SQLiteReader, "_get_fields") as _get_fields:
            match testcase:
                case "happy-path":
                    _get_fields.return_value = ItemTuple._fields
                case "no-filefields":
                    _get_fields.return_value = None
                case "wrong-filefields":
                    _get_fields.return_value = ("all", "the", "wrong", "fields")

            item_reader = readers.SQLiteReader(item_reader_file, ItemTuple, foo="bar")

        match testcase:
            case "happy-path":
                assert item_reader._fp == item_reader_file
                assert item_reader._itemtuple == ItemTuple
                assert item_reader._itemfields == ItemTuple._fields
                assert item_reader._itemfactory == ItemTuple._make
                _get_reader.assert_called_once_with(foo="bar")
                _get_fields.assert_called_once_with()
            case "no-filefields":
                assert str(exc_info.value) == "no field names found"
            case "wrong-filefields":
                assert str(exc_info.value).startswith("field mismatch:")

    def test__iter_rows(self, item_reader):
        with mock.patch.object(item_reader, "_cursor") as _cursor:
            _cursor.execute.return_value = expected = object()
            result = item_reader._iter_rows()

            assert result is expected
            fields = ",".join(item_reader._itemfields)
            _cursor.execute.assert_called_once_with(
                f"SELECT {fields} FROM {item_reader._tablename}"
            )

    def test_has_attr(self, item_reader_file):
        with mock.patch.object(readers.SQLiteReader, "_get_fields") as get_fields, open(
            item_reader_file, "w+"
        ) as obj:
            get_fields.return_value = ItemTuple._fields
            reader = readers.SQLiteReader(obj, ItemTuple, foo="moo")
            assert reader._filename == obj.name
