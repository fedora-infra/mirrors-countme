from contextlib import nullcontext
from io import StringIO
from typing import NamedTuple
from unittest import mock

import pytest

from mirrors_countme import writers


class Boo(NamedTuple):
    timestamp: int


@pytest.fixture
def item_writer_file():
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
