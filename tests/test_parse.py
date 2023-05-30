from collections.abc import Generator
from itertools import chain, repeat
from unittest import mock

import pytest

from mirrors_countme.parse import parse, parse_from_iterator


@pytest.mark.parametrize(
    "testcase",
    (
        "happy-path",
        "without-header",
        "without-countme-workaround",
        "without-dupcheck",
        "without-index",
    ),
)
def test_parse_from_iterator(testcase):
    lines = [
        "not this",
        "this als not",
        "but this (pick me)",
        "and this (pick me, too)",
    ]
    logf = mock.Mock(lines=lines)
    writer = mock.Mock()

    class matcher:
        def __init__(self, logf):
            self.logf = logf

        def __iter__(self):
            for line in logf.lines:
                if "pick me" in line:
                    yield (line,)

    manager = mock.Mock()
    manager.attach_mock(writer.write_header, "write_header")
    manager.attach_mock(writer.write_index, "write_index")
    manager.attach_mock(writer.has_item, "has_item")
    writer.has_item.side_effect = chain([True], repeat(False))
    manager.attach_mock(writer.write_item, "write_item")
    manager.attach_mock(writer.commit, "commit")
    manager.attach_mock(writer.write_items, "write_items")

    header = sqlite = "without-header" not in testcase
    matchmode = "countme" if "without-countme-workaround" not in testcase else "mirrors"
    dupcheck = "without-dupcheck" not in testcase
    index = "without-index" not in testcase

    parse_from_iterator(
        [logf],
        writer=writer,
        matcher=matcher,
        header=header,
        sqlite=sqlite,
        matchmode=matchmode,
        dupcheck=dupcheck,
        index=index,
    )

    expected_calls = []

    if "without-header" not in testcase:
        expected_calls.append(mock.call.write_header())

    if "without-index" not in testcase:
        expected_calls.append(mock.call.write_index())

    if "without-dupcheck" not in testcase:
        had_item = False
        for line in lines:
            if "pick me" in line:
                expected_calls.append(mock.call.has_item((line,)))
                if had_item:
                    expected_calls.append(mock.call.write_item((line,)))
                else:
                    had_item = True
        expected_calls.append(mock.call.commit())
        assert manager.mock_calls == expected_calls
    else:
        mock_calls = manager.mock_calls[:-1]
        assert mock_calls == expected_calls

        # This one is hard to mock precisely, its argument is an on-the-fly-created generator
        last_call = manager.mock_calls[-1]
        assert last_call[0] == "write_items"
        assert len(last_call[1]) == 1
        assert isinstance(last_call[1][0], Generator)
        assert len(last_call[2]) == 0


@pytest.mark.parametrize("use_default", (True, False))
@mock.patch("mirrors_countme.parse.parse_from_iterator")
@mock.patch("mirrors_countme.parse.ReadProgress")
def test_parse(ReadProgress, parse_from_iterator, use_default):
    kwargs = {
        "writer": object(),
        "matcher": object(),
        "header": object(),
        "sqlite": object(),
        "dupcheck": object(),
        "index": object(),
    }

    if use_default:
        expected_kwargs = kwargs.copy()
        expected_kwargs["matchmode"] = "countme"
    else:
        kwargs["matchmode"] = object()
        expected_kwargs = kwargs

    ReadProgress.return_value = read_progress = object()
    expected_args = (read_progress,)

    parse(**kwargs)

    parse_from_iterator.assert_called_once_with(*expected_args, **expected_kwargs)
