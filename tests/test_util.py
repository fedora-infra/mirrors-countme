import datetime as dt
from contextlib import nullcontext

import pytest

from mirrors_countme import util
from mirrors_countme.constants import COUNTME_EPOCH


@pytest.mark.parametrize(
    "timestamp, expected",
    [
        (COUNTME_EPOCH, 0),
        (1683208046.7402434, 2782),
        ("1683208046.7402434", ValueError),
    ],
)
def test_weeknum(timestamp, expected):
    if isinstance(expected, int):
        expectation = nullcontext()
    else:
        expectation = pytest.raises(expected)

    with expectation:
        obtained = util.weeknum(timestamp)

    if isinstance(expected, int):
        assert obtained == expected


@pytest.mark.parametrize(
    "offset, expected",
    [
        ("-0400", dt.timezone(dt.timedelta(days=-1, seconds=72000))),
        ("+0500", dt.timezone(dt.timedelta(seconds=18000))),
        ("1683208046.7402434", ValueError),
    ],
)
def test_offset_to_timezone(offset, expected):
    if isinstance(expected, dt.timezone):
        expectation = nullcontext()
    else:
        expectation = pytest.raises(expected)

    with expectation:
        obtained = util.offset_to_timezone(offset)

    if isinstance(expected, dt.timezone):
        assert obtained == expected


@pytest.mark.parametrize(
    "logtime, expected",
    (
        ("29/Mar/2020:16:04:28 +0000", dt.datetime(2020, 3, 29, 16, 4, 28, 0, dt.UTC)),
        ("29/Mar/2020:16:04:28 -0000", dt.datetime(2020, 3, 29, 16, 4, 28, 0, dt.UTC)),
        (
            "29/Mar/2020:16:04:28 +0200",
            dt.datetime(2020, 3, 29, 16, 4, 28, 0, dt.timezone(dt.timedelta(hours=2))),
        ),
        (
            "29/Mar/2020:16:04:28 -0800",
            dt.datetime(2020, 3, 29, 16, 4, 28, 0, dt.timezone(dt.timedelta(hours=-8))),
        ),
    ),
)
def test_parse_logtime(logtime, expected):
    assert util.parse_logtime(logtime) == expected


@pytest.mark.parametrize(
    "querystr, expected",
    (
        ("foo=bar&baz=gnu", {"foo": "bar", "baz": "gnu"}),
        ("foo=bar&foo=baz", {"foo": "baz"}),
    ),
)
def test_parse_querydict(querystr, expected):
    assert util.parse_querydict(querystr) == expected
