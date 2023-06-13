import datetime as dt
import re
from contextlib import nullcontext
from unittest import mock

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


@pytest.mark.parametrize("has_result", (True, False), ids=("with-result", "without-result"))
def test__fetchone_or_none(has_result):
    cursor = mock.Mock()
    if has_result:
        cursor.fetchone.return_value = [result_sentinel := object()]
    else:
        cursor.fetchone.return_value = None

    result = util._fetchone_or_none(cursor)

    if has_result:
        assert result is result_sentinel
    else:
        assert result is None


class MinMaxProp(util.MinMaxPropMixin):
    def __init__(self):
        self._cursor = mock.Mock()
        self._timefield = "timestamp"
        self._tablename = "countme_raw"


class TestMinMaxPropMixin:
    @pytest.mark.parametrize(
        "property_name",
        (
            "mintime_countme",
            "maxtime_countme",
            "mintime_unique",
            "maxtime_unique",
            "mintime",
            "maxtime",
        ),
    )
    def test_minmaxtime_properties(self, property_name):
        test_obj = MinMaxProp()

        with mock.patch("mirrors_countme.util._fetchone_or_none") as _fetchone_or_none:
            min_or_max = property_name[:3]  # "min" or "max"
            if property_name.endswith("_countme"):
                query_filter_snippet = r"\s+where\s+sys_age\s*>=\s*0"
            elif property_name.endswith("_unique"):
                query_filter_snippet = r"\s+where\s+sys_age\s*<\s*0"
            else:
                query_filter_snippet = ""
            expected_query_re = re.compile(
                rf"^select\s+{min_or_max}\({test_obj._timefield}\)\s+from\s+"
                + rf"{test_obj._tablename}{query_filter_snippet}\s*$",
                re.IGNORECASE,
            )

            _fetchone_or_none.return_value = result_sentinel = object()

            result = getattr(test_obj, property_name)

            assert result is result_sentinel
            test_obj._cursor.execute.assert_called_once()
            assert expected_query_re.match(test_obj._cursor.execute.call_args.args[0])
