import datetime
from contextlib import nullcontext

import pytest

from mirrors_countme.constants import COUNTME_EPOCH
from mirrors_countme.util import offset_to_timezone, weeknum


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
        obtained = weeknum(timestamp)

    if isinstance(expected, int):
        assert obtained == expected


@pytest.mark.parametrize(
    "offset, expected",
    [
        ("-0400", datetime.timezone(datetime.timedelta(days=-1, seconds=72000))),
        ("+0500", datetime.timezone(datetime.timedelta(seconds=18000))),
        ("1683208046.7402434", ValueError),
    ],
)
def test_offest_to_timezone(offset, expected):
    if isinstance(expected, datetime.timezone):
        expectation = nullcontext()
    else:
        expectation = pytest.raises(expected)

    with expectation:
        obtained = offset_to_timezone(offset)

    if isinstance(expected, datetime.timezone):
        assert obtained == expected
