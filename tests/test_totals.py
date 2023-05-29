import datetime
from contextlib import nullcontext

import pytest

from mirrors_countme.totals import weekdate


@pytest.mark.parametrize(
    "weeknum, weekday, expected",
    [
        (0, 1, datetime.date(1970, 1, 6)),
        (7, 7, ValueError),
    ],
)
def test_weekdate(weeknum, weekday, expected):
    if isinstance(expected, datetime.date):
        expectation = nullcontext()
    else:
        expectation = pytest.raises(expected)

    with expectation:
        obtained = weekdate(weeknum, weekday)

    if isinstance(expected, datetime.date):
        assert obtained == expected
