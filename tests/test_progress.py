from contextlib import nullcontext

import pytest

from mirrors_countme.progress import log_date

log_line = (
    '121.43.225.226 - - [01/May/2023:00:00:02 +0000] "GET '
    "/metalink?repo=epel-8&arch=x86_64&infra=stock&content=centos&countme=4 "
    'HTTP/1.1" 200 7547 "-" "libdnf (CentOS Linux 8; generic; Linux.x86_64)"'
)

log_line_no_date = (
    r'"121.43.225.226 - -" GET /metalink?repo=epel-8&arch=x86_64&infra=stock&'
    r'content=centos&countme=4 HTTP/1.1" 200 7547 "-" "libdnf '
    r'(CentOS Linux 8; generic; Linux.x86_64)"'
)


@pytest.mark.parametrize(
    "date, expected",
    [
        (log_line, "01/May/2023"),
        (log_line_no_date, "??/??/????"),
        (1683208046.7402434, TypeError),
    ],
)
def test_log_date(date, expected):
    if isinstance(expected, str):
        expectation = nullcontext()
    else:
        expectation = pytest.raises(expected)

    with expectation:
        obtained = log_date(date)

    if isinstance(expected, str):
        assert obtained == expected
