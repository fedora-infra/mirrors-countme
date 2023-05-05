from contextlib import nullcontext

import pytest

import mirrors_countme

COUNTME_LOG_RE_INPUTS_RESULTS = [
    (
        r"220.245.77.146 - - [31/May/2021:00:00:05 +0000] "
        r'"GET /metalink?repo=fedora-33&arch=x86_64&countme=3 HTTP/2.0" 200 '
        r'4044 "-" "libdnf (Fedora 33; workstation; Linux.x86_64)"',
        {
            "host": "220.245.77.146",
            "identity": "-",
            "user": "-",
            "os_arch": "x86_64",
            "os_canon": "Linux",
            "os_name": "Fedora",
            "os_variant": "workstation",
            "os_version": "33",
            "time": "31/May/2021:00:00:05 +0000",
            "method": "GET",
            "path": "/metalink",
            "product": "libdnf",
            "product_version": None,
            "query": "repo=fedora-33&arch=x86_64&countme=3",
            "protocol": "HTTP/2.0",
            "status": "200",
            "nbytes": "4044",
            "referrer": "-",
            "user_agent": "libdnf (Fedora 33; workstation; Linux.x86_64)",
        },
    )
]


@pytest.mark.parametrize(
    "timestamp, expected",
    [
        (mirrors_countme.COUNTME_EPOCH, 0),
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
        obtained = mirrors_countme.weeknum(timestamp)

    if isinstance(expected, int):
        assert obtained == expected
