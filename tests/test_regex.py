import pytest

from mirrors_countme.regex import (
    COUNTME_LOG_RE,
    COUNTME_USER_AGENT_RE,
    LOG_DATE_RE,
    LOG_RE,
    MIRRORS_LOG_RE,
)

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


@pytest.mark.parametrize("test_case", COUNTME_LOG_RE_INPUTS_RESULTS)
def test_countme_log_re(test_case):
    input_string, expected_output = test_case
    groups = COUNTME_LOG_RE.match(input_string).groupdict()
    assert groups == expected_output


COUNTME_USER_AGENT_RE_INPUTS_RESULTS = [
    (
        "libdnf (os_name os_version; os_variant; os_canon.os_arch)",
        {
            "product": "libdnf",
            "product_version": None,
            "os_name": "os_name",
            "os_version": "os_version",
            "os_variant": "os_variant",
            "os_canon": "os_canon",
            "os_arch": "os_arch",
        },
    ),
    (
        "libdnf (Fedora 33; workstation; Linux.x86_64)",
        {
            "product": "libdnf",
            "product_version": None,
            "os_name": "Fedora",
            "os_version": "33",
            "os_variant": "workstation",
            "os_canon": "Linux",
            "os_arch": "x86_64",
        },
    ),
    (
        "libdnf (os_name_mäkčeň os_version; os_variant; os_canon.os_arch)",
        {
            "product": "libdnf",
            "product_version": None,
            "os_name": "os_name_mäkčeň",
            "os_version": "os_version",
            "os_variant": "os_variant",
            "os_canon": "os_canon",
            "os_arch": "os_arch",
        },
    ),
]


COUNTME_USER_AGENT_RE_INVALID_INPUTS = [
    (
        r"16.160.95.167 - - [31/May/2021:00:00:02 +0000] "
        r'"GET /badpath?repo=epel-8&arch=x86_64&infra=stock&content=almalinux&countme=2 HTTP/1.1" '
        r'200 26137 "-" "libdnf (AlmaLinux 8.3; generic; Linux.x86_64)"'
    )
]


@pytest.mark.parametrize("test_case", COUNTME_USER_AGENT_RE_INPUTS_RESULTS)
def test_countme_user_agent_re(test_case):
    input_string, expected_output = test_case
    groups = COUNTME_USER_AGENT_RE.match(input_string).groupdict()
    assert groups == expected_output


@pytest.mark.parametrize("test_case", COUNTME_USER_AGENT_RE_INVALID_INPUTS)
def test_countme_user_agent_re_invalid(test_case):
    invalid_input = test_case
    assert COUNTME_USER_AGENT_RE.match(invalid_input) is None


LOG_DATE_RE_INPUTS_RESULTS = [
    (
        r"220.245.77.146 - - [31/May/2021:00:00:05 +0000] "
        r'"GET /metalink?repo=fedora-33&arch=x86_64&countme=3 HTTP/2.0" 200 '
        r'4044 "-" "libdnf (Fedora 33; workstation; Linux.x86_64)"',
        {
            "host": "220.245.77.146",
            "identity": "-",
            "user": "-",
            "date": "31/May/2021",
            "time": "31/May/2021:00:00:05 +0000",
            "method": "GET",
            "path": "/metalink",
            "query": "repo=fedora-33&arch=x86_64&countme=3",
            "protocol": "HTTP/2.0",
            "status": "200",
            "nbytes": "4044",
            "referrer": "-",
            "user_agent": "libdnf (Fedora 33; workstation; Linux.x86_64)",
        },
    )
]


@pytest.mark.parametrize("test_case", LOG_DATE_RE_INPUTS_RESULTS)
def test_log_date_re(test_case):
    input_string, expected_output = test_case
    groups = LOG_DATE_RE.match(input_string).groupdict()
    assert groups == expected_output


LOG_RE_INPUTS_RESULTS = [
    (
        r"16.160.95.167 - - [31/May/2021:00:00:02 +0000] "
        r'"GET /metalink?repo=epel-8&arch=x86_64&infra=stock&content=almalinux&countme=2 HTTP/1.1" '
        r'200 26137 "-" "libdnf (AlmaLinux 8.3; generic; Linux.x86_64)"',
        {
            "host": "16.160.95.167",
            "identity": "-",
            "user": "-",
            "time": "31/May/2021:00:00:02 +0000",
            "method": "GET",
            "path": "/metalink",
            "query": "repo=epel-8&arch=x86_64&infra=stock&content=almalinux&countme=2",
            "protocol": "HTTP/1.1",
            "status": "200",
            "nbytes": "26137",
            "referrer": "-",
            "user_agent": "libdnf (AlmaLinux 8.3; generic; Linux.x86_64)",
        },
    )
]


@pytest.mark.parametrize("test_case", LOG_RE_INPUTS_RESULTS)
def test_log_re(test_case):
    input_string, expected_output = test_case
    groups = LOG_RE.match(input_string).groupdict()
    assert groups == expected_output


MIRRORS_LOG_RE_INPUTS_RESULTS = [
    (
        r"16.160.95.167 - - [31/May/2021:00:00:02 +0000] "
        r'"GET /metalink?repo=epel-8&arch=x86_64&infra=stock&content=almalinux&countme=2 HTTP/1.1" '
        r'200 26137 "-" "libdnf (AlmaLinux 8.3; generic; Linux.x86_64)"',
        {
            "host": "16.160.95.167",
            "identity": "-",
            "method": "GET",
            "nbytes": "26137",
            "path": "/metalink",
            "protocol": "HTTP/1.1",
            "query": "repo=epel-8&arch=x86_64&infra=stock&content=almalinux&countme=2",
            "referrer": "-",
            "status": "200",
            "time": "31/May/2021:00:00:02 +0000",
            "user": "-",
            "user_agent": "libdnf (AlmaLinux 8.3; generic; Linux.x86_64)",
        },
    )
]


MIRRORS_LOG_RE_INVALID_INPUTS = [
    (
        r"16.160.95.167 - - [31/May/2021:00:00:02 +0000] "
        r'"GET /badpath?repo=epel-8&arch=x86_64&infra=stock&content=almalinux&countme=2 HTTP/1.1" '
        r'200 26137 "-" "libdnf (AlmaLinux 8.3; generic; Linux.x86_64)"'
    )
]


@pytest.mark.parametrize("test_case", MIRRORS_LOG_RE_INPUTS_RESULTS)
def test_mirrors_log_re(test_case):
    input_string, expected_output = test_case
    groups = MIRRORS_LOG_RE.match(input_string).groupdict()
    assert groups == expected_output


@pytest.mark.parametrize("test_case", MIRRORS_LOG_RE_INVALID_INPUTS)
def test_mirrors_log_re_invalid(test_case):
    invalid_input = test_case
    assert MIRRORS_LOG_RE.match(invalid_input) is None
