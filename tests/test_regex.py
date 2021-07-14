from countme.regex import COUNTME_USER_AGENT_RE


def test_useragent_re():
    groups = COUNTME_USER_AGENT_RE.match(
        "libdnf (os_name os_version; os_variant; os_canon.os_arch)"
    ).groups()
    assert groups == (
        "libdnf",
        "libdnf",
        None,
        "os_name",
        "os_version",
        "os_variant",
        "os_canon",
        "os_arch",
    )
