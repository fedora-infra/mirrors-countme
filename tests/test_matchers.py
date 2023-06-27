import re
from io import StringIO
from typing import NamedTuple
from unittest import mock

import pytest

from mirrors_countme import matchers, output_items


class InTestItem(NamedTuple):
    before: str | None
    after: str | None


class InTestMatcher(matchers.LogMatcher):
    regex = re.compile(r"^(?:\s*(?P<before>\S.*)\s+)?matches(?:\s+(?P<after>\S.*)\s*)?$")
    itemtuple = InTestItem

    @classmethod
    def make_item(cls, match):
        before = match["before"]
        after = match["after"]
        if before and "trip" in before or after and "trip" in after:
            raise ValueError("it made me trip!")
        return cls.itemtuple(before=before, after=after)


class TestLogMatcher:
    def test_iter(self, capsys):
        fileobj = StringIO(
            """
            this matches
            this doesn’t match
            and this matches again
            and this matches but makes make_item() trip
            """
        )
        matcher = InTestMatcher(fileobj)
        matched_items = list(iter(matcher))
        assert matched_items == [
            InTestItem(before="this", after=None),
            InTestItem(before="and this", after="again"),
        ]

        _, stderr = capsys.readouterr()
        assert re.match(
            r"^IGNORING MALFORMED LINE: '\s*and this matches but makes make_item\(\) trip", stderr
        )

    def test_make_item(self):
        with pytest.raises(NotImplementedError):
            matchers.LogMatcher.make_item(object())


class TestMirrorMatcher:
    @mock.patch("mirrors_countme.matchers.parse_querydict")
    @mock.patch("mirrors_countme.matchers.parse_logtime")
    def test_make_item(self, parse_logtime, parse_querydict):
        parse_logtime.return_value = parsed_logtime = mock.Mock()
        parsed_logtime.timestamp.return_value = timestamp = "123"
        parse_querydict.return_value = query = {
            "countme": 1,
            "repo": "the repo",
            "arch": "the repo arch",
        }

        match = {
            "time": "1970-01-01 00:02:03",
            "query": "BOOP",
            "host": "an IP address",
        }

        item = matchers.MirrorMatcher.make_item(match)

        assert isinstance(item, output_items.MirrorItem)

        assert item.timestamp == int(timestamp)
        assert item.host == match["host"]
        assert item.repo_tag == query["repo"]
        assert item.repo_arch == query["arch"]


class TestCountmeMatcher:
    @mock.patch("mirrors_countme.matchers.parse_querydict")
    @mock.patch("mirrors_countme.matchers.parse_logtime")
    def test_make_item(self, parse_logtime, parse_querydict):
        parse_logtime.return_value = parsed_logtime = mock.Mock()
        parsed_logtime.timestamp.return_value = timestamp = "123"
        parse_querydict.return_value = query = {
            "countme": 1,
            "repo": "the repo",
            "arch": "the repo arch",
        }

        match = {
            "time": "1970-01-01 00:02:03",
            "query": "BOOP",
            "host": "an IP address",
            "os_name": "Fedora",
            "os_version": "38",
            "os_variant": "server",
            "os_arch": "x86_64",
            "sys_age": "1",
        }

        item = matchers.CountmeMatcher.make_item(match)

        assert isinstance(item, output_items.CountmeItem)

        for key in ("host", "os_name", "os_version", "os_arch"):
            assert getattr(item, key) == match[key]
        assert item.timestamp == int(timestamp)
        assert item.sys_age == int(query["countme"])
        assert item.repo_tag == query["repo"]
        assert item.repo_arch == query["arch"]
