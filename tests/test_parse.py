import datetime
import tarfile
import tempfile
import sqlite3
from hypothesis import given, strategies as st
from hypothesis import settings, HealthCheck
from pathlib import Path
from typing import Any, List, NamedTuple

import pytest

from countme import CountmeMatcher, make_writer
from countme.parse import parse, parse_from_iterator


HERE = Path(__file__).parent
TEST_DATA_DIR = HERE.parent / "test_data"


def _test_tarfile_factory(tarfile_path):
    """Wrap tarfile.open() context manager for fixtures

    This attempts to open the tarfile and if successful, extracts its contents
    to the current working directory and yields the Tarfile object. On
    failure, it yields None.
    """
    try:
        tarfp = tarfile.open(tarfile_path, "r:xz")
    except FileNotFoundError:
        yield None
    else:
        with tarfp:
            tarfp.extractall()
            yield tarfp


@pytest.fixture
def log_tar(tmp_path_cwd):
    yield from _test_tarfile_factory(TEST_DATA_DIR / "mirrors.tar.xz")


@pytest.fixture
def db_tar(tmp_path_cwd):
    yield from _test_tarfile_factory(TEST_DATA_DIR / "test_result_cmp.tar.xz")


class Args(NamedTuple):
    writer: Any
    matcher: Any
    dupcheck: bool
    index: Any
    header: bool
    progress: bool
    matchmode: str
    format: str
    sqlite: str
    logs: List[str]


def test_read_file(tmp_path_cwd, log_tar, db_tar):
    if not log_tar or not db_tar:
        pytest.skip("Test data not found")
    matcher = CountmeMatcher
    args = Args(
        writer=make_writer("sqlite", str(tmp_path_cwd / "test_result.db"), matcher.itemtuple),
        matcher=matcher,
        dupcheck=True,
        index=True,
        header=True,
        progress=False,
        matchmode="countme",
        format="csv",
        sqlite=str(tmp_path_cwd / "test_result.db"),
        logs=[
            str(tmp_path_cwd / "mirrors" / str(i) / "mirrors.fedoraproject.org-access.log")
            for i in range(1, 32)
        ],
    )
    parse(args)
    db = sqlite3.connect(args.sqlite)
    tmp_db = tmp_path_cwd / "test_result_cmp.db"
    db.execute(f"ATTACH DATABASE '{tmp_db}' AS test_db;")
    rows_missing = db.execute("select * from test_db.countme_raw except select * from countme_raw;")
    missing = rows_missing.fetchone()
    rows_extra = db.execute("select * from countme_raw except select * from test_db.countme_raw;")
    extra = rows_extra.fetchone()
    assert (
        missing is None and extra is None
    ), f"When comparing db's\n {missing} was missing and\n {extra} was extra"


def create_logline(ip, date, repo):
    dstr = date.strftime("%d/%b/%Y:%H:%M:%S +0000")
    url = "/metalink?repo=updates-released-f33&arch=x86_64&countme=1"
    agent = "libdnf (Fedora 33; workstation; Linux.x86_64)"
    return f'{ip} - - [{dstr}] "GET {url} HTTP/1.1" 200 32015 "-" "{agent}"'


@st.composite
def log_data(draw):
    ip_sample = st.lists(st.ip_addresses(), 10, unique=True)
    repo = st.sampled_from(["Fedora", "epel-7", "centos8"])
    ips = draw(ip_sample)
    # datetime.fromisoformat('2020-12-04')
    dates = st.lists(st.datetimes(datetime.datetime(2021, 8, 8, 0)), 2, unique=True)

    return list(
        sorted(((date, ip, draw(repo)) for ip in ips for date in draw(dates)), key=lambda x: x[0])
    )


@settings(suppress_health_check=(HealthCheck.too_slow,))
@given(log_data())
def test_log(loglines):
    with tempfile.TemporaryDirectory() as tmp_dir:
        matcher = CountmeMatcher
        args = Args(
            writer=make_writer("sqlite", str(tmp_dir + "/test.db"), matcher.itemtuple),
            matcher=matcher,
            dupcheck=True,
            index=True,
            header=True,
            progress=False,
            matchmode="countme",
            format="csv",
            logs=[],
            sqlite=str(tmp_dir + "/test.db"),
        )
        parse_from_iterator(args, [(create_logline(ip, date, repo) for date, ip, repo in loglines)])
        db = sqlite3.connect(args.sqlite)
        rows_no = db.execute("select count(*) from countme_raw;").fetchone()[0]
        assert rows_no == len(loglines)
