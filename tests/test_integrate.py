import datetime
import sqlite3
import tarfile
import tempfile
from pathlib import Path
from typing import Any, List, NamedTuple

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from mirrors_countme.matchers import CountmeMatcher
from mirrors_countme.parse import parse, parse_from_iterator
from mirrors_countme.totals import totals
from mirrors_countme.writers import make_writer

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


@pytest.fixture
def raw_db_tar(tmp_path_cwd):
    yield from _test_tarfile_factory(TEST_DATA_DIR / "test_result_cmp.tar.xz")


@pytest.fixture
def totals_db_tar(tmp_path_cwd):
    yield from _test_tarfile_factory(TEST_DATA_DIR / "countme_totals.tar.xz")


class ArgsTotal(NamedTuple):
    countme_totals: Any
    countme_raw: Any
    progress: bool
    csv_dump: Any
    sqlite: str


def test_count_totals(tmp_path_cwd, raw_db_tar, totals_db_tar):
    if not raw_db_tar or not totals_db_tar:
        pytest.skip("Test data not found")
    args = ArgsTotal(
        countme_totals=str(tmp_path_cwd / "test_result_totals.db"),
        countme_raw=str(tmp_path_cwd / "test_result_cmp.db"),
        progress=False,
        csv_dump=False,
        sqlite=str(tmp_path_cwd / "test_result_totals"),
    )
    totals(args)
    db = sqlite3.connect(args.sqlite)
    tmp_db = tmp_path_cwd / "countme_totals"
    db.execute(f"ATTACH DATABASE '{tmp_db}' AS test_db;")
    rows_missing = db.execute(
        "select * from test_db.countme_totals except select * from countme_totals;"
    )
    missing = rows_missing.fetchone()
    rows_extra = db.execute(
        "select * from countme_totals except select * from test_db.countme_totals;"
    )
    extra = rows_extra.fetchone()
    assert (
        missing is None and extra is None
    ), f"When comparing db's\n {missing} was missing and\n {extra} was extra"


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
    ip_sample = st.lists(st.ip_addresses(), min_size=10, max_size=10, unique=True)
    repo = st.sampled_from(["Fedora", "epel-7", "centos8"])
    ips = draw(ip_sample)
    today = datetime.datetime.now()
    dates = [today - datetime.timedelta(days=d, hours=i) for i in range(1, 2) for d in range(1, 14)]

    return sorted(((date, ip, draw(repo)) for ip in ips for date in dates), key=lambda x: x[0])


@settings(suppress_health_check=(HealthCheck.too_slow,), deadline=datetime.timedelta(seconds=1))
@given(loglines=log_data())
def test_log(loglines):
    with tempfile.TemporaryDirectory() as tmp_dir:
        rawdb = f"{tmp_dir}/test.db"
        totalsdb = f"{tmp_dir}/test_generated_totals.db"
        matcher = CountmeMatcher
        parse_from_iterator(
            [(create_logline(ip, date, repo) for date, ip, repo in loglines)],
            matcher=matcher,
            writer=make_writer("sqlite", rawdb, matcher.itemtuple),
            dupcheck=True,
            index=True,
            matchmode="countme",
            sqlite=rawdb,
            header=True,
        )
        db = sqlite3.connect(rawdb)
        rows_no = db.execute("select count(*) from countme_raw;").fetchone()[0]
        assert rows_no == len(loglines)

        csv_dump = open(f"{tmp_dir}/test.csv", "w+")
        totals(
            countme_totals=totalsdb,
            countme_raw=rawdb,
            progress=False,
            csv_dump=csv_dump,
        )
        db = sqlite3.connect(totalsdb)
        rows_no = db.execute("select count(*) from countme_totals;").fetchone()[0]
        assert int(rows_no) > 0

        csv_dump.seek(0)
        assert len(csv_dump.readlines()) > 0
