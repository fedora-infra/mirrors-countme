import tarfile
import sqlite3
import pytest
from pathlib import Path
from typing import Any, NamedTuple
from countme.totals import totals


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
def raw_db_tar(tmp_path_cwd):
    yield from _test_tarfile_factory(TEST_DATA_DIR / "test_result_cmp.tar.xz")


@pytest.fixture
def totals_db_tar(tmp_path_cwd):
    yield from _test_tarfile_factory(TEST_DATA_DIR / "countme_totals.tar.xz")


class Args(NamedTuple):
    countme_totals: Any
    countme_raw: Any
    progress: bool
    csv_dump: Any
    sqlite: str


def test_count_totals(tmp_path_cwd, raw_db_tar, totals_db_tar):
    if not raw_db_tar or not totals_db_tar:
        pytest.skip("Test data not found")
    args = Args(
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
