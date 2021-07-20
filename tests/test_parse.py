import tarfile
import os
import sqlite3
from typing import Any, List, NamedTuple


from countme import CountmeMatcher, make_writer
from countme.parse import parse


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


def test_read_file(tmp_path):
    matcher = CountmeMatcher
    args = Args(
        writer=make_writer("sqlite", str(tmp_path / "test_result.db"), matcher.itemtuple),
        matcher=matcher,
        dupcheck=True,
        index=True,
        header=True,
        progress=False,
        matchmode="countme",
        format="csv",
        sqlite=str(tmp_path / "test_result.db"),
        logs=[str(tmp_path / "mirrors.fedoraproject.org-access.log.processed")],
    )
    with tarfile.open("./test_data/mirrors.tar.xz", "r:xz") as log_tar:
        with tarfile.open("./test_data/test_result_cmp.tar.xz", "r:xz") as db_tar:
            os.chdir(tmp_path)
            log_tar.extractall()
            parse(args)
            db_tar.extractall()
            print(tmp_path)
            db = sqlite3.connect(args.sqlite)
            tmp_db = tmp_path / "test_result_cmp.db"
            db.execute(f"ATTACH DATABASE '{tmp_db}' AS test_db;")
            rows_missing = db.execute(
                "select * from test_db.countme_raw except select * from countme_raw;"
            )
            missing = rows_missing.fetchone()
            rows_extra = db.execute(
                "select * from countme_raw except select * from test_db.countme_raw;"
            )
            extra = rows_extra.fetchone()
            assert (
                missing is None and extra is None
            ), f"When comparing db's\n {missing} was missing and\n {extra} was extra"
