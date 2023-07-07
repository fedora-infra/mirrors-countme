import datetime as dt
from contextlib import nullcontext
from unittest import mock

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import data, integers, none, one_of

from mirrors_countme import totals, util
from mirrors_countme.constants import (
    COUNTME_EPOCH,
    COUNTME_EPOCH_ORDINAL,
    COUNTME_START_WEEKNUM,
    LOG_JITTER_WINDOW,
    WEEK_LEN,
)


@pytest.mark.parametrize(
    "weeknum, weekday, expected",
    [
        (0, 1, dt.date(1970, 1, 6)),
        (7, 7, ValueError),
    ],
)
def test_weekdate(weeknum, weekday, expected):
    if isinstance(expected, dt.date):
        expectation = nullcontext()
    else:
        expectation = pytest.raises(expected)

    with expectation:
        obtained = totals.weekdate(weeknum, weekday)

    if isinstance(expected, dt.date):
        assert obtained == expected


def test_daterange():
    with mock.patch("mirrors_countme.totals.weekdate") as weekdate:
        TEST_WEEKNUM = 15  # why not
        weekdate.side_effect = lambda weeknum, weekday: (weeknum, weekday)

        assert totals.daterange(weeknum=TEST_WEEKNUM) == ((TEST_WEEKNUM, 0), (TEST_WEEKNUM, 6))
        weekdate.assert_has_calls((mock.call(TEST_WEEKNUM, 0), mock.call(TEST_WEEKNUM, 6)))


class TestCSVCountItem:
    @given(
        weeknum=integers(
            min_value=0,
            max_value=(
                (dt.date(dt.MAXYEAR, 12, 31).toordinal() - dt.date(1970, 1, 1).toordinal()) // 7 - 2
            ),
        )
    )
    def test_from_totalsitem(self, weeknum):
        totals_item = totals.TotalsItem(
            hits=0,
            weeknum=str(weeknum),
            os_name="os_name",
            os_version="os_version",
            os_variant="os_variant",
            os_arch="os_arch",
            sys_age="sys_age",
            repo_tag="repo_tag",
            repo_arch="repo_arch",
        )
        csv_count_item = totals.CSVCountItem.from_totalsitem(totals_item)
        week_start, week_end = totals.daterange(weeknum)
        assert csv_count_item == totals.CSVCountItem(
            week_start=week_start,
            week_end=week_end,
            hits=0,
            os_name="os_name",
            os_version="os_version",
            os_variant="os_variant",
            os_arch="os_arch",
            sys_age="sys_age",
            repo_tag="repo_tag",
            repo_arch="repo_arch",
        )


@pytest.fixture
def rawdb(request, tmp_path):
    cls = request.instance.cls

    with mock.patch.object(cls, "_get_fields") as _get_fields:
        _get_fields.return_value = totals.CountmeItem._fields
        rawdb = cls(str(tmp_path / "raw.db"))

    return rawdb


class TestRawDB:
    cls = totals.RawDB
    minmax_default_prop = "countme"

    def test___init__(self):
        with mock.patch.object(totals.SQLiteReader, "__init__") as super_init:
            obj = self.cls("filename", foo="bar")

        assert isinstance(obj, self.cls)
        super_init.assert_called_once_with(
            "filename", totals.CountmeItem, tablename="countme_raw", foo="bar"
        )

    @pytest.mark.parametrize("propname", ("mintime", "maxtime"))
    def test_minmaxtime(self, propname, rawdb):
        # properties have to be mocked on the class
        with mock.patch.object(
            self.cls, f"{propname}_{self.minmax_default_prop}", new_callable=mock.PropertyMock
        ) as minmaxtime_prop:
            minmaxtime_prop.return_value = sentinel = object()
            assert getattr(rawdb, propname) is sentinel

    @settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
    @given(data=data())
    def test_complete_weeks(self, data, rawdb):
        mintime = data.draw(one_of(integers(min_value=COUNTME_EPOCH_ORDINAL), none()))
        if mintime is not None:
            maxtime = data.draw(integers(min_value=mintime))
        else:
            maxtime = None

        with mock.patch.object(self.cls, "mintime", new=mintime), mock.patch.object(
            self.cls, "maxtime", new=maxtime
        ):
            result = rawdb.complete_weeks()

        if mintime is None:
            assert result == []
        else:
            assert result.start == max(util.weeknum(mintime), COUNTME_START_WEEKNUM)
            assert result.stop == max(
                util.weeknum(maxtime - LOG_JITTER_WINDOW), COUNTME_START_WEEKNUM
            )

    def test_week_iter(self, rawdb):
        weeknum = 15

        with mock.patch.object(rawdb, "_connection") as _connection:
            _connection.execute.return_value = sentinel = object()
            result = rawdb.week_iter(weeknum, ("COUNT(*)",))

        assert result is sentinel

        start_ts = weeknum * WEEK_LEN + COUNTME_EPOCH
        end_ts = start_ts + WEEK_LEN
        _connection.execute.assert_called_once_with(
            f"SELECT COUNT(*) FROM {rawdb._tablename}"
            + f" WHERE timestamp >= {start_ts} AND timestamp < {end_ts} AND sys_age >= 0"
        )

    def test_week_count(self, rawdb):
        weeknum = 37

        with mock.patch.object(rawdb, "week_iter") as week_iter:
            cursor = week_iter.return_value
            cursor.fetchone.return_value = ("bloop",)
            result = rawdb.week_count(weeknum)

        assert result == "bloop"
        week_iter.assert_called_once_with(weeknum, ("COUNT(*)",))
        cursor.fetchone.assert_called_once_with()


class TestRawDBU(TestRawDB):
    cls = totals.RawDBU
    minmax_default_prop = "unique"

    def test_week_iter(self, rawdb):
        weeknum = 59
        select = totals.BucketSelectUniqueIP

        with mock.patch.object(totals, "SplitWeekDays") as SplitWeekDays:
            SplitWeekDays.return_value = sentinel = object()

            result = rawdb.week_iter(weeknum, select)

        assert result is sentinel
        start_ts = weeknum * WEEK_LEN + COUNTME_EPOCH
        SplitWeekDays.assert_called_once_with(rawdb, start_ts, select)

    def test_week_count(self, rawdb):
        weeknum = 23
        num_entries = 100000

        with mock.patch.object(rawdb, "_connection") as _connection:
            cursor = _connection.execute.return_value
            cursor.fetchone.return_value = (num_entries,)

            result = rawdb.week_count(weeknum)

        assert result == num_entries / 8
        start_ts = weeknum * WEEK_LEN + COUNTME_EPOCH
        end_ts = start_ts + WEEK_LEN
        _connection.execute.assert_called_once_with(
            f"SELECT COUNT(*) FROM {rawdb._tablename}"
            + f" WHERE timestamp >= {start_ts} AND timestamp < {end_ts} AND sys_age < 0"
        )
        cursor.fetchone.assert_called_once_with()


@pytest.fixture
def splitweekdays():
    return totals.SplitWeekDays(mock.Mock(), mock.Mock(), mock.Mock())


class TestSplitWeekDays:
    def test___init__(self):
        rawdb = object()
        start_ts = object()
        select = object()

        swd = totals.SplitWeekDays(rawdb=rawdb, start_ts=start_ts, select=select)

        assert swd.rawdb is rawdb
        assert swd.start_ts is start_ts
        assert swd.select is select

    def test___iter__(self, splitweekdays):
        with mock.patch.object(splitweekdays, "fetchall") as fetchall:
            sentinel = object()
            fetchall.return_value = iter([sentinel])

            result = iter(splitweekdays)

        assert list(result) == [sentinel]
        fetchall.assert_called_once_with()

    def test_fetchone(self, splitweekdays):
        with mock.patch.object(splitweekdays, "fetchall") as fetchall:
            sentinel = object()
            fetchall.return_value = [sentinel, object()]

            result = splitweekdays.fetchone()

        assert result is sentinel
        fetchall.assert_called_once_with()

    def test_fetchall(self, splitweekdays):
        splitweekdays.select = ("foo", "bar")
        splitweekdays.start_ts = 0

        rawdb = splitweekdays.rawdb
        cursors = [mock.Mock() for d in range(7)]
        for d, cursor in enumerate(cursors):
            cursor.fetchall.return_value = [f"entry for day #{d + 1}"]
        rawdb._connection.execute.side_effect = cursors
        rawdb._tablename = "tablename"

        result = list(splitweekdays.fetchall())

        assert result == [f"entry for day #{d + 1}" for d in range(7)]
        day_len = WEEK_LEN / 7
        rawdb._connection.execute.assert_has_calls(
            [
                mock.call(
                    "SELECT foo,bar FROM tablename WHERE"
                    + f" timestamp >= {d * day_len} AND timestamp < {(d + 1) * day_len}"
                    + " GROUP BY"
                    + " host, os_name, os_version, os_variant, os_arch, repo_tag, repo_arch"
                )
            ]
        )
        for cursor in cursors:
            cursor.fetchall.assert_called_once_with()


@pytest.mark.parametrize("with_progress", (True, False), ids=("with-progress", "without-progress"))
@pytest.mark.parametrize("with_csv_dump", (True, False), ids=("with-csv_dump", "without-csv_dump"))
@pytest.mark.parametrize(
    "with_countme_raw", (True, False), ids=("with-countme-raw", "without-countme-raw")
)
def test_totals(with_countme_raw, with_csv_dump, with_progress):
    START_WEEK = 1234
    BUCKET_NUM = 5
    ENTRIES_PER_BUCKET = 3

    with mock.patch.object(totals, "SQLiteWriter") as SQLiteWriter, mock.patch.object(
        totals, "RawDB"
    ) as RawDB, mock.patch.object(totals, "DIYProgress") as DIYProgress, mock.patch.object(
        totals, "RawDBU"
    ) as RawDBU, mock.patch.object(
        totals, "SQLiteReader"
    ) as SQLiteReader, mock.patch.object(
        totals, "CSVWriter"
    ) as CSVWriter:
        totals_db = SQLiteWriter.return_value
        progress = DIYProgress.return_value
        rawdb = RawDB.return_value
        rawdbu = RawDBU.return_value
        csv_writer = CSVWriter.return_value

        totals_db.maxtime_countme = totals_db.maxtime_unique = START_WEEK

        # 1 old, 4 new weeks
        complete_weeks = [START_WEEK + w for w in range(5)]
        rawdb.complete_weeks.return_value = rawdbu.complete_weeks.return_value = complete_weeks
        expected_new_weeks = complete_weeks[1:]

        rawdb.week_iter.side_effect = [
            [
                (f"bucket #{i} week {weeknum} for countme",)
                for i in range(BUCKET_NUM)
                for j in range(ENTRIES_PER_BUCKET)
            ]
            for weeknum in expected_new_weeks
        ]
        rawdbu.week_iter.side_effect = [
            [
                (f"bucket #{i} week {weeknum} for unique",)
                for i in range(BUCKET_NUM)
                for j in range(ENTRIES_PER_BUCKET)
            ]
            for weeknum in expected_new_weeks
        ]
        rawdb.week_count.return_value = rawdbu.week_count.return_value = (
            BUCKET_NUM * ENTRIES_PER_BUCKET
        )

        SQLiteReader.return_value = totalreader_results = [
            totals.TotalsItem(
                hits=week,  # as good as any
                weeknum=str(week),
                os_name="os_name",
                os_version="os_version",
                os_variant="os_variant",
                os_arch="os_arch",
                sys_age="sys_age",
                repo_tag="repo_tag",
                repo_arch="repo_arch",
            )
            for week in expected_new_weeks
        ]

        totals.totals(
            countme_totals="totals.db",
            countme_raw="raw.db" if with_countme_raw else None,
            progress=with_progress,
            csv_dump="totals.csv" if with_csv_dump else None,
        )

        SQLiteWriter.assert_called_once_with(
            "totals.db", totals.TotalsItem, timefield="weeknum", tablename="countme_totals"
        )

        if with_countme_raw:
            totals_db.write_index.assert_called_once_with()
            RawDB.assert_called_once_with("raw.db" if with_countme_raw else None)
            RawDBU.assert_called_once_with("raw.db" if with_countme_raw else None)

            expected_calls = []
            for unique in (False, True):
                for week in expected_new_weeks:
                    start_date = dt.date.fromordinal(totals.COUNTME_EPOCH_ORDINAL) + dt.timedelta(
                        weeks=week
                    )
                    end_date = start_date + dt.timedelta(days=6)
                    start_date_str = start_date.strftime("%F")
                    end_date_str = end_date.strftime("%F")
                    expected_calls.append(
                        mock.call(
                            total=BUCKET_NUM * ENTRIES_PER_BUCKET if with_progress else 1,
                            desc=(
                                f"{'week' if not unique else 'weeku'} {week}"
                                + f" ({start_date_str} -- {end_date_str})"
                            ),
                            disable=not with_progress,
                            unit="row" if not unique else "~ip",
                            unit_scale=False,
                        )
                    )
            assert DIYProgress.call_args_list == expected_calls

            # (RawDB, RawDBU object) * each entry
            assert progress.update.call_count == (
                2 * len(expected_new_weeks) * BUCKET_NUM * ENTRIES_PER_BUCKET
            )
            written_items_by_call = [
                list(call.args[0]) for call in totals_db.write_items.call_args_list
            ]
            assert written_items_by_call == [
                [
                    (ENTRIES_PER_BUCKET, f"bucket #{bucket} week {week} for {what}")
                    for bucket in range(BUCKET_NUM)
                ]
                for what in ("countme", "unique")
                for week in expected_new_weeks
            ]
            assert all(call == mock.call() for call in progress.update.call_args_list)
            assert rawdb.week_iter.call_args_list == [
                mock.call(week, select=totals.BucketSelect) for week in expected_new_weeks
            ]
            assert rawdbu.week_iter.call_args_list == [
                mock.call(week, select=totals.BucketSelectUniqueIP) for week in expected_new_weeks
            ]
        else:
            totals_db.write_index.assert_not_called()
            RawDB.assert_not_called()
            RawDBU.assert_not_called()
            DIYProgress.assert_not_called()

        if with_csv_dump:
            SQLiteReader.assert_called_once_with(
                "totals.db", totals.TotalsItem, timefield="weeknum", tablename="countme_totals"
            )
            CSVWriter.assert_called_once_with(
                "totals.csv", totals.CSVCountItem, timefield="week_start"
            )
            csv_writer.write_header.assert_called_once_with()
            assert csv_writer.write_item.call_args_list == [
                mock.call(totals.CSVCountItem.from_totalsitem(totalsitem))
                for totalsitem in totalreader_results
            ]
