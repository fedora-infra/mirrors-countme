import sqlite3
from random import randint
from unittest import mock

import pytest

from mirrors_countme import constants
from mirrors_countme.scripts import countme_delete_totals


def test_parse_args_with_version_argument():
    argv = ["-V"]
    with pytest.raises(SystemExit) as exc_info:
        countme_delete_totals.parse_args(argv)

    assert exc_info.value.code == 0


def test_parse_args_with_sqlite():
    argv = ["--sqlite", "countme.db"]
    args = countme_delete_totals.parse_args(argv)

    assert args.sqlite == "countme.db"


def test_parse_args_with_noop():
    argv = ["--noop"]
    args = countme_delete_totals.parse_args(argv)

    assert not args.rw


def test_parse_args_with_default_noop():
    argv = []
    args = countme_delete_totals.parse_args(argv)

    assert args.rw


def test_last_week_with_data():
    connection = sqlite3.connect(":memory:")
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE countme_totals (weeknum INT)")
    cursor.execute("INSERT INTO countme_totals VALUES (1), (2), (3)")

    result = countme_delete_totals.last_week(connection)
    assert result == 3


def test_num_entries_for_with_data():
    connection = sqlite3.connect(":memory:")
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE countme_totals (weeknum INT, entry TEXT)")
    cursor.execute("INSERT INTO countme_totals VALUES (1, 'entry1'), (1, 'entry2'), (2, 'entry3')")

    result = countme_delete_totals._num_entries_for(connection, 1)
    assert result == 2


def test_num_entries_for_without_data():
    connection = sqlite3.connect(":memory:")
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE countme_totals (weeknum INT, entry TEXT)")

    result = countme_delete_totals._num_entries_for(connection, 1)
    assert result == 0


def test_num_entries_with_data():
    connection = sqlite3.connect(":memory:")
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE countme_totals (entry TEXT)")
    cursor.execute("INSERT INTO countme_totals VALUES ('entry1'), ('entry2'), ('entry3')")

    result = countme_delete_totals._num_entries(connection)
    assert result == 3


def test_num_entries_without_data():
    connection = sqlite3.connect(":memory:")
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE countme_totals (entry TEXT)")

    result = countme_delete_totals._num_entries(connection)
    assert result == 0


def test_del_entries_for():
    connection = sqlite3.connect(":memory:")
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE countme_totals (weeknum INT, entry TEXT)")
    cursor.execute("INSERT INTO countme_totals VALUES (1, 'entry1'), (1, 'entry2'), (2, 'entry3')")

    countme_delete_totals._del_entries_for(connection, 1)

    cursor.execute("SELECT COUNT(*) FROM countme_totals WHERE weeknum = 1")
    result = cursor.fetchone()[0]
    assert result == 0

    cursor.execute("SELECT COUNT(*) FROM countme_totals WHERE weeknum = 2")
    result = cursor.fetchone()[0]
    assert result == 1

    cursor.execute("SELECT COUNT(*) FROM countme_totals")
    result = cursor.fetchone()[0]
    assert result == 1


def test_tm2ui():
    timestamp = 1687176000

    result = countme_delete_totals.tm2ui(timestamp)

    assert result == "2023-06-19 12:00:00"


def test_weeknum2tm():
    weeknum = randint(1, 53)

    result = countme_delete_totals.weeknum2tm(weeknum)

    expected_timestamp = constants.COUNTME_EPOCH + weeknum * constants.WEEK_LEN

    assert result == expected_timestamp


@pytest.mark.parametrize(
    "number, expected_number",
    (
        (1234567890, "1,234,567,890"),
        (123456789, "123,456,789"),
        (123, "        123"),
    ),
)
def test_num2ui(number, expected_number):
    result = countme_delete_totals.num2ui(number)
    assert result == expected_number


def test_get_trim_data(capsys):
    with mock.patch("mirrors_countme.scripts.countme_delete_totals.sqlite3") as sqlite3, mock.patch(
        "mirrors_countme.scripts.countme_delete_totals.last_week"
    ) as last_week, mock.patch(
        "mirrors_countme.scripts.countme_delete_totals._num_entries"
    ) as _num_entries, mock.patch(
        "mirrors_countme.scripts.countme_delete_totals._num_entries_for"
    ) as _num_entries_for:
        sqlite3.connect.return_value = connection_sentinel = object()
        last_week.return_value = 5
        _num_entries.return_value = 1234
        _num_entries_for.return_value = 123

        connection, week = countme_delete_totals.get_trim_data("test.db")

    stdout, _ = capsys.readouterr()
    assert connection is connection_sentinel
    assert week == 5
    assert "Next week     : 5" in stdout
    assert "Entries       :       1,234" in stdout
    assert "Entries to del:         123" in stdout


def test_trim_data(capsys):
    connection = sqlite3.connect(":memory:")
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE countme_totals (weeknum INT, entry TEXT)")
    cursor.execute("INSERT INTO countme_totals VALUES (1, 'entry1'), (1, 'entry2'), (2, 'entry3')")

    countme_delete_totals.trim_data(connection, 1)

    cursor.execute("SELECT COUNT(*) FROM countme_totals")
    result = cursor.fetchone()[0]
    assert result == 1

    stdout, _ = capsys.readouterr()
    assert " ** About to DELETE data. **" in stdout


@pytest.mark.parametrize("rw", (True, False), ids=("readwrite", "dryrun"))
@mock.patch("mirrors_countme.scripts.countme_delete_totals.trim_data")
@mock.patch("mirrors_countme.scripts.countme_delete_totals.get_trim_data")
@mock.patch("mirrors_countme.scripts.countme_delete_totals.parse_args")
def test_cli(mock_parse_args, mock_get_trim_data, mock_trim_data, rw):
    mock_parse_args.return_value = args = mock.Mock(sqlite="test.db", rw=rw)
    mock_get_trim_data.return_value = (connection := object(), week := object())

    countme_delete_totals.cli()

    mock_parse_args.assert_called_once_with()
    mock_get_trim_data.assert_called_once_with(args.sqlite)

    if rw:
        mock_trim_data.assert_called_once_with(connection, week)
    else:
        mock_trim_data.assert_not_called()


@mock.patch("mirrors_countme.scripts.countme_delete_totals.parse_args")
def test_cli_keyboard_interrupt(mock_parse_args):
    mock_parse_args.side_effect = KeyboardInterrupt

    with pytest.raises(SystemExit) as exc_info:
        countme_delete_totals.cli()
    assert exc_info.value.code == 3
