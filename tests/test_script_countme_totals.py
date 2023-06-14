from unittest import mock

import pytest

from mirrors_countme.scripts.countme_totals import cli, parse_args


# Test parse_args
def test_parse_args_with_required_arguments():
    argv = ["countme.db"]
    args = parse_args(argv)
    assert args.countme_totals == "countme.db"


def test_parse_args_with_optional_arguments():
    argv = ["countme.db", "--update-from", "raw.db", "--csv-dump", "output.csv", "--progress"]
    args = parse_args(argv)
    assert args.countme_totals == "countme.db"
    assert args.countme_raw == "raw.db"
    assert args.csv_dump.name == "output.csv"
    assert args.progress


def test_parse_args_with_version_argument():
    argv = ["-V"]
    with pytest.raises(SystemExit) as exc_info:
        parse_args(argv)

    assert exc_info.value.code == 0


def test_parse_args_with_missing_required_argument():
    argv = ["--update-from", "raw.db", "--csv-dump", "output.csv", "--progress"]
    with pytest.raises(SystemExit) as exc_info:
        parse_args(argv)

    assert exc_info.value.code == 2


# Test CLI
@mock.patch("mirrors_countme.scripts.countme_totals.parse_args")
@mock.patch("mirrors_countme.scripts.countme_totals.totals")
def test_cli(mock_totals, mock_parse_args):
    mock_args = mock.Mock(
        countme_totals="countme.db",
        countme_raw="",
        progress="--progress",
        csv_dump="",
    )
    mock_parse_args.return_value = mock_args

    cli()

    mock_parse_args.assert_called_once()

    mock_totals.assert_called_once_with(
        countme_totals="countme.db", countme_raw="", progress="--progress", csv_dump=""
    )


@mock.patch("mirrors_countme.scripts.countme_totals.parse_args")
def test_cli_keyboard_interrupt(mock_parse_args):
    mock_parse_args.side_effect = KeyboardInterrupt

    with pytest.raises(SystemExit) as exc_info:
        cli()
    assert exc_info.value.code == 3
