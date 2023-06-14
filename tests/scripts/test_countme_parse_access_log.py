from unittest import mock

import pytest

from mirrors_countme.matchers import CountmeMatcher, MirrorMatcher
from mirrors_countme.scripts import countme_parse_access_log
from mirrors_countme.version import __version__
from mirrors_countme.writers import CSVWriter, SQLiteWriter


@pytest.fixture
def argv():
    return ["--sqlite=raw.db", "a log file"]


class TestParseArgs:
    def test_version(self, capsys):
        with pytest.raises(SystemExit) as excinfo:
            countme_parse_access_log.parse_args(["-V"])

        assert excinfo.value.code == 0
        stdout, _ = capsys.readouterr()
        assert __version__ in stdout

    @pytest.mark.parametrize("missing", ("logfile", "format"))
    def test_missing_mandatory(self, missing, capsys):
        argv = []
        if missing != "logfile":
            argv.append("alogfile")
        if missing != "format":
            argv.extend(("--format", "csv"))

        with pytest.raises(SystemExit):
            countme_parse_access_log.parse_args(argv)

        stdout, stderr = capsys.readouterr()

        assert not stdout

        if missing == "logfile":
            assert "error: the following arguments are required: LOG" in stderr

        if missing == "format":
            assert "error: one of the arguments --sqlite -f/--format is required" in stderr

    def test_logs(self, argv):
        args = countme_parse_access_log.parse_args(argv)
        assert args.logs == [item for item in argv if not item.startswith("-")]

    @pytest.mark.parametrize("option, expected", ((None, False), ("--progress", True)))
    def test_progress(self, option, expected, argv):
        if option:
            argv.append(option)

        args = countme_parse_access_log.parse_args(argv)

        assert bool(args.progress) == expected

    @pytest.mark.parametrize(
        "option, matchmode, matcher",
        (
            (None, "countme", CountmeMatcher),
            ("--matchmode=countme", "countme", CountmeMatcher),
            ("--matchmode=mirrors", "mirrors", MirrorMatcher),
        ),
    )
    def test_matchmode(self, option, matchmode, matcher, argv):
        if matchmode:
            argv.append(option)

        args = countme_parse_access_log.parse_args(argv)

        assert args.matchmode == matchmode
        assert args.matcher == matcher

    def test_mutually_exclusive_group(self, argv, capsys):
        with pytest.raises(SystemExit) as excinfo:
            countme_parse_access_log.parse_args(argv + ["--format=csv"])

        assert excinfo.value.code != 0
        stdout, stderr = capsys.readouterr()
        assert not stdout
        assert "error: argument -f/--format: not allowed with argument --sqlite" in stderr

    @pytest.mark.parametrize(
        "option, modified_flag",
        (("--no-header", "header"), ("--no-index", "index"), ("--no-dup-check", "dupcheck")),
    )
    def test_negating_flags(self, option, modified_flag, argv):
        ALL_FLAGS = ("header", "index", "dupcheck")

        args = countme_parse_access_log.parse_args(argv + [option])

        for flag in ALL_FLAGS:
            if flag == modified_flag:
                assert not getattr(args, flag)
            else:
                assert getattr(args, flag)

    @pytest.mark.parametrize("output_format", ("sqlite", "csv"))
    def test_formats(self, output_format, tmp_path):
        match output_format:
            case "sqlite":
                expected_dupcheck = True
                expected_writer = SQLiteWriter
                rawdb_path = tmp_path / "raw.db"
                argv = [f"--sqlite={rawdb_path}"]
            case "csv":
                expected_dupcheck = False
                argv = ["--format=csv"]
                expected_writer = CSVWriter

        argv.append("a log file")

        args = countme_parse_access_log.parse_args(argv)

        match output_format.split("-")[0]:
            case "sqlite":
                assert args.sqlite.name == str(rawdb_path)
                assert not args.format
            case "csv":
                assert args.format == "csv"
                assert not args.sqlite

        assert args.dupcheck == expected_dupcheck
        assert isinstance(args.writer, expected_writer)


@mock.patch("mirrors_countme.scripts.countme_parse_access_log.parse")
@mock.patch("mirrors_countme.scripts.countme_parse_access_log.parse_args")
def test_cli(parse_args, parse):
    parse_args.return_value = args = mock.Mock()

    countme_parse_access_log.cli()

    parse_args.assert_called_once_with()
    parse.assert_called_once_with(
        matchmode=args.matchmode,
        matcher=args.matcher,
        sqlite=args.sqlite,
        header=args.header,
        index=args.index,
        dupcheck=args.dupcheck,
        writer=args.writer,
        logs=args.logs,
        progress=args.progress,
    )
