import gzip
import lzma
import re
import sys
from contextlib import nullcontext
from unittest import mock

import pytest
from hypothesis import example, given
from hypothesis.strategies import integers

from mirrors_countme import progress

log_line = (
    '121.43.225.226 - - [01/May/2023:00:00:02 +0000] "GET '
    "/metalink?repo=epel-8&arch=x86_64&infra=stock&content=centos&countme=4 "
    'HTTP/1.1" 200 7547 "-" "libdnf (CentOS Linux 8; generic; Linux.x86_64)"'
)

log_line_no_date = (
    r'121.43.225.226 - - "GET /metalink?repo=epel-8&arch=x86_64&infra=stock&'
    r'content=centos&countme=4 HTTP/1.1" 200 7547 "-" "libdnf '
    r'(CentOS Linux 8; generic; Linux.x86_64)"'
)


@pytest.mark.parametrize(
    "date, expected",
    [
        (log_line, "01/May/2023"),
        (log_line_no_date, "??/??/????"),
        (1683208046.7402434, TypeError),
    ],
)
def test_log_date(date, expected):
    if isinstance(expected, str):
        expectation = nullcontext()
    else:
        expectation = pytest.raises(expected)

    with expectation:
        obtained = progress.log_date(date)

    if isinstance(expected, str):
        assert obtained == expected


@pytest.mark.parametrize("filetype", ("plain", "gzip", "xz"))
def test_log_reader(filetype):
    with mock.patch("mirrors_countme.progress.lzma") as lzma, mock.patch(
        "mirrors_countme.progress.gzip"
    ) as gzip, mock.patch("mirrors_countme.progress.open") as open:
        match filetype:
            case "plain":
                fn = open
                filename = "test.log"
            case "gzip":
                fn = gzip.open
                filename = "test.log.gz"
            case "xz":
                fn = lzma.open
                filename = "test.log.xz"

        fn.return_value = result_sentinel = object()

        result = progress.log_reader(filename)

    assert result is result_sentinel
    for open_fn in (lzma.open, gzip.open, open):
        if open_fn is fn:
            open_fn.assert_called_once_with(filename, mode="rt", errors="replace")
        else:
            open_fn.assert_not_called()


@pytest.fixture
def logfile_content() -> bytes:
    return b"x" * 1024


@pytest.fixture
def plain_logfile(logfile_content, tmp_path):
    plain_logfile = tmp_path / "test.log"
    plain_logfile.write_bytes(logfile_content)
    return plain_logfile


@pytest.fixture
def xz_logfile(logfile_content, tmp_path):
    xz_logfile = tmp_path / "test.log.xz"
    with lzma.LZMAFile(xz_logfile, mode="w", format=lzma.FORMAT_XZ) as fp:
        fp.write(logfile_content)
    return xz_logfile


@pytest.fixture
def gz_logfile(logfile_content, tmp_path):
    gz_logfile = tmp_path / "test.log.gz"
    with gzip.GzipFile(gz_logfile, mode="w") as fp:
        fp.write(logfile_content)
    return gz_logfile


@pytest.mark.parametrize("file_exists", (True, False), ids=("file-exists", "file-missing"))
@pytest.mark.parametrize("filetype", ("plain", "gzip", "xz"))
def test_log_total_size(
    filetype, file_exists, logfile_content, plain_logfile, gz_logfile, xz_logfile
):
    with mock.patch(
        "mirrors_countme.progress.xz_log_size", wraps=progress.xz_log_size
    ) as xz_log_size, mock.patch(
        "mirrors_countme.progress.gz_log_size", wraps=progress.gz_log_size
    ) as gz_log_size, mock.patch.object(
        progress.os, "stat", wraps=progress.os.stat
    ) as stat:
        match filetype:
            case "plain":
                filepath = plain_logfile
                wrapped_fn = stat
            case "gzip":
                filepath = gz_logfile
                wrapped_fn = gz_log_size
            case "xz":
                filepath = xz_logfile
                wrapped_fn = xz_log_size

        if not file_exists:
            filepath.unlink()

        result = progress.log_total_size(str(filepath))

    if file_exists:
        assert result == len(logfile_content)
    else:
        assert result is None

    for fn in (stat, gz_log_size, xz_log_size):
        if fn is wrapped_fn:
            fn.assert_called_once_with(str(filepath))
        else:
            fn.assert_not_called()


class TestDIYProgress:
    DISPLAY_TEXT_RE = re.compile(
        r"^Test:\s+(?P<pct>\d+)%\s+\[(?P<hashes_tail>#*[-_=#])(?P<spaces> *)\]\s+"
        + r"(?P<count>\d+(?:\.\d+[kmgtp]?)?b)/"
        + r"(?P<total>\d+(?:\.\d+[kmgtp]?)?b)\s*$"
    )
    TEST_TOTAL = 1000000

    @pytest.mark.parametrize("with_file", (False, True), ids=("without-file", "with-file"))
    def test__init__(self, with_file):
        params = ("desc", "total", "disable", "unit", "unit_scale", "barchar")

        kwargs = {p: p for p in params}
        if with_file:
            kwargs["file"] = "file"

        obj = progress.DIYProgress(**kwargs)

        for param in params:
            assert getattr(obj, param) == param

        if with_file:
            assert obj.file == "file"
        else:
            assert obj.file == sys.stderr

        assert obj.count == 0
        assert obj.showat == 0

    @pytest.mark.parametrize("testcase", ("happy-path", "no-refresh", "disabled"))
    def test_set_description(self, testcase):
        obj = progress.DIYProgress(disable="disabled" in testcase)

        desc_sentinel = object()

        with mock.patch.object(progress.DIYProgress, "display") as display:
            obj.set_description(desc_sentinel, refresh="no-refresh" not in testcase)

        assert obj.desc is desc_sentinel
        if "no-refresh" in testcase or "disabled" in testcase:
            display.assert_not_called()
        else:
            display.assert_called_once_with()

    @pytest.mark.parametrize("testcase", ("tick", "no-tick", "disabled"))
    def test_update(self, testcase):
        obj = progress.DIYProgress(total=self.TEST_TOTAL, disable="disabled" in testcase)

        # update() "ticks" at hundredths and increments before checking
        if "no-tick" in testcase:
            obj.count = self.TEST_TOTAL // 100 - 2
        else:
            obj.count = self.TEST_TOTAL // 100 - 1

        count_prev = obj.count
        obj.showat = showat_prev = self.TEST_TOTAL // 100

        with mock.patch.object(progress.DIYProgress, "display") as display:
            obj.update()

        if "disabled" in testcase:
            # Assert no-op
            assert obj.count == count_prev
            assert obj.showat == showat_prev
            display.assert_not_called()
        else:
            assert obj.count == count_prev + 1
            if "no-tick" in testcase:
                assert obj.showat == showat_prev
                display.assert_not_called()
            else:
                assert obj.showat == showat_prev + self.TEST_TOTAL // 100
                display.assert_called_once_with()

    @given(size=integers(min_value=0, max_value=20))
    def test_iter(self, size):
        obj = progress.DIYProgress()

        with mock.patch.object(progress.DIYProgress, "update") as update:
            result = list(obj.iter(range(size)))

        assert result == list(range(size))
        assert len(update.call_args_list) == size

    @pytest.mark.parametrize("scale", " kmgtpe")
    def test_hrsize(self, scale):
        exp = " kmgtpe".index(scale) * 3
        value = 12.3456 * 10**exp
        result = progress.DIYProgress.hrsize(value)
        if scale == " ":
            scale = ""
        if scale != "e":
            assert result == f"12.3{scale}"
        else:
            assert result == "12345.6p"

    @pytest.mark.parametrize("unit_scale", (True, False), ids=("scaled", "unscaled"))
    @given(count=integers(min_value=0, max_value=int(TEST_TOTAL * 1.01 + 1)))
    @example(count=0)  # 0%
    @example(count=TEST_TOTAL)  # 100%
    @example(count=int(TEST_TOTAL * 1.01 + 1))  # >= 101%
    def test_display(self, unit_scale, count):
        obj = progress.DIYProgress(desc="Test", total=self.TEST_TOTAL, unit_scale=unit_scale)
        obj.count = count

        with mock.patch("mirrors_countme.progress.print") as print, mock.patch.object(
            progress.DIYProgress, "hrsize", wraps=progress.DIYProgress.hrsize
        ) as hrsize:
            obj.display()

        if unit_scale:
            assert hrsize.call_count == 2
        else:
            hrsize.assert_not_called()

        print.assert_called_once()
        assert print.call_args.kwargs == {
            "flush": True,
            "file": obj.file,
            "end": "\r",
        }
        text = print.call_args.args[0]
        match = self.DISPLAY_TEXT_RE.match(text)
        groups = match.groupdict()

        #####################
        # Verify the bar, ugh
        #####################

        pct = (count * 100) // self.TEST_TOTAL
        num_leading_hashes = min(pct, 100) // 4
        expected_tail = "_-=#"[pct % 4]
        num_spaces = 25 - 1 - num_leading_hashes
        assert int(groups["pct"]) == pct
        hashes = groups["hashes_tail"][:-1]
        tail = groups["hashes_tail"][-1]
        if pct < 100:
            assert len(hashes) == num_leading_hashes
            assert tail == expected_tail
            assert len(groups["spaces"]) == num_spaces
        else:
            assert len(hashes) == num_leading_hashes - 1
            assert tail == "#"
            assert len(groups["spaces"]) == num_spaces + 1

        ##########################
        # Verify numbers displayed
        ##########################

        # Cut off trailing "b"
        reported_count = groups["count"][:-1]
        reported_total = groups["total"][:-1]

        if unit_scale:
            assert reported_count == obj.hrsize(count)
            assert reported_total == obj.hrsize(self.TEST_TOTAL)
        else:
            assert reported_count == str(count)
            assert reported_total == str(self.TEST_TOTAL)

    @pytest.mark.parametrize("disable", (False, True), ids=("enabled", "disabled"))
    def test_close(self, disable):
        obj = progress.DIYProgress(disable=disable)

        with mock.patch("mirrors_countme.progress.print") as print:
            obj.close()

        if disable:
            print.assert_not_called()
        else:
            print.assert_called_with(flush=True, file=obj.file)


class TestReadProgress:
    def test___init__(self):
        logs_sentinel = object()
        obj = progress.ReadProgress(logs_sentinel)
        assert obj.logs is logs_sentinel
        assert obj.display

    def test___iter__(self):
        logs = [f"log{i}" for i in range(10)]
        obj = progress.ReadProgress(logs)

        with mock.patch("mirrors_countme.progress.log_reader") as log_reader, mock.patch(
            "mirrors_countme.progress.log_total_size"
        ) as log_total_size, mock.patch.object(
            progress.ReadProgress, "_iter_log_lines"
        ) as _iter_log_lines:
            log_reader.side_effect = lambda logfn: f"log_reader({logfn})"
            log_total_size.side_effect = lambda logfn: f"log_total_size({logfn})"
            _iter_log_lines.side_effect = lambda logf, num, total: (
                f"_iter_log_lines({logf}, {num}, {total})"
            )

            iterables = list(iter(obj))

            assert iterables == [
                f"_iter_log_lines(log_reader(log{i}), {i}, log_total_size(log{i}))"
                for i in range(10)
            ]

    def test__progress_obj(self):
        obj = progress.ReadProgress(object())
        with mock.patch("mirrors_countme.progress.DIYProgress") as DIYProgress:
            DIYProgress.return_value = result_sentinel = object()
            result = obj._progress_obj()
        assert result is result_sentinel
        DIYProgress.assert_called_once_with()

    def test__iter_log_lines(self):
        NUM_LINES = 100
        LOG_LINES = [f"line {i + 1}\n" for i in range(NUM_LINES)]
        TOTAL_SIZE = sum(len(line) for line in LOG_LINES)

        obj = progress.ReadProgress([object()])

        with mock.patch.object(progress.ReadProgress, "_progress_obj") as _progress_obj, mock.patch(
            "mirrors_countme.progress.log_date"
        ) as log_date:
            _progress_obj.return_value = prog = mock.Mock()
            log_date.side_effect = lambda line: f"log_date({line})"

            iterated_lines = list(obj._iter_log_lines(logf=LOG_LINES, num=0, total=TOTAL_SIZE))

        prog.set_description.assert_called_once_with("log 1/1, date=log_date(line 1\n)")
        assert prog.update.call_count == NUM_LINES
        prog.close.assert_called_once_with()

        assert iterated_lines == LOG_LINES
