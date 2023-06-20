import locale
import os
from pathlib import Path

import pytest

if [int(x) for x in pytest.__version__.split(".")] < [3, 9, 0]:
    # the tmp_path fixture is only available in pytest >= 3.9.0
    @pytest.fixture
    def tmp_path(tmpdir):
        return Path(tmpdir)


@pytest.fixture
def tmp_path_cwd(tmp_path):
    """Return a temporary path and change into it"""
    old_wd = os.getcwd()
    os.chdir(tmp_path)
    yield tmp_path
    os.chdir(old_wd)


@pytest.fixture(autouse=True)
def use_english_locale():
    locale.setlocale(locale.LC_ALL, "en_US.UTF-8")
