import os

import pytest


@pytest.fixture
def tmp_path_cwd(tmp_path):
    """Return a temporary path and change into it"""
    old_wd = os.getcwd()
    os.chdir(tmp_path)
    yield tmp_path
    os.chdir(old_wd)
