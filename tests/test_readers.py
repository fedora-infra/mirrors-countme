import pytest

from mirrors_countme.readers import ItemReader


class TestReaders:
    def test_NotImplementedError__get_reader(self):
        with pytest.raises(NotImplementedError):
            ItemReader._get_reader(None)

    def test_NotImplementedError__get_fields(self):
        with pytest.raises(NotImplementedError):
            ItemReader._get_fields(None)

    def test_NotImplementedError__iter_rows(self):
        with pytest.raises(NotImplementedError):
            ItemReader._iter_rows(None)

    def test_NotImplementedError__find_item(self):
        with pytest.raises(NotImplementedError):
            ItemReader._find_item(None, None)
