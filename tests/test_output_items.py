import datetime as dt

from hypothesis import given
from hypothesis.strategies import integers

from mirrors_countme import output_items


class TestLogItem:
    def make_item(self, dt_value):
        return output_items.LogItem(
            host="127.0.0.1",
            identity="-",
            time=dt_value.strftime("%d/%b/%Y:%H:%M:%S %z"),
            method="GET",
            path="/",
            query=None,
            protocol="https",
            status=200,
            nbytes=None,
            referrer="127.0.0.1",
            user_agent="provocateur",
        )

    @given(timestamp=integers(min_value=0, max_value=2**32))
    def test_datetime(self, timestamp):
        dt_value = dt.datetime.utcfromtimestamp(timestamp).replace(tzinfo=dt.UTC)
        item = self.make_item(dt_value)

        assert item.datetime() == dt_value

    @given(timestamp=integers(min_value=0, max_value=2**32))
    def test_timestamp(self, timestamp):
        dt_value = dt.datetime.utcfromtimestamp(timestamp).replace(tzinfo=dt.UTC)
        item = self.make_item(dt_value)

        assert item.timestamp() == timestamp
