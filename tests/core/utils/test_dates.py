"""Tests for rocketstocks.core.utils.dates."""
import datetime
from unittest.mock import patch

import pytest

from rocketstocks.core.utils.dates import date_utils


class TestFormatDateYmd:
    def test_datetime_object(self):
        dt = datetime.datetime(2024, 3, 15)
        assert date_utils.format_date_ymd(dt) == "2024-03-15"

    def test_string_mdy(self):
        assert date_utils.format_date_ymd("03/15/2024") == "2024-03-15"

    def test_month_padding(self):
        dt = datetime.datetime(2024, 1, 5)
        assert date_utils.format_date_ymd(dt) == "2024-01-05"


class TestFormatDateMdy:
    def test_datetime_object(self):
        dt = datetime.datetime(2024, 3, 15)
        assert date_utils.format_date_mdy(dt) == "03/15/2024"

    def test_string_ymd(self):
        assert date_utils.format_date_mdy("2024-03-15") == "03/15/2024"

    def test_single_digit_month_day(self):
        dt = datetime.datetime(2024, 1, 5)
        assert date_utils.format_date_mdy(dt) == "01/05/2024"


class TestFormatDateFromIso:
    def test_basic_iso_string(self):
        result = date_utils.format_date_from_iso("2024-03-15T10:30:00")
        assert isinstance(result, datetime.datetime)
        assert result.year == 2024
        assert result.month == 3
        assert result.day == 15


class TestDtRoundDown:
    def test_rounds_down_to_nearest_5min(self):
        dt = datetime.datetime(2024, 1, 1, 10, 37, 45)
        result = date_utils.dt_round_down(dt)
        assert result.minute == 35
        assert result.second == 45  # seconds preserved

    def test_on_exact_5min_boundary(self):
        dt = datetime.datetime(2024, 1, 1, 10, 35, 0)
        result = date_utils.dt_round_down(dt)
        assert result.minute == 35

    def test_on_zero_minute(self):
        dt = datetime.datetime(2024, 1, 1, 10, 0, 0)
        result = date_utils.dt_round_down(dt)
        assert result.minute == 0


class TestSecondsUntilMinuteInterval:
    def test_returns_zero_when_on_interval(self):
        fake_now = datetime.datetime(2024, 1, 1, 10, 5, 0).astimezone()
        with patch("rocketstocks.core.utils.dates.datetime.datetime") as mock_dt:
            mock_dt.now.return_value = fake_now
            result = date_utils.seconds_until_minute_interval(5)
        assert result == 0

    def test_returns_positive_when_not_on_interval(self):
        fake_now = datetime.datetime(2024, 1, 1, 10, 3, 30).astimezone()
        with patch("rocketstocks.core.utils.dates.datetime.datetime") as mock_dt:
            mock_dt.now.return_value = fake_now
            mock_dt.timedelta = datetime.timedelta
            result = date_utils.seconds_until_minute_interval(5)
        assert result > 0


class TestTimezone:
    def test_returns_zoneinfo(self):
        from zoneinfo import ZoneInfo
        with patch("rocketstocks.core.utils.dates.get_env", return_value="America/Chicago"):
            tz = date_utils.timezone()
        assert isinstance(tz, ZoneInfo)

    def test_defaults_to_chicago_on_none(self):
        from zoneinfo import ZoneInfo
        with patch("rocketstocks.core.utils.dates.get_env", return_value=None):
            tz = date_utils.timezone()
        assert tz == ZoneInfo("America/Chicago")
