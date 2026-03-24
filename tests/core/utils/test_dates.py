"""Tests for rocketstocks.core.utils.dates."""
import datetime
from unittest.mock import patch

import pytest

from rocketstocks.core.utils.dates import (
    configure_tz, timezone, format_date_ymd, format_date_mdy,
    format_date_from_iso, dt_round_down, seconds_until_minute_interval,
    round_down_nearest_minute, format_duration_since, today,
)
import rocketstocks.core.utils.dates as date_utils
import rocketstocks.core.utils.dates as dates_module  # noqa: F811 (also aliased as date_utils above)


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
        with patch("rocketstocks.core.utils.dates.settings") as mock_settings:
            mock_settings.tz = "America/Chicago"
            tz = date_utils.timezone()
        assert isinstance(tz, ZoneInfo)

    def test_uses_configured_tz(self):
        from zoneinfo import ZoneInfo
        with patch("rocketstocks.core.utils.dates.settings") as mock_settings:
            mock_settings.tz = "America/New_York"
            tz = date_utils.timezone()
        assert tz == ZoneInfo("America/New_York")


class TestConfigureTz:
    def setup_method(self):
        """Reset runtime tz before each test."""
        dates_module._runtime_tz = None

    def teardown_method(self):
        """Reset runtime tz after each test."""
        dates_module._runtime_tz = None

    def test_configure_tz_sets_runtime_tz(self):
        from rocketstocks.core.utils.dates import configure_tz
        configure_tz("UTC")
        assert dates_module._runtime_tz == "UTC"

    def test_timezone_uses_runtime_tz_when_set(self):
        from zoneinfo import ZoneInfo
        from rocketstocks.core.utils.dates import configure_tz
        configure_tz("Europe/London")
        with patch("rocketstocks.core.utils.dates.settings") as mock_settings:
            mock_settings.tz = "America/Chicago"
            tz = date_utils.timezone()
        assert tz == ZoneInfo("Europe/London")

    def test_timezone_falls_back_to_settings_when_runtime_not_set(self):
        from zoneinfo import ZoneInfo
        assert dates_module._runtime_tz is None
        with patch("rocketstocks.core.utils.dates.settings") as mock_settings:
            mock_settings.tz = "America/Chicago"
            tz = date_utils.timezone()
        assert tz == ZoneInfo("America/Chicago")

    def test_configure_tz_overrides_previous_value(self):
        from rocketstocks.core.utils.dates import configure_tz
        configure_tz("UTC")
        configure_tz("Asia/Tokyo")
        assert dates_module._runtime_tz == "Asia/Tokyo"


class TestFormatDurationSince:
    def test_none_returns_empty_string(self):
        result = date_utils.format_duration_since(None)
        assert result == ""

    def test_seconds_ago(self):
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        past = now - datetime.timedelta(seconds=30)
        with patch("rocketstocks.core.utils.dates.timezone", return_value=datetime.timezone.utc):
            result = date_utils.format_duration_since(past)
        assert "seconds ago" in result

    def test_minutes_ago(self):
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        past = now - datetime.timedelta(minutes=5)
        with patch("rocketstocks.core.utils.dates.timezone", return_value=datetime.timezone.utc):
            result = date_utils.format_duration_since(past)
        assert "5 minute" in result and "ago" in result

    def test_minutes_and_seconds_ago(self):
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        past = now - datetime.timedelta(minutes=2, seconds=30)
        with patch("rocketstocks.core.utils.dates.timezone", return_value=datetime.timezone.utc):
            result = date_utils.format_duration_since(past)
        assert "2 minute" in result and "30 second" in result and "ago" in result

    def test_hours_ago(self):
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        past = now - datetime.timedelta(hours=3)
        with patch("rocketstocks.core.utils.dates.timezone", return_value=datetime.timezone.utc):
            result = date_utils.format_duration_since(past)
        assert "3 hour" in result and "ago" in result

    def test_hours_and_minutes_ago(self):
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        past = now - datetime.timedelta(hours=2, minutes=15)
        with patch("rocketstocks.core.utils.dates.timezone", return_value=datetime.timezone.utc):
            result = date_utils.format_duration_since(past)
        assert "2 hour" in result and "15 minute" in result and "ago" in result

    def test_days_ago(self):
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        past = now - datetime.timedelta(days=5)
        with patch("rocketstocks.core.utils.dates.timezone", return_value=datetime.timezone.utc):
            result = date_utils.format_duration_since(past)
        assert "5 day" in result and "ago" in result

    def test_days_and_hours_ago(self):
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        past = now - datetime.timedelta(days=2, hours=6)
        with patch("rocketstocks.core.utils.dates.timezone", return_value=datetime.timezone.utc):
            result = date_utils.format_duration_since(past)
        assert "2 day" in result and "6 hour" in result and "ago" in result

    def test_singular_hour(self):
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        past = now - datetime.timedelta(hours=1)
        with patch("rocketstocks.core.utils.dates.timezone", return_value=datetime.timezone.utc):
            result = date_utils.format_duration_since(past)
        assert "1 hour ago" in result  # singular form

    def test_singular_minute(self):
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        past = now - datetime.timedelta(minutes=1)
        with patch("rocketstocks.core.utils.dates.timezone", return_value=datetime.timezone.utc):
            result = date_utils.format_duration_since(past)
        assert "1 minute ago" in result  # singular form

    def test_singular_day(self):
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        past = now - datetime.timedelta(days=1)
        with patch("rocketstocks.core.utils.dates.timezone", return_value=datetime.timezone.utc):
            result = date_utils.format_duration_since(past)
        assert "1 day ago" in result  # singular form

    def test_naive_datetime_assumed_utc(self):
        # Create a naive datetime that's X minutes ago
        past_naive = datetime.datetime.utcnow() - datetime.timedelta(minutes=3)
        with patch("rocketstocks.core.utils.dates.timezone", return_value=datetime.timezone.utc):
            result = date_utils.format_duration_since(past_naive)
        assert "minute" in result and "ago" in result
