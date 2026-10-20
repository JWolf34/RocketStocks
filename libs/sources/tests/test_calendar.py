"""Known-date tests for the XNYS calendar adapter. No network - exchange_calendars bundles its data.

Dates used:
- 2026-07-06 (Mon): regular session, EDT (UTC-4) → open 13:30Z, close 20:00Z.
- 2026-07-03 (Fri): Independence Day observed — holiday.
- 2026-11-27 (Fri): day after Thanksgiving — 13:00 ET early close, asserted via session-length delta.
- 2026-01-05 (Mon): regular session, EST (UTC-5) → open 14:30Z (DST coverage).
"""

from datetime import UTC, date, datetime, timedelta

import pytest

from rs_domain import TradingCalendar
from rs_sources.calendar import ExchangeCalendar


@pytest.fixture(scope="module")
def cal() -> ExchangeCalendar:
    return ExchangeCalendar()


class TestSessions:
    def test_regular_day_is_session(self, cal):
        assert cal.is_session(date(2026, 7, 6))

    def test_holiday_is_not_session(self, cal):
        assert not cal.is_session(date(2026, 7, 3))

    def test_weekend_is_not_session(self, cal):
        assert not cal.is_session(date(2026, 7, 5))


class TestSessionBounds:
    def test_regular_open_close_edt(self, cal):
        day = date(2026, 7, 6)
        assert cal.session_open(day) == datetime(2026, 7, 6, 13, 30, tzinfo=UTC)
        assert cal.session_close(day) == datetime(2026, 7, 6, 20, 0, tzinfo=UTC)

    def test_regular_open_est(self, cal):
        # Same 09:30 ET wall clock, different UTC instant across DST.
        assert cal.session_open(date(2026, 1, 5)) == datetime(2026, 1, 5, 14, 30, tzinfo=UTC)

    def test_early_close_thanksgiving_friday(self, cal):
        day = date(2026, 11, 27)
        open_, close = cal.session_open(day), cal.session_close(day)
        # 09:30 → 13:00 ET: a 3.5-hour session instead of 6.5.
        assert close - open_ == timedelta(hours=3, minutes=30)

    def test_all_bounds_are_utc(self, cal):
        for instant in (cal.session_open(date(2026, 7, 6)), cal.session_close(date(2026, 11, 27))):
            assert instant.utcoffset() == timedelta(0)

    def test_non_session_raises(self, cal):
        with pytest.raises(ValueError, match="not a session"):
            cal.session_open(date(2026, 7, 3))


class TestSessionsInRange:
    def test_skips_holiday_and_weekend_and_includes_both_ends(self, cal):
        # Thu 07-02, Fri 07-03 holiday, weekend, Mon 07-06.
        assert cal.sessions_in_range(date(2026, 7, 2), date(2026, 7, 6)) == [
            date(2026, 7, 2),
            date(2026, 7, 6),
        ]

    def test_range_without_sessions_is_empty(self, cal):
        assert cal.sessions_in_range(date(2026, 7, 3), date(2026, 7, 5)) == []


class TestSupportedRange:
    def test_dates_outside_the_supported_range_raise(self, cal):
        with pytest.raises(ValueError):
            cal.is_session(date(1985, 1, 2))
        with pytest.raises(ValueError):
            cal.is_session(date(2060, 1, 2))


def test_satisfies_protocol(cal):
    calendar: TradingCalendar = cal
    assert calendar.is_session(date(2026, 7, 6))
