"""The sweep's session times against the real exchange calendar: the evening's start, the chain
deadline, the morning slot, the bars' stop time and the last settled session, across weekends,
holidays and daylight saving."""

from datetime import UTC, date, datetime, time

from rs_ingest.sessions import (
    bars_stop_time,
    chain_deadline,
    evening_start,
    last_evening_session,
    last_settled_session,
    morning_slot,
)
from rs_sources import ExchangeCalendar

CALENDAR = ExchangeCalendar()
# Tuesday 2026-11-17.
SESSION = date(2026, 11, 17)
# Wednesday 2026-11-18, 07:30 EST: the morning that settles Tuesday's session, and its open.
MORNING_NOW = datetime(2026, 11, 18, 12, 30, tzinfo=UTC)
OPEN = datetime(2026, 11, 18, 14, 30, tzinfo=UTC)


# --- the evening -----------------------------------------------------------------------------------


def test_an_evening_starts_at_5_past_6_eastern_on_the_sessions_date():
    assert evening_start(date(2026, 11, 17)) == datetime(2026, 11, 17, 23, 5, tzinfo=UTC)  # EST
    assert evening_start(date(2026, 10, 20)) == datetime(2026, 10, 20, 22, 5, tzinfo=UTC)  # EDT


def test_a_night_belongs_to_the_session_whose_evening_last_started():
    assert last_evening_session(CALENDAR, datetime(2026, 11, 17, 23, 5, tzinfo=UTC)) == SESSION
    # A minute before the evening, and the afternoon, belong to the session before.
    assert last_evening_session(CALENDAR, datetime(2026, 11, 17, 23, 4, tzinfo=UTC)) == date(2026, 11, 16)
    # The weekend belongs to Friday's evening.
    assert last_evening_session(CALENDAR, datetime(2026, 11, 15, 17, 0, tzinfo=UTC)) == date(2026, 11, 13)


def test_the_deadline_is_7pm_eastern_on_the_sessions_own_evening():
    # Tuesday 2026-11-17: 19:00 EST is 00:00 UTC on the 18th.
    assert chain_deadline(SESSION) == datetime(2026, 11, 18, 0, 0, tzinfo=UTC)
    # Friday's evening ends the same evening, not on the weekend.
    assert chain_deadline(date(2026, 11, 13)) == datetime(2026, 11, 14, 0, 0, tzinfo=UTC)
    # Before the clocks fall back (2026-11-01) it is 19:00 EDT, 23:00 UTC.
    assert chain_deadline(date(2026, 10, 20)) == datetime(2026, 10, 20, 23, 0, tzinfo=UTC)


# --- the bars ---------------------------------------------------------------------------------------


def test_the_bars_pass_stops_at_the_next_3am_central():
    now = datetime(2026, 10, 20, 22, 0, tzinfo=UTC)
    # Wednesday 2026-10-21 03:00 CDT is 08:00 UTC.
    assert bars_stop_time(now) == datetime(2026, 10, 21, 8, 0, tzinfo=UTC)
    assert bars_stop_time(datetime(2026, 10, 21, 7, 59, tzinfo=UTC)) == datetime(2026, 10, 21, 8, 0, tzinfo=UTC)
    assert bars_stop_time(datetime(2026, 10, 21, 8, 0, tzinfo=UTC)) == datetime(2026, 10, 22, 8, 0, tzinfo=UTC)
    # After the clocks fall back (2026-11-01) 03:00 CST is 09:00 UTC.
    assert bars_stop_time(datetime(2026, 11, 1, 5, 0, tzinfo=UTC)) == datetime(2026, 11, 1, 9, 0, tzinfo=UTC)
    # The clock time is configurable: 04:30 CDT is 09:30 UTC.
    assert bars_stop_time(now, time(4, 30)) == datetime(2026, 10, 21, 9, 30, tzinfo=UTC)


def test_a_session_is_settled_an_hour_after_its_close():
    # Tuesday's 16:00 EST close is 21:00 UTC.
    assert last_settled_session(CALENDAR, datetime(2026, 11, 17, 22, 0, tzinfo=UTC)) == SESSION
    assert last_settled_session(CALENDAR, datetime(2026, 11, 17, 21, 59, tzinfo=UTC)) == date(2026, 11, 16)
    # Thanksgiving (2026-11-26) has no session; the day after it closes early, at 13:00 EST.
    assert last_settled_session(CALENDAR, datetime(2026, 11, 26, 17, 0, tzinfo=UTC)) == date(2026, 11, 25)
    assert last_settled_session(CALENDAR, datetime(2026, 11, 27, 19, 0, tzinfo=UTC)) == date(2026, 11, 27)


# --- the morning ------------------------------------------------------------------------------------


def _slot(now: datetime) -> tuple[date, datetime]:
    return morning_slot(CALENDAR, now)


def test_a_morning_settles_the_session_before_it_and_is_owed_until_its_open():
    assert _slot(MORNING_NOW) == (SESSION, OPEN)
    # The slot starts at 07:00 ET exactly (12:00 UTC); a minute earlier still belongs to Tuesday's morning.
    assert _slot(datetime(2026, 11, 18, 12, 0, tzinfo=UTC)) == (SESSION, OPEN)
    assert _slot(datetime(2026, 11, 18, 11, 59, tzinfo=UTC)) == (
        date(2026, 11, 16),
        datetime(2026, 11, 17, 14, 30, tzinfo=UTC),
    )
    # Hours after the open the morning is still that morning's, until the next 07:00.
    assert _slot(datetime(2026, 11, 18, 23, 0, tzinfo=UTC)) == (SESSION, OPEN)


def test_a_monday_morning_settles_fridays_session_and_a_weekend_belongs_to_fridays_morning():
    assert _slot(datetime(2026, 11, 16, 12, 30, tzinfo=UTC)) == (
        date(2026, 11, 13),
        datetime(2026, 11, 16, 14, 30, tzinfo=UTC),
    )
    assert _slot(datetime(2026, 11, 14, 17, 0, tzinfo=UTC)) == (
        date(2026, 11, 12),
        datetime(2026, 11, 13, 14, 30, tzinfo=UTC),
    )


def test_the_morning_after_a_holiday_settles_the_session_before_it_and_the_holiday_owns_none():
    # Thanksgiving is Thursday 2026-11-26: Friday's morning settles Wednesday; Thursday belongs to Wednesday's morning.
    assert _slot(datetime(2026, 11, 27, 12, 30, tzinfo=UTC)) == (
        date(2026, 11, 25),
        datetime(2026, 11, 27, 14, 30, tzinfo=UTC),
    )
    assert _slot(datetime(2026, 11, 26, 15, 0, tzinfo=UTC)) == (
        date(2026, 11, 24),
        datetime(2026, 11, 25, 14, 30, tzinfo=UTC),
    )


def test_the_open_follows_daylight_saving():
    # 09:30 EDT is 13:30 UTC.
    assert _slot(datetime(2026, 10, 21, 12, 0, tzinfo=UTC)) == (
        date(2026, 10, 20),
        datetime(2026, 10, 21, 13, 30, tzinfo=UTC),
    )
