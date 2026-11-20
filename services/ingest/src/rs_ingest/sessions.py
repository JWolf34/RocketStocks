"""sessions: which trading session each pass of the nightly sweep works on, and its times.

A night belongs to the session whose evening last started (18:05 ET on the session's date), so
a run in the afternoon belongs to the session before and never waits for hours. The evening's
chains are owed until 19:00 ET; the daily bars pass stops at the first 03:00 Central after the
evening starts; the morning pass starts at 07:00 ET, once open interest has settled, and is
owed until the open. Every wall-clock time is Eastern on the session's date unless it says
otherwise.
"""

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from rs_domain import EASTERN, TradingCalendar, eastern_day

# Calendar days that always hold a session (the longest market closure is four days).
SESSION_SEARCH = timedelta(days=10)
# The evening's start: the Universe snapshot and the evening sweep's slot.
EVENING_STARTS_AT = time(18, 5)
# The evening chain pass is owed until then: past it Schwab's volume for the session starts to be
# reset to zero (the sweep probe saw it between 19:00 and 21:00).
CHAIN_DEADLINE_AT = time(19, 0)
# The morning pass starts once open interest has settled (the sweep probe saw it settle between
# 06:00 and 07:00 and hold until the open).
MORNING_STARTS_AT = time(7, 0)
# The daily bars pass stops at the first 03:00 Central after the evening it belongs to starts
# (04:00 ET), three hours before the morning pass begins. The clock time is a setting
# (RS_INGEST_DAILY_BARS_STOP_AT); the zone is fixed.
BARS_STOP_ZONE = ZoneInfo("America/Chicago")
BARS_STOP_AT = time(3, 0)
# A session's candle is trusted this long after the close; the bars pass starts once the evening's
# chains are done, well after it.
SETTLE = timedelta(hours=1)


def evening_start(session: date) -> datetime:
    """The moment `session`'s evening begins."""
    return datetime.combine(session, EVENING_STARTS_AT, tzinfo=EASTERN)


def last_evening_session(calendar: TradingCalendar, now: datetime) -> date:
    """The session whose evening last started at `now`: the session the night belongs to."""
    today = eastern_day(now)
    for day in reversed(calendar.sessions_in_range(today - SESSION_SEARCH, today)):
        if evening_start(day) <= now:
            return day
    raise RuntimeError(f"no session evening has started in the {SESSION_SEARCH.days} days before {today}")


def chain_deadline(session: date) -> datetime:
    """The evening chain pass's deadline for `session`."""
    return datetime.combine(session, CHAIN_DEADLINE_AT, tzinfo=EASTERN)


def morning_slot(calendar: TradingCalendar, now: datetime) -> tuple[date, datetime]:
    """The morning pass owed at `now`: the session its open interest settled for and its deadline.

    The latest trading day whose 07:00 ET has passed owns the morning; the session is the one
    before it and the deadline is its open. A weekend or holiday owns no morning, so it belongs
    to the last trading day's.
    """
    today = eastern_day(now)
    for day in reversed(calendar.sessions_in_range(today - SESSION_SEARCH, today)):
        if datetime.combine(day, MORNING_STARTS_AT, tzinfo=EASTERN) <= now:
            settled = calendar.sessions_in_range(day - SESSION_SEARCH, day - timedelta(days=1))
            if not settled:
                raise RuntimeError(f"no session in the {SESSION_SEARCH.days} days before the morning of {day}")
            return settled[-1], calendar.session_open(day)
    raise RuntimeError(f"no session morning has started in the {SESSION_SEARCH.days} days before {today}")


def bars_stop_time(now: datetime, at: time = BARS_STOP_AT) -> datetime:
    """The next `at` Central after `now`: when the daily bars pass begun at `now` stops."""
    today = datetime.combine(now.astimezone(BARS_STOP_ZONE).date(), at, tzinfo=BARS_STOP_ZONE)
    return today if today > now else today + timedelta(days=1)


def last_settled_session(calendar: TradingCalendar, now: datetime) -> date:
    """The latest session whose candle is settled at `now`."""
    today = eastern_day(now)
    for day in reversed(calendar.sessions_in_range(today - SESSION_SEARCH, today)):
        if calendar.session_close(day) + SETTLE <= now:
            return day
    raise RuntimeError(f"no settled session in the {SESSION_SEARCH.days} days before {today}")
