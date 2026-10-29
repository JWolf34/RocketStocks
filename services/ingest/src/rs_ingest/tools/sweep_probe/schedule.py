"""When the close-to-morning chains are taken.

The first sample is five minutes after the session's close (an early close shifts it
with the close), then 18:05 ET, hourly on the hour overnight, half-hourly from 05:00
to 09:00 ET and a last one at 09:15 ET the next morning.
"""

from datetime import UTC, date, datetime, time, timedelta

from rs_domain import EASTERN, TradingCalendar, eastern_day

POST_CLOSE_DELAY = timedelta(minutes=5)
# A sample later than this past its slot is no longer that slot's observation.
MAX_LATE = timedelta(minutes=10)
_SEARCH_DAYS = 14


def first_sample_time(
    now: datetime, calendar: TradingCalendar, start: datetime | None = None
) -> tuple[date, datetime]:
    """(session day, first sample instant): `start` when given, else the next post-close sample
    that is not already past its grace period.

    The morning after the session day must itself be a session, or its chain would answer
    nothing about the next open; a schedule that cannot has no valid start (ValueError, naming one).
    """
    if start is None:
        return _next_valid(now, calendar)
    day = eastern_day(start)
    morning = day + timedelta(days=1)
    for problem, other in (
        (f"{day} is not a trading session", day),
        (f"the next morning, {morning}, is not a trading session", morning),
    ):
        if not calendar.is_session(other):
            _, valid = _next_valid(start, calendar)
            raise ValueError(
                f"--start {start.astimezone(EASTERN).isoformat(timespec='minutes')}: {problem}; "
                f"the next valid start is {valid.astimezone(EASTERN).isoformat(timespec='minutes')}"
            )
    return day, start.astimezone(UTC)


def _next_valid(now: datetime, calendar: TradingCalendar) -> tuple[date, datetime]:
    day = eastern_day(now)
    for _ in range(_SEARCH_DAYS):
        if calendar.is_session(day) and calendar.is_session(day + timedelta(days=1)):
            first = calendar.session_close(day) + POST_CLOSE_DELAY
            if now - first <= MAX_LATE:
                return day, first
        day += timedelta(days=1)
    raise ValueError(f"no valid session within {_SEARCH_DAYS} days of {now.isoformat()}")


def _eastern(day: date, hour: int, minute: int) -> datetime:
    return datetime.combine(day, time(hour, minute), tzinfo=EASTERN).astimezone(UTC)


def evening_sample_time(session_day: date) -> datetime:
    """The 18:05 ET sample, where the evening pass would start."""
    return _eastern(session_day, 18, 5)


def sample_times(session_day: date, first: datetime) -> tuple[datetime, ...]:
    """Every sample instant (UTC, ascending), `first` included; grid points before it are dropped."""
    morning = session_day + timedelta(days=1)
    grid = {first, evening_sample_time(session_day)}
    grid |= {_eastern(session_day, hour, 0) for hour in range(19, 24)}
    grid |= {_eastern(morning, hour, 0) for hour in range(0, 5)}
    for hour in range(5, 9):
        grid |= {_eastern(morning, hour, 0), _eastern(morning, hour, 30)}
    grid |= {_eastern(morning, 9, 0), _eastern(morning, 9, 15)}
    return tuple(sorted(t for t in grid if t >= first))
