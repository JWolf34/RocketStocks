import datetime
import logging
from zoneinfo import ZoneInfo
from rocketstocks.core.config.settings import settings

logger = logging.getLogger(__name__)

_runtime_tz: str | None = None


def configure_tz(tz: str) -> None:
    """Override the runtime timezone (e.g. loaded from DB at startup)."""
    global _runtime_tz
    _runtime_tz = tz


def timezone() -> ZoneInfo:
    return ZoneInfo(_runtime_tz or settings.tz)


def format_date_ymd(date: str | datetime.date) -> str:
    try:
        if isinstance(date, str):
            date = datetime.datetime.strptime(date, "%m/%d/%Y")
        return date.strftime("%Y-%m-%d")
    except ValueError as e:
        logger.warning(f"format_date_ymd: invalid date {date!r}: {e}")
        return str(date)


def format_date_mdy(date: str | datetime.date) -> str:
    try:
        if isinstance(date, str):
            date = datetime.datetime.strptime(date, "%Y-%m-%d")
        return date.strftime("%m/%d/%Y")
    except ValueError as e:
        logger.warning(f"format_date_mdy: invalid date {date!r}: {e}")
        return str(date)


def format_date_from_iso(date: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(date)


def dt_round_down(dt: datetime.datetime) -> datetime.datetime:
    delta = dt.minute % 5
    return dt.replace(minute=dt.minute - delta)


def seconds_until_minute_interval(minute: int) -> float:
    if minute <= 0:
        raise ValueError(f"minute must be > 0, got {minute}")
    now = datetime.datetime.now().astimezone()
    if now.minute % minute == 0:
        return 0
    minutes_by_increment = now.minute // minute
    diff = (minutes_by_increment + 1) * minute - now.minute
    future = (now + datetime.timedelta(minutes=diff)).replace(second=0, microsecond=0)
    return (future - now).total_seconds()


def round_down_nearest_minute(minute: int, dt: datetime.datetime | None = None) -> datetime.datetime:
    if dt is None:
        dt = datetime.datetime.now()
    rounded = dt - (dt - datetime.datetime.min) % datetime.timedelta(minutes=minute)
    return rounded


def format_duration_since(dt: datetime.datetime | None) -> str:
    """Format the elapsed time since a datetime, timezone-aware.

    Returns empty string if dt is None.
    """
    if dt is None:
        return ""

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)

    tz = timezone()
    now = datetime.datetime.now(tz=tz)
    elapsed = now - dt
    total_seconds = int(elapsed.total_seconds())

    if total_seconds < 0:
        return "(future)"

    if total_seconds < 60:
        return f"{total_seconds} seconds ago"
    elif total_seconds < 3600:
        minutes = total_seconds // 60
        seconds = total_seconds % 60
        if seconds == 0:
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        else:
            return f"{minutes} minute{'s' if minutes != 1 else ''} and {seconds} seconds ago"
    elif total_seconds < 86400:
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        if minutes == 0:
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        else:
            return f"{hours} hour{'s' if hours != 1 else ''} and {minutes} minute{'s' if minutes != 1 else ''} ago"
    else:
        days = total_seconds // 86400
        hours = (total_seconds % 86400) // 3600
        if hours == 0:
            return f"{days} day{'s' if days != 1 else ''} ago"
        else:
            return f"{days} day{'s' if days != 1 else ''} and {hours} hour{'s' if hours != 1 else ''} ago"


def today() -> datetime.date:
    """Return today's date in the configured timezone."""
    return datetime.datetime.now(tz=timezone()).date()

"""
# Backward-compat alias — remove once all callers updated
class date_utils:
    format_date_ymd = staticmethod(format_date_ymd)
    format_date_mdy = staticmethod(format_date_mdy)
    format_date_from_iso = staticmethod(format_date_from_iso)
    dt_round_down = staticmethod(dt_round_down)
    seconds_until_minute_interval = staticmethod(seconds_until_minute_interval)
    round_down_nearest_minute = staticmethod(round_down_nearest_minute)
    format_duration_since = staticmethod(format_duration_since)
    timezone = staticmethod(timezone)
    """
