"""The US Eastern calendar day: the day for one Alert per stock and for the budget."""

from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo("America/New_York")


def eastern_day(moment: datetime) -> date:
    """The Eastern calendar date of an instant (it must be timezone-aware)."""
    if moment.tzinfo is None:
        raise ValueError("naive datetime - the Eastern day of a wall-clock reading is ambiguous")
    return moment.astimezone(EASTERN).date()


def eastern_day_start(day: date) -> datetime:
    """The instant the Eastern `day` begins, in UTC (23 or 25 hours long on a DST change)."""
    return datetime.combine(day, time.min, tzinfo=EASTERN).astimezone(UTC)
