"""Shared annotated types enforcing the v2 domain conventions."""

from datetime import UTC, datetime
from typing import Annotated

from pydantic import AfterValidator


def _require_utc(value: datetime) -> datetime:
    """Reject naive datetimes and non-UTC offsets at the validation boundary.

    Binding convention (S002): every datetime in the domain is timezone-aware
    UTC. Eastern Time exists only inside the TradingCalendar adapter and
    presentation code.

    Accepted values are normalized to `timezone.utc`: a zero-offset variable
    zone (e.g. Europe/London in winter) passes the offset check but must not
    survive as the tzinfo, or later wall-clock arithmetic could cross a DST
    boundary and drift off the true instant.
    """
    if value.tzinfo is None:
        raise ValueError("naive datetime — domain datetimes must be tz-aware UTC")
    if value.utcoffset() != UTC.utcoffset(None):
        raise ValueError(f"non-UTC datetime (offset {value.utcoffset()}) — convert to UTC at the edge")
    return value if value.tzinfo is UTC else value.astimezone(UTC)


UtcDatetime = Annotated[datetime, AfterValidator(_require_utc)]
