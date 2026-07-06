"""Shared annotated types enforcing the v2 domain conventions."""

from datetime import datetime, timezone
from typing import Annotated

from pydantic import AfterValidator


def _require_utc(value: datetime) -> datetime:
    """Reject naive datetimes and non-UTC offsets at the validation boundary.

    Binding convention (S002): every datetime in the domain is timezone-aware
    UTC. Eastern Time exists only inside the TradingCalendar adapter and
    presentation code.
    """
    if value.tzinfo is None:
        raise ValueError("naive datetime — domain datetimes must be tz-aware UTC")
    if value.utcoffset() != timezone.utc.utcoffset(None):
        raise ValueError(f"non-UTC datetime (offset {value.utcoffset()}) — convert to UTC at the edge")
    return value


UtcDatetime = Annotated[datetime, AfterValidator(_require_utc)]
