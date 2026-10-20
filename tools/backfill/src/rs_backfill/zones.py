"""The timezone v1 wrote its naive `datetime` column in, and conversion to UTC.

A zone is named by a string: an IANA name ("America/Chicago") or a fixed
offset ("-06:00"). UTC also covers the fixed +00:00 candidate: the two are the
same conversion.
"""

import re
from datetime import UTC, datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# Tried in this order by verify-offset.
CANDIDATE_ZONES = ("UTC", "America/Chicago", "America/New_York", "-06:00", "-05:00", "-04:00")

BUCKET = timedelta(minutes=30)

_FIXED = re.compile(r"^([+-])(\d{2}):(\d{2})$")


def resolve_zone(name: str) -> tzinfo:
    fixed = _FIXED.match(name)
    if fixed:
        offset = timedelta(hours=int(fixed.group(2)), minutes=int(fixed.group(3)))
        return timezone(-offset if fixed.group(1) == "-" else offset)
    if name == "UTC":
        return UTC
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"unknown zone {name!r} (IANA name or fixed offset like -06:00)") from exc


def is_local_zone(name: str) -> bool:
    """True for zones with DST (an IANA name other than UTC)."""
    return not _FIXED.match(name) and name != "UTC"


def naive_to_utc(naive: datetime, zone: tzinfo) -> datetime | None:
    """Interpret a naive wall-clock time in `zone`. The repeated hour of a
    fall-back resolves to its first pass (what v1's primary key kept); None for a
    wall-clock time that does not exist (the spring-forward gap)."""
    aware = naive.replace(tzinfo=zone)
    instant = aware.astimezone(UTC)
    if instant.astimezone(zone).replace(tzinfo=None) != naive:
        return None
    return instant


def floor_bucket(instant: datetime) -> datetime:
    """The UTC half-hour boundary at or before `instant`."""
    return instant - (instant - datetime.min.replace(tzinfo=UTC)) % BUCKET
