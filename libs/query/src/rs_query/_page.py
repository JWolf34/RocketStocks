"""Shared pagination: the Page result, opaque keyset cursors, and the row cap.

A cursor wraps the last row's sort key. It is opaque to callers (pass it back
unchanged) and tagged with the query that issued it, so a cursor from one
function is rejected by another instead of silently skipping rows.
"""

import base64
import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class InvalidCursor(ValueError):
    """The cursor is malformed or was issued by a different query."""


class Page[T](BaseModel):
    """One page of results. `next_cursor` is None on the last page."""

    model_config = ConfigDict(frozen=True)

    items: tuple[T, ...]
    next_cursor: str | None = None


def clamp_limit(limit: int, maximum: int) -> int:
    """Reject a non-positive limit; cap an oversized one (the cursor lets the
    caller continue, so a cap never loses rows)."""
    if limit < 1:
        raise ValueError(f"limit must be at least 1, got {limit}")
    return min(limit, maximum)


def require_utc(value: datetime, name: str) -> datetime:
    """A naive bound would be read in the session zone; refuse it at the edge."""
    if value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def encode_cursor(kind: str, key: Any) -> str:
    raw = json.dumps([kind, key], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor[K](kind: str, cursor: str, parse: Callable[[Any], K]) -> K:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        tag, key = json.loads(base64.urlsafe_b64decode(padded))
        if tag != kind:
            raise ValueError(f"cursor was issued by {tag!r}")
        return parse(key)
    except (ValueError, TypeError) as exc:
        raise InvalidCursor(f"invalid cursor for {kind}: {exc}") from exc


def page_of[R, T](
    rows: list[R], limit: int, to_item: Callable[[R], T], cursor_key: Callable[[R], Any], kind: str
) -> Page[T]:
    """Build a Page from a `limit + 1` fetch: the extra row only signals that
    more exist, and the cursor is the last row actually returned."""
    kept = rows[:limit]
    has_more = len(rows) > limit
    return Page(
        items=tuple(to_item(row) for row in kept),
        next_cursor=encode_cursor(kind, cursor_key(kept[-1])) if has_more else None,
    )
