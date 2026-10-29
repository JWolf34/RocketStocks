"""Nasdaq earnings-calendar adapter (unofficial JSON endpoint).

The endpoint is undocumented and bot-filtered, so it needs a browser-like
User-Agent and can change shape without notice; a day whose payload is the wrong
shape as a whole surfaces as SourceError. It serves one date per call. A past day
carries `eps` and `surprise`, a forward day `lastYearEPS` and `lastYearRptDt`
instead; the last two are not kept. Values arrive as display text (`$3.33`,
`($0.12)` for a negative, `N/A`, empty) and parse to numbers or None; a value that
is missing or unreadable is None and counted per field, and a row with no usable
symbol is skipped and counted under `symbol`, so one bad row never costs the day.
"""

import math
import re
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

from rs_domain import Clock, EarningsTime
from rs_sources._http import get_json
from rs_sources._ratelimit import MinInterval
from rs_sources.errors import SourceError

EARNINGS_URL = "https://api.nasdaq.com/api/calendar/earnings"
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
)
# A day Nasdaq has nothing for (a holiday, or past the end of its forward window) answers
# `"data": null` with this code in the status; a weekend answers `"rows": null` instead.
NO_RECORD_CODE = 1002

_TIME_OF_DAY = {
    "time-pre-market": EarningsTime.PRE_MARKET,
    "time-after-hours": EarningsTime.AFTER_HOURS,
    "time-not-supplied": None,
}
_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
_NUMBER = re.compile(r"-?(\d+\.?\d*|\.\d+)")
_QUARTER = re.compile(r"([A-Z][a-z]{2})/(\d{4})")


class EarningsListing(BaseModel):
    """A company's earnings report as Nasdaq's calendar lists it for one date."""

    model_config = ConfigDict(frozen=True)

    ticker: str
    report_date: date
    time_of_day: EarningsTime | None
    fiscal_quarter: str | None
    eps_forecast: float | None
    estimate_count: int | None
    eps_actual: float | None
    surprise_pct: float | None


@dataclass(frozen=True)
class EarningsDay:
    """One date's answer. `unparsed` counts, per listing field, the values that came back
    `N/A`, empty, unreadable or missing, and under `symbol` the rows skipped for lacking a
    usable ticker; a field a forward day does not carry (actuals) is not counted."""

    day: date
    fetched_at: datetime
    listings: tuple[EarningsListing, ...]
    unparsed: Mapping[str, int]


class NasdaqEarningsCalendar:
    def __init__(self, http: httpx.AsyncClient, clock: Clock, *, min_interval: float = 0.25) -> None:
        self._http = http
        self._clock = clock
        self._limiter = MinInterval(min_interval)

    async def day(self, day: date) -> EarningsDay:
        """Everything Nasdaq lists for `day`; no listings means Nasdaq has none for it.

        A failed request or an unexpected shape raises SourceError, because a gap
        would make "nothing listed" unprovable.
        """
        await self._limiter.wait()
        payload = await get_json(
            self._http,
            EARNINGS_URL,
            headers={"User-Agent": BROWSER_USER_AGENT, "Accept": "application/json"},
            params={"date": day.isoformat()},
        )
        fetched_at = self._clock.now()
        listings, unparsed = _parse_day(payload, day)
        return EarningsDay(day=day, fetched_at=fetched_at, listings=listings, unparsed=unparsed)


def _parse_day(payload: Any, day: date) -> tuple[tuple[EarningsListing, ...], Counter[str]]:
    try:
        data = payload["data"]
        if data is None:
            if _is_no_record(payload):
                return (), Counter()
            raise SourceError(f"Nasdaq earnings calendar for {day} has no data")
        # Weekends and some holidays come back as `"rows": null`.
        rows = data["rows"] or []
        if not isinstance(rows, list):
            raise SourceError(f"Nasdaq earnings calendar for {day} has rows that are not a list")
    except (KeyError, TypeError, AttributeError) as exc:
        raise SourceError(f"Nasdaq earnings calendar for {day} has an unexpected shape") from exc
    unparsed: Counter[str] = Counter()
    listings: dict[str, EarningsListing] = {}
    for row in rows:
        listing = _parse_row(row, day, unparsed)
        if listing is not None:
            listings.setdefault(listing.ticker, listing)
    return tuple(listings.values()), unparsed


def _is_no_record(payload: Any) -> bool:
    status = payload.get("status") or {}
    messages = status.get("bCodeMessage") or []
    return status.get("rCode") == 200 and any(message.get("code") == NO_RECORD_CODE for message in messages)


def _parse_row(row: Any, day: date, unparsed: Counter[str]) -> EarningsListing | None:
    """The row's listing, or None (counted under `symbol`) when it has no usable ticker."""
    symbol = row.get("symbol") if isinstance(row, dict) else None
    if not isinstance(symbol, str) or not symbol.strip():
        unparsed["symbol"] += 1
        return None
    # A forward day carries last year's figures where a past day carries the actuals.
    forward = "lastYearEPS" in row or "lastYearRptDt" in row
    wire_time = row.get("time")
    if not isinstance(wire_time, str) or wire_time not in _TIME_OF_DAY:
        unparsed["time_of_day"] += 1
    return EarningsListing(
        ticker=symbol.strip().upper(),
        report_date=day,
        time_of_day=_TIME_OF_DAY.get(wire_time) if isinstance(wire_time, str) else None,
        fiscal_quarter=_value(row, "fiscalQuarterEnding", "fiscal_quarter", _parse_quarter, unparsed),
        eps_forecast=_value(row, "epsForecast", "eps_forecast", _parse_amount, unparsed),
        estimate_count=_value(row, "noOfEsts", "estimate_count", _parse_count, unparsed),
        eps_actual=_value(row, "eps", "eps_actual", _parse_amount, unparsed, optional=forward),
        surprise_pct=_value(row, "surprise", "surprise_pct", _parse_amount, unparsed, optional=forward),
    )


def _value[T](
    row: dict[str, Any],
    key: str,
    field: str,
    parse: Callable[[object], T | None],
    unparsed: Counter[str],
    *,
    optional: bool = False,
) -> T | None:
    """The parsed value of `key`, or None, counted under `field`. A key the row does not carry
    is counted too, unless `optional` (the actuals on a forward day)."""
    if key not in row and optional:
        return None
    value = parse(row.get(key))
    if value is None:
        unparsed[field] += 1
    return value


def _parse_amount(raw: object) -> float | None:
    """`$3.33` -> 3.33, `($0.12)` -> -0.12, `-123.6` -> -123.6; `N/A`, empty and anything else -> None."""
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1]
    text = text.replace("$", "").replace(",", "").replace("%", "").strip()
    if not _NUMBER.fullmatch(text):
        return None
    value = float(text)
    if not math.isfinite(value):
        return None
    return -value if negative else value


def _parse_count(raw: object) -> int | None:
    value = _parse_amount(raw)
    if value is None or value < 0 or value != int(value):
        return None
    return int(value)


def _parse_quarter(raw: object) -> str | None:
    """`Sep/2026` -> `2026-09`: the month the fiscal quarter ends."""
    match = _QUARTER.fullmatch(raw.strip()) if isinstance(raw, str) else None
    if match is None or match.group(1) not in _MONTHS:
        return None
    return f"{match.group(2)}-{_MONTHS.index(match.group(1)) + 1:02d}"
