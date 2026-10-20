"""Nasdaq earnings-calendar adapter (unofficial JSON endpoint).

The endpoint is undocumented and bot-filtered, so it needs a browser-like
User-Agent and can change shape without notice; every deviation surfaces as
SourceError so callers can fall back. It serves one date per call, so day
results are cached and a burst of ticker lookups shares the same scan.
"""

import logging
from collections.abc import Iterator
from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

from rs_domain import Clock
from rs_sources._http import get_json
from rs_sources._ratelimit import MinInterval
from rs_sources.errors import SourceError

logger = logging.getLogger(__name__)

EARNINGS_URL = "https://api.nasdaq.com/api/calendar/earnings"
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
)


class EarningsTime(StrEnum):
    PRE_MARKET = "pre_market"
    AFTER_HOURS = "after_hours"


_TIME_OF_DAY = {
    "time-pre-market": EarningsTime.PRE_MARKET,
    "time-after-hours": EarningsTime.AFTER_HOURS,
}


class ScheduledEarnings(BaseModel):
    """A company's earnings report as Nasdaq's calendar lists it."""

    model_config = ConfigDict(frozen=True)

    ticker: str
    date: date
    # None when Nasdaq does not say ("time-not-supplied").
    time_of_day: EarningsTime | None


class NasdaqEarningsCalendar:
    def __init__(
        self,
        http: httpx.AsyncClient,
        clock: Clock,
        *,
        cache_ttl: timedelta = timedelta(hours=6),
        min_interval: float = 0.5,
    ) -> None:
        self._http = http
        self._clock = clock
        self._cache_ttl = cache_ttl
        self._limiter = MinInterval(min_interval)
        self._days: dict[date, tuple[datetime, dict[str, ScheduledEarnings]]] = {}

    async def next_for(self, ticker: str, start: date, max_weekdays: int) -> ScheduledEarnings | None:
        """The ticker's first listing on or after `start`, scanning `max_weekdays` weekdays.

        None means the whole window was read and the ticker is not in it.
        Weekends are skipped; holidays come back as empty days. A failed day
        raises SourceError, because a gap would make "not listed" unprovable.
        """
        symbol = ticker.strip().upper()
        for day in _weekdays(start, max_weekdays):
            listing = (await self._day(day)).get(symbol)
            if listing is not None:
                return listing
        return None

    async def _day(self, day: date) -> dict[str, ScheduledEarnings]:
        now = self._clock.now()
        cached = self._days.get(day)
        if cached is not None and now - cached[0] < self._cache_ttl:
            return cached[1]
        await self._limiter.wait()
        payload = await get_json(
            self._http,
            EARNINGS_URL,
            headers={"User-Agent": BROWSER_USER_AGENT, "Accept": "application/json"},
            params={"date": day.isoformat()},
        )
        listings = _parse_day(payload, day)
        self._days = {d: entry for d, entry in self._days.items() if now - entry[0] < self._cache_ttl}
        self._days[day] = (now, listings)
        return listings


def _weekdays(start: date, count: int) -> Iterator[date]:
    day = start
    while count > 0:
        if day.weekday() < 5:
            count -= 1
            yield day
        day += timedelta(days=1)


def _parse_day(payload: Any, day: date) -> dict[str, ScheduledEarnings]:
    try:
        data = payload["data"]
        # Weekends and holidays come back as `"rows": null`.
        rows = data["rows"] or []
        listings: dict[str, ScheduledEarnings] = {}
        for row in rows:
            ticker = row["symbol"].strip().upper()
            listings.setdefault(
                ticker,
                ScheduledEarnings(ticker=ticker, date=day, time_of_day=_TIME_OF_DAY.get(row.get("time"))),
            )
    except (KeyError, TypeError, AttributeError) as exc:
        raise SourceError(f"Nasdaq earnings calendar for {day} has an unexpected shape") from exc
    return listings
