"""Next-earnings lookup: Nasdaq calendar first, EDGAR-cadence estimate second.

Nasdaq's unofficial calendar is the primary source. When it fails or does not
list the ticker, the estimate is the last 8-K with Item 2.02 (results of
operations) plus one quarter, labelled `estimated`. With neither, the answer
is an explicit unknown (date None), never a guess.
"""

import asyncio
import logging
from calendar import monthrange
from datetime import date, datetime, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict

from rs_sources._http import require_aware
from rs_sources.edgar import EIGHT_K_FORM, RESULTS_OF_OPERATIONS_ITEM, Filing8K
from rs_sources.errors import SourceError
from rs_sources.nasdaq import EarningsTime, ScheduledEarnings

logger = logging.getLogger(__name__)

# Nasdaq lists dates on the US market calendar, so "today" is the Eastern date.
_MARKET_ZONE = ZoneInfo("America/New_York")


SCAN_WEEKDAYS = 30
# A cold Nasdaq scan stops here so the EDGAR estimate still fits the caller's budget.
NASDAQ_BUDGET = timedelta(seconds=8)


class NextEarnings(BaseModel):
    """`date` is None when no source could say; `estimated` marks the fallback."""

    model_config = ConfigDict(frozen=True)

    date: date | None
    time_of_day: EarningsTime | None = None
    estimated: bool = False

    @classmethod
    def unknown(cls) -> "NextEarnings":
        return cls(date=None)


class EarningsCalendar(Protocol):
    async def next_for(self, ticker: str, start: date, max_weekdays: int) -> ScheduledEarnings | None: ...


class FilingSource(Protocol):
    async def recent_8ks(self, ticker: str, as_of: datetime, limit: int | None = 5) -> list[Filing8K]: ...


class EarningsLookup:
    def __init__(
        self,
        nasdaq: EarningsCalendar,
        filings: FilingSource,
        *,
        scan_weekdays: int = SCAN_WEEKDAYS,
        nasdaq_budget: timedelta = NASDAQ_BUDGET,
    ) -> None:
        self._nasdaq = nasdaq
        self._filings = filings
        self._scan_weekdays = scan_weekdays
        self._nasdaq_budget = nasdaq_budget.total_seconds()

    async def next_earnings(self, ticker: str, as_of: datetime) -> NextEarnings:
        """The ticker's next earnings date as of `as_of` (tz-aware); never raises on source failure."""
        require_aware(as_of)
        today = as_of.astimezone(_MARKET_ZONE).date()
        try:
            async with asyncio.timeout(self._nasdaq_budget):
                scheduled = await self._nasdaq.next_for(ticker, today, self._scan_weekdays)
        except SourceError:
            logger.warning("Nasdaq earnings lookup failed for %s; estimating from EDGAR", ticker, exc_info=True)
        except TimeoutError:
            logger.warning("Nasdaq earnings scan for %s ran past its budget; estimating from EDGAR", ticker)
        else:
            if scheduled is not None:
                return NextEarnings(date=scheduled.date, time_of_day=scheduled.time_of_day)
        return await self._estimate(ticker, as_of, today)

    async def _estimate(self, ticker: str, as_of: datetime, today: date) -> NextEarnings:
        try:
            filings = await self._filings.recent_8ks(ticker, as_of, limit=None)
        except SourceError:
            logger.warning("EDGAR lookup failed for %s; next earnings unknown", ticker, exc_info=True)
            return NextEarnings.unknown()
        # Original 8-Ks only: an 8-K/A is filed later and would skew the cadence.
        last_results = next(
            (f for f in filings if f.form == EIGHT_K_FORM and RESULTS_OF_OPERATIONS_ITEM in f.items), None
        )
        if last_results is None:
            return NextEarnings.unknown()
        estimate = _add_quarter(last_results.filing_date)
        # An estimate already in the past means the company is overdue or stopped
        # reporting; a past date would read as a fact, so say unknown instead.
        if estimate < today:
            return NextEarnings.unknown()
        return NextEarnings(date=estimate, estimated=True)


def _add_quarter(day: date) -> date:
    """Three calendar months later, clamped to the month's last day (Nov 30 -> Feb 28)."""
    month_index = day.month - 1 + 3
    year, month = day.year + month_index // 12, month_index % 12 + 1
    return date(year, month, min(day.day, monthrange(year, month)[1]))
