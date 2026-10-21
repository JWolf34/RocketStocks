"""The facts block of an attention-surge alert, gathered at write time.

Each fact is independent and optional: a source that fails, times out or has
nothing to say becomes `FactUnavailable` with a reason, so a slow or broken
vendor can delay an alert by at most `fact_timeout` and never blocks it.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import (
    BenchmarkFact,
    FactUnavailable,
    NextEarningsFact,
    PriceMoveFact,
    SurgeFacts,
    TradingCalendar,
    VolumeFact,
)
from rs_ingest.rate_limit import RateLimiter
from rs_persistence import DailyBarRepository
from rs_sources.earnings import NextEarnings
from rs_sources.schwab import Quote, SchwabAuthError, TokenMissingError

logger = logging.getLogger(__name__)

_EASTERN = ZoneInfo("America/New_York")

AVERAGE_SESSIONS = 20
# Wide enough to hold 20 sessions across holiday weeks.
_BAR_LOOKBACK = timedelta(days=45)
# The newest stored bar must be this recent, or the "20-day" average is old data.
_MAX_BAR_AGE = timedelta(days=7)
BENCHMARK_SYMBOL = "SPY"
FACT_TIMEOUT = timedelta(seconds=20)


class QuoteSource(Protocol):
    async def quote(self, symbol: str) -> Quote | None: ...


class EarningsSource(Protocol):
    async def next_earnings(self, ticker: str, as_of: datetime) -> NextEarnings: ...


@dataclass(frozen=True)
class GatheredFacts:
    facts: SurgeFacts
    # The quote behind the price facts; both None when no quote was obtained.
    alert_price: float | None
    quote_time: datetime | None


class SurgeFactsGatherer:
    """One poll run's facts source. `quotes` is called per lookup so a Schwab
    client rebuilt after a re-login is picked up; once a quote fails on a dead
    token, later lookups in this run skip Schwab instead of retrying it.
    Each quote waits on the shared Schwab `limiter`. `on_token_dead` is told once
    so the daemon can pause its Schwab jobs."""

    def __init__(
        self,
        *,
        quotes: Callable[[], QuoteSource],
        limiter: RateLimiter,
        earnings: EarningsSource,
        session_factory: async_sessionmaker[AsyncSession],
        calendar: TradingCalendar,
        on_token_dead: Callable[[str], Awaitable[None]],
        fact_timeout: timedelta = FACT_TIMEOUT,
    ) -> None:
        self._quotes = quotes
        self._limiter = limiter
        self._earnings = earnings
        self._session_factory = session_factory
        self._calendar = calendar
        self._on_token_dead = on_token_dead
        self._fact_timeout = fact_timeout.total_seconds()
        self._token_dead_reason: str | None = None

    async def gather(self, symbol: str, as_of: datetime) -> GatheredFacts:
        quote_result, benchmark, earnings = await asyncio.gather(
            self._guarded(self._quote(symbol)),
            self._guarded(self._benchmark()),
            self._guarded(self._next_earnings(symbol, as_of)),
        )
        quote: Quote | FactUnavailable = quote_result
        if isinstance(quote, Quote):
            price: PriceMoveFact | FactUnavailable = _price_move(quote)
            volume = await self._guarded(self._volume(quote))
        else:
            price = volume = quote
        return GatheredFacts(
            facts=SurgeFacts(
                price_move=price,
                volume=volume,
                next_earnings=earnings,
                benchmark=benchmark,
            ),
            alert_price=quote.last_price if isinstance(quote, Quote) else None,
            quote_time=quote.quote_time if isinstance(quote, Quote) else None,
        )

    async def _guarded[T](self, fact: Awaitable[T]) -> T | FactUnavailable:
        """The fact, or why there is none. Cancellation is never swallowed."""
        try:
            async with asyncio.timeout(self._fact_timeout):
                return await fact
        except TimeoutError:
            return FactUnavailable(reason=f"timed out after {self._fact_timeout:g}s")
        except Exception as exc:
            logger.warning("surge fact failed: %s", type(exc).__name__, exc_info=True)
            # The type only: exception text can carry URLs or credentials.
            return FactUnavailable(reason=f"source failed ({type(exc).__name__})")

    async def _quote(self, symbol: str) -> Quote | FactUnavailable:
        if self._token_dead_reason is not None:
            return FactUnavailable(reason=self._token_dead_reason)
        try:
            await self._limiter.acquire()
            quote = await self._quotes().quote(symbol)
        except TokenMissingError:
            return self._dead_token("no Schwab token stored")
        except SchwabAuthError as exc:
            reason = self._dead_token("Schwab token is dead")
            await self._on_token_dead(f"quote lookup: {exc}")
            return reason
        if quote is None:
            return FactUnavailable(reason="Schwab has no quote for this symbol")
        return quote

    async def _benchmark(self) -> BenchmarkFact | FactUnavailable:
        quote = await self._quote(BENCHMARK_SYMBOL)
        if isinstance(quote, FactUnavailable):
            return quote
        return BenchmarkFact(symbol=quote.symbol, price=quote.last_price, quote_time=quote.quote_time)

    def _dead_token(self, reason: str) -> FactUnavailable:
        self._token_dead_reason = reason
        return FactUnavailable(reason=reason)

    async def _volume(self, quote: Quote) -> VolumeFact | FactUnavailable:
        quote_day = quote.quote_time.astimezone(_EASTERN).date()
        async with self._session_factory() as session:
            bars = await DailyBarRepository(session).list_range(
                quote.symbol, quote_day - _BAR_LOOKBACK, quote_day - timedelta(days=1)
            )
        recent = bars[-AVERAGE_SESSIONS:]
        if len(recent) < AVERAGE_SESSIONS:
            return FactUnavailable(
                reason=f"only {len(recent)} of {AVERAGE_SESSIONS} daily bars stored for the average"
            )
        newest = recent[-1].session_date
        if newest < quote_day - _MAX_BAR_AGE:
            return FactUnavailable(reason=f"daily bars are stale (newest is {newest.isoformat()})")
        average = sum(bar.volume for bar in recent) / AVERAGE_SESSIONS
        if average <= 0:
            return FactUnavailable(reason=f"{AVERAGE_SESSIONS}-day average volume is zero")
        return VolumeFact(
            volume=quote.volume,
            average_volume=average,
            sessions=AVERAGE_SESSIONS,
            ratio=quote.volume / average,
            intraday=self._is_intraday(quote_day, quote.quote_time),
        )

    def _is_intraday(self, day: date, moment: datetime) -> bool:
        if not self._calendar.is_session(day):
            return False
        return self._calendar.session_open(day) <= moment < self._calendar.session_close(day)

    async def _next_earnings(self, symbol: str, as_of: datetime) -> NextEarningsFact | FactUnavailable:
        upcoming = await self._earnings.next_earnings(symbol, as_of)
        if upcoming.date is None:
            return FactUnavailable(reason="no upcoming earnings date found")
        return NextEarningsFact(
            date=upcoming.date,
            estimated=upcoming.estimated,
            time_of_day=None if upcoming.time_of_day is None else upcoming.time_of_day.value,
        )


def _price_move(quote: Quote) -> PriceMoveFact:
    return PriceMoveFact(
        last_price=quote.last_price,
        previous_close=quote.previous_close,
        change_pct=(quote.last_price / quote.previous_close - 1) * 100,
        quote_time=quote.quote_time,
    )
