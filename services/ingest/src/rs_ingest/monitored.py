"""Source adapters wrapped to report their health.

Each wrapper sits at the narrowest place that knows whether a call failed
because the source is unwell or because of one symbol: it passes the call
through unchanged, reports the outcome to the `HealthTracker`, and re-raises
whatever the call raised. A vendor's answer about one symbol (a 400 or 404, an
unknown ticker) reports ok: the source did answer.

A dead or missing Schwab token is deliberately not reported: it already has its
own Ops ping through the token gate.
"""

from collections.abc import Awaitable
from datetime import date, datetime

from rs_ingest.daily_bars import DailyHistorySource
from rs_ingest.popularity_poll import PopularitySource
from rs_ingest.surge_facts import QuoteSource
from rs_observability import HealthTracker, Source, SourceFailed, SweepTally
from rs_sources import ApeWisdomError, ApeWisdomRow
from rs_sources.earnings import FilingSource
from rs_sources.edgar import Filing8K
from rs_sources.errors import is_per_symbol_answer
from rs_sources.schwab import (
    TOKEN_ERRORS,
    DailyHistory,
    Quote,
    SchwabFailure,
    schwab_failure,
)


async def _observed[T](
    health: HealthTracker, source: Source, call: Awaitable[T], *, per_symbol: bool = True
) -> T:
    """`call`'s result, after reporting it; a failure is reported by exception type and re-raised.
    `per_symbol` is False for a source with no per-symbol requests, where every error is its own."""
    try:
        result = await call
    except TOKEN_ERRORS:
        raise  # the token gate pings for a dead or missing token, not the Schwab health
    except Exception as exc:
        if per_symbol and is_per_symbol_answer(exc):
            await health.report_ok(source)
        else:
            await health.report_failed(source, type(exc).__name__)
        raise
    await health.report_ok(source)
    return result


class MonitoredPopularitySource:
    """The ApeWisdom listing. A failed or empty fetch is the source failing; it raises
    `SourceFailed`, so the poll fails without a second, job-level Ops ping."""

    def __init__(self, source: PopularitySource, health: HealthTracker) -> None:
        self._source = source
        self._health = health

    async def fetch_all(self) -> list[ApeWisdomRow]:
        try:
            rows = await self._source.fetch_all()
            if not rows:
                raise ApeWisdomError("ApeWisdom returned no rows")
        except Exception as exc:
            await self._health.report_failed(Source.APEWISDOM, type(exc).__name__)
            raise SourceFailed(Source.APEWISDOM) from exc
        await self._health.report_ok(Source.APEWISDOM)
        return rows


class MonitoredQuotes:
    """Schwab quotes: a call that fails for a source-level reason is Schwab failing."""

    def __init__(self, quotes: QuoteSource, health: HealthTracker) -> None:
        self._quotes = quotes
        self._health = health

    async def quote(self, symbol: str) -> Quote | None:
        return await _observed(self._health, Source.SCHWAB, self._quotes.quote(symbol))


class TalliedHistory:
    """Schwab daily history for a sweep: counts every request in `tally` for the sweep's
    owner to judge once it ends (more than about 5% failing is Schwab failing, one stock is not).
    A 429 speaks for the whole source, however few requests were made; a dead or missing token
    is not counted."""

    def __init__(self, history: DailyHistorySource, tally: SweepTally) -> None:
        self._history = history
        self._tally = tally

    async def daily_history(self, symbol: str, start: date, end: date) -> DailyHistory:
        try:
            history = await self._history.daily_history(symbol, start, end)
        except Exception as exc:
            match schwab_failure(exc):
                case SchwabFailure.TOKEN:
                    pass  # the token gate's to report, not Schwab's health
                case SchwabFailure.PER_SYMBOL:
                    self._tally.record_ok()
                case failure:
                    self._tally.record_failure(source_wide=failure is SchwabFailure.RATE_LIMITED)
            raise
        self._tally.record_ok()
        return history


class MonitoredFilings:
    """EDGAR filings as the earnings lookup reads them."""

    def __init__(self, filings: FilingSource, health: HealthTracker) -> None:
        self._filings = filings
        self._health = health

    async def recent_8ks(self, ticker: str, as_of: datetime, limit: int | None = 5) -> list[Filing8K]:
        return await _observed(self._health, Source.EDGAR, self._filings.recent_8ks(ticker, as_of, limit))


async def finish_sweep(
    health: HealthTracker, source: Source, *tallies: SweepTally, error: Exception | None = None
) -> None:
    """Judge finished sweeps, each against its own requests; raises `SourceFailed` (from `error`)
    when one says `source` is failing."""
    if await health.report_sweep(source, *tallies):
        raise SourceFailed(source) from error
