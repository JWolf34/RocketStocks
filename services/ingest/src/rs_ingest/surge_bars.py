"""surge_bars: daily bars for every ticker that ever had a popularity-surge event, plus SPY.

An Outcome needs about a year of bars before its case (the stock's typical
move is measured over the 250 sessions before entry) and the sessions through
its 20-session horizon, and the benchmark's over the same span. The job is a
catch-up, not a per-day append: each run works out from the stored snapshots,
bars and requests what is missing up to the last settled session and fetches
only that, so the daily slot, the startup run and a rerun are the same code and
a rerun with nothing new makes no Schwab call. A symbol's history is extended
back when it begins after the start the job needs, unless Schwab was already
asked for that start and has nothing earlier: a young listing is asked once.
"""

import logging
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import Clock, StoredDailyBar, TradingCalendar
from rs_ingest.rate_limit import RateLimiter
from rs_ingest.replay import replayed_hits
from rs_persistence import DailyBarRepository
from rs_sources.schwab import DailyHistory, SchwabAuthError, SchwabResponseError

logger = logging.getLogger(__name__)

BENCHMARK = "SPY"

EASTERN = ZoneInfo("America/New_York")

# An Outcome's typical move is measured over the 250 sessions before entry; 270 leaves slack.
LOOKBACK_SESSIONS = 270
# Calendar days that always hold LOOKBACK_SESSIONS sessions (holiday weeks included).
LOOKBACK_CALENDAR_DAYS = 400
# Progress is logged this often during a long catch-up.
PROGRESS_EVERY = 100
# A session's candle is trusted this long after the close; the daily slot is 90 minutes after.
SETTLE = timedelta(hours=1)
# A symbol Schwab cannot price is asked about again this often, never sooner.
UNPRICED_RETRY = timedelta(days=7)
# Not yet measured against a known split (needs a live Schwab session): do not claim adjustment.
PRICES_ADJUSTED = False
# Schwab answers these for a symbol it will never price; anything else is transient.
PERMANENT_STATUSES = frozenset({400, 404})


class DailyHistorySource(Protocol):
    async def daily_history(self, symbol: str, start: date, end: date) -> DailyHistory: ...


class SurgeBarsError(Exception):
    """Some symbols could not be fetched; the rest of the run was kept."""


@dataclass
class SurgeBarsReport:
    needed: int = 0
    fetched: int = 0
    bars_inserted: int = 0
    rejected: Counter[str] = field(default_factory=Counter)
    unpriced: int = 0
    deferred: int = 0
    failed: list[str] = field(default_factory=list)


async def first_event_dates(
    session_factory: async_sessionmaker[AsyncSession], now: datetime
) -> dict[str, date]:
    """Eastern date of each surge ticker's first event, over both popularity origins."""
    first: dict[str, date] = {}
    async for event in replayed_hits(session_factory, now):
        day = event.polled_at.astimezone(EASTERN).date()
        if event.ticker not in first or day < first[event.ticker]:
            first[event.ticker] = day
    return first


def lookback_start(calendar: TradingCalendar, day: date) -> date:
    """The session LOOKBACK_SESSIONS sessions before `day` (or the earliest in range)."""
    sessions = calendar.sessions_in_range(day - timedelta(days=LOOKBACK_CALENDAR_DAYS), day)
    return sessions[-(LOOKBACK_SESSIONS + 1) :][0] if sessions else day


def last_settled_session(calendar: TradingCalendar, now: datetime) -> date:
    today = now.astimezone(EASTERN).date()
    for day in reversed(calendar.sessions_in_range(today - timedelta(days=10), today)):
        if calendar.session_close(day) + SETTLE <= now:
            return day
    raise RuntimeError(f"no settled session in the 10 days before {today}")


def _needs(event_dates: Mapping[str, date], calendar: TradingCalendar, last_session: date) -> dict[str, date]:
    """Each symbol's earliest needed session: the lookback before its first event."""
    needs = {ticker: lookback_start(calendar, day) for ticker, day in event_dates.items()}
    # The benchmark must cover every event, so it starts at the earliest one.
    needs[BENCHMARK] = lookback_start(calendar, min(event_dates.values(), default=last_session))
    return needs


def _fetch_from(
    need: date, stored: tuple[date, date] | None, requested: date | None, last_session: date
) -> date | None:
    """Where this symbol's fetch must start, or None when it is complete.

    History that begins after `need` is extended back, unless Schwab was
    already asked from `need` or earlier: a listing younger than the lookback
    has nothing before its first bar, and must not be asked every night.
    """
    if stored is None:
        return need
    first, last = stored
    if first > need and (requested is None or requested > need):
        return need
    if last < last_session:
        return last + timedelta(days=1)
    return None


def _to_stored(history: DailyHistory, fetched_at: datetime) -> list[StoredDailyBar]:
    return [
        StoredDailyBar(
            symbol=bar.symbol,
            session_date=bar.session_date,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            fetched_at=fetched_at,
            adjusted=PRICES_ADJUSTED,
        )
        for bar in history.bars
    ]


def _unpriced_reason(history: DailyHistory) -> str:
    if not history.rejects:
        return "no_history"
    return "all_rejected:" + ",".join(sorted({r.reason.value for r in history.rejects}))


async def sync_surge_bars(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    source: DailyHistorySource,
    limiter: RateLimiter,
    calendar: TradingCalendar,
    clock: Clock,
) -> SurgeBarsReport:
    now = clock.now()
    last_session = last_settled_session(calendar, now)
    needs = _needs(await first_event_dates(session_factory, now), calendar, last_session)
    report = SurgeBarsReport(needed=len(needs))

    async with session_factory() as session:
        repo = DailyBarRepository(session)
        stored = await repo.session_bounds(needs)
        requested = await repo.earliest_requested(needs)
        unpriced = {u.symbol: u for u in await repo.list_unpriced()}

    todo: list[tuple[str, date]] = []
    # The benchmark first: without it no event has an excess return.
    for symbol in sorted(needs, key=lambda s: (s != BENCHMARK, s)):
        start = _fetch_from(needs[symbol], stored.get(symbol), requested.get(symbol), last_session)
        if start is None:
            continue
        previous = unpriced.get(symbol)
        if symbol not in stored and previous is not None and now - previous.last_attempt < UNPRICED_RETRY:
            report.deferred += 1
            continue
        todo.append((symbol, start))

    if len(todo) > PROGRESS_EVERY:
        logger.info("surge_bars: %d of %d symbols to fetch, one call each", len(todo), len(needs))
    try:  # the partial report is worth logging when a 429 or dead token stops the run
        for done, (symbol, start) in enumerate(todo):
            if done and done % PROGRESS_EVERY == 0:
                logger.info("surge_bars: %d of %d symbols done", done, len(todo))
            await limiter.acquire()
            try:
                history = await source.daily_history(symbol, start, last_session)
            except SchwabAuthError:
                raise  # the daemon pauses Schwab jobs on this
            except SchwabResponseError as exc:
                if exc.status_code == 429:
                    raise  # over budget: stop, the next run resumes
                if exc.status_code in PERMANENT_STATUSES:
                    # A 400 can also be our own bad request, so the reason stays visible in the log.
                    logger.warning("%s: Schwab refuses the symbol: %s", symbol, exc)
                    if symbol not in stored:
                        await _record_unpriced(session_factory, symbol, f"http_{exc.status_code}", clock.now())
                        report.unpriced += 1
                    continue
                logger.warning("%s: daily history failed: %s", symbol, exc)
                report.failed.append(symbol)
                continue
            except Exception:
                logger.exception("%s: daily history failed", symbol)
                report.failed.append(symbol)
                continue

            report.fetched += 1
            report.rejected.update(r.reason.value for r in history.rejects)
            async with session_factory() as session:
                repo = DailyBarRepository(session)
                if history.bars:
                    report.bars_inserted += await repo.insert_many(_to_stored(history, clock.now()))
                    await repo.clear_unpriced(symbol)
                await repo.record_request(symbol, start)
                await session.commit()
            if not history.bars and symbol not in stored:
                await _record_unpriced(session_factory, symbol, _unpriced_reason(history), clock.now())
                report.unpriced += 1
    finally:
        _log(report, last_session)
    if report.failed:
        raise SurgeBarsError(f"{len(report.failed)} of {len(todo)} symbols failed: {_head(report.failed)}")
    return report


async def _record_unpriced(
    session_factory: async_sessionmaker[AsyncSession], symbol: str, reason: str, now: datetime
) -> None:
    async with session_factory() as session:
        await DailyBarRepository(session).record_unpriced(symbol, reason, now)
        await session.commit()


def _head(symbols: Iterable[str], limit: int = 10) -> str:
    shown = list(symbols)
    return ", ".join(shown[:limit]) + (f" (+{len(shown) - limit} more)" if len(shown) > limit else "")


def _log(report: SurgeBarsReport, last_session: date) -> None:
    logger.info(
        "surge_bars through %s: %d symbols tracked, %d fetched, %d bars inserted, "
        "%d unpriced, %d unpriced deferred, %d failed",
        last_session,
        report.needed,
        report.fetched,
        report.bars_inserted,
        report.unpriced,
        report.deferred,
        len(report.failed),
    )
    if report.rejected:
        logger.warning("surge_bars rejected bars (never stored): %s", dict(report.rejected))
