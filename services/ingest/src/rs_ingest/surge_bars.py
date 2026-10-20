"""surge_bars: daily bars for every ticker that ever had an attention-surge event, plus SPY.

The event study needs each event's neighbourhood of daily bars and the
benchmark's. The job is a catch-up, not a per-day append: each run works out
from the stored snapshots and bars what is missing up to the last settled
session and fetches only that, so the daily slot, the startup run and a rerun
are the same code and a rerun with nothing new makes no Schwab call.
"""

import logging
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_analysis import attention_surge
from rs_domain import Clock, StoredDailyBar, TradingCalendar
from rs_ingest.rate_limit import RateLimiter
from rs_persistence import DailyBarRepository, PopularityRepository
from rs_sources.schwab import DailyHistory, SchwabAuthError, SchwabResponseError

logger = logging.getLogger(__name__)

BENCHMARK = "SPY"

EASTERN = ZoneInfo("America/New_York")

# The event study's 20-day volume average needs 20 prior sessions; 30 leaves slack.
LOOKBACK_SESSIONS = 30
# Calendar days that always hold LOOKBACK_SESSIONS sessions (holiday weeks included).
LOOKBACK_CALENDAR_DAYS = 70
# A session's candle is trusted this long after the close; the daily slot is 90 minutes after.
SETTLE = timedelta(hours=1)
# A symbol Schwab cannot price is asked about again this often, never sooner.
UNPRICED_RETRY = timedelta(days=7)
# History is replayed in windows so memory stays bounded however long it runs.
REPLAY_WINDOW = timedelta(days=7)
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


@dataclass(frozen=True)
class _Need:
    # The first session on or after the first event: the earliest bar the event study cannot do without.
    anchor: date
    # Where a fetch begins: the lookback before the event.
    start: date


async def first_event_dates(
    session_factory: async_sessionmaker[AsyncSession], now: datetime
) -> dict[str, date]:
    """Eastern date of each surge ticker's first event, over both popularity origins.

    Judges each snapshot with no prior events: the cooldown only suppresses
    repeats, so a ticker's first event, the only one that matters here, is the
    same as in `attention_surge.replay` without holding the whole history.
    """
    first: dict[str, date] = {}
    async with session_factory() as session:
        repo = PopularityRepository(session)
        window_start = await repo.earliest_polled_at()
        if window_start is None:
            return first
        while window_start <= now:
            window_end = window_start + REPLAY_WINDOW
            for snapshot in await repo.list_between(window_start, window_end):
                if attention_surge.evaluate_latest(snapshot, ()) is None:
                    continue
                day = snapshot.polled_at.astimezone(EASTERN).date()
                if snapshot.ticker not in first or day < first[snapshot.ticker]:
                    first[snapshot.ticker] = day
            window_start = window_end
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


def _needs(event_dates: Mapping[str, date], calendar: TradingCalendar, last_session: date) -> dict[str, _Need]:
    def need(day: date) -> _Need:
        # A weekend or holiday event is anchored on the next session, so a listing that
        # starts there is complete; the 10 days always reach one.
        following = calendar.sessions_in_range(day, day + timedelta(days=10))
        return _Need(anchor=following[0] if following else day, start=lookback_start(calendar, day))

    needs = {ticker: need(day) for ticker, day in event_dates.items()}
    # The benchmark must cover every event, so it starts at the earliest one.
    needs[BENCHMARK] = need(min(event_dates.values(), default=last_session))
    return needs


def _fetch_from(need: _Need, stored: tuple[date, date] | None, last_session: date) -> date | None:
    """Where this symbol's fetch must start, or None when it is complete.

    Bars before the anchor are padding: a listing younger than the lookback
    has none, so only bars missing at or after the anchor count as a gap.
    """
    if stored is None:
        return need.start
    first, last = stored
    if need.anchor < first:
        return need.start
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
        unpriced = {u.symbol: u for u in await repo.list_unpriced()}

    todo: list[tuple[str, date]] = []
    # The benchmark first: without it no event has an excess return.
    for symbol in sorted(needs, key=lambda s: (s != BENCHMARK, s)):
        start = _fetch_from(needs[symbol], stored.get(symbol), last_session)
        if start is None:
            continue
        previous = unpriced.get(symbol)
        if symbol not in stored and previous is not None and now - previous.last_attempt < UNPRICED_RETRY:
            report.deferred += 1
            continue
        todo.append((symbol, start))

    try:  # the partial report is worth logging when a 429 or dead token stops the run
        for symbol, start in todo:
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
            if history.bars:
                async with session_factory() as session:
                    repo = DailyBarRepository(session)
                    report.bars_inserted += await repo.insert_many(_to_stored(history, clock.now()))
                    await repo.clear_unpriced(symbol)
                    await session.commit()
            elif symbol not in stored:
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
