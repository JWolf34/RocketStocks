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

Schwab's history is split-adjusted as of the moment it is fetched, so a split
after bars were stored leaves them at the old scale (a reverse split, common
among Popularity names, is the worst case). A tail fetch therefore starts a few
sessions before the newest stored bar, and the overlap's closes are compared
with the stored ones: when they disagree the symbol's stored bars are replaced
by a fresh full fetch. The overlap rides on the call the tail needs anyway.
"""

import logging
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import Clock, StoredDailyBar, TradingCalendar, eastern_day
from rs_ingest.rate_limit import RateLimiter
from rs_ingest.replay import replayed_hits
from rs_persistence import DailyBarRepository
from rs_sources.schwab import DailyHistory, SchwabAuthError, SchwabResponseError

logger = logging.getLogger(__name__)

BENCHMARK = "SPY"

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
# Schwab's daily history is split-adjusted as of the fetch (measured on four known splits).
PRICES_ADJUSTED = True
# A tail fetch re-reads this many of the newest stored sessions. A split rescales every earlier
# close, so one session would show it; three keep a single vendor correction or a halted session
# from deciding it, and cost almost nothing on a call the tail makes anyway.
OVERLAP_SESSIONS = 3
# Calendar days that always hold OVERLAP_SESSIONS sessions (holiday weeks included).
OVERLAP_CALENDAR_DAYS = 10
# Stored and refetched closes of one session may differ this much (a fraction of the fresh close).
# Vendor corrections to a settled close are cents; the smallest split anyone would call one
# (5-for-4) moves it by 20 percent, so 1 percent sits well clear of both. A false alarm costs
# one extra call.
CLOSE_TOLERANCE = 0.01
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
    repaired: int = 0
    unverified: int = 0
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
        day = eastern_day(event.polled_at)
        if event.ticker not in first or day < first[event.ticker]:
            first[event.ticker] = day
    return first


def lookback_start(calendar: TradingCalendar, day: date) -> date:
    """The session LOOKBACK_SESSIONS sessions before `day` (or the earliest in range)."""
    sessions = calendar.sessions_in_range(day - timedelta(days=LOOKBACK_CALENDAR_DAYS), day)
    return sessions[-(LOOKBACK_SESSIONS + 1) :][0] if sessions else day


def last_settled_session(calendar: TradingCalendar, now: datetime) -> date:
    today = eastern_day(now)
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


def _overlap_start(calendar: TradingCalendar, last: date) -> date:
    """The oldest of the OVERLAP_SESSIONS sessions ending at `last`."""
    sessions = calendar.sessions_in_range(last - timedelta(days=OVERLAP_CALENDAR_DAYS), last)
    return sessions[-OVERLAP_SESSIONS:][0] if sessions else last


def _fetch_from(
    need: date,
    stored: tuple[date, date] | None,
    requested: date | None,
    last_session: date,
    calendar: TradingCalendar,
) -> date | None:
    """Where this symbol's fetch must start, or None when it is complete.

    History that begins after `need` is extended back, unless Schwab was
    already asked from `need` or earlier: a listing younger than the lookback
    has nothing before its first bar, and must not be asked every night.
    A tail fetch starts inside the stored bars so that it overlaps them.
    """
    if stored is None:
        return need
    first, last = stored
    if first > need and (requested is None or requested > need):
        return need
    if last < last_session:
        return _overlap_start(calendar, last)
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


def _off(stored: float, fresh: float) -> bool:
    return abs(stored - fresh) > CLOSE_TOLERANCE * fresh


def _compared(held: Sequence[StoredDailyBar], history: DailyHistory) -> list[tuple[StoredDailyBar, float]]:
    """Each stored bar with the fresh close of the same session; sessions the answer lacks are left out."""
    fresh = {bar.session_date: bar.close for bar in history.bars}
    return [(bar, fresh[bar.session_date]) for bar in held if bar.session_date in fresh]


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
    logger.info("surge_bars: replaying the Popularity Trigger to find the stocks to track")
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
        start = _fetch_from(needs[symbol], stored.get(symbol), requested.get(symbol), last_session, calendar)
        if start is None:
            continue
        previous = unpriced.get(symbol)
        if symbol not in stored and previous is not None and now - previous.last_attempt < UNPRICED_RETRY:
            report.deferred += 1
            continue
        todo.append((symbol, start))

    if len(todo) > PROGRESS_EVERY:
        logger.info("surge_bars: %d of %d symbols to fetch, one call each", len(todo), len(needs))

    async def fetch(symbol: str, start: date) -> DailyHistory | None:
        """One paced call; None when the symbol was refused or failed and has been accounted for."""
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
                return None
            logger.warning("%s: daily history failed: %s", symbol, exc)
            report.failed.append(symbol)
            return None
        except Exception:
            logger.exception("%s: daily history failed", symbol)
            report.failed.append(symbol)
            return None
        report.rejected.update(r.reason.value for r in history.rejects)
        return history

    try:  # the partial report is worth logging when a 429 or dead token stops the run
        for done, (symbol, start) in enumerate(todo):
            if done and done % PROGRESS_EVERY == 0:
                logger.info("surge_bars: %d of %d symbols done", done, len(todo))
            history = await fetch(symbol, start)
            if history is None:
                continue
            report.fetched += 1

            replace = False
            if symbol in stored and history.bars:
                async with session_factory() as session:
                    held = await DailyBarRepository(session).list_range(
                        symbol, history.bars[0].session_date, stored[symbol][1]
                    )
                compared = _compared(held, history)
                if not compared:
                    # A halted stock, say: the new bars go in unchecked, and the gap is the log's to show.
                    logger.warning(
                        "%s: no session in common with the stored bars (stored through %s, Schwab's answer starts %s): "
                        "storing the new bars unverified",
                        symbol, stored[symbol][1], history.bars[0].session_date,
                    )
                    report.unverified += 1
                if stale := [(bar, fresh) for bar, fresh in compared if _off(bar.close, fresh)]:
                    bar, fresh = stale[0]
                    logger.warning(
                        "%s: stored bars no longer match Schwab (%d of %d overlap sessions, %s closed %.4f stored "
                        "against %.4f now): replacing them with a fresh fetch from %s",
                        symbol, len(stale), len(compared), bar.session_date, bar.close, fresh, needs[symbol],
                    )
                    if start > needs[symbol]:
                        start = needs[symbol]
                        history = await fetch(symbol, start)
                        if history is None:
                            continue  # the stored bars stay as they are and the next run tries again
                    if not history.bars:
                        logger.warning("%s: the fresh fetch came back empty, keeping the stored bars", symbol)
                        continue
                    replace = True

            async with session_factory() as session:
                repo = DailyBarRepository(session)
                if replace:
                    report.bars_inserted += await repo.replace_symbol(symbol, _to_stored(history, clock.now()))
                    report.repaired += 1
                elif history.bars:
                    report.bars_inserted += await repo.insert_many(_to_stored(history, clock.now()))
                if history.bars:
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
        "surge_bars through %s: %d symbols tracked, %d fetched, %d bars inserted, %d repaired, "
        "%d unverified, %d unpriced, %d unpriced deferred, %d failed",
        last_session,
        report.needed,
        report.fetched,
        report.bars_inserted,
        report.repaired,
        report.unverified,
        report.unpriced,
        report.deferred,
        len(report.failed),
    )
    if report.rejected:
        logger.warning("surge_bars rejected bars (never stored): %s", dict(report.rejected))
