"""daily_bars: daily history for every listing of the latest Universe, seeded from 1990.

Each run is a catch-up, not a per-day append: it works out from the stored bars and requests
what is missing up to the last settled session and fetches only that, so the daily slot, the
startup run and a rerun are the same code, a rerun with nothing new makes no Schwab call, and a
pass that stopped resumes where it left off. SPY goes first, then the stocks the Popularity
Trigger ever hit (the Outcomes job reads only those), then the rest of the Universe.

A symbol with no bars asks from SEED_START and Schwab serves what it has: the seed is the same
one call as a tail, with more rows. A symbol whose history begins after the seed is extended
back, unless Schwab was already asked for that start and has nothing earlier: a young listing is
asked once. Bars of a listing that has left the Universe stay; the pass just stops asking.

A symbol's bars are written in one transaction, one commit per symbol, so a write never
decompresses more than that symbol's own history (Timescale aborts a transaction that
decompresses more than 100,000 tuples).

Schwab's history is split-adjusted as of the moment it is fetched, so a split after bars were
stored leaves them at the old scale (a reverse split, common among Popularity names, is the
worst case). A tail fetch therefore starts a few sessions before the newest stored bar, and the
overlap's closes are compared with the stored ones: when they disagree the symbol's stored bars
are replaced by a fresh full fetch. The overlap rides on the call the tail needs anyway. A tail
that shares no session with the stored bars at all is a ticker reused by another company and
replaces the history the same way; the old company's bars are not kept.

The pass stops at the stop time (STOP_AT unless configured), before the morning pass starts, and the
next run resumes it.
"""

import logging
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import Clock, StoredDailyBar, TradingCalendar, eastern_day
from rs_ingest.rate_limit import RateLimiter
from rs_ingest.replay import replayed_hits
from rs_persistence import DailyBarRepository, UniverseRepository
from rs_sources.schwab import DailyHistory, SchwabAuthError, SchwabResponseError

logger = logging.getLogger(__name__)

BENCHMARK = "SPY"

# A symbol with no bars asks from here; Schwab serves what it has (about 30 years of daily history).
SEED_START = date(1990, 1, 1)
# The pass stops at the next 03:00 Central after it starts, before the morning pass begins. This is
# the chain deadline until the probe settles when open interest does. The clock time is a setting
# (RS_INGEST_DAILY_BARS_STOP_AT); the zone is fixed.
STOP_ZONE = ZoneInfo("America/Chicago")
STOP_AT = time(3, 0)
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


class DailyBarsError(Exception):
    """Some symbols could not be fetched; the rest of the run was kept."""


class _Stopped(Exception):
    """The stop time came before a call could be made."""


@dataclass
class DailyBarsReport:
    needed: int = 0
    fetched: int = 0
    bars_inserted: int = 0
    repaired: int = 0
    reused: int = 0
    rejected: Counter[str] = field(default_factory=Counter)
    unpriced: int = 0
    deferred: int = 0
    failed: list[str] = field(default_factory=list)
    # Symbols still to fetch when the stop time came; the next run resumes with them.
    left: int = 0


def price_history_symbol(symbol: str) -> str:
    """The spelling Schwab's price history knows a Universe symbol by.

    The Universe keeps the listing file's own symbol (`BRK.B`), and bars are stored under it;
    only the request is spelled this way. One place, so a spelling the probe finds Schwab wants
    for class shares and preferreds is a change here and nowhere else.
    """
    return symbol


def stop_time(now: datetime, at: time = STOP_AT) -> datetime:
    """The next `at` Central after `now`."""
    today = datetime.combine(now.astimezone(STOP_ZONE).date(), at, tzinfo=STOP_ZONE)
    return today if today > now else today + timedelta(days=1)


async def triggered_tickers(session_factory: async_sessionmaker[AsyncSession], now: datetime) -> set[str]:
    """Every ticker the Popularity Trigger ever hit, over both popularity origins."""
    return {event.ticker async for event in replayed_hits(session_factory, now)}


async def bar_scope(
    session_factory: async_sessionmaker[AsyncSession], triggered: Iterable[str]
) -> list[str]:
    """Symbols to keep bars for, in fetch order: the benchmark, the Triggered stocks, then the rest
    of the latest Universe. A Triggered stock that has left the Universe is still kept up."""
    async with session_factory() as session:
        universe = await UniverseRepository(session).latest()
    if universe is None:
        logger.warning("daily_bars: no Universe stored yet; fetching the benchmark and Triggered stocks only")
    listed = {listing.symbol for listing in universe.listings} if universe is not None else set()
    first = [BENCHMARK, *sorted(set(triggered) - {BENCHMARK})]
    return first + sorted(listed - set(first))


def last_settled_session(calendar: TradingCalendar, now: datetime) -> date:
    today = eastern_day(now)
    for day in reversed(calendar.sessions_in_range(today - timedelta(days=10), today)):
        if calendar.session_close(day) + SETTLE <= now:
            return day
    raise RuntimeError(f"no settled session in the 10 days before {today}")


def _overlap_start(calendar: TradingCalendar, last: date) -> date:
    """The oldest of the OVERLAP_SESSIONS sessions ending at `last`."""
    sessions = calendar.sessions_in_range(last - timedelta(days=OVERLAP_CALENDAR_DAYS), last)
    return sessions[-OVERLAP_SESSIONS:][0] if sessions else last


def _fetch_from(
    stored: tuple[date, date] | None,
    requested: date | None,
    last_session: date,
    calendar: TradingCalendar,
) -> date | None:
    """Where this symbol's fetch must start, or None when it is complete.

    History that begins after the seed is extended back, unless Schwab was
    already asked from the seed: a listing younger than that has nothing
    earlier, and must not be asked every night. A tail fetch starts inside the
    stored bars so that it overlaps them.
    """
    if stored is None:
        return SEED_START
    first, last = stored
    if first > SEED_START and (requested is None or requested > SEED_START):
        return SEED_START
    if last < last_session:
        return _overlap_start(calendar, last)
    return None


def _to_stored(history: DailyHistory, symbol: str, fetched_at: datetime) -> list[StoredDailyBar]:
    """The answer's bars under the Universe's `symbol`, not the spelling the request used."""
    return [
        StoredDailyBar(
            symbol=symbol,
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


async def sync_daily_bars(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    source: DailyHistorySource,
    limiter: RateLimiter,
    calendar: TradingCalendar,
    clock: Clock,
    stop_at: datetime,
) -> DailyBarsReport:
    now = clock.now()
    last_session = last_settled_session(calendar, now)
    logger.info("daily_bars: replaying the Popularity Trigger to find the stocks to fetch first")
    scope = await bar_scope(session_factory, await triggered_tickers(session_factory, now))
    report = DailyBarsReport(needed=len(scope))

    async with session_factory() as session:
        repo = DailyBarRepository(session)
        stored = await repo.session_bounds(scope)
        requested = await repo.earliest_requested(scope)
        unpriced = {u.symbol: u for u in await repo.list_unpriced()}

    todo: list[tuple[str, date]] = []
    for symbol in scope:
        start = _fetch_from(stored.get(symbol), requested.get(symbol), last_session, calendar)
        if start is None:
            continue
        previous = unpriced.get(symbol)
        if symbol not in stored and previous is not None and now - previous.last_attempt < UNPRICED_RETRY:
            report.deferred += 1
            continue
        todo.append((symbol, start))

    if len(todo) > PROGRESS_EVERY:
        logger.info("daily_bars: %d of %d symbols to fetch, one call each", len(todo), len(scope))

    async def fetch(symbol: str, start: date) -> DailyHistory | None:
        """One paced call; None when the symbol was refused or failed and has been accounted for."""
        await limiter.acquire()
        if clock.now() >= stop_at:
            raise _Stopped
        try:
            history = await source.daily_history(price_history_symbol(symbol), start, last_session)
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

    done = 0
    try:  # the partial report is worth logging when a 429, a dead token or the stop time ends the run
        for done, (symbol, start) in enumerate(todo):
            if done and done % PROGRESS_EVERY == 0:
                logger.info("daily_bars: %d of %d symbols done", done, len(todo))
            history = await fetch(symbol, start)
            if history is None:
                continue
            report.fetched += 1

            replacing: str | None = None
            if symbol in stored and history.bars:
                async with session_factory() as session:
                    held = await DailyBarRepository(session).list_range(
                        symbol, history.bars[0].session_date, stored[symbol][1]
                    )
                compared = _compared(held, history)
                if not compared:
                    # Another company on a reused ticker, or a stock that was halted for the whole
                    # overlap: either way these bars cannot be told to continue the stored ones.
                    logger.warning(
                        "%s: no session in common with the stored bars (stored through %s, Schwab's answer starts %s): "
                        "replacing them with a fresh fetch from %s",
                        symbol, stored[symbol][1], history.bars[0].session_date, SEED_START,
                    )
                    replacing = "reused"
                elif stale := [(bar, fresh) for bar, fresh in compared if _off(bar.close, fresh)]:
                    bar, fresh = stale[0]
                    logger.warning(
                        "%s: stored bars no longer match Schwab (%d of %d overlap sessions, %s closed %.4f stored "
                        "against %.4f now): replacing them with a fresh fetch from %s",
                        symbol, len(stale), len(compared), bar.session_date, bar.close, fresh, SEED_START,
                    )
                    replacing = "split"
                if replacing is not None:
                    if start > SEED_START:
                        start = SEED_START
                        history = await fetch(symbol, start)
                        if history is None:
                            continue  # the stored bars stay as they are and the next run tries again
                    if not history.bars:
                        logger.warning("%s: the fresh fetch came back empty, keeping the stored bars", symbol)
                        continue

            async with session_factory() as session:
                repo = DailyBarRepository(session)
                if replacing is not None:
                    report.bars_inserted += await repo.replace_symbol(
                        symbol, _to_stored(history, symbol, clock.now())
                    )
                    if replacing == "reused":
                        report.reused += 1
                    else:
                        report.repaired += 1
                elif history.bars:
                    report.bars_inserted += await repo.insert_many(_to_stored(history, symbol, clock.now()))
                if history.bars:
                    await repo.clear_unpriced(symbol)
                await repo.record_request(symbol, start)
                await session.commit()
            if not history.bars and symbol not in stored:
                await _record_unpriced(session_factory, symbol, _unpriced_reason(history), clock.now())
                report.unpriced += 1
    except _Stopped:
        report.left = len(todo) - done
        logger.info(
            "daily_bars: stop time %s reached with %d of %d symbols left; the next run resumes them",
            stop_at, report.left, len(todo),
        )
    finally:
        _log(report, last_session)
    if report.failed:
        raise DailyBarsError(f"{len(report.failed)} of {len(todo)} symbols failed: {_head(report.failed)}")
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


def _log(report: DailyBarsReport, last_session: date) -> None:
    logger.info(
        "daily_bars through %s: %d symbols in scope, %d fetched, %d bars inserted, %d repaired, "
        "%d replaced (reused ticker), %d unpriced, %d unpriced deferred, %d left at the stop time, %d failed",
        last_session,
        report.needed,
        report.fetched,
        report.bars_inserted,
        report.repaired,
        report.reused,
        report.unpriced,
        report.deferred,
        report.left,
        len(report.failed),
    )
    if report.rejected:
        logger.warning("daily_bars rejected bars (never stored): %s", dict(report.rejected))
