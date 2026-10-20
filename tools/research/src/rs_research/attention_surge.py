"""Attention-surge event study: the base rates the bot shows beside an alert.

Two rows are written per run, for the current `rs_analysis.attention_surge`
rule version:

- `reconstructed`: replay the rule over every stored popularity snapshot and
  enter each event at the close of the first session after its event date
  (the signal evidence study convention).
- `logged`: the alerts the rule actually posted, entered at their `alert_price`.
  The benchmark and exits use the close of the last session before the alert as
  the reference, so the first horizon is the next close after the alert.
  Alerts without a price are counted in `n_unpriced`.

Metrics (see rs_research.returns): returns at 1, 5 and 10 trading days, excess
versus SPY over the same window, hit rate = share of events with a strictly
positive excess return, median excess return, N, and `n_unpriced`, the events
whose ticker has no bars (or no price) and so could not be measured.
"""

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import numpy as np
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_analysis.attention_surge import RULE_VERSION, SurgeEvent, replay
from rs_domain import BaseRate, BaseRateInput, Clock, PopularitySnapshot, StoredDailyBar
from rs_persistence import BaseRateRepository
from rs_query import AlertRecord, Page, daily_bars, popularity_between, recent_alerts
from rs_research.returns import (
    MAX_CLOSURE_DAYS,
    Summary,
    first_session_after,
    last_close_before,
    measure,
    session_date,
)

logger = logging.getLogger(__name__)

ALERT_TYPE = "attention_surge"
BENCHMARK = "SPY"

HISTORY_START = datetime(2000, 1, 1, tzinfo=UTC)
PAGE_SIZE = 1000
# Calendar days of bars fetched around an event: after it, the entry session and
# the 10-session horizon with room for holidays; before it, the reference
# session a weekend alert needs and the benchmark history the placement check
# looks for.
LOOKAHEAD = timedelta(days=30)
LOOKBACK = timedelta(days=7)


class StudyError(RuntimeError):
    """The study cannot run on what is stored (the message says what to fix)."""


async def _drain[T](fetch: Callable[[str | None], Awaitable[Page[T]]]) -> list[T]:
    items: list[T] = []
    cursor: str | None = None
    while True:
        page = await fetch(cursor)
        items.extend(page.items)
        cursor = page.next_cursor
        if cursor is None:
            return items


async def _load_bars(
    session: AsyncSession, windows: dict[str, tuple[date, date]]
) -> pd.Series:
    """Close prices indexed by (symbol, date ordinal) for each symbol's window."""
    rows: list[StoredDailyBar] = []
    for symbol in sorted(windows):
        start, end = windows[symbol]

        async def fetch(cursor: str | None, symbol: str = symbol, start: date = start, end: date = end) -> Page[StoredDailyBar]:
            return await daily_bars(session, symbol, start, end, limit=PAGE_SIZE, cursor=cursor)

        rows.extend(await _drain(fetch))
    index = pd.MultiIndex.from_arrays(
        [[r.symbol for r in rows], [r.session_date.toordinal() for r in rows]],
        names=["symbol", "session"],
    )
    return pd.Series([r.close for r in rows], index=index, dtype=float)


def _widen(windows: dict[str, tuple[date, date]], symbol: str, instant: datetime) -> None:
    day = session_date(instant)
    lo, hi = windows.get(symbol, (day, day))
    windows[symbol] = (min(lo, day), max(hi, day))


def _bar_windows(*groups: list[tuple[str, datetime]]) -> dict[str, tuple[date, date]]:
    """Per symbol, from its first event day less the lookback to its last plus
    the lookahead, and the benchmark's window covering them all."""
    windows: dict[str, tuple[date, date]] = {}
    for group in groups:
        for symbol, instant in group:
            _widen(windows, symbol, instant)
            _widen(windows, BENCHMARK, instant)
    return {s: (lo - LOOKBACK, hi + LOOKAHEAD) for s, (lo, hi) in windows.items()}


def _caveat(summary: Summary, total: int, what: str) -> str:
    return (
        f"{summary.n_unpriced} of {total} {what} could not be priced and are excluded; "
        "they are mostly delisted or renamed tickers, so results lean toward survivors."
    )


def _rate(
    kind: BaseRateInput, summary: Summary, total: int, what: str, computed_at: datetime
) -> BaseRate:
    return BaseRate(
        alert_type=ALERT_TYPE,
        rule_version=RULE_VERSION,
        computed_at=computed_at,
        input=kind,
        n=summary.n,
        n_unpriced=summary.n_unpriced,
        hit_rate_1d=summary.hit_rate[1],
        hit_rate_5d=summary.hit_rate[5],
        hit_rate_10d=summary.hit_rate[10],
        median_excess_1d=summary.median_excess[1],
        median_excess_5d=summary.median_excess[5],
        median_excess_10d=summary.median_excess[10],
        survivorship_caveat=_caveat(summary, total, what),
    )


def reconstructed_summary(
    events: list[SurgeEvent], sessions: np.ndarray, spy_close: np.ndarray, closes: pd.Series
) -> tuple[Summary, int]:
    """Measure replayed events entered at the close of the first session after
    the event date. Returns the summary and the events considered (those whose
    entry session has happened)."""
    entry = first_session_after(sessions, [e.polled_at for e in events])
    # An event before the stored benchmark history cannot be placed on it.
    before_history = np.array(
        [
            session_date(e.polled_at).toordinal() < sessions[0] - MAX_CLOSURE_DAYS
            for e in events
        ],
        dtype=bool,
    )
    happened = entry < len(sessions)
    symbols = [e.ticker for e, ok in zip(events, happened, strict=True) if ok]
    ref = entry[happened]
    target = pd.MultiIndex.from_arrays([symbols, sessions[ref]])
    price = np.where(
        before_history[happened], np.nan, closes.reindex(target).to_numpy(dtype=float)
    )
    return measure(symbols, ref, price, sessions, spy_close, closes), len(symbols)


def logged_summary(
    alerts: list[AlertRecord], sessions: np.ndarray, spy_close: np.ndarray, closes: pd.Series
) -> Summary:
    """Measure posted alerts entered at their alert price."""
    instants = [(a.alert.quote_time or a.alert.triggered_at) for a in alerts]
    ref = last_close_before(sessions, instants)
    price = np.array(
        [np.nan if a.alert.alert_price is None else a.alert.alert_price for a in alerts],
        dtype=float,
    )
    # No bars at all for the symbol: nothing to measure the exit against.
    barred = set(closes.index.get_level_values("symbol"))
    price = np.where([a.alert.symbol not in barred for a in alerts], np.nan, price)
    return measure([a.alert.symbol for a in alerts], ref, price, sessions, spy_close, closes)


@dataclass(frozen=True)
class StudyResult:
    rates: list[BaseRate]
    events: int
    alerts: int


async def compute(
    session_factory: async_sessionmaker[AsyncSession], clock: Clock
) -> StudyResult:
    """Read everything, measure both inputs and return the rows to write."""
    now = clock.now()
    async with session_factory() as session:
        snapshots: list[PopularitySnapshot] = await _drain(
            lambda cursor: popularity_between(
                session, HISTORY_START, now, limit=PAGE_SIZE, cursor=cursor
            )
        )
        alerts = [
            record
            for record in await _drain(
                lambda cursor: recent_alerts(session, limit=PAGE_SIZE, cursor=cursor)
            )
            if record.alert.alert_type == ALERT_TYPE and record.alert.rule_version == RULE_VERSION
        ]
        events = replay(snapshots)
        del snapshots
        if not events and not alerts:
            return StudyResult([], 0, 0)
        windows = _bar_windows(
            [(e.ticker, e.polled_at) for e in events],
            [(a.alert.symbol, a.alert.quote_time or a.alert.triggered_at) for a in alerts],
        )
        closes = await _load_bars(session, windows)

    if BENCHMARK not in closes.index.get_level_values("symbol"):
        raise StudyError(f"no {BENCHMARK} daily bars stored; the surge-bars job has not fetched the benchmark")
    spy = closes.xs(BENCHMARK, level="symbol").sort_index()
    sessions = spy.index.to_numpy(dtype=np.int64)
    spy_close = spy.to_numpy(dtype=float)

    rates: list[BaseRate] = []
    if events:
        summary, considered = reconstructed_summary(events, sessions, spy_close, closes)
        if summary.n:
            rates.append(
                _rate(BaseRateInput.RECONSTRUCTED, summary, considered, "replayed events", now)
            )
    if alerts:
        summary = logged_summary(alerts, sessions, spy_close, closes)
        if summary.n:
            rates.append(_rate(BaseRateInput.LOGGED, summary, len(alerts), "posted alerts", now))
    return StudyResult(rates, len(events), len(alerts))


async def run(session_factory: async_sessionmaker[AsyncSession], clock: Clock) -> StudyResult:
    """Compute and append the base-rate rows (a rerun appends new rows; readers
    take the latest). Nothing is written when there is nothing to measure."""
    result = await compute(session_factory, clock)
    if not result.rates:
        logger.warning(
            "attention_surge event study measured nothing (%d events, %d alerts)",
            result.events,
            result.alerts,
        )
        return result
    async with session_factory() as session:
        repository = BaseRateRepository(session)
        for rate in result.rates:
            await repository.insert(rate)
        await session.commit()
    for rate in result.rates:
        logger.info(
            "attention_surge base rate written: input=%s n=%d n_unpriced=%d",
            rate.input.value,
            rate.n,
            rate.n_unpriced,
        )
    return result
