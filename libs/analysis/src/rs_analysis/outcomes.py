"""Outcome maths: what a stock did after a case, pure and deterministic.

Everything is measured on a SessionGrid (the exchange sessions in order) and on
daily bars aligned to it, so a missing bar is a gap in an array and never shifts
a horizon.

- Entry is the open of the first session that begins after the case time; the
  h-day exit is the close of the h-th session counting the entry session.
- Excess is the stock's return from the entry open to the exit close minus
  SPY's over the same sessions.
- A stock's typical h-day move is 1.4826 x the median absolute deviation of its
  daily excess moves (close to close, stock minus SPY) over the 250 sessions
  before entry, x sqrt(h). The median absolute deviation is used because a
  squeeze or a crash in the history must not stretch what "typical" means.
  Fewer than MIN_TYPICAL_SESSIONS valid daily moves, or a deviation of zero,
  leave a case without a move size.
- Move size is the absolute excess move over the typical move. The stock's
  ordinary share is the share of its own rolling h-day windows inside those 250
  sessions that moved BIG_MOVE x typical or more, measured the same way as the
  Outcome, so the two shares compare like with like.

Bars that are not known to be split-adjusted are guarded: a case whose window
holds a close-to-close jump that looks like a split is excluded, not measured.
"""

import math
from bisect import bisect_right
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from statistics import fmean, median

import numpy as np
from pydantic import BaseModel, ConfigDict

from rs_domain import StoredDailyBar, TradingCalendar

HORIZONS = (1, 5, 20)

TYPICAL_SESSIONS = 250
MIN_TYPICAL_SESSIONS = 120
# Scales a median absolute deviation to the standard deviation of a normal distribution.
MAD_TO_SIGMA = 1.4826
# A move this many typical moves or more is "big".
BIG_MOVE = 2.0

# Close-to-close ratios (or their inverses) that look like a split in unadjusted bars.
SPLIT_RATIOS = (2.0, 3.0, 4.0, 5.0, 10.0, 20.0)
SPLIT_TOLERANCE = 0.05


@dataclass(frozen=True)
class SessionGrid:
    """Exchange sessions in order, with each one's open instant (UTC)."""

    days: tuple[date, ...]
    opens: tuple[datetime, ...]

    @classmethod
    def build(cls, calendar: TradingCalendar, start: date, end: date) -> "SessionGrid":
        days = tuple(calendar.sessions_in_range(start, end))
        return cls(days, tuple(calendar.session_open(day) for day in days))

    def index_of(self, day: date) -> int | None:
        index = bisect_right(self.days, day) - 1
        return index if index >= 0 and self.days[index] == day else None

    def entry_index(self, case_at: datetime) -> int | None:
        """The first session that begins after `case_at`, None when the grid ends before it.
        The grid must start before the case (a case before it would enter session 0)."""
        index = bisect_right(self.opens, case_at)
        return index if index < len(self.opens) else None


@dataclass(frozen=True)
class AlignedBars:
    """One symbol's opens, closes and adjusted flags on a grid; NaN where it has no bar."""

    open: np.ndarray
    close: np.ndarray
    adjusted: np.ndarray


def align(grid: SessionGrid, bars: Iterable[StoredDailyBar]) -> AlignedBars:
    size = len(grid.days)
    opens = np.full(size, np.nan)
    closes = np.full(size, np.nan)
    adjusted = np.ones(size, dtype=bool)
    for bar in bars:
        index = grid.index_of(bar.session_date)
        if index is not None:
            opens[index], closes[index], adjusted[index] = bar.open, bar.close, bar.adjusted
    return AlignedBars(opens, closes, adjusted)


class Status(StrEnum):
    MEASURED = "measured"
    NOT_MATURED = "not_matured"
    # The exit session is due but a bar the measure needs is missing.
    UNPRICED = "unpriced"
    # The bars are not known to be split-adjusted and the window holds a jump that looks like a split.
    SUSPECTED_SPLIT = "suspected_split"


class Measure(BaseModel):
    """One case's Outcome at one horizon."""

    model_config = ConfigDict(frozen=True)

    entry_session: date
    exit_session: date
    stock_return: float
    benchmark_return: float
    excess_return: float
    # None when the stock's history gives no typical move.
    typical_move: float | None
    move_multiple: float | None
    ordinary_share: float | None


@dataclass(frozen=True)
class HorizonResult:
    horizon: int
    status: Status
    measure: Measure | None = None


def _usable(price: float) -> bool:
    return math.isfinite(price) and price > 0


def _suspected_split(closes: np.ndarray) -> bool:
    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = closes[1:] / closes[:-1]
    ratio = ratio[np.isfinite(ratio) & (ratio > 0)]
    ratio = np.maximum(ratio, 1.0 / ratio)
    return any(bool(np.any(np.abs(ratio / split - 1.0) <= SPLIT_TOLERANCE)) for split in SPLIT_RATIOS)


def _typical_daily_move(stock: AlignedBars, benchmark: AlignedBars, first: int, entry: int) -> float | None:
    """1.4826 x the median absolute deviation of the daily excess moves in sessions [first, entry)."""
    lo = max(first, 1)
    if entry <= lo:
        return None
    with np.errstate(divide="ignore", invalid="ignore"):
        excess = (stock.close[lo:entry] / stock.close[lo - 1 : entry - 1]) - (
            benchmark.close[lo:entry] / benchmark.close[lo - 1 : entry - 1]
        )
    excess = excess[np.isfinite(excess)]
    if excess.size < MIN_TYPICAL_SESSIONS:
        return None
    sigma = MAD_TO_SIGMA * float(np.median(np.abs(excess - np.median(excess))))
    return sigma if sigma > 0 else None


def _ordinary_share(
    stock: AlignedBars, benchmark: AlignedBars, first: int, entry: int, horizon: int, typical: float
) -> float | None:
    """The share of the rolling `horizon`-session windows inside [first, entry) whose
    excess move is BIG_MOVE x `typical` or more."""
    lo = max(first, 0)
    if entry - lo < horizon:
        return None
    starts, ends = slice(lo, entry - horizon + 1), slice(lo + horizon - 1, entry)
    with np.errstate(divide="ignore", invalid="ignore"):
        excess = (stock.close[ends] / stock.open[starts]) - (benchmark.close[ends] / benchmark.open[starts])
    excess = excess[np.isfinite(excess)]
    if excess.size == 0:
        return None
    return float(np.mean(np.abs(excess) >= BIG_MOVE * typical))


def measure_case(
    grid: SessionGrid, stock: AlignedBars, benchmark: AlignedBars, entry: int, last_settled: int
) -> list[HorizonResult]:
    """The case entered at grid session `entry`, at every horizon. A horizon is due
    once its exit session is at or before `last_settled`; a due horizon with a missing
    bar is UNPRICED, one not yet due is NOT_MATURED."""
    first = entry - TYPICAL_SESSIONS
    daily = _typical_daily_move(stock, benchmark, first, entry)
    results = []
    for horizon in HORIZONS:
        exit_ = entry + horizon - 1
        if exit_ > last_settled or exit_ >= len(grid.days):
            results.append(HorizonResult(horizon, Status.NOT_MATURED))
            continue
        prices = (stock.open[entry], stock.close[exit_], benchmark.open[entry], benchmark.close[exit_])
        if not all(_usable(float(p)) for p in prices):
            results.append(HorizonResult(horizon, Status.UNPRICED))
            continue
        window = slice(max(first - 1, 0), exit_ + 1)
        if not bool(np.all(stock.adjusted[window])) and _suspected_split(stock.close[window]):
            results.append(HorizonResult(horizon, Status.SUSPECTED_SPLIT))
            continue
        stock_open, stock_close, bench_open, bench_close = (float(p) for p in prices)
        stock_return = stock_close / stock_open - 1.0
        benchmark_return = bench_close / bench_open - 1.0
        excess = stock_return - benchmark_return
        typical = None if daily is None else daily * math.sqrt(horizon)
        results.append(
            HorizonResult(
                horizon,
                Status.MEASURED,
                Measure(
                    entry_session=grid.days[entry],
                    exit_session=grid.days[exit_],
                    stock_return=stock_return,
                    benchmark_return=benchmark_return,
                    excess_return=excess,
                    typical_move=typical,
                    move_multiple=None if typical is None else abs(excess) / typical,
                    ordinary_share=(
                        None
                        if typical is None
                        else _ordinary_share(stock, benchmark, first, entry, horizon, typical)
                    ),
                ),
            )
        )
    return results


class HorizonSummary(BaseModel):
    """What one horizon's results come to. Shares and medians are None when nothing backs them."""

    model_config = ConfigDict(frozen=True)

    outcomes: int
    unpriced: int
    split_excluded: int
    no_move_size: int
    # Over the Outcomes that have a move size.
    share_at_2x: float | None
    ordinary_share: float | None
    median_multiple: float | None
    # Over every Outcome: direction needs no move size.
    median_excess: float | None


def summarize(results: Sequence[HorizonResult]) -> HorizonSummary:
    """Summarise one horizon's results (one per case)."""
    measures = [r.measure for r in results if r.measure is not None]
    multiples = [m.move_multiple for m in measures if m.move_multiple is not None]
    ordinary = [
        m.ordinary_share
        for m in measures
        if m.move_multiple is not None and m.ordinary_share is not None
    ]
    return HorizonSummary(
        outcomes=len(measures),
        unpriced=sum(r.status is Status.UNPRICED for r in results),
        split_excluded=sum(r.status is Status.SUSPECTED_SPLIT for r in results),
        no_move_size=len(measures) - len(multiples),
        share_at_2x=fmean(x >= BIG_MOVE for x in multiples) if multiples else None,
        ordinary_share=fmean(ordinary) if ordinary else None,
        median_multiple=median(multiples) if multiples else None,
        median_excess=median(m.excess_return for m in measures) if measures else None,
    )
