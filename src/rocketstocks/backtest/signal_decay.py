"""Signal decay analysis — forward return curves.

Answers the question: "How long after a signal fires does the edge last?"

For each trade entry in a completed backtest run, this module looks up the
forward close prices at multiple horizons (1, 3, 5, 10, 20 bars) and computes
statistical properties of those forward returns across all signals.

The result is a decay curve showing:
- When the mean forward return peaks (optimal hold period)
- How quickly the edge fades with time
- Whether the edge is statistically significant at each horizon

All analysis is purely post-hoc — no backtest re-runs required. Uses stored
trade entry timestamps and the existing price_history tables.
"""
import datetime
import logging
import math
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

logger = logging.getLogger(__name__)

_DEFAULT_HORIZONS = [1, 2, 3, 5, 10, 20]


@dataclass
class DecayPoint:
    """Forward return statistics at a single horizon."""
    horizon: int            # number of bars forward
    mean_return: float      # mean forward return across all signals
    median_return: float
    std_return: float
    win_rate: float         # % of signals with positive forward return
    n_signals: int          # number of signals with data at this horizon
    t_stat: float
    p_value: float
    significant: bool       # p < 0.05


def compute_signal_decay(
    trades: list[dict],
    price_data: dict[str, pd.DataFrame],
    horizons: list[int] | None = None,
) -> list[DecayPoint]:
    """Compute forward return statistics at multiple horizons for all trade entries.

    For each trade, looks up the forward close price at each horizon using the
    price data for that ticker, then computes aggregate statistics.

    Args:
        trades: List of trade dicts from backtest_trades, each with at least
            'ticker' and 'entry_time'.
        price_data: Dict mapping ticker → daily OHLCV DataFrame in backtesting.py
            format (DatetimeIndex, capitalized columns). Fetched externally.
        horizons: List of forward bar counts (default: [1, 2, 3, 5, 10, 20]).

    Returns:
        List of DecayPoint, one per horizon, sorted by horizon ascending.
        Empty list if insufficient data.
    """
    if horizons is None:
        horizons = _DEFAULT_HORIZONS

    # Build per-ticker sorted close series for fast horizon lookups
    close_series: dict[str, pd.Series] = {}
    for ticker, df in price_data.items():
        if df.empty or 'Close' not in df.columns:
            continue
        close_series[ticker] = df['Close'].sort_index()

    # For each trade, find the entry close and compute forward returns
    # Structure: horizon → list of forward returns
    horizon_returns: dict[int, list[float]] = {h: [] for h in horizons}

    for trade in trades:
        ticker = trade.get('ticker')
        entry_time = trade.get('entry_time')
        if not ticker or entry_time is None or ticker not in close_series:
            continue

        cs = close_series[ticker]
        # Find the index position of the entry bar (nearest date on or after entry)
        entry_ts = pd.Timestamp(entry_time)
        if entry_ts.tzinfo is not None:
            # Normalize to date for daily price lookups
            entry_date = entry_ts.date()
            entry_ts = pd.Timestamp(entry_date)

        try:
            # Find the entry bar position in the price series
            loc = cs.index.searchsorted(entry_ts)
            if loc >= len(cs):
                continue
            entry_price = float(cs.iloc[loc])
            if entry_price <= 0 or math.isnan(entry_price):
                continue

            for horizon in horizons:
                forward_loc = loc + horizon
                if forward_loc >= len(cs):
                    continue
                forward_price = float(cs.iloc[forward_loc])
                if forward_price <= 0 or math.isnan(forward_price):
                    continue
                fwd_return = (forward_price / entry_price - 1) * 100
                horizon_returns[horizon].append(fwd_return)

        except Exception as exc:
            logger.debug(f"signal_decay: skipping trade {ticker}@{entry_time}: {exc}")

    # Aggregate stats per horizon
    points = []
    for horizon in sorted(horizons):
        returns = horizon_returns.get(horizon, [])
        n = len(returns)
        if n < 2:
            points.append(DecayPoint(
                horizon=horizon,
                mean_return=float('nan'),
                median_return=float('nan'),
                std_return=float('nan'),
                win_rate=float('nan'),
                n_signals=n,
                t_stat=float('nan'),
                p_value=float('nan'),
                significant=False,
            ))
            continue

        arr = np.array(returns)
        t_stat, p_value = scipy_stats.ttest_1samp(arr, 0)
        points.append(DecayPoint(
            horizon=horizon,
            mean_return=float(arr.mean()),
            median_return=float(np.median(arr)),
            std_return=float(arr.std(ddof=1)),
            win_rate=float((arr > 0).mean() * 100),
            n_signals=n,
            t_stat=float(t_stat),
            p_value=float(p_value),
            significant=float(p_value) < 0.05,
        ))

    return points


def find_peak_horizon(points: list[DecayPoint]) -> int | None:
    """Return the horizon with the highest mean return among significant points.

    If no points are significant, returns the horizon with the highest mean
    return overall. Returns None if points is empty.

    Args:
        points: List of DecayPoint from compute_signal_decay().

    Returns:
        Horizon (int) with the best mean return, or None.
    """
    if not points:
        return None

    valid = [p for p in points if not math.isnan(p.mean_return)]
    if not valid:
        return None

    significant = [p for p in valid if p.significant]
    candidates = significant if significant else valid
    return max(candidates, key=lambda p: p.mean_return).horizon
