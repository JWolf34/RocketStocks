"""Forward return event study engine.

For a given set of events (ticker + datetime), computes the distribution of
forward returns at multiple horizons.  Source-agnostic — works with any
events DataFrame in the standard format.

Statistical approach mirrors backtest/signal_decay.py (searchsorted + horizon
lookup, one-sample t-test) but operates on raw event sets rather than
backtest trades, and adds control-group comparison + signal_value stratification.
"""
import logging
import math
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

from rocketstocks.eda.events.base import build_control_group
from rocketstocks.eda.formatting import (
    fmt_pct, fmt_float, fmt_pvalue, significant_marker,
    print_table, print_separator,
)

logger = logging.getLogger(__name__)

_DAILY_HORIZONS = [1, 2, 3, 5, 10, 20]
_INTRADAY_HORIZONS = [6, 12, 39, 78, 390]   # 30m, 1h, half-day, full day, 5 days

_INTRADAY_HORIZON_LABELS = {
    6: '30m', 12: '1h', 39: '~half-day', 78: '~1 day', 390: '~5 days',
}


@dataclass
class HorizonResult:
    """Forward return statistics at a single horizon."""
    horizon: int
    horizon_label: str
    n_events: int
    mean_return: float
    median_return: float
    std_return: float
    win_rate: float          # % of events with positive forward return
    t_stat: float
    p_value: float
    significant: bool


@dataclass
class ForwardReturnResult:
    """Complete forward return analysis result for one event set."""
    source_detail: str        # e.g. 'mention_ratio>=3.0' or 'all'
    n_events_total: int
    n_tickers: int
    horizons: list[HorizonResult]
    control: list[HorizonResult] = field(default_factory=list)
    strata: dict[str, list[HorizonResult]] = field(default_factory=dict)


def run_forward_returns(
    events: pd.DataFrame,
    close_dict: dict[str, pd.Series],
    timeframe: str = 'daily',
    custom_horizons: list[int] | None = None,
    n_control: int = 500,
    stratify: bool = True,
) -> list[ForwardReturnResult]:
    """Compute forward return distributions for each source_detail group.

    Args:
        events: Standard events DataFrame (must include ticker, datetime,
            signal_value, source_detail columns).
        close_dict: Per-ticker close Series with DatetimeIndex.
        timeframe: 'daily' or '5m' — controls default horizons.
        custom_horizons: Override default horizon list.
        n_control: Number of control-group samples.
        stratify: Whether to stratify by signal_value buckets.

    Returns:
        List of ForwardReturnResult, one per unique source_detail value.
        If events has no source_detail column, returns a single result for all.
    """
    if events.empty or not close_dict:
        logger.warning("forward_returns: no events or price data")
        return []

    n_tickers = events['ticker'].nunique() if 'ticker' in events.columns else 0
    n_events = len(events)
    print(f"Computing forward returns: {n_events} events across {n_tickers} tickers "
          f"({len(close_dict)} tickers with price data)")

    horizons = custom_horizons or (
        _DAILY_HORIZONS if timeframe == 'daily' else _INTRADAY_HORIZONS
    )
    horizon_labels = _INTRADAY_HORIZON_LABELS if timeframe == '5m' else {}

    # Build sorted close series for fast searchsorted lookups
    sorted_closes: dict[str, pd.Series] = {
        t: s.sort_index() for t, s in close_dict.items()
    }

    # Control group (computed once, shared across source_details)
    control_events = build_control_group(events, close_dict, n_samples=n_control)
    control_horizons = _compute_horizons(control_events, sorted_closes, horizons, horizon_labels)

    # Group by source_detail
    if 'source_detail' in events.columns:
        groups = {detail: grp for detail, grp in events.groupby('source_detail')}
    else:
        groups = {'all': events}

    n_groups = len(groups)
    results: list[ForwardReturnResult] = []
    for i, (detail, grp) in enumerate(groups.items(), 1):
        print(f"  [{i}/{n_groups}] {detail} ({len(grp)} events, "
              f"{grp['ticker'].nunique()} tickers)")
        grp = grp.dropna(subset=['ticker', 'datetime'])
        main_horizons = _compute_horizons(grp, sorted_closes, horizons, horizon_labels)

        # Stratify by signal_value buckets
        strata: dict[str, list[HorizonResult]] = {}
        if stratify and 'signal_value' in grp.columns:
            strata = _compute_strata(grp, sorted_closes, horizons, horizon_labels)

        results.append(ForwardReturnResult(
            source_detail=detail,
            n_events_total=len(grp),
            n_tickers=grp['ticker'].nunique(),
            horizons=main_horizons,
            control=control_horizons,
            strata=strata,
        ))

    return results


def print_results(results: list[ForwardReturnResult], timeframe: str = 'daily') -> None:
    """Print forward return results as formatted terminal tables."""
    if not results:
        print("No forward return results to display.")
        return

    for r in results:
        print_separator()
        print(f"\nForward Returns — {r.source_detail}")
        print(f"  Events: {r.n_events_total}  |  Tickers: {r.n_tickers}")

        _print_horizon_table(r.horizons, label="Events", timeframe=timeframe)
        if r.control:
            _print_horizon_table(r.control, label="Control (random non-event dates)", timeframe=timeframe)

        if r.strata:
            for stratum_label, stratum_horizons in r.strata.items():
                _print_horizon_table(stratum_horizons, label=f"Stratum: {stratum_label}", timeframe=timeframe)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _compute_horizons(
    events: pd.DataFrame,
    sorted_closes: dict[str, pd.Series],
    horizons: list[int],
    horizon_labels: dict[int, str],
) -> list[HorizonResult]:
    """Compute forward return statistics at each horizon for given events."""
    horizon_returns: dict[int, list[float]] = {h: [] for h in horizons}

    for _, row in events.iterrows():
        ticker = row['ticker']
        dt = pd.Timestamp(row['datetime'])
        if ticker not in sorted_closes:
            continue

        cs = sorted_closes[ticker]
        loc = cs.index.searchsorted(dt)
        if loc >= len(cs):
            continue

        entry_price = float(cs.iloc[loc])
        if entry_price <= 0 or math.isnan(entry_price):
            continue

        for h in horizons:
            fwd_loc = loc + h
            if fwd_loc >= len(cs):
                continue
            fwd_price = float(cs.iloc[fwd_loc])
            if fwd_price <= 0 or math.isnan(fwd_price):
                continue
            horizon_returns[h].append((fwd_price / entry_price - 1) * 100.0)

    results = []
    for h in sorted(horizons):
        returns = horizon_returns[h]
        label = horizon_labels.get(h, f'{h}d' if h <= 30 else f'{h}b')
        n = len(returns)
        if n < 2:
            results.append(HorizonResult(
                horizon=h, horizon_label=label, n_events=n,
                mean_return=float('nan'), median_return=float('nan'),
                std_return=float('nan'), win_rate=float('nan'),
                t_stat=float('nan'), p_value=float('nan'), significant=False,
            ))
            continue

        arr = np.array(returns)
        t_stat, p_value = scipy_stats.ttest_1samp(arr, 0)
        results.append(HorizonResult(
            horizon=h,
            horizon_label=label,
            n_events=n,
            mean_return=float(arr.mean()),
            median_return=float(np.median(arr)),
            std_return=float(arr.std(ddof=1)),
            win_rate=float((arr > 0).mean() * 100),
            t_stat=float(t_stat),
            p_value=float(p_value),
            significant=float(p_value) < 0.05,
        ))

    return results


def _compute_strata(
    events: pd.DataFrame,
    sorted_closes: dict[str, pd.Series],
    horizons: list[int],
    horizon_labels: dict[int, str],
) -> dict[str, list[HorizonResult]]:
    """Stratify events by signal_value buckets and compute horizons per stratum."""
    sv = events['signal_value'].dropna()
    if sv.empty:
        return {}

    # Use quartile-based buckets so each stratum has reasonable sample size
    try:
        labels_quantile = ['Q1 (low)', 'Q2', 'Q3', 'Q4 (high)']
        events = events.copy()
        events['_stratum'] = pd.qcut(sv, q=4, labels=labels_quantile, duplicates='drop')
    except Exception:
        return {}

    strata: dict[str, list[HorizonResult]] = {}
    for label, grp in events.groupby('_stratum', observed=True):
        if len(grp) < 5:
            continue
        strata[str(label)] = _compute_horizons(grp, sorted_closes, horizons, horizon_labels)

    return strata


def _print_horizon_table(
    horizons: list[HorizonResult],
    label: str,
    timeframe: str,
) -> None:
    """Print a single horizon table."""
    if not horizons:
        return

    print(f"\n  {label}")
    headers = ['Horizon', 'N', 'Mean %', 'Median %', 'Std %', 'Win %', 't-stat', 'p-value', 'Sig']
    rows = []
    for h in horizons:
        rows.append([
            h.horizon_label,
            str(h.n_events),
            fmt_pct(h.mean_return),
            fmt_pct(h.median_return),
            fmt_pct(h.std_return),
            fmt_pct(h.win_rate),
            fmt_float(h.t_stat),
            fmt_pvalue(h.p_value),
            significant_marker(h.p_value),
        ])
    print_table(headers, rows, col_widths=[10, 6, 9, 10, 9, 8, 8, 8, 4])
