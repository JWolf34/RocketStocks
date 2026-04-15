"""Forward return event study engine.

For a given set of events (ticker + datetime), computes the distribution of
forward returns at multiple horizons.  Source-agnostic — works with any
events DataFrame in the standard format.

Statistical approach mirrors backtest/signal_decay.py (searchsorted + horizon
lookup, one-sample t-test) but operates on raw event sets rather than
backtest trades, and adds control-group comparison.

Memory model: streams price data one ticker at a time via stream_tickers(),
keeping peak RSS bounded regardless of universe size.
"""
import gc
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
from rocketstocks.eda.streaming import fetch_bar_counts, stream_tickers

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


async def run_forward_returns(
    events: pd.DataFrame,
    stock_data,
    timeframe: str = 'daily',
    start_date=None,
    end_date=None,
    custom_horizons: list[int] | None = None,
    n_control: int = 500,
    stratify: bool = True,
) -> list[ForwardReturnResult]:
    """Compute forward return distributions for each source_detail group.

    Streams price data one ticker at a time so peak RSS stays bounded
    regardless of universe × history size.

    Args:
        events: Standard events DataFrame (must include ticker, datetime,
            signal_value columns; source_detail column is optional).
        stock_data: StockData singleton with DB access.
        timeframe: 'daily' or '5m' — controls default horizons.
        start_date: Earliest date for price lookups.
        end_date: Latest date for price lookups.
        custom_horizons: Override default horizon list.
        n_control: Number of control-group samples.
        stratify: Accepted for API compatibility; stratification is omitted
            in the streaming engine (Phase 2 work).

    Returns:
        List of ForwardReturnResult, one per unique source_detail value.
        If events has no source_detail column, returns a single result for all.
    """
    if events.empty:
        logger.warning("forward_returns: no events")
        return []

    n_tickers = events['ticker'].nunique()
    n_events = len(events)
    horizons = custom_horizons or (
        _DAILY_HORIZONS if timeframe == 'daily' else _INTRADAY_HORIZONS
    )
    horizon_labels = _INTRADAY_HORIZON_LABELS if timeframe == '5m' else {}
    time_col = 'date' if timeframe == 'daily' else 'datetime'

    # Build (ticker, date_str) exclusion set for control sampling
    event_pairs: set[tuple[str, str]] = set()
    for _, row in events.iterrows():
        dt = pd.Timestamp(row['datetime'])
        event_pairs.add((row['ticker'], dt.date().isoformat()))

    # Group events by source_detail
    if 'source_detail' in events.columns:
        groups = {str(detail): grp for detail, grp in events.groupby('source_detail')}
    else:
        groups = {'all': events}

    # One SQL query for bar counts; build control offsets (no price data loaded yet)
    event_tickers = events['ticker'].unique().tolist()
    bar_counts = await fetch_bar_counts(stock_data, event_tickers, timeframe, start_date, end_date)
    control_with_offsets = build_control_group(events, bar_counts, n_samples=n_control)

    control_tickers = (
        control_with_offsets['ticker'].unique().tolist()
        if not control_with_offsets.empty else []
    )
    stream_ticker_list = sorted(set(event_tickers) | set(control_tickers))
    n_stream = len(stream_ticker_list)

    print(
        f"Computing forward returns: {n_events} events across {n_tickers} tickers "
        f"({n_stream} tickers to stream)"
    )

    # Per-group return accumulators: {source_detail: {horizon: [return_pct, ...]}}
    group_accumulators: dict[str, dict[int, list[float]]] = {
        detail: {h: [] for h in horizons} for detail in groups
    }
    control_accumulator: dict[int, list[float]] = {h: [] for h in horizons}

    n_streamed = 0
    async for ticker, price_df, pop_df in stream_tickers(
        stock_data, stream_ticker_list, timeframe, start_date, end_date
    ):
        gc.collect()
        if price_df.empty or time_col not in price_df.columns or 'close' not in price_df.columns:
            continue

        price_df = price_df.sort_values(time_col)
        idx = pd.to_datetime(price_df[time_col].astype(str))
        close_series = pd.Series(price_df['close'].values, index=idx).sort_index()

        # Accumulate event returns for each source_detail group
        for detail, grp in groups.items():
            ticker_events = grp[grp['ticker'] == ticker]
            if not ticker_events.empty:
                _accumulate_horizons(
                    ticker_events, close_series, group_accumulators[detail], horizons
                )

        # Accumulate control returns — offset indexes directly into close_series
        if not control_with_offsets.empty:
            ticker_controls = control_with_offsets[control_with_offsets['ticker'] == ticker]
            for _, ctrl_row in ticker_controls.iterrows():
                offset = int(ctrl_row['_bar_offset'])
                if offset >= len(close_series):
                    continue
                ts = pd.Timestamp(close_series.index[offset])
                if (ticker, ts.date().isoformat()) in event_pairs:
                    continue
                entry_price = float(close_series.iloc[offset])
                if entry_price <= 0 or math.isnan(entry_price):
                    continue
                for h in horizons:
                    fwd_loc = offset + h
                    if fwd_loc >= len(close_series):
                        continue
                    fwd_price = float(close_series.iloc[fwd_loc])
                    if fwd_price <= 0 or math.isnan(fwd_price):
                        continue
                    control_accumulator[h].append((fwd_price / entry_price - 1) * 100.0)

        n_streamed += 1
        if n_streamed % 50 == 0:
            from rocketstocks.eda._memlog import log_memory
            log_memory(f"forward-returns after {n_streamed} tickers")

    control_horizons = _horizons_from_accumulators(control_accumulator, horizons, horizon_labels)

    n_groups = len(groups)
    results: list[ForwardReturnResult] = []
    for i, (detail, grp) in enumerate(groups.items(), 1):
        print(f"  [{i}/{n_groups}] {detail} ({len(grp)} events, {grp['ticker'].nunique()} tickers)")
        main_horizons = _horizons_from_accumulators(
            group_accumulators[detail], horizons, horizon_labels
        )
        results.append(ForwardReturnResult(
            source_detail=detail,
            n_events_total=len(grp),
            n_tickers=grp['ticker'].nunique(),
            horizons=main_horizons,
            control=control_horizons,
            strata={},
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

def _accumulate_horizons(
    ticker_events: pd.DataFrame,
    close_series: pd.Series,
    accumulators: dict[int, list[float]],
    horizons: list[int],
) -> None:
    """Accumulate forward returns for a single ticker into per-horizon lists.

    Mutates *accumulators* in-place.  Each entry appended is a percentage
    return from event bar to the bar at ``horizon`` bars later.
    """
    for _, row in ticker_events.iterrows():
        dt = pd.Timestamp(row['datetime'])
        loc = close_series.index.searchsorted(dt)
        if loc >= len(close_series):
            continue
        entry_price = float(close_series.iloc[loc])
        if entry_price <= 0 or math.isnan(entry_price):
            continue
        for h in horizons:
            fwd_loc = loc + h
            if fwd_loc >= len(close_series):
                continue
            fwd_price = float(close_series.iloc[fwd_loc])
            if fwd_price <= 0 or math.isnan(fwd_price):
                continue
            accumulators[h].append((fwd_price / entry_price - 1) * 100.0)


def _horizons_from_accumulators(
    accumulators: dict[int, list[float]],
    horizons: list[int],
    horizon_labels: dict[int, str],
) -> list[HorizonResult]:
    """Convert per-horizon return lists to HorizonResult statistics."""
    results = []
    for h in sorted(horizons):
        returns = accumulators[h]
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


def _compute_horizons(
    events: pd.DataFrame,
    sorted_closes: dict[str, pd.Series],
    horizons: list[int],
    horizon_labels: dict[int, str],
) -> list[HorizonResult]:
    """Compute forward return statistics at each horizon for given events.

    Pure helper — unchanged from original implementation.  Still used by
    regime_analysis and tests that pass a pre-built close_dict.
    """
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
