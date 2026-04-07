"""Regime-conditional analysis engine.

Partitions events by market regime (bull/bear/correction/recovery) and
computes forward returns within each regime.  Also runs a breadth analysis
to test whether high numbers of simultaneous events predict negative market
returns (contrarian indicator).

Reuses:
  backtest/regime.py:classify_regimes() — SPY-based regime classification
  engines/forward_returns.py:_compute_horizons() — forward return computation
"""
import datetime
import logging
import math
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

from rocketstocks.backtest.regime import classify_regimes, MarketRegime
from rocketstocks.backtest.data_prep import prep_daily
from rocketstocks.eda.engines.forward_returns import (
    _compute_horizons, HorizonResult,
    _DAILY_HORIZONS, _INTRADAY_HORIZONS, _INTRADAY_HORIZON_LABELS,
)
from rocketstocks.eda.formatting import (
    fmt_pct, fmt_float, fmt_pvalue, significant_marker,
    print_table, print_separator,
)

logger = logging.getLogger(__name__)


@dataclass
class RegimeSlice:
    """Forward return statistics for events within one market regime."""
    regime: str
    n_events: int
    horizons: list[HorizonResult]


@dataclass
class BreadthPoint:
    """Correlation between event breadth and SPY forward return on one date."""
    date: datetime.date
    n_events: int
    spy_fwd_return: float     # SPY return over forward_days


@dataclass
class RegimeAnalysisResult:
    """Complete regime-conditional analysis result."""
    source_detail: str
    timeframe: str
    regime_slices: list[RegimeSlice]
    breadth_corr: float | None       # correlation: daily event count vs SPY fwd return
    breadth_pvalue: float | None
    breadth_n: int
    breadth_horizon_days: int
    n_events_total: int
    n_tickers: int
    regime_coverage: dict[str, int]  # regime → total days classified as that regime


def run_regime_analysis(
    events: pd.DataFrame,
    close_dict: dict[str, pd.Series],
    spy_df: pd.DataFrame,
    timeframe: str = 'daily',
    custom_horizons: list[int] | None = None,
    breadth_horizon_days: int = 5,
) -> list[RegimeAnalysisResult]:
    """Run regime-conditional forward return analysis.

    Args:
        events: Standard events DataFrame.
        close_dict: Per-ticker close Series for forward return lookups.
        spy_df: SPY daily OHLCV in DB format (lowercase columns, 'date' column).
        timeframe: 'daily' or '5m'.
        custom_horizons: Override default horizon list.
        breadth_horizon_days: Horizon (in trading days) for breadth analysis.

    Returns:
        List of RegimeAnalysisResult, one per unique source_detail.
    """
    if events.empty or not close_dict:
        logger.warning("regime_analysis: no events or price data")
        return []

    horizons = custom_horizons or (
        _DAILY_HORIZONS if timeframe == 'daily' else _INTRADAY_HORIZONS
    )
    horizon_labels = _INTRADAY_HORIZON_LABELS if timeframe == '5m' else {}

    # Build regime map from SPY data
    regime_map: dict[datetime.date, MarketRegime] = {}
    spy_close_series: pd.Series | None = None

    if not spy_df.empty:
        spy_bt = prep_daily(spy_df)
        if not spy_bt.empty:
            regime_map = classify_regimes(spy_bt)
            spy_close_series = spy_bt['Close'].sort_index()
    else:
        logger.warning("SPY data unavailable — regime labels will be 'unknown'")

    regime_coverage = {r.value: 0 for r in MarketRegime}
    for regime in regime_map.values():
        regime_coverage[regime.value] += 1

    sorted_closes: dict[str, pd.Series] = {
        t: s.sort_index() for t, s in close_dict.items()
    }

    # Tag each event with its regime
    events = events.copy()
    events['datetime'] = pd.to_datetime(events['datetime'])
    events['_regime'] = events['datetime'].apply(
        lambda dt: _lookup_regime(dt, regime_map)
    )

    if 'source_detail' in events.columns:
        groups = {detail: grp for detail, grp in events.groupby('source_detail')}
    else:
        groups = {'all': events}

    results: list[RegimeAnalysisResult] = []
    for detail, grp in groups.items():
        regime_slices: list[RegimeSlice] = []

        for regime in MarketRegime:
            subset = grp[grp['_regime'] == regime.value]
            if subset.empty:
                regime_slices.append(RegimeSlice(
                    regime=regime.value, n_events=0, horizons=[],
                ))
                continue

            horizons_result = _compute_horizons(
                subset, sorted_closes, horizons, horizon_labels
            )
            regime_slices.append(RegimeSlice(
                regime=regime.value,
                n_events=len(subset),
                horizons=horizons_result,
            ))

        # Breadth analysis: daily event count vs SPY forward return
        breadth_corr, breadth_pvalue, breadth_n = _breadth_analysis(
            grp, spy_close_series, breadth_horizon_days
        )

        results.append(RegimeAnalysisResult(
            source_detail=detail,
            timeframe=timeframe,
            regime_slices=regime_slices,
            breadth_corr=breadth_corr,
            breadth_pvalue=breadth_pvalue,
            breadth_n=breadth_n,
            breadth_horizon_days=breadth_horizon_days,
            n_events_total=len(grp),
            n_tickers=grp['ticker'].nunique(),
            regime_coverage=regime_coverage,
        ))

    return results


def print_results(
    results: list[RegimeAnalysisResult],
    primary_horizon_idx: int = 2,
) -> None:
    """Print regime analysis results as formatted terminal tables.

    Args:
        results: List of RegimeAnalysisResult from run_regime_analysis().
        primary_horizon_idx: Which horizon index to feature in the summary table
            (default 2 = 3rd horizon, e.g. 3d for daily or half-day for 5m).
    """
    if not results:
        print("No regime analysis results to display.")
        return

    for r in results:
        print_separator()
        print(f"\nRegime Analysis — {r.source_detail}")
        print(f"  Events: {r.n_events_total}  |  Tickers: {r.n_tickers}")

        # Regime coverage summary
        cov_parts = [f"{k}: {v}d" for k, v in r.regime_coverage.items() if v > 0]
        print(f"  Market regime coverage: {', '.join(cov_parts)}")

        # Summary table: pick one horizon per regime
        slices_with_data = [s for s in r.regime_slices if s.n_events > 0 and s.horizons]
        if slices_with_data:
            idx = min(primary_horizon_idx, len(slices_with_data[0].horizons) - 1)
            horizon_label = slices_with_data[0].horizons[idx].horizon_label if slices_with_data[0].horizons else '?'
            print(f"\n  Regime summary (horizon = {horizon_label}):")
            headers = ['Regime', 'N Events', 'Mean %', 'Win %', 't-stat', 'p-value', 'Sig']
            rows = []
            for s in r.regime_slices:
                if s.n_events == 0:
                    rows.append([s.regime, '0', 'n/a', 'n/a', 'n/a', 'n/a', ''])
                    continue
                if not s.horizons:
                    rows.append([s.regime, str(s.n_events), 'n/a', 'n/a', 'n/a', 'n/a', ''])
                    continue
                h_idx = min(idx, len(s.horizons) - 1)
                h = s.horizons[h_idx]
                rows.append([
                    s.regime, str(s.n_events),
                    fmt_pct(h.mean_return),
                    fmt_pct(h.win_rate),
                    fmt_float(h.t_stat),
                    fmt_pvalue(h.p_value),
                    significant_marker(h.p_value),
                ])
            print_table(headers, rows, col_widths=[12, 10, 9, 8, 8, 8, 4])

        # Full horizon tables per regime
        for s in r.regime_slices:
            if s.n_events == 0 or not s.horizons:
                continue
            headers_h = ['Horizon', 'N', 'Mean %', 'Median %', 'Win %', 't-stat', 'p-value', 'Sig']
            rows_h = []
            for h in s.horizons:
                rows_h.append([
                    h.horizon_label, str(h.n_events),
                    fmt_pct(h.mean_return), fmt_pct(h.median_return),
                    fmt_pct(h.win_rate), fmt_float(h.t_stat),
                    fmt_pvalue(h.p_value), significant_marker(h.p_value),
                ])
            print_table(headers_h, rows_h,
                        title=f"\n  {s.regime.upper()} regime (n={s.n_events})",
                        col_widths=[10, 6, 9, 10, 8, 8, 8, 4])

        # Breadth analysis
        print(f"\n  Breadth Analysis (event count per day vs SPY {r.breadth_horizon_days}d return)")
        if r.breadth_n >= 5 and r.breadth_corr is not None:
            direction = 'contrarian' if r.breadth_corr < 0 else 'momentum'
            sig = significant_marker(r.breadth_pvalue)
            print(f"  n={r.breadth_n}, corr={fmt_float(r.breadth_corr, 3)}, "
                  f"p={fmt_pvalue(r.breadth_pvalue)} {sig} ({direction})")
            if r.breadth_pvalue is not None and r.breadth_pvalue < 0.05:
                if r.breadth_corr < 0:
                    print("  → High daily event breadth weakly predicts negative SPY returns (retail crowd top)")
                else:
                    print("  → High daily event breadth weakly predicts positive SPY returns (momentum)")
        else:
            print("  Insufficient data for breadth analysis (need >= 5 dated event days with SPY data)")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _lookup_regime(dt: pd.Timestamp, regime_map: dict) -> str:
    """Return regime string for a given datetime."""
    if not regime_map:
        return MarketRegime.UNKNOWN.value
    date = dt.date() if hasattr(dt, 'date') else dt
    regime = regime_map.get(date, MarketRegime.UNKNOWN)
    return regime.value


def _breadth_analysis(
    events: pd.DataFrame,
    spy_close: pd.Series | None,
    forward_days: int,
) -> tuple[float | None, float | None, int]:
    """Correlate daily event count with SPY forward return.

    Returns (correlation, p_value, n_days).
    """
    if spy_close is None or spy_close.empty:
        return None, None, 0

    events = events.copy()
    events['_date'] = pd.to_datetime(events['datetime']).dt.date
    daily_counts = events.groupby('_date').size()

    spy_close = spy_close.sort_index()
    spy_dates = [pd.Timestamp(d).date() for d in spy_close.index]
    spy_close_by_date = dict(zip(spy_dates, spy_close.values))

    rows: list[tuple[int, float]] = []
    for date, count in daily_counts.items():
        entry_ts = pd.Timestamp(date)
        loc = spy_close.index.searchsorted(entry_ts)
        fwd_loc = loc + forward_days
        if fwd_loc >= len(spy_close) or loc >= len(spy_close):
            continue
        entry_p = float(spy_close.iloc[loc])
        fwd_p = float(spy_close.iloc[fwd_loc])
        if entry_p <= 0 or math.isnan(entry_p):
            continue
        fwd_ret = (fwd_p / entry_p - 1) * 100.0
        rows.append((int(count), fwd_ret))

    n = len(rows)
    if n < 5:
        return None, None, n

    counts = np.array([r[0] for r in rows], dtype=float)
    returns = np.array([r[1] for r in rows], dtype=float)
    if np.std(counts) == 0 or np.std(returns) == 0:
        return None, None, n
    try:
        corr, pvalue = scipy_stats.pearsonr(counts, returns)
        return float(corr), float(pvalue), n
    except Exception:
        return None, None, n
