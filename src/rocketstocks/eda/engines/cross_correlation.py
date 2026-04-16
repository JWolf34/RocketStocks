"""Lead-lag cross-correlation engine.

Answers the fundamental question: does the signal lead price, or does price
lead the signal?

For each ticker with sufficient data, computes the cross-correlation function
(CCF) between a standardized signal series and future price returns at lags
from -max_lag to +max_lag.  Positive lag means signal leads price.

Also runs a simple panel regression to test whether the signal has
cross-sectional predictive power for next-bar returns.

Memory model: streams price + popularity data one ticker at a time.
The regression accumulator uses running sums (10 scalars) rather than a
per-row list, so peak RSS stays bounded regardless of universe size.
"""
import gc
import logging
import math
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

from rocketstocks.eda.formatting import (
    fmt_float, fmt_pvalue, significant_marker,
    print_table, print_separator,
)
from rocketstocks.eda.streaming import fetch_distinct_tickers, stream_tickers

logger = logging.getLogger(__name__)


@dataclass
class CCFPoint:
    """Cross-correlation at a single lag."""
    lag: int                  # positive = signal leads price
    mean_corr: float          # average correlation across tickers
    ci_low: float             # 95% CI lower bound (bootstrap or SE)
    ci_high: float            # 95% CI upper bound


@dataclass
class RegressionResult:
    """Panel regression: return_{t+1} = b0 + b1*signal_t + b2*return_t."""
    b0: float
    b1: float                 # signal coefficient — positive = signal predicts next return
    b2: float                 # lagged return coefficient
    b1_stderr: float
    b1_tstat: float
    b1_pvalue: float
    n_obs: int
    r_squared: float


@dataclass
class CrossCorrelationResult:
    """Complete cross-correlation analysis result."""
    signal_name: str          # e.g. 'mention_delta' or 'volume_zscore'
    n_tickers: int
    min_periods: int
    max_lag: int
    ccf: list[CCFPoint]       # sorted by lag ascending
    peak_lag: int | None      # lag with highest positive mean_corr
    peak_corr: float | None
    regression: RegressionResult | None


async def run_cross_correlation(
    stock_data,
    signal_col: str,
    return_col: str,
    tickers: list[str] | None,
    timeframe: str = 'daily',
    start_date=None,
    end_date=None,
    max_lag: int = 10,
    min_periods: int = 30,
) -> CrossCorrelationResult:
    """Compute cross-correlation between signal and price returns.

    Streams price + popularity data one ticker at a time so peak RSS stays
    bounded regardless of universe × history size.

    Args:
        stock_data: StockData singleton with DB access.
        signal_col: Signal column to analyse.  Computed on-the-fly from
            the streamed data: 'mention_ratio', 'mention_delta', 'rank_change'
            come from popularity; '_volume_zscore' from price; any column
            already present in the price DataFrame is used directly (useful
            for tests with pre-built data).
        return_col: Return column name — 'daily_return' or 'bar_return'.
            Computed as pct_change of close if not already present.
        tickers: Explicit ticker list.  If None the engine queries all
            distinct tickers in the popularity table for the date window.
        timeframe: 'daily' or '5m'.
        start_date: Earliest date for data queries.
        end_date: Latest date for data queries.
        max_lag: Maximum lag in bars (both directions).
        min_periods: Minimum non-NaN observations per ticker.

    Returns:
        CrossCorrelationResult with per-lag statistics and regression.
    """
    lags = list(range(-max_lag, max_lag + 1))

    if tickers is None:
        tickers = await fetch_distinct_tickers(stock_data, start_date, end_date)

    if not tickers:
        logger.warning("cross_correlation: no tickers available")
        return _empty_result(signal_col, max_lag)

    n_total = len(tickers)
    print(
        f"Running cross-correlation on {n_total} tickers "
        f"(signal={signal_col}, min_periods={min_periods})..."
    )

    # Per-ticker CCF accumulators
    ticker_corrs: dict[int, list[float]] = {lag: [] for lag in lags}

    # Running-sum regression accumulators (model: y_{t+1} = b0 + b1*x_t + b2*y_t)
    # x = z-scored signal, y = return, z = lagged return
    n_reg = 0
    sum_x = sum_y = sum_z = 0.0
    sum_xx = sum_xz = sum_zz = 0.0
    sum_xy = sum_zy = sum_yy = 0.0

    n_analyzed = 0
    n_skipped = 0

    async for ticker, price_df, pop_df in stream_tickers(
        stock_data, tickers, timeframe, start_date, end_date
    ):
        gc.collect()

        df = _build_ticker_frame(price_df, pop_df, timeframe, signal_col, return_col)
        if df is None or df.empty:
            n_skipped += 1
            continue

        time_col = 'date' if timeframe == 'daily' else 'datetime'
        df = df.sort_values(time_col)
        sig = df[signal_col].values.astype(float)
        ret = df[return_col].values.astype(float)

        valid = ~np.isnan(sig)
        if valid.sum() < min_periods:
            n_skipped += 1
            continue

        n_analyzed += 1
        if n_analyzed == 1 or n_analyzed % 10 == 0 or n_analyzed == n_total:
            print(f"  [{n_analyzed}/{n_total}] analyzed")

        sig_z = _zscore_series(sig)
        n = len(sig_z)

        # CCF at each lag
        for lag in lags:
            if lag >= 0:
                s = sig_z[:n - lag] if lag > 0 else sig_z
                r = ret[lag:] if lag > 0 else ret
            else:
                abs_lag = abs(lag)
                s = sig_z[abs_lag:]
                r = ret[:n - abs_lag]

            valid_mask = ~(np.isnan(s) | np.isnan(r))
            if valid_mask.sum() < 5:
                continue
            corr = float(np.corrcoef(s[valid_mask], r[valid_mask])[0, 1])
            if not math.isnan(corr):
                ticker_corrs[lag].append(corr)

        # Update running regression sums
        for i in range(n - 1):
            if math.isnan(sig_z[i]) or math.isnan(ret[i + 1]):
                continue
            x_i = float(sig_z[i])
            y_i = float(ret[i + 1])
            z_i = float(ret[i]) if not math.isnan(ret[i]) else 0.0
            n_reg += 1
            sum_x += x_i;  sum_y += y_i;  sum_z += z_i
            sum_xx += x_i * x_i
            sum_xz += x_i * z_i
            sum_zz += z_i * z_i
            sum_xy += x_i * y_i
            sum_zy += z_i * y_i
            sum_yy += y_i * y_i

    print(
        f"  {n_analyzed} tickers analyzed, {n_skipped} skipped "
        f"(fewer than {min_periods} non-NaN signal values)"
    )
    if n_analyzed == 0:
        print("  Tip: lower --min-periods or use --signal-col mention_ratio for sparser data")

    # Aggregate CCF across tickers
    ccf_points: list[CCFPoint] = []
    for lag in lags:
        corrs = ticker_corrs[lag]
        if not corrs:
            ccf_points.append(CCFPoint(lag=lag, mean_corr=float('nan'),
                                       ci_low=float('nan'), ci_high=float('nan')))
            continue
        arr = np.array(corrs)
        mean_c = float(arr.mean())
        if len(arr) >= 2:
            se = float(arr.std(ddof=1) / math.sqrt(len(arr)))
            ci_low = mean_c - 1.96 * se
            ci_high = mean_c + 1.96 * se
        else:
            ci_low = ci_high = float('nan')
        ccf_points.append(CCFPoint(lag=lag, mean_corr=mean_c, ci_low=ci_low, ci_high=ci_high))

    # Peak lag (highest positive mean_corr at positive lags)
    positive_lags = [p for p in ccf_points if p.lag > 0 and not math.isnan(p.mean_corr)]
    peak_lag = None
    peak_corr = None
    if positive_lags:
        best = max(positive_lags, key=lambda p: p.mean_corr)
        peak_lag = best.lag
        peak_corr = best.mean_corr

    # Panel regression from running sums
    regression = _run_regression(
        n_reg, sum_x, sum_y, sum_z,
        sum_xx, sum_xz, sum_zz,
        sum_xy, sum_zy, sum_yy,
    )

    return CrossCorrelationResult(
        signal_name=signal_col,
        n_tickers=n_analyzed,
        min_periods=min_periods,
        max_lag=max_lag,
        ccf=ccf_points,
        peak_lag=peak_lag,
        peak_corr=peak_corr,
        regression=regression,
    )


def print_results(result: CrossCorrelationResult) -> None:
    """Print cross-correlation results as formatted terminal tables."""
    print_separator()
    print(f"\nCross-Correlation — signal: {result.signal_name}")
    print(f"  Tickers analyzed: {result.n_tickers}  |  min_periods: {result.min_periods}")

    headers = ['Lag', 'Mean Corr', '95% CI Low', '95% CI High', 'Direction']
    rows = []
    for p in result.ccf:
        if p.lag == 0:
            direction = 'contemporaneous'
        elif p.lag > 0:
            direction = 'signal leads ←'
        else:
            direction = 'price leads →'

        marker = ' ← PEAK' if p.lag == result.peak_lag else ''
        rows.append([
            str(p.lag),
            fmt_float(p.mean_corr, 4),
            fmt_float(p.ci_low, 4),
            fmt_float(p.ci_high, 4),
            direction + marker,
        ])

    print_table(headers, rows, title='\n  CCF table (lag = bars; positive = signal leads price)',
                col_widths=[6, 12, 12, 12, 26])

    if result.peak_lag is not None:
        print(f"\n  Peak positive correlation: lag={result.peak_lag}, corr={fmt_float(result.peak_corr, 4)}")
        if result.peak_corr and result.peak_corr > 0:
            print(f"  → Signal leads price by {result.peak_lag} bar(s) — potential predictive edge")
        else:
            print("  → No positive peak detected at positive lags — limited predictive edge")
    else:
        print("\n  No valid positive-lag CCF points — insufficient data or no relationship")

    if result.regression:
        r = result.regression
        print(f"\n  Panel regression:  return_{{t+1}} = {fmt_float(r.b0)} "
              f"+ {fmt_float(r.b1)}*signal_t + {fmt_float(r.b2)}*return_t")
        print(f"  signal coefficient: {fmt_float(r.b1)}  "
              f"(SE={fmt_float(r.b1_stderr)}, t={fmt_float(r.b1_tstat)}, "
              f"p={fmt_pvalue(r.b1_pvalue)}) {significant_marker(r.b1_pvalue)}")
        print(f"  R²={fmt_float(r.r_squared, 4)}  n={r.n_obs}")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_ticker_frame(
    price_df: pd.DataFrame,
    pop_df: pd.DataFrame,
    timeframe: str,
    signal_col: str,
    return_col: str,
) -> pd.DataFrame | None:
    """Build a per-ticker DataFrame with signal and return columns for CCF.

    Signal column resolution order:
    1. Already present in price_df (useful for pre-computed test data).
    2. '_volume_zscore' — rolling z-score computed from price_df volume.
    3. 'mention_ratio', 'mention_delta', 'rank_change' — merged from pop_df.

    Returns None if the requested signal cannot be computed.
    """
    if price_df.empty:
        return None

    time_col = 'date' if timeframe == 'daily' else 'datetime'
    if time_col not in price_df.columns or 'close' not in price_df.columns:
        return None

    df = price_df.copy()
    df = df.sort_values(time_col)
    df[time_col] = pd.to_datetime(df[time_col].astype(str))

    # Return column
    if return_col not in df.columns:
        df[return_col] = df['close'].pct_change() * 100.0

    # Signal column
    if signal_col in df.columns:
        pass  # already present (test convenience or pre-computed)

    elif signal_col == '_volume_zscore':
        if 'volume' not in df.columns:
            return None
        vol = df['volume'].astype(float)
        roll_mean = vol.shift(1).rolling(20, min_periods=3).mean()
        roll_std = vol.shift(1).rolling(20, min_periods=3).std()
        df['_volume_zscore'] = (vol - roll_mean) / roll_std.replace(0, float('nan'))

    elif signal_col in ('mention_ratio', 'mention_delta', 'rank_change'):
        if pop_df.empty:
            return None
        pop = pop_df.copy()
        pop['datetime'] = pd.to_datetime(pop['datetime'])
        pop['mention_ratio'] = (
            pop['mentions'] / pop['mentions_24h_ago'].replace(0, float('nan'))
        )
        pop['rank_change'] = pop['rank_24h_ago'] - pop['rank']

        if timeframe == 'daily':
            pop['_date'] = pop['datetime'].dt.normalize()
            pop_agg = (
                pop.sort_values('mention_ratio', ascending=False, na_position='last')
                .groupby('_date', sort=False)
                .first()
                .reset_index()
                .rename(columns={'_date': time_col})
            )
            pop_agg[time_col] = pd.to_datetime(pop_agg[time_col])
            merge_cols = [c for c in ('mention_ratio', 'rank_change', 'mentions')
                          if c in pop_agg.columns]
            df = df.merge(pop_agg[[time_col] + merge_cols], on=time_col, how='left')
        else:
            from rocketstocks.eda.data_loader import _merge_popularity_intraday
            df = _merge_popularity_intraday(df, pop)

        if signal_col == 'mention_delta' and 'mentions' in df.columns:
            df['mention_delta'] = df['mentions'].diff()

    else:
        return None

    if signal_col not in df.columns:
        return None

    needed = [time_col, signal_col, return_col]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        return None

    return df[needed].dropna(subset=[return_col])


def _zscore_series(arr: np.ndarray) -> np.ndarray:
    """Z-score a 1-D array, returning NaN where std is zero."""
    valid = ~np.isnan(arr)
    result = np.full_like(arr, float('nan'))
    if valid.sum() < 2:
        return result
    mu = np.nanmean(arr)
    std = np.nanstd(arr, ddof=1)
    if std == 0 or math.isnan(std):
        return result
    result[valid] = (arr[valid] - mu) / std
    return result


def _run_regression(
    n: int,
    sum_x: float,
    sum_y: float,
    sum_z: float,
    sum_xx: float,
    sum_xz: float,
    sum_zz: float,
    sum_xy: float,
    sum_zy: float,
    sum_yy: float,
) -> RegressionResult | None:
    """OLS regression from aggregated normal-equation components.

    Model: return_{t+1} = b0 + b1*signal_t + b2*return_t
    X columns: [1 (intercept), signal (x), prev_return (z)]
    y: next_return

    Uses the identity e'e = y'y − 2β'(X'y) + β'(X'X)β to compute
    residual SS without materialising the full N-row residual vector.
    """
    if n < 10:
        return None

    k = 3
    XtX = np.array([
        [float(n), sum_x,  sum_z ],
        [sum_x,    sum_xx, sum_xz],
        [sum_z,    sum_xz, sum_zz],
    ])
    XtY = np.array([sum_y, sum_xy, sum_zy])

    try:
        coeffs = np.linalg.solve(XtX, XtY)
        b0, b1, b2 = float(coeffs[0]), float(coeffs[1]), float(coeffs[2])

        # Residual SS via quadratic form — no need to store residual vector
        ss_res = float(sum_yy - 2.0 * float(np.dot(coeffs, XtY)) + float(coeffs @ XtX @ coeffs))
        sigma2 = ss_res / max(n - k, 1)

        xtx_inv = np.linalg.inv(XtX)
        se_vec = np.sqrt(np.abs(np.diag(sigma2 * xtx_inv)))
        b1_se = float(se_vec[1])
        b1_t = float(b1 / b1_se) if b1_se > 0 else float('nan')
        b1_p = float(2 * scipy_stats.t.sf(abs(b1_t), df=n - k))

        y_mean = sum_y / n
        ss_tot = float(sum_yy - n * y_mean ** 2)
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else float('nan')

        return RegressionResult(
            b0=b0, b1=b1, b2=b2,
            b1_stderr=b1_se, b1_tstat=b1_t, b1_pvalue=b1_p,
            n_obs=n, r_squared=float(r2),
        )
    except (np.linalg.LinAlgError, ZeroDivisionError, ValueError):
        return None


def _empty_result(signal_name: str, max_lag: int) -> CrossCorrelationResult:
    lags = list(range(-max_lag, max_lag + 1))
    return CrossCorrelationResult(
        signal_name=signal_name,
        n_tickers=0,
        min_periods=0,
        max_lag=max_lag,
        ccf=[CCFPoint(lag, float('nan'), float('nan'), float('nan')) for lag in lags],
        peak_lag=None,
        peak_corr=None,
        regression=None,
    )
