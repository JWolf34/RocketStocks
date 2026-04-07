"""Lead-lag cross-correlation engine.

Answers the fundamental question: does the signal lead price, or does price
lead the signal?

For each ticker with sufficient data, computes the cross-correlation function
(CCF) between a standardized signal series and future price returns at lags
from -max_lag to +max_lag.  Positive lag means signal leads price.

Also runs a simple panel regression to test whether the signal has
cross-sectional predictive power for next-bar returns.
"""
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


def run_cross_correlation(
    panel: pd.DataFrame,
    signal_col: str,
    return_col: str,
    timeframe: str = 'daily',
    max_lag: int = 10,
    min_periods: int = 30,
) -> CrossCorrelationResult:
    """Compute cross-correlation between signal and price returns.

    Args:
        panel: Panel DataFrame with at least columns [ticker, signal_col,
            return_col] plus either 'date' (daily) or 'datetime' (5m).
        signal_col: Column name for the signal values (will be z-scored
            per ticker before computing CCF).
        return_col: Column name for price returns.
        timeframe: 'daily' or '5m' — used only for labelling.
        max_lag: Maximum lag in bars (both directions).
        min_periods: Minimum non-NaN observations per ticker.

    Returns:
        CrossCorrelationResult with per-lag statistics and regression.
    """
    if panel.empty or signal_col not in panel.columns or return_col not in panel.columns:
        logger.warning(f"cross_correlation: missing required columns ({signal_col}, {return_col})")
        return _empty_result(signal_col, max_lag)

    time_col = 'date' if timeframe == 'daily' else 'datetime'
    lags = list(range(-max_lag, max_lag + 1))

    all_tickers = panel['ticker'].unique()
    n_total = len(all_tickers)
    print(f"Running cross-correlation on {n_total} tickers (min_periods={min_periods})...")

    # Per-ticker z-scored signal and return series
    ticker_corrs: dict[int, list[float]] = {lag: [] for lag in lags}
    regression_rows: list[dict] = []
    n_analyzed = 0
    n_skipped = 0

    for ticker, grp in panel.groupby('ticker'):
        grp = grp.sort_values(time_col).copy()
        sig = grp[signal_col].values.astype(float)
        ret = grp[return_col].values.astype(float)

        # Z-score the signal per ticker (NaN-safe)
        valid = ~np.isnan(sig)
        if valid.sum() < min_periods:
            n_skipped += 1
            continue

        n_analyzed += 1
        if n_analyzed == 1 or n_analyzed % 10 == 0 or n_analyzed == n_total:
            print(f"  [{n_analyzed}/{n_total}] analyzed")

        sig_z = _zscore_series(sig)

        # CCF at each lag
        n = len(sig_z)
        for lag in lags:
            if lag >= 0:
                # signal at t, return at t+lag
                s = sig_z[:n - lag] if lag > 0 else sig_z
                r = ret[lag:] if lag > 0 else ret
            else:
                # signal at t, return at t+lag (lag < 0 means price leads)
                abs_lag = abs(lag)
                s = sig_z[abs_lag:]
                r = ret[:n - abs_lag]

            valid_mask = ~(np.isnan(s) | np.isnan(r))
            if valid_mask.sum() < 5:
                continue
            corr = float(np.corrcoef(s[valid_mask], r[valid_mask])[0, 1])
            if not math.isnan(corr):
                ticker_corrs[lag].append(corr)

        # Collect regression observations: (signal_t, return_{t+1}, return_t)
        for i in range(len(sig_z) - 1):
            if not math.isnan(sig_z[i]) and not math.isnan(ret[i + 1]):
                regression_rows.append({
                    'signal': sig_z[i],
                    'next_return': ret[i + 1],
                    'prev_return': ret[i] if not math.isnan(ret[i]) else 0.0,
                })

    print(f"  {n_analyzed} tickers analyzed, {n_skipped} skipped (fewer than {min_periods} non-NaN signal values)")
    if n_analyzed == 0:
        print(f"  Tip: lower --min-periods or use --signal-col mention_ratio for sparser data")

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
        ccf_points.append(CCFPoint(
            lag=lag,
            mean_corr=mean_c,
            ci_low=ci_low,
            ci_high=ci_high,
        ))

    # Peak lag (highest positive mean_corr at positive lags)
    positive_lags = [p for p in ccf_points if p.lag > 0 and not math.isnan(p.mean_corr)]
    peak_lag = None
    peak_corr = None
    if positive_lags:
        best = max(positive_lags, key=lambda p: p.mean_corr)
        peak_lag = best.lag
        peak_corr = best.mean_corr

    # Panel regression
    regression = None
    if regression_rows:
        df_reg = pd.DataFrame(regression_rows).dropna()
        if len(df_reg) >= 10:
            regression = _run_regression(df_reg)

    n_tickers = len([t for t in panel['ticker'].unique()
                     if panel[panel['ticker'] == t][signal_col].notna().sum() >= min_periods])

    return CrossCorrelationResult(
        signal_name=signal_col,
        n_tickers=n_tickers,
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


def _run_regression(df: pd.DataFrame) -> RegressionResult:
    """OLS regression of next_return on signal + prev_return."""
    X = np.column_stack([
        np.ones(len(df)),
        df['signal'].values,
        df['prev_return'].values,
    ])
    y = df['next_return'].values

    try:
        coeffs, residuals, rank, sv = np.linalg.lstsq(X, y, rcond=None)
        b0, b1, b2 = coeffs

        # Standard errors via OLS formula
        y_hat = X @ coeffs
        e = y - y_hat
        n, k = X.shape
        sigma2 = float(np.dot(e, e) / (n - k))
        xtx_inv = np.linalg.inv(X.T @ X)
        se = np.sqrt(np.diag(sigma2 * xtx_inv))
        b1_se = float(se[1])
        b1_t = float(b1 / b1_se) if b1_se > 0 else float('nan')
        b1_p = float(2 * scipy_stats.t.sf(abs(b1_t), df=n - k))

        ss_res = float(np.dot(e, e))
        ss_tot = float(np.dot(y - y.mean(), y - y.mean()))
        r2 = 1 - ss_res / ss_tot if ss_tot > 0 else float('nan')

        return RegressionResult(
            b0=float(b0), b1=float(b1), b2=float(b2),
            b1_stderr=b1_se, b1_tstat=b1_t, b1_pvalue=b1_p,
            n_obs=n, r_squared=float(r2),
        )
    except np.linalg.LinAlgError:
        return None


def _empty_result(signal_name: str, max_lag: int) -> CrossCorrelationResult:
    lags = list(range(-max_lag, max_lag + 1))
    return CrossCorrelationResult(
        signal_name=signal_name,
        n_tickers=0,
        min_periods=0,
        max_lag=max_lag,
        ccf=[CCFPoint(l, float('nan'), float('nan'), float('nan')) for l in lags],
        peak_lag=None,
        peak_corr=None,
        regression=None,
    )
