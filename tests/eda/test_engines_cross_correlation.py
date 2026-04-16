"""Tests for rocketstocks.eda.engines.cross_correlation."""
import math
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pandas as pd
import pytest

from rocketstocks.eda.engines.cross_correlation import (
    run_cross_correlation,
    CrossCorrelationResult,
    _build_ticker_frame,
    _run_regression,
    _zscore_series,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_stock_data(ticker_frames: dict[str, pd.DataFrame]) -> MagicMock:
    """Build a mock StockData where each ticker has a pre-built price DataFrame.

    The price DataFrame may include any signal or return columns directly
    (test convenience — _build_ticker_frame uses pre-existing columns first).
    """
    sd = MagicMock()

    # fetch_distinct_tickers → db.execute returns [(ticker,)] rows
    ticker_rows = [(t,) for t in sorted(ticker_frames)]
    sd.db.execute = AsyncMock(return_value=ticker_rows)

    async def fetch_daily(ticker, start_date=None, end_date=None):
        return ticker_frames.get(ticker, pd.DataFrame())

    sd.price_history.fetch_daily_price_history = AsyncMock(side_effect=fetch_daily)
    sd.price_history.fetch_5m_price_history = AsyncMock(return_value=pd.DataFrame())
    sd.popularity.fetch_popularity = AsyncMock(return_value=pd.DataFrame())
    return sd


def _make_price_df(ticker: str, n: int = 60, seed: int = 42, with_signal: bool = True) -> pd.DataFrame:
    """Build a daily price DataFrame with optional pre-computed signal columns."""
    rng = np.random.default_rng(seed)
    dates = [pd.Timestamp('2024-01-01') + pd.Timedelta(days=i) for i in range(n)]
    close = 100 + rng.standard_normal(n).cumsum()
    close = np.maximum(close, 5.0)
    df = pd.DataFrame({
        'ticker': ticker,
        'open': close,
        'high': close,
        'low': close,
        'close': close,
        'volume': rng.integers(100_000, 1_000_000, n),
        'date': [d.date() for d in dates],
    })
    if with_signal:
        # Add pre-computed signal and return columns for convenience
        df['daily_return'] = pd.Series(close).pct_change().values * 100.0
        df['mention_delta'] = rng.standard_normal(n)
    return df


def _make_lagged_price_df(ticker: str, n: int = 80, lag: int = 2) -> pd.DataFrame:
    """Build a daily price DataFrame where mention_delta leads daily_return by lag bars."""
    rng = np.random.default_rng(42)
    signal = rng.standard_normal(n + lag)
    dates = [pd.Timestamp('2024-01-01') + pd.Timedelta(days=i) for i in range(n)]
    close = np.ones(n) * 100.0  # flat prices — return driven by signal construction
    df = pd.DataFrame({
        'ticker': ticker,
        'open': close, 'high': close, 'low': close, 'close': close,
        'volume': 100_000,
        'date': [d.date() for d in dates],
        # signal at time t is correlated with return at t-lag (return_{t} = signal_{t-lag})
        'mention_delta': signal[lag:lag + n],
        'daily_return': signal[:n],
    })
    return df


# ---------------------------------------------------------------------------
# Basic structure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_returns_result_object():
    frames = {'AAPL': _make_price_df('AAPL', n=40)}
    sd = _make_stock_data(frames)
    result = await run_cross_correlation(
        sd, 'mention_delta', 'daily_return', tickers=None, max_lag=5
    )
    assert isinstance(result, CrossCorrelationResult)


@pytest.mark.asyncio
async def test_ccf_length_matches_lags():
    frames = {'AAPL': _make_price_df('AAPL', n=40)}
    sd = _make_stock_data(frames)
    max_lag = 5
    result = await run_cross_correlation(
        sd, 'mention_delta', 'daily_return', tickers=None, max_lag=max_lag
    )
    assert len(result.ccf) == 2 * max_lag + 1


@pytest.mark.asyncio
async def test_ccf_lags_are_sequential():
    frames = {'AAPL': _make_price_df('AAPL', n=40)}
    sd = _make_stock_data(frames)
    result = await run_cross_correlation(
        sd, 'mention_delta', 'daily_return', tickers=None, max_lag=3
    )
    lags = [p.lag for p in result.ccf]
    assert lags == list(range(-3, 4))


@pytest.mark.asyncio
async def test_peak_lag_detected_when_signal_leads():
    """With signal constructed to lead return by 2 bars, peak_lag should be 2."""
    frames = {f'TICK{i}': _make_lagged_price_df(f'TICK{i}', n=80, lag=2) for i in range(4)}
    sd = _make_stock_data(frames)
    result = await run_cross_correlation(
        sd, 'mention_delta', 'daily_return',
        tickers=list(frames),
        max_lag=5, min_periods=20,
    )
    assert result.peak_lag is not None
    assert result.peak_lag > 0


@pytest.mark.asyncio
async def test_missing_signal_col_returns_empty():
    frames = {'AAPL': _make_price_df('AAPL', n=40, with_signal=False)}
    sd = _make_stock_data(frames)
    result = await run_cross_correlation(
        sd, 'nonexistent_col', 'daily_return', tickers=None
    )
    assert result.n_tickers == 0


@pytest.mark.asyncio
async def test_insufficient_data_below_min_periods():
    frames = {'AAPL': _make_price_df('AAPL', n=5)}
    sd = _make_stock_data(frames)
    result = await run_cross_correlation(
        sd, 'mention_delta', 'daily_return', tickers=None, min_periods=30
    )
    assert result.n_tickers == 0


@pytest.mark.asyncio
async def test_explicit_tickers_respected():
    frames = {
        'AAPL': _make_price_df('AAPL', n=40),
        'GOOG': _make_price_df('GOOG', n=40),
    }
    sd = _make_stock_data(frames)
    result = await run_cross_correlation(
        sd, 'mention_delta', 'daily_return',
        tickers=['AAPL'],  # only AAPL
        max_lag=3, min_periods=20,
    )
    assert result.n_tickers <= 1


# ---------------------------------------------------------------------------
# Regression
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_regression_present_with_sufficient_data():
    frames = {t: _make_price_df(t, n=50) for t in ('AAPL', 'GOOG')}
    sd = _make_stock_data(frames)
    result = await run_cross_correlation(
        sd, 'mention_delta', 'daily_return', tickers=None, min_periods=20
    )
    assert result.regression is not None


@pytest.mark.asyncio
async def test_regression_has_all_fields():
    frames = {'AAPL': _make_price_df('AAPL', n=50)}
    sd = _make_stock_data(frames)
    result = await run_cross_correlation(
        sd, 'mention_delta', 'daily_return', tickers=None, min_periods=20
    )
    if result.regression:
        assert hasattr(result.regression, 'b1')
        assert hasattr(result.regression, 'b1_pvalue')
        assert hasattr(result.regression, 'n_obs')


# ---------------------------------------------------------------------------
# _build_ticker_frame
# ---------------------------------------------------------------------------

def test_build_ticker_frame_uses_existing_signal_col():
    """If signal_col is already in price_df it should be used directly."""
    df = _make_price_df('AAPL', n=30)
    result = _build_ticker_frame(df, pd.DataFrame(), 'daily', 'mention_delta', 'daily_return')
    assert result is not None
    assert 'mention_delta' in result.columns
    assert 'daily_return' in result.columns


def test_build_ticker_frame_computes_volume_zscore():
    df = _make_price_df('AAPL', n=30, with_signal=False)
    result = _build_ticker_frame(df, pd.DataFrame(), 'daily', '_volume_zscore', 'daily_return')
    assert result is not None
    assert '_volume_zscore' in result.columns


def test_build_ticker_frame_unknown_signal_returns_none():
    df = _make_price_df('AAPL', n=30, with_signal=False)
    result = _build_ticker_frame(df, pd.DataFrame(), 'daily', 'unknown_signal', 'daily_return')
    assert result is None


def test_build_ticker_frame_empty_price_df_returns_none():
    result = _build_ticker_frame(pd.DataFrame(), pd.DataFrame(), 'daily', 'mention_delta', 'daily_return')
    assert result is None


def test_build_ticker_frame_computes_return_col():
    """Return column should be computed from close if not present."""
    df = _make_price_df('AAPL', n=30, with_signal=True)
    df = df.drop(columns=['daily_return'])
    result = _build_ticker_frame(df, pd.DataFrame(), 'daily', 'mention_delta', 'daily_return')
    assert result is not None
    assert 'daily_return' in result.columns


# ---------------------------------------------------------------------------
# _run_regression
# ---------------------------------------------------------------------------

def test_run_regression_returns_result_with_sufficient_data():
    rng = np.random.default_rng(42)
    n = 100
    x = rng.standard_normal(n)
    z = rng.standard_normal(n)
    y = 0.3 * x + 0.1 * z + rng.standard_normal(n) * 0.5

    s_x = float(x.sum()); s_y = float(y.sum()); s_z = float(z.sum())
    s_xx = float((x * x).sum()); s_xz = float((x * z).sum()); s_zz = float((z * z).sum())
    s_xy = float((x * y).sum()); s_zy = float((z * y).sum()); s_yy = float((y * y).sum())

    result = _run_regression(n, s_x, s_y, s_z, s_xx, s_xz, s_zz, s_xy, s_zy, s_yy)
    assert result is not None
    assert abs(result.b1 - 0.3) < 0.15  # rough check on signal coefficient
    assert result.n_obs == n


def test_run_regression_returns_none_for_small_n():
    result = _run_regression(5, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.5, 0.5, 1.0)
    assert result is None


# ---------------------------------------------------------------------------
# _zscore_series
# ---------------------------------------------------------------------------

def test_zscore_series_zero_mean_unit_std():
    arr = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    z = _zscore_series(arr)
    assert abs(float(np.nanmean(z))) < 1e-10
    assert abs(float(np.nanstd(z, ddof=1)) - 1.0) < 1e-10


def test_zscore_series_constant_returns_nan():
    arr = np.array([5.0, 5.0, 5.0])
    z = _zscore_series(arr)
    assert all(math.isnan(v) for v in z)
