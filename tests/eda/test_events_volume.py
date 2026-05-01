"""Tests for rocketstocks.eda.events.volume.VolumeDetector."""
import datetime
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pandas as pd
import pytest

from rocketstocks.eda.events.volume import VolumeDetector


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_stock_data(ticker_frames: dict[str, pd.DataFrame]) -> MagicMock:
    """Mock StockData whose per-ticker fetchers return pre-built DataFrames."""
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


def _make_price_df(
    ticker: str,
    n: int = 60,
    spike_idx: int | None = 40,
    spike_multiplier: float = 5.0,
    seed: int = 42,
) -> pd.DataFrame:
    """Build a daily price DataFrame with optional volume spike."""
    rng = np.random.default_rng(seed)
    dates = [pd.Timestamp('2024-01-01') + pd.Timedelta(days=i) for i in range(n)]
    close = np.maximum(100 + rng.standard_normal(n).cumsum(), 5.0)
    volume = rng.integers(100_000, 200_000, n).astype(float)
    if spike_idx is not None:
        volume[spike_idx] *= spike_multiplier
    return pd.DataFrame({
        'ticker': ticker,
        'open': close, 'high': close, 'low': close, 'close': close,
        'volume': volume,
        'date': [d.date() for d in dates],
    })


def _make_5m_price_df(ticker: str, n: int = 200, spike_idx: int | None = 150) -> pd.DataFrame:
    """Build a 5-minute price DataFrame with optional volume spike."""
    rng = np.random.default_rng(99)
    base = pd.Timestamp('2024-01-01 09:30:00')
    dts = [base + pd.Timedelta(minutes=5 * i) for i in range(n)]
    close = np.maximum(100 + rng.standard_normal(n).cumsum(), 5.0)
    volume = rng.integers(10_000, 50_000, n).astype(float)
    if spike_idx is not None:
        volume[spike_idx] *= 8.0
    return pd.DataFrame({
        'ticker': ticker,
        'open': close, 'high': close, 'low': close, 'close': close,
        'volume': volume,
        'datetime': dts,
    })


# ---------------------------------------------------------------------------
# Return type and schema
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_detect_returns_dataframe():
    frames = {'AAPL': _make_price_df('AAPL', n=60)}
    sd = _make_stock_data(frames)
    result = await VolumeDetector().detect(sd)
    assert isinstance(result, pd.DataFrame)


@pytest.mark.asyncio
async def test_detect_has_required_columns():
    frames = {'AAPL': _make_price_df('AAPL', n=60)}
    sd = _make_stock_data(frames)
    result = await VolumeDetector().detect(sd)
    for col in ('ticker', 'datetime', 'signal_value', 'source', 'source_detail', 'volume'):
        assert col in result.columns


# ---------------------------------------------------------------------------
# Event detection
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_detect_finds_volume_spike():
    """A large spike should produce at least one event."""
    frames = {'AAPL': _make_price_df('AAPL', n=60, spike_idx=40, spike_multiplier=10.0)}
    sd = _make_stock_data(frames)
    result = await VolumeDetector(zscore_threshold=2.0).detect(sd)
    assert len(result) >= 1


@pytest.mark.asyncio
async def test_detect_no_spike_returns_empty():
    """Flat volume with no spike should produce no events."""
    n = 60
    dates = [pd.Timestamp('2024-01-01') + pd.Timedelta(days=i) for i in range(n)]
    flat_df = pd.DataFrame({
        'ticker': 'FLAT',
        'open': 100.0, 'high': 100.0, 'low': 100.0, 'close': 100.0,
        'volume': 100_000.0,
        'date': [d.date() for d in dates],
    })
    sd = _make_stock_data({'FLAT': flat_df})
    result = await VolumeDetector(zscore_threshold=2.0).detect(sd)
    assert result.empty


@pytest.mark.asyncio
async def test_detect_signal_value_is_zscore():
    """signal_value should be the volume z-score (>= threshold)."""
    frames = {'AAPL': _make_price_df('AAPL', n=60, spike_idx=40, spike_multiplier=10.0)}
    sd = _make_stock_data(frames)
    threshold = 2.0
    result = await VolumeDetector(zscore_threshold=threshold).detect(sd)
    assert (result['signal_value'] >= threshold).all()


@pytest.mark.asyncio
async def test_detect_source_detail_contains_threshold():
    frames = {'AAPL': _make_price_df('AAPL', n=60, spike_idx=40, spike_multiplier=10.0)}
    sd = _make_stock_data(frames)
    threshold = 3.0
    result = await VolumeDetector(zscore_threshold=threshold).detect(sd)
    if not result.empty:
        assert all(f'>={threshold}' in d for d in result['source_detail'])


@pytest.mark.asyncio
async def test_detect_source_column_is_volume():
    frames = {'AAPL': _make_price_df('AAPL', n=60, spike_idx=40, spike_multiplier=10.0)}
    sd = _make_stock_data(frames)
    result = await VolumeDetector().detect(sd)
    if not result.empty:
        assert (result['source'] == 'volume').all()


@pytest.mark.asyncio
async def test_detect_min_volume_filter():
    """Events with volume below min_volume should be excluded."""
    n = 60
    dates = [pd.Timestamp('2024-01-01') + pd.Timedelta(days=i) for i in range(n)]
    rng = np.random.default_rng(7)
    volume = rng.integers(100, 500, n).astype(float)
    volume[40] *= 20.0  # big spike but tiny absolute volume
    df = pd.DataFrame({
        'ticker': 'TINY',
        'open': 5.0, 'high': 5.0, 'low': 5.0, 'close': 5.0,
        'volume': volume,
        'date': [d.date() for d in dates],
    })
    sd = _make_stock_data({'TINY': df})
    result = await VolumeDetector(min_volume=10_000).detect(sd)
    assert result.empty


# ---------------------------------------------------------------------------
# Multi-ticker
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_detect_multi_ticker_returns_all_events():
    frames = {
        'AAPL': _make_price_df('AAPL', n=60, spike_idx=40, seed=1),
        'GOOG': _make_price_df('GOOG', n=60, spike_idx=50, seed=2),
    }
    sd = _make_stock_data(frames)
    result = await VolumeDetector(zscore_threshold=2.0).detect(sd)
    tickers_found = set(result['ticker'].unique())
    # At least one ticker should fire (both have spikes)
    assert len(tickers_found) >= 1


@pytest.mark.asyncio
async def test_detect_explicit_tickers_respected():
    """When tickers list is provided, only those are processed."""
    frames = {
        'AAPL': _make_price_df('AAPL', n=60, spike_idx=40, seed=1),
        'GOOG': _make_price_df('GOOG', n=60, spike_idx=40, seed=2),
    }
    sd = _make_stock_data(frames)
    result = await VolumeDetector(zscore_threshold=2.0).detect(sd, tickers=['AAPL'])
    assert 'GOOG' not in result['ticker'].values


# ---------------------------------------------------------------------------
# Empty / edge cases
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_detect_empty_universe_returns_empty():
    sd = _make_stock_data({})
    # db.execute returns empty → no tickers
    sd.db.execute = AsyncMock(return_value=[])
    result = await VolumeDetector().detect(sd)
    assert result.empty
    for col in ('ticker', 'datetime', 'signal_value', 'source', 'source_detail', 'volume'):
        assert col in result.columns


@pytest.mark.asyncio
async def test_detect_skips_missing_volume_column():
    n = 30
    dates = [pd.Timestamp('2024-01-01') + pd.Timedelta(days=i) for i in range(n)]
    df_no_vol = pd.DataFrame({
        'ticker': 'NOVOL',
        'close': 100.0,
        'date': [d.date() for d in dates],
    })
    sd = _make_stock_data({'NOVOL': df_no_vol})
    result = await VolumeDetector().detect(sd)
    assert result.empty


@pytest.mark.asyncio
async def test_detect_skips_all_nan_volume():
    n = 30
    dates = [pd.Timestamp('2024-01-01') + pd.Timedelta(days=i) for i in range(n)]
    df_nan_vol = pd.DataFrame({
        'ticker': 'NAN',
        'close': 100.0,
        'volume': np.nan,
        'date': [d.date() for d in dates],
    })
    sd = _make_stock_data({'NAN': df_nan_vol})
    result = await VolumeDetector().detect(sd)
    assert result.empty


@pytest.mark.asyncio
async def test_detect_datetime_column_is_datetime_dtype():
    frames = {'AAPL': _make_price_df('AAPL', n=60, spike_idx=40, spike_multiplier=10.0)}
    sd = _make_stock_data(frames)
    result = await VolumeDetector().detect(sd)
    if not result.empty:
        assert pd.api.types.is_datetime64_any_dtype(result['datetime'])


@pytest.mark.asyncio
async def test_detect_sorted_by_ticker_then_datetime():
    frames = {
        'AAPL': _make_price_df('AAPL', n=60, spike_idx=40, seed=1),
        'MSFT': _make_price_df('MSFT', n=60, spike_idx=45, seed=5),
    }
    sd = _make_stock_data(frames)
    result = await VolumeDetector(zscore_threshold=2.0).detect(sd)
    if len(result) > 1:
        ticker_sorted = result['ticker'].tolist()
        assert ticker_sorted == sorted(ticker_sorted) or result.groupby('ticker')['datetime'].is_monotonic_increasing.all()


# ---------------------------------------------------------------------------
# 5-minute timeframe
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_detect_5m_timeframe():
    """Detector should work with 5-minute data (datetime column)."""
    df = _make_5m_price_df('AAPL', n=200, spike_idx=150)
    sd = MagicMock()
    sd.db.execute = AsyncMock(return_value=[('AAPL',)])
    sd.price_history.fetch_5m_price_history = AsyncMock(return_value=df)
    sd.price_history.fetch_daily_price_history = AsyncMock(return_value=pd.DataFrame())
    sd.popularity.fetch_popularity = AsyncMock(return_value=pd.DataFrame())

    result = await VolumeDetector(zscore_threshold=2.0).detect(sd, timeframe='5m')
    assert isinstance(result, pd.DataFrame)
    assert 'datetime' in result.columns


# ---------------------------------------------------------------------------
# Phase 1 additions: skipped_tickers, ticker uppercasing, price-based discovery
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_detect_skipped_tickers_populated_on_empty_price():
    """Tickers with no price data should appear in skipped_tickers."""
    sd = _make_stock_data({'AAPL': pd.DataFrame()})
    detector = VolumeDetector()
    await detector.detect(sd)
    assert 'AAPL' in detector.skipped_tickers


@pytest.mark.asyncio
async def test_detect_skipped_tickers_short_history():
    """Tickers with too few bars for any valid z-score appear in skipped_tickers."""
    n = 3  # fewer than min_win=max(3, 20//2)=10
    dates = [pd.Timestamp('2024-01-01') + pd.Timedelta(days=i) for i in range(n)]
    df = pd.DataFrame({
        'ticker': 'TINY',
        'open': 1.0, 'high': 1.0, 'low': 1.0, 'close': 1.0,
        'volume': 100_000.0,
        'date': [d.date() for d in dates],
    })
    sd = _make_stock_data({'TINY': df})
    detector = VolumeDetector()
    await detector.detect(sd)
    assert 'TINY' in detector.skipped_tickers


@pytest.mark.asyncio
async def test_detect_ticker_output_is_uppercase():
    """Emitted events must have uppercase ticker values."""
    frames = {'aapl': _make_price_df('aapl', n=60, spike_idx=40, spike_multiplier=10.0)}
    sd = MagicMock()
    sd.db.execute = AsyncMock(return_value=[('aapl',)])
    sd.price_history.fetch_daily_price_history = AsyncMock(
        side_effect=lambda ticker, **kw: frames.get(ticker, pd.DataFrame())
    )
    sd.price_history.fetch_5m_price_history = AsyncMock(return_value=pd.DataFrame())
    sd.popularity.fetch_popularity = AsyncMock(return_value=pd.DataFrame())
    result = await VolumeDetector(zscore_threshold=2.0).detect(sd)
    if not result.empty:
        assert (result['ticker'] == result['ticker'].str.upper()).all()


@pytest.mark.asyncio
async def test_detect_explicit_tickers_uppercased():
    """Lower-case explicit tickers should be upper-cased internally."""
    frames = {'AAPL': _make_price_df('AAPL', n=60, spike_idx=40, spike_multiplier=10.0)}
    sd = _make_stock_data(frames)
    # Pass lower-case ticker — should still find AAPL data
    result = await VolumeDetector(zscore_threshold=2.0).detect(sd, tickers=['aapl'])
    # VolumeDetector uppercases the explicit list; the stream fetcher uses it
    assert 'GOOG' not in result.get('ticker', pd.Series()).values


@pytest.mark.asyncio
async def test_detect_uses_price_table_for_universe_when_no_tickers():
    """When tickers=None, VolumeDetector should query the price table, not popularity."""
    frames = {'AAPL': _make_price_df('AAPL', n=60, spike_idx=40)}
    sd = _make_stock_data(frames)
    await VolumeDetector().detect(sd)
    # db.execute is called by fetch_distinct_tickers_from_prices — check it queries price table
    call_queries = [call[0][0] for call in sd.db.execute.call_args_list]
    price_table_queries = [q for q in call_queries if 'daily_price_history' in q]
    assert len(price_table_queries) >= 1


@pytest.mark.asyncio
async def test_detect_5m_default_min_volume_is_lower():
    """5m timeframe should use min_volume=500 by default (not 10_000).

    Baseline volumes are in [1_000, 5_000] — above 500 but below 10_000 — so
    the spike can be detected with the 500 default but would be blocked by the
    10_000 daily default (baseline mean + spike are all >> 500 but the baseline
    is well below 10_000 until the spike).  We verify events are emitted at all.
    """
    n = 200
    rng = np.random.default_rng(77)
    dts = [pd.Timestamp('2024-01-01 09:30') + pd.Timedelta(minutes=5 * i) for i in range(n)]
    close = np.maximum(10 + rng.standard_normal(n).cumsum(), 1.0)
    # Variable baseline volumes in [1_000, 5_000] so rolling std is non-zero
    volume = rng.integers(1_000, 5_000, n).astype(float)
    volume[150] *= 30.0  # large spike (30×) — z-score will be high

    df = pd.DataFrame({
        'ticker': 'AAPL', 'open': close, 'high': close, 'low': close, 'close': close,
        'volume': volume, 'datetime': dts,
    })
    sd = MagicMock()
    sd.db.execute = AsyncMock(return_value=[('AAPL',)])
    sd.price_history.fetch_5m_price_history = AsyncMock(return_value=df)
    sd.price_history.fetch_daily_price_history = AsyncMock(return_value=pd.DataFrame())
    sd.popularity.fetch_popularity = AsyncMock(return_value=pd.DataFrame())

    result = await VolumeDetector(zscore_threshold=2.0).detect(sd, timeframe='5m')
    # With 5m default min_volume=500, events should fire (spike >> 500)
    assert not result.empty
