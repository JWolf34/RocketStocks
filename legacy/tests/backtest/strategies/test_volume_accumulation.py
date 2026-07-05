"""Tests for VolumeAccumulationStrategy."""
import math

import numpy as np
import pandas as pd
import pytest
from backtesting import Backtest
from unittest.mock import patch

from rocketstocks.backtest.strategies.volume_accumulation import VolumeAccumulationStrategy


def _make_daily_df(n: int = 60, high_vol_at: int | None = None) -> pd.DataFrame:
    """Daily DataFrame in backtesting.py format. Optionally spike volume at a bar."""
    idx = pd.date_range('2025-01-02', periods=n, freq='B')
    prices = [100.0 + i * 0.1 for i in range(n)]
    volumes = [1_000_000] * n
    if high_vol_at is not None and 0 <= high_vol_at < n:
        volumes[high_vol_at] = 5_000_000  # 5x spike
    return pd.DataFrame({
        'Open': prices, 'High': [p + 1 for p in prices],
        'Low': [p - 1 for p in prices], 'Close': prices,
        'Volume': volumes,
    }, index=idx)


def _make_5m_df_enriched(n: int = 50) -> pd.DataFrame:
    """5m DataFrame pre-enriched with daily context columns.
    Cumulative volume is kept well below Daily_Vol_Mean so vol_zscore stays negative.
    """
    idx = pd.date_range('2025-01-02 09:30', periods=n, freq='5min')
    prices = [100.0 + i * 0.01 for i in range(n)]
    df = pd.DataFrame({
        'Open': prices, 'High': [p + 0.5 for p in prices],
        'Low': [p - 0.5 for p in prices], 'Close': prices,
        'Volume': [5_000] * n,
        'Daily_Vol_Mean': [1_000_000] * n,
        'Daily_Vol_Std': [100_000] * n,
        'Daily_Return_Mean': [0.0] * n,
        'Daily_Return_Std': [1.0] * n,
        # Cumulative volume max = 50*5000 = 250_000, well below 1_000_000 mean
        # vol_zscore = (250_000 - 1_000_000) / 100_000 = -7.5 (always below threshold)
        'Cumulative_Volume': [5_000 * (i + 1) for i in range(n)],
        'Intraday_Pct_Change': [0.05] * n,
        'Is_Regular_Hours': [True] * n,
    }, index=idx)
    return df


# ---------------------------------------------------------------------------
# Strategy attributes
# ---------------------------------------------------------------------------

def test_requires_daily_flag():
    assert VolumeAccumulationStrategy.requires_daily is True


def test_default_parameters():
    assert VolumeAccumulationStrategy.vol_threshold == 2.0
    assert VolumeAccumulationStrategy.price_ceiling == 1.0


# ---------------------------------------------------------------------------
# Daily mode
# ---------------------------------------------------------------------------

def test_daily_mode_runs_without_error():
    df = _make_daily_df(60)
    bt = Backtest(df, VolumeAccumulationStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='bar_hold', hold_bars=3)
    assert isinstance(stats['# Trades'], (int, np.integer))


def test_daily_mode_skips_first_bars():
    """Strategy needs 21 bars before it can signal — no trades in first 21 bars."""
    df = _make_daily_df(60)
    bt = Backtest(df, VolumeAccumulationStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='bar_hold', hold_bars=3)
    if stats['# Trades'] > 0:
        entry_bar = stats['_trades'].iloc[0]['EntryBar']
        assert entry_bar >= 20


# ---------------------------------------------------------------------------
# 5m enriched mode
# ---------------------------------------------------------------------------

def test_5m_mode_signals_on_high_volume():
    """With Cumulative_Volume well above Daily_Vol_Mean (vol_zscore >> 2) and
    near-zero price change, the signal should fire."""
    n = 50
    idx = pd.date_range('2025-01-02 09:30', periods=n, freq='5min')
    prices = [100.0] * n
    # vol_zscore = (Cumulative_Volume - 1_000_000) / 100_000
    # At cumvol=1_300_000, vol_zscore = 3.0 (above threshold)
    cumvols = [1_300_000] * n
    df = pd.DataFrame({
        'Open': prices, 'High': [p + 0.5 for p in prices],
        'Low': [p - 0.5 for p in prices], 'Close': prices,
        'Volume': [50_000] * n,
        'Daily_Vol_Mean': [1_000_000] * n,
        'Daily_Vol_Std': [100_000] * n,
        'Daily_Return_Mean': [0.0] * n,
        'Daily_Return_Std': [1.0] * n,
        'Cumulative_Volume': cumvols,
        'Intraday_Pct_Change': [0.05] * n,  # price_zscore = 0.05
        'Is_Regular_Hours': [True] * n,
    }, index=idx)
    bt = Backtest(df, VolumeAccumulationStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='bar_hold', hold_bars=3, vol_threshold=2.0, price_ceiling=1.0)
    assert stats['# Trades'] >= 1


def test_5m_mode_no_signal_on_normal_volume():
    """With volume near the mean, vol_zscore < 2 and no signal should fire."""
    df = _make_5m_df_enriched()
    # Cumulative_Volume starts at 50_000, Daily_Vol_Mean=1_000_000 → very negative zscore
    bt = Backtest(df, VolumeAccumulationStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='bar_hold', hold_bars=3, vol_threshold=2.0)
    assert stats['# Trades'] == 0


def test_5m_mode_no_signal_when_price_moves():
    """Vol spike + big price move → price_zscore exceeds ceiling → no signal."""
    n = 50
    idx = pd.date_range('2025-01-02 09:30', periods=n, freq='5min')
    prices = [100.0] * n
    df = pd.DataFrame({
        'Open': prices, 'High': [p + 0.5 for p in prices],
        'Low': [p - 0.5 for p in prices], 'Close': prices,
        'Volume': [50_000] * n,
        'Daily_Vol_Mean': [1_000_000] * n,
        'Daily_Vol_Std': [100_000] * n,
        'Daily_Return_Mean': [0.0] * n,
        'Daily_Return_Std': [1.0] * n,
        'Cumulative_Volume': [1_300_000] * n,  # vol_zscore = 3
        'Intraday_Pct_Change': [3.0] * n,      # price_zscore = 3.0 (above ceiling)
        'Is_Regular_Hours': [True] * n,
    }, index=idx)
    bt = Backtest(df, VolumeAccumulationStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='bar_hold', hold_bars=3, vol_threshold=2.0, price_ceiling=1.0)
    assert stats['# Trades'] == 0


def test_5m_mode_skips_nan_enrichment():
    """Bars with NaN enrichment columns should not trigger a signal."""
    df = _make_5m_df_enriched()
    df['Daily_Vol_Mean'] = float('nan')
    bt = Backtest(df, VolumeAccumulationStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='bar_hold', hold_bars=3)
    assert stats['# Trades'] == 0
