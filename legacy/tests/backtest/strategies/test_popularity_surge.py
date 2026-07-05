"""Tests for PopularitySurgeStrategy."""
import math

import numpy as np
import pandas as pd
import pytest
from backtesting import Backtest

from rocketstocks.backtest.strategies.popularity_surge import PopularitySurgeStrategy


def _make_price_df_with_popularity(n: int = 50,
                                    rank: float | None = 50,
                                    mentions: float = 200,
                                    rank_24h_ago: float | None = 100,
                                    mentions_24h_ago: float = 50) -> pd.DataFrame:
    """Price DataFrame with popularity columns pre-merged."""
    idx = pd.date_range('2025-01-02 09:30', periods=n, freq='5min')
    prices = [100.0 + i * 0.1 for i in range(n)]
    df = pd.DataFrame({
        'Open': prices, 'High': [p + 0.5 for p in prices],
        'Low': [p - 0.5 for p in prices], 'Close': prices,
        'Volume': [10_000] * n,
        'Rank': [rank] * n,
        'Mentions': [mentions] * n,
        'Rank_24h_ago': [rank_24h_ago] * n,
        'Mentions_24h_ago': [mentions_24h_ago] * n,
        'Is_Regular_Hours': [True] * n,
    }, index=idx)
    return df


# ---------------------------------------------------------------------------
# Strategy attributes
# ---------------------------------------------------------------------------

def test_requires_popularity_flag():
    assert PopularitySurgeStrategy.requires_popularity is True


def test_default_min_mentions():
    assert PopularitySurgeStrategy.min_mentions == 15


# ---------------------------------------------------------------------------
# Signal detection
# ---------------------------------------------------------------------------

def test_no_signal_when_rank_is_nan():
    """Strategy should not fire when Rank column is NaN (no popularity data)."""
    df = _make_price_df_with_popularity(rank=float('nan'))
    bt = Backtest(df, PopularitySurgeStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='bar_hold', hold_bars=3)
    assert stats['# Trades'] == 0


def test_no_signal_when_rank_column_absent():
    idx = pd.date_range('2025-01-02', periods=30, freq='B')
    prices = [100.0 + i for i in range(30)]
    df = pd.DataFrame({
        'Open': prices, 'High': [p + 1 for p in prices],
        'Low': [p - 1 for p in prices], 'Close': prices,
        'Volume': [1_000_000] * 30,
    }, index=idx)
    bt = Backtest(df, PopularitySurgeStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='bar_hold', hold_bars=3)
    assert stats['# Trades'] == 0


def test_no_signal_when_mentions_below_minimum():
    """mentions < min_mentions → surge function returns False early."""
    df = _make_price_df_with_popularity(mentions=5, mentions_24h_ago=1)
    bt = Backtest(df, PopularitySurgeStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='bar_hold', hold_bars=3, min_mentions=15)
    assert stats['# Trades'] == 0


def test_mention_surge_fires():
    """3x mention ratio with sufficient base → MENTION_SURGE → should buy."""
    # mentions=300, mentions_24h_ago=50, ratio=6x (>3x), rank_24h_ago=100 → rank_change=50
    df = _make_price_df_with_popularity(
        rank=50, mentions=300, rank_24h_ago=100, mentions_24h_ago=50,
    )
    bt = Backtest(df, PopularitySurgeStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='bar_hold', hold_bars=3, min_mentions=15)
    assert stats['# Trades'] >= 1


def test_runs_without_error():
    """Basic smoke test."""
    df = _make_price_df_with_popularity()
    bt = Backtest(df, PopularitySurgeStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='bar_hold', hold_bars=5)
    assert isinstance(stats['# Trades'], (int, np.integer))
