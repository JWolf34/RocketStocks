"""Tests for LeadingIndicatorStrategy base class exit modes.

Uses a concrete minimal subclass that fires on every bar for deterministic testing.
"""
import math

import numpy as np
import pandas as pd
import pytest
from backtesting import Backtest

from rocketstocks.backtest.strategies.base import LeadingIndicatorStrategy
from rocketstocks.backtest.registry import register


# ---------------------------------------------------------------------------
# Test helper: strategy that fires on first bar only
# ---------------------------------------------------------------------------

class _OnceStrategy(LeadingIndicatorStrategy):
    """Fires signal on the very first bar, then never again."""
    _fired = False

    def init(self):
        super().init()
        self._fired = False

    def _detect_signal(self) -> bool:
        if not self._fired:
            self._fired = True
            return True
        return False


def _make_price_df(n: int = 60, trend: float = 0.5) -> pd.DataFrame:
    """Rising price DataFrame for exit testing."""
    idx = pd.date_range('2025-01-02', periods=n, freq='B')
    prices = [100.0 + i * trend for i in range(n)]
    return pd.DataFrame({
        'Open': prices,
        'High': [p + 1 for p in prices],
        'Low': [p - 1 for p in prices],
        'Close': prices,
        'Volume': [1_000_000] * n,
    }, index=idx)


# ---------------------------------------------------------------------------
# bar_hold exit
# ---------------------------------------------------------------------------

def test_bar_hold_closes_after_n_bars():
    df = _make_price_df(60)
    bt = Backtest(df, _OnceStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='bar_hold', hold_bars=5)
    assert stats['# Trades'] == 1
    trade = stats['_trades'].iloc[0]
    bars_held = trade['ExitBar'] - trade['EntryBar']
    assert bars_held == 5


def test_bar_hold_shorter_hold():
    df = _make_price_df(60)
    bt = Backtest(df, _OnceStrategy, cash=10_000, commission=0.0)
    stats_3 = bt.run(exit_mode='bar_hold', hold_bars=3)
    stats_10 = bt.run(exit_mode='bar_hold', hold_bars=10)
    trade_3 = stats_3['_trades'].iloc[0]
    trade_10 = stats_10['_trades'].iloc[0]
    assert trade_3['ExitBar'] - trade_3['EntryBar'] == 3
    assert trade_10['ExitBar'] - trade_10['EntryBar'] == 10


# ---------------------------------------------------------------------------
# breakout exit
# ---------------------------------------------------------------------------

def test_breakout_exits_on_max_hold_when_no_zscore_threshold_met():
    """With very flat data the z-score breakout won't trigger; max hold applies."""
    df = _make_price_df(60, trend=0.0)   # flat prices → zscore stays near 0
    bt = Backtest(df, _OnceStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='breakout', exit_zscore=10.0, stop_zscore=10.0, hold_bars=5)
    trade = stats['_trades'].iloc[0]
    assert trade['ExitBar'] - trade['EntryBar'] == 5


def test_breakout_produces_a_trade():
    df = _make_price_df(60)
    bt = Backtest(df, _OnceStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='breakout', exit_zscore=2.0, stop_zscore=2.0, hold_bars=20)
    assert stats['# Trades'] == 1


# ---------------------------------------------------------------------------
# momentum exit
# ---------------------------------------------------------------------------

def test_momentum_produces_a_trade():
    df = _make_price_df(60)
    bt = Backtest(df, _OnceStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='momentum', exit_zscore=2.0, stop_zscore=2.0,
                   trail_zscore=0.5, hold_bars=20)
    assert stats['# Trades'] == 1


def test_momentum_uses_hold_bars_as_failsafe():
    df = _make_price_df(60, trend=0.0)  # flat → no zscore trigger
    bt = Backtest(df, _OnceStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='momentum', exit_zscore=10.0, stop_zscore=10.0,
                   trail_zscore=10.0, hold_bars=5)
    trade = stats['_trades'].iloc[0]
    assert trade['ExitBar'] - trade['EntryBar'] == 5


# ---------------------------------------------------------------------------
# pending signal (Is_Regular_Hours column)
# ---------------------------------------------------------------------------

def _make_price_df_with_hours(n: int = 20, regular_from: int = 5) -> pd.DataFrame:
    """Price DF with Is_Regular_Hours=False for first `regular_from` bars."""
    idx = pd.date_range('2025-01-02 09:00', periods=n, freq='5min')
    prices = [100.0 + i * 0.1 for i in range(n)]
    df = pd.DataFrame({
        'Open': prices, 'High': [p + 0.5 for p in prices],
        'Low': [p - 0.5 for p in prices], 'Close': prices,
        'Volume': [10_000] * n,
        'Is_Regular_Hours': [i >= regular_from for i in range(n)],
    }, index=idx)
    return df


class _FireOnBarZeroStrategy(LeadingIndicatorStrategy):
    """Always fires signal on bar 0 (pre-market), to test pending pattern."""
    _done = False

    def init(self):
        super().init()
        self._done = False

    def _detect_signal(self) -> bool:
        if len(self.data) == 1 and not self._done:
            self._done = True
            return True
        return False


def test_pending_signal_delays_buy_to_regular_hours():
    df = _make_price_df_with_hours(n=20, regular_from=5)
    bt = Backtest(df, _FireOnBarZeroStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='bar_hold', hold_bars=3)
    if stats['# Trades'] >= 1:
        entry_bar = stats['_trades'].iloc[0]['EntryBar']
        # Entry should be at bar 5 or later (first regular-hours bar)
        assert entry_bar >= 5
