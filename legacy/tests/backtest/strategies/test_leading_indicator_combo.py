"""Tests for LeadingIndicatorComboStrategy."""
import numpy as np
import pandas as pd
import pytest
from backtesting import Backtest

from rocketstocks.backtest.strategies.leading_indicator_combo import LeadingIndicatorComboStrategy


def _make_combo_df(n: int = 50,
                   rank: float = 50,
                   mentions: float = 300,
                   rank_24h_ago: float = 100,
                   mentions_24h_ago: float = 50,
                   vol_zscore_val: float = 3.0,
                   price_zscore_val: float = 0.05) -> pd.DataFrame:
    """DataFrame with both popularity and volume enrichment columns."""
    idx = pd.date_range('2025-01-02 09:30', periods=n, freq='5min')
    prices = [100.0] * n
    vol_mean = 1_000_000
    vol_std = 100_000
    cumvol = vol_mean + vol_zscore_val * vol_std
    ret_std = 1.0
    intraday_pct = price_zscore_val  # price_zscore = intraday_pct / ret_std = price_zscore_val

    return pd.DataFrame({
        'Open': prices, 'High': [p + 0.5 for p in prices],
        'Low': [p - 0.5 for p in prices], 'Close': prices,
        'Volume': [50_000] * n,
        'Rank': [rank] * n,
        'Mentions': [mentions] * n,
        'Rank_24h_ago': [rank_24h_ago] * n,
        'Mentions_24h_ago': [mentions_24h_ago] * n,
        'Daily_Vol_Mean': [vol_mean] * n,
        'Daily_Vol_Std': [vol_std] * n,
        'Daily_Return_Mean': [0.0] * n,
        'Daily_Return_Std': [ret_std] * n,
        'Cumulative_Volume': [cumvol] * n,
        'Intraday_Pct_Change': [intraday_pct] * n,
        'Is_Regular_Hours': [True] * n,
    }, index=pd.DatetimeIndex(idx, name='Datetime'))


# ---------------------------------------------------------------------------
# Strategy attributes
# ---------------------------------------------------------------------------

def test_requires_both_flags():
    assert LeadingIndicatorComboStrategy.requires_popularity is True
    assert LeadingIndicatorComboStrategy.requires_daily is True


# ---------------------------------------------------------------------------
# Signal logic
# ---------------------------------------------------------------------------

def test_fires_when_both_signals_present():
    """Both popularity surge and volume accumulation active → should buy."""
    df = _make_combo_df(vol_zscore_val=3.0, price_zscore_val=0.05,
                        mentions=300, mentions_24h_ago=50)
    bt = Backtest(df, LeadingIndicatorComboStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='bar_hold', hold_bars=3,
                   vol_threshold=2.0, price_ceiling=1.0, min_mentions=15)
    assert stats['# Trades'] >= 1


def test_no_signal_when_rank_is_nan():
    """No popularity data → combo must not fire."""
    df = _make_combo_df(rank=float('nan'))
    bt = Backtest(df, LeadingIndicatorComboStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='bar_hold', hold_bars=3)
    assert stats['# Trades'] == 0


def test_no_signal_when_volume_below_threshold():
    """Volume below threshold → combo must not fire even with popularity surge."""
    df = _make_combo_df(vol_zscore_val=0.5)  # below default 2.0 threshold
    bt = Backtest(df, LeadingIndicatorComboStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='bar_hold', hold_bars=3, vol_threshold=2.0)
    assert stats['# Trades'] == 0


def test_no_signal_when_price_moving():
    """Price moving significantly → price_ceiling exceeded → no accumulation signal."""
    df = _make_combo_df(vol_zscore_val=3.0, price_zscore_val=3.0)
    bt = Backtest(df, LeadingIndicatorComboStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='bar_hold', hold_bars=3, vol_threshold=2.0, price_ceiling=1.0)
    assert stats['# Trades'] == 0


def test_no_signal_when_mentions_too_low():
    """Low mention count → popularity surge suppressed → combo doesn't fire."""
    df = _make_combo_df(vol_zscore_val=3.0, mentions=5, mentions_24h_ago=1)
    bt = Backtest(df, LeadingIndicatorComboStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='bar_hold', hold_bars=3, min_mentions=15)
    assert stats['# Trades'] == 0


def test_runs_without_error():
    df = _make_combo_df()
    bt = Backtest(df, LeadingIndicatorComboStrategy, cash=10_000, commission=0.0)
    stats = bt.run(exit_mode='bar_hold', hold_bars=3)
    assert isinstance(stats['# Trades'], (int, np.integer))
