"""Tests for rocketstocks.backtest.regime."""
import datetime

import numpy as np
import pandas as pd
import pytest

from rocketstocks.backtest.regime import MarketRegime, classify_regimes, tag_trades_with_regime

_MIN_BARS_200 = 150  # mirrors regime._MIN_BARS_200


def _make_spy_df(n: int, seed: int = 0, trend: str = 'flat') -> pd.DataFrame:
    """Build a synthetic SPY DataFrame in backtesting.py format (DatetimeIndex, capitalized cols)."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range('2020-01-01', periods=n, freq='B')
    if trend == 'up':
        base = np.linspace(200, 400, n)
    elif trend == 'down':
        base = np.linspace(400, 100, n)
    else:
        base = np.full(n, 300.0)
    noise = rng.standard_normal(n) * 2
    close = base + noise
    close = np.maximum(close, 10.0)  # keep positive
    return pd.DataFrame({
        'Open': close - 1,
        'High': close + 2,
        'Low': close - 2,
        'Close': close,
        'Volume': 1_000_000,
    }, index=dates)


# ---------------------------------------------------------------------------
# classify_regimes — edge cases
# ---------------------------------------------------------------------------

def test_classify_regimes_empty_df_returns_empty():
    assert classify_regimes(pd.DataFrame()) == {}


def test_classify_regimes_none_returns_empty():
    assert classify_regimes(None) == {}


def test_classify_regimes_returns_dict():
    df = _make_spy_df(200)
    result = classify_regimes(df)
    assert isinstance(result, dict)


def test_classify_regimes_all_dates_covered():
    n = 250
    df = _make_spy_df(n)
    result = classify_regimes(df)
    assert len(result) == n


def test_classify_regimes_first_150_bars_unknown():
    df = _make_spy_df(300)
    result = classify_regimes(df)
    dates = sorted(result.keys())
    for d in dates[:_MIN_BARS_200]:
        assert result[d] == MarketRegime.UNKNOWN, f"Expected UNKNOWN at index {d}"


# ---------------------------------------------------------------------------
# classify_regimes — regime logic
# ---------------------------------------------------------------------------

def test_classify_regimes_strong_uptrend_yields_bull():
    """With a strong, steady uptrend (close always above sma200), we should see BULL."""
    df = _make_spy_df(400, trend='up')
    result = classify_regimes(df)
    dates = sorted(result.keys())
    # Look at the last quarter (well past warmup, price > sma200 in uptrend)
    late_dates = dates[300:]
    bull_count = sum(1 for d in late_dates if result[d] == MarketRegime.BULL)
    # In a clean uptrend the majority should be BULL
    assert bull_count > len(late_dates) // 2


def test_classify_regimes_strong_downtrend_yields_bear():
    """With a steady downtrend (close below sma200), we should see BEAR."""
    df = _make_spy_df(400, trend='down')
    result = classify_regimes(df)
    dates = sorted(result.keys())
    # Price falls well below sma200; check that BEAR appears
    mid_to_end = dates[200:]
    bear_count = sum(1 for d in mid_to_end if result[d] == MarketRegime.BEAR)
    assert bear_count > 0


def test_classify_regimes_values_are_market_regime_instances():
    df = _make_spy_df(200)
    result = classify_regimes(df)
    for val in result.values():
        assert isinstance(val, MarketRegime)


def test_classify_regimes_keys_are_date_objects():
    df = _make_spy_df(200)
    result = classify_regimes(df)
    for k in result:
        assert isinstance(k, datetime.date)


# ---------------------------------------------------------------------------
# tag_trades_with_regime
# ---------------------------------------------------------------------------

def _make_trades(dates_and_tickers: list[tuple]) -> list[dict]:
    """Create trade dicts from (date_str, ticker) pairs."""
    trades = []
    for date_str, ticker in dates_and_tickers:
        trades.append({
            'ticker': ticker,
            'entry_time': datetime.datetime.fromisoformat(date_str),
            'return_pct': 1.0,
        })
    return trades


def test_tag_trades_with_regime_assigns_known_regime():
    regime_map = {datetime.date(2024, 1, 15): MarketRegime.BULL}
    trades = _make_trades([('2024-01-15', 'AAPL')])
    result = tag_trades_with_regime(trades, regime_map)
    assert result[0]['regime'] == 'bull'


def test_tag_trades_with_regime_unknown_for_missing_date():
    regime_map = {}
    trades = _make_trades([('2024-06-01', 'SPY')])
    result = tag_trades_with_regime(trades, regime_map)
    assert result[0]['regime'] == 'unknown'


def test_tag_trades_with_regime_none_entry_time():
    trades = [{'ticker': 'AAPL', 'entry_time': None, 'return_pct': 1.0}]
    result = tag_trades_with_regime(trades, {})
    assert result[0]['regime'] == 'unknown'


def test_tag_trades_with_regime_modifies_in_place():
    regime_map = {datetime.date(2024, 3, 1): MarketRegime.CORRECTION}
    trades = _make_trades([('2024-03-01', 'MSFT')])
    returned = tag_trades_with_regime(trades, regime_map)
    assert returned is trades  # same list object


def test_tag_trades_with_regime_multiple_trades():
    d1 = datetime.date(2024, 1, 2)
    d2 = datetime.date(2024, 2, 5)
    regime_map = {d1: MarketRegime.BULL, d2: MarketRegime.BEAR}
    trades = [
        {'ticker': 'AAPL', 'entry_time': datetime.datetime(2024, 1, 2), 'return_pct': 2.0},
        {'ticker': 'GME', 'entry_time': datetime.datetime(2024, 2, 5), 'return_pct': -1.0},
    ]
    tag_trades_with_regime(trades, regime_map)
    assert trades[0]['regime'] == 'bull'
    assert trades[1]['regime'] == 'bear'


def test_tag_trades_with_regime_empty_trades():
    result = tag_trades_with_regime([], {})
    assert result == []
