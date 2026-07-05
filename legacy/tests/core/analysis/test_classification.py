"""Tests for rocketstocks.core.analysis.classification."""
import numpy as np
import pandas as pd
import pytest

from rocketstocks.core.analysis.classification import (
    StockClass,
    classify_ticker,
    compute_volatility,
    compute_return_stats,
    dynamic_zscore_threshold,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _price_df(n=30, start=100.0, step=0.5):
    """Return a simple daily OHLCV-like DataFrame with 'close' column."""
    close = [start + i * step for i in range(n)]
    return pd.DataFrame({'close': close, 'volume': [1_000_000] * n})


def _volatile_price_df(n=30, seed=42):
    """Return a DataFrame with high daily volatility."""
    rng = np.random.default_rng(seed)
    # 5% daily moves
    returns = rng.normal(0, 5, n) / 100
    close = [100.0]
    for r in returns[1:]:
        close.append(close[-1] * (1 + r))
    return pd.DataFrame({'close': close, 'volume': [1_000_000] * n})


def _low_vol_price_df(n=30, seed=42):
    """Return a DataFrame with low daily volatility (0.5% moves)."""
    rng = np.random.default_rng(seed)
    returns = rng.normal(0, 0.5, n) / 100
    close = [100.0]
    for r in returns[1:]:
        close.append(close[-1] * (1 + r))
    return pd.DataFrame({'close': close, 'volume': [1_000_000] * n})


# ---------------------------------------------------------------------------
# compute_volatility
# ---------------------------------------------------------------------------

class TestComputeVolatility:
    def test_returns_float(self):
        df = _volatile_price_df()
        v = compute_volatility(df)
        assert isinstance(v, float)

    def test_high_volatility_series(self):
        df = _volatile_price_df()
        v = compute_volatility(df)
        # 5% std-dev moves → volatility should be around 5%
        assert v > 2.0

    def test_low_volatility_series(self):
        df = _low_vol_price_df()
        v = compute_volatility(df)
        assert v < 3.0

    def test_empty_df_returns_nan(self):
        v = compute_volatility(pd.DataFrame())
        assert np.isnan(v)

    def test_missing_close_column_returns_nan(self):
        df = pd.DataFrame({'open': [1, 2, 3]})
        v = compute_volatility(df)
        assert np.isnan(v)

    def test_insufficient_data_returns_nan(self):
        df = pd.DataFrame({'close': [100.0]})
        v = compute_volatility(df)
        assert np.isnan(v)


# ---------------------------------------------------------------------------
# compute_return_stats
# ---------------------------------------------------------------------------

class TestComputeReturnStats:
    def test_returns_tuple_of_two_floats(self):
        df = _volatile_price_df()
        mean, std = compute_return_stats(df)
        assert isinstance(mean, float)
        assert isinstance(std, float)

    def test_empty_df_returns_nan_pair(self):
        mean, std = compute_return_stats(pd.DataFrame())
        assert np.isnan(mean)
        assert np.isnan(std)

    def test_std_positive_for_volatile_series(self):
        df = _volatile_price_df()
        mean, std = compute_return_stats(df)
        assert std > 0


# ---------------------------------------------------------------------------
# classify_ticker
# ---------------------------------------------------------------------------

class TestClassifyTicker:
    def test_watchlist_override_takes_priority(self):
        cls = classify_ticker(
            ticker='GME',
            market_cap=500_000_000_000,  # $500B — would be blue chip
            volatility_20d=0.1,           # extremely low vol
            popularity_rank=None,
            watchlist_override='volatile',
        )
        assert cls == StockClass.VOLATILE

    def test_watchlist_override_meme(self):
        cls = classify_ticker(
            ticker='AMC',
            market_cap=1e9,
            volatility_20d=6.0,
            watchlist_override='meme',
        )
        assert cls == StockClass.MEME

    def test_watchlist_override_blue_chip(self):
        cls = classify_ticker(
            ticker='XYZ',
            market_cap=1e6,  # tiny cap
            volatility_20d=10.0,  # very volatile
            watchlist_override='blue_chip',
        )
        assert cls == StockClass.BLUE_CHIP

    def test_invalid_watchlist_override_falls_through(self):
        # Invalid category should be ignored; falls through to classification logic
        cls = classify_ticker(
            ticker='XYZ',
            market_cap=500e9,
            volatility_20d=0.5,
            watchlist_override='unknown_class',
        )
        # Should still classify based on market cap + vol
        assert cls == StockClass.BLUE_CHIP

    def test_meme_classification(self):
        cls = classify_ticker(
            ticker='GME',
            market_cap=1_500_000_000,  # $1.5B (below $2B)
            volatility_20d=5.0,         # above 4%
            popularity_rank=20,          # top 50
        )
        assert cls == StockClass.MEME

    def test_meme_requires_both_popularity_and_volatility(self):
        # High popularity but low volatility → not meme
        cls = classify_ticker(
            ticker='GME',
            market_cap=1_500_000_000,
            volatility_20d=1.0,   # low vol
            popularity_rank=10,
        )
        assert cls != StockClass.MEME

    def test_volatile_classification(self):
        cls = classify_ticker(
            ticker='SPCE',
            market_cap=500_000_000,  # $500M < $2B
            volatility_20d=5.0,       # > 4%
            popularity_rank=200,      # not in top 50
        )
        assert cls == StockClass.VOLATILE

    def test_blue_chip_classification(self):
        cls = classify_ticker(
            ticker='AAPL',
            market_cap=3_000_000_000_000,  # $3T >> $10B
            volatility_20d=1.0,             # < 1.5%
        )
        assert cls == StockClass.BLUE_CHIP

    def test_standard_classification_default(self):
        # Mid-cap, moderate volatility — no special bucket
        cls = classify_ticker(
            ticker='MID',
            market_cap=5_000_000_000,  # $5B — between $2B and $10B
            volatility_20d=2.5,         # between 1.5% and 4%
        )
        assert cls == StockClass.STANDARD

    def test_none_market_cap_does_not_raise(self):
        cls = classify_ticker(ticker='X', market_cap=None, volatility_20d=5.0)
        # Can't classify as volatile (no market cap) — standard or meme
        assert isinstance(cls, StockClass)

    def test_none_volatility_does_not_raise(self):
        cls = classify_ticker(ticker='X', market_cap=500_000_000_000, volatility_20d=None)
        # Can't classify as blue chip (no volatility) — standard
        assert isinstance(cls, StockClass)

    def test_stock_class_values_are_strings(self):
        assert StockClass.VOLATILE.value == 'volatile'
        assert StockClass.MEME.value == 'meme'
        assert StockClass.BLUE_CHIP.value == 'blue_chip'
        assert StockClass.STANDARD.value == 'standard'


# ---------------------------------------------------------------------------
# dynamic_zscore_threshold
# ---------------------------------------------------------------------------

class TestDynamicZscoreThreshold:
    def test_zero_volatility_returns_max_threshold(self):
        assert dynamic_zscore_threshold(0.0) == 3.0

    def test_max_volatility_returns_min_threshold(self):
        assert dynamic_zscore_threshold(8.0) == pytest.approx(1.5)

    def test_above_max_volatility_clamps_to_floor(self):
        assert dynamic_zscore_threshold(100.0) == pytest.approx(1.5)

    def test_midpoint_volatility_returns_midpoint_threshold(self):
        # vol=4.0 → normalized=0.5 → 3.0 - (0.5 * 1.5) = 2.25
        assert dynamic_zscore_threshold(4.0) == pytest.approx(2.25)

    def test_low_volatility_returns_high_threshold(self):
        # vol=1% → threshold > 2.5
        assert dynamic_zscore_threshold(1.0) > 2.5

    def test_high_volatility_returns_low_threshold(self):
        # vol=7% → threshold < 2.0
        assert dynamic_zscore_threshold(7.0) < 2.0

    def test_nan_volatility_returns_neutral_default(self):
        assert dynamic_zscore_threshold(float('nan')) == 2.5

    def test_negative_volatility_returns_neutral_default(self):
        assert dynamic_zscore_threshold(-1.0) == 2.5

    def test_monotonically_decreasing(self):
        vols = [0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 7.0, 8.0]
        thresholds = [dynamic_zscore_threshold(v) for v in vols]
        assert all(thresholds[i] > thresholds[i + 1] for i in range(len(thresholds) - 1))

    def test_no_cliff_near_old_class_boundary(self):
        # Old system: stocks near 4% vol boundary jumped from threshold 2.5 → 2.0.
        # New system: thresholds at 3.9% and 4.1% should be within 0.1 of each other.
        t1 = dynamic_zscore_threshold(3.9)
        t2 = dynamic_zscore_threshold(4.1)
        assert abs(t1 - t2) < 0.1

    def test_custom_max_volatility(self):
        # With max_volatility=4.0, vol=4.0 should hit the floor (1.5)
        assert dynamic_zscore_threshold(4.0, max_volatility=4.0) == pytest.approx(1.5)

    def test_result_always_in_valid_range(self):
        for vol in [0.0, 1.0, 4.0, 8.0, 20.0]:
            t = dynamic_zscore_threshold(vol)
            assert 1.5 <= t <= 3.0
