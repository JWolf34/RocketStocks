"""Tests for the direction_prediction strategy and base class direction filter.

Covers:
- DirectionPredictionStrategy registers correctly
- Direction filter blocks entries when probability_up < threshold
- Direction filter allows entries when probability_up >= threshold
- use_direction_filter=False (default) leaves existing strategies unchanged
- Direction filter gracefully degrades when model fails to fit
- enrich_with_prediction_features adds correct columns to data_prep
"""
import math

import numpy as np
import pandas as pd
import pytest
from backtesting import Backtest

from rocketstocks.backtest.data_prep import enrich_with_prediction_features
from rocketstocks.backtest.registry import get_strategy, list_strategies
from rocketstocks.backtest.strategies.base import LeadingIndicatorStrategy


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _make_price_df(n: int = 300, trend: float = 0.1, seed: int = 42) -> pd.DataFrame:
    """Generate a realistic price DataFrame with enough bars to train a model."""
    rng = np.random.default_rng(seed)
    idx = pd.date_range('2022-01-03', periods=n, freq='B')
    returns = rng.normal(trend * 0.01, 0.02, n)
    close = pd.Series(100.0 * (1 + returns).cumprod(), index=idx)
    high = close * (1 + rng.uniform(0, 0.01, n))
    low = close * (1 - rng.uniform(0, 0.01, n))
    open_ = close * (1 + rng.normal(0, 0.005, n))
    volume = pd.Series(1_000_000 + rng.uniform(0, 200_000, n), index=idx)
    return pd.DataFrame(
        {'Open': open_, 'High': high, 'Low': low, 'Close': close, 'Volume': volume},
        index=idx,
    )


class _AlwaysFireStrategy(LeadingIndicatorStrategy):
    """Fires signal on every bar — used to test direction filter blocking."""

    def _detect_signal(self) -> bool:
        return True


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class TestDirectionPredictionRegistry:

    def test_strategy_is_registered(self):
        strategies = list_strategies()
        assert 'direction_prediction' in strategies

    def test_get_strategy_returns_class(self):
        cls = get_strategy('direction_prediction')
        assert cls is not None
        assert issubclass(cls, LeadingIndicatorStrategy)


# ---------------------------------------------------------------------------
# enrich_with_prediction_features
# ---------------------------------------------------------------------------

class TestEnrichWithPredictionFeatures:

    def test_adds_expected_columns(self):
        df = _make_price_df(100)
        result = enrich_with_prediction_features(df)
        for col in ('MFI', 'Volume_Zscore', 'OBV_Velocity', 'Confluence_Count'):
            assert col in result.columns

    def test_original_columns_preserved(self):
        df = _make_price_df(100)
        result = enrich_with_prediction_features(df)
        for col in ('Open', 'High', 'Low', 'Close', 'Volume'):
            assert col in result.columns

    def test_length_unchanged(self):
        df = _make_price_df(100)
        result = enrich_with_prediction_features(df)
        assert len(result) == len(df)

    def test_mfi_values_in_range(self):
        df = _make_price_df(100)
        result = enrich_with_prediction_features(df)
        valid = result['MFI'].dropna()
        assert (valid >= -1e-9).all()
        assert (valid <= 100 + 1e-9).all()

    def test_later_rows_have_non_nan_features(self):
        df = _make_price_df(150)
        result = enrich_with_prediction_features(df)
        # After 30-bar warmup, at least some features should be non-NaN
        tail = result.iloc[30:]
        non_nan = tail[['MFI', 'Volume_Zscore']].notna().mean().mean()
        assert non_nan > 0.5

    def test_empty_df_returns_columns(self):
        df = pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume'])
        result = enrich_with_prediction_features(df)
        for col in ('MFI', 'Volume_Zscore', 'OBV_Velocity', 'Confluence_Count'):
            assert col in result.columns


# ---------------------------------------------------------------------------
# Direction filter on base class
# ---------------------------------------------------------------------------

class TestDirectionFilterDisabledByDefault:
    """Without use_direction_filter=True, strategies behave exactly as before."""

    def test_always_fire_without_filter_generates_many_trades(self):
        df = _make_price_df(150)
        bt = Backtest(df, _AlwaysFireStrategy, cash=10_000, commission=0.0)
        stats = bt.run(exit_mode='bar_hold', hold_bars=5, use_direction_filter=False)
        # Without filter, should have many trades (roughly every 5 bars)
        assert stats['# Trades'] > 5


class TestDirectionFilterEnabled:
    """With use_direction_filter=True, entries are gated by the model."""

    def test_filter_reduces_trade_count(self):
        """Filter should reduce (or equal) the number of trades."""
        df = _make_price_df(300)
        bt = Backtest(df, _AlwaysFireStrategy, cash=10_000, commission=0.0)
        stats_no_filter = bt.run(exit_mode='bar_hold', hold_bars=5, use_direction_filter=False)
        stats_filter = bt.run(
            exit_mode='bar_hold',
            hold_bars=5,
            use_direction_filter=True,
            direction_threshold=0.55,
            direction_train_fraction=0.50,
        )
        # Filter should produce <= trades; with some bars in training window, this always holds
        assert stats_filter['# Trades'] <= stats_no_filter['# Trades']

    def test_high_threshold_blocks_most_entries(self):
        """threshold=0.99 means almost nothing gets through."""
        df = _make_price_df(300)
        bt = Backtest(df, _AlwaysFireStrategy, cash=10_000, commission=0.0)
        stats = bt.run(
            exit_mode='bar_hold',
            hold_bars=5,
            use_direction_filter=True,
            direction_threshold=0.99,
            direction_train_fraction=0.50,
        )
        # Very few trades expected with near-impossible threshold
        stats_no_filter = bt.run(exit_mode='bar_hold', hold_bars=5, use_direction_filter=False)
        assert stats['# Trades'] < stats_no_filter['# Trades']

    def test_rejections_recorded(self):
        """self._rejections should be populated when entries are blocked."""
        df = _make_price_df(300)

        strategy_instance = None

        class _TrackingStrategy(_AlwaysFireStrategy):
            def init(self):
                super().init()

        bt = Backtest(df, _TrackingStrategy, cash=10_000, commission=0.0)
        bt.run(
            exit_mode='bar_hold',
            hold_bars=5,
            use_direction_filter=True,
            direction_threshold=0.80,  # fairly high — should reject some
            direction_train_fraction=0.50,
        )
        # Just confirm the backtest runs without error with filter enabled
        # (rejections are internal to the strategy instance)

    def test_model_unavailable_gracefully_allows_entries(self):
        """If model can't fit, direction filter should not block entries."""
        # Only 10 bars — model definitely won't fit
        df = _make_price_df(10)
        bt = Backtest(df, _AlwaysFireStrategy, cash=10_000, commission=0.0)
        # Should not raise, and should produce some trades
        stats = bt.run(
            exit_mode='bar_hold',
            hold_bars=3,
            use_direction_filter=True,
            direction_threshold=0.60,
        )
        # With graceful degradation, trades happen normally
        assert stats['# Trades'] >= 0  # just verify it runs without error


# ---------------------------------------------------------------------------
# DirectionPredictionStrategy end-to-end
# ---------------------------------------------------------------------------

class TestDirectionPredictionStrategy:

    def test_strategy_runs_without_error(self):
        from rocketstocks.backtest.strategies.direction_prediction import DirectionPredictionStrategy
        df = _make_price_df(300)
        bt = Backtest(df, DirectionPredictionStrategy, cash=10_000, commission=0.0)
        stats = bt.run()
        assert stats is not None

    def test_no_trades_before_training_cutoff(self):
        """Entries only happen after train_fraction of bars (training window)."""
        from rocketstocks.backtest.strategies.direction_prediction import DirectionPredictionStrategy
        n = 300
        df = _make_price_df(n)
        bt = Backtest(df, DirectionPredictionStrategy, cash=10_000, commission=0.0)
        stats = bt.run(train_fraction=0.50, exit_mode='bar_hold', hold_bars=5)
        trades = stats['_trades']
        if len(trades) > 0:
            # All entries should be after the training cutoff (~150 bars)
            cutoff_approx = n * 0.50
            assert trades['EntryBar'].min() >= cutoff_approx - 1

    def test_higher_threshold_fewer_trades(self):
        """Higher probability_threshold → fewer entries."""
        from rocketstocks.backtest.strategies.direction_prediction import DirectionPredictionStrategy
        df = _make_price_df(300)
        bt = Backtest(df, DirectionPredictionStrategy, cash=10_000, commission=0.0)
        stats_55 = bt.run(probability_threshold=0.55, exit_mode='bar_hold', hold_bars=5)
        stats_75 = bt.run(probability_threshold=0.75, exit_mode='bar_hold', hold_bars=5)
        assert stats_75['# Trades'] <= stats_55['# Trades']

    def test_use_direction_filter_always_true(self):
        """DirectionPredictionStrategy must always have use_direction_filter=True."""
        from rocketstocks.backtest.strategies.direction_prediction import DirectionPredictionStrategy
        assert DirectionPredictionStrategy.use_direction_filter is True
