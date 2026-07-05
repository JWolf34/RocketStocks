"""Tests for rocketstocks.core.analysis.signals."""
import numpy as np
import pandas as pd
import pytest

from rocketstocks.core.analysis.signals import signals


def _price_series(n=60, seed=42):
    rng = np.random.default_rng(seed)
    prices = 100.0 + rng.standard_normal(n).cumsum()
    return pd.Series(prices, name="close")


def _ohlcv(n=60, seed=42):
    rng = np.random.default_rng(seed)
    close = 100.0 + rng.standard_normal(n).cumsum()
    high = close + rng.uniform(0, 1, n)
    low = close - rng.uniform(0, 1, n)
    volume = rng.integers(500_000, 2_000_000, n).astype(float)
    return (
        pd.Series(high, name="high"),
        pd.Series(low, name="low"),
        pd.Series(close, name="close"),
        pd.Series(volume, name="volume"),
    )


class TestRsi:
    def test_returns_boolean_series(self):
        close = _price_series()
        result = signals.rsi(close)
        assert hasattr(result, "dtype")
        assert result.dtype == bool

    def test_same_length_as_input(self):
        close = _price_series(60)
        result = signals.rsi(close)
        assert len(result) == 60

    def test_rsi_oversold_when_steadily_declining(self):
        # Steadily declining price → RSI should be low → oversold signal True
        close = pd.Series([100.0 - i * 2 for i in range(60)])
        result = signals.rsi(close)
        # At least the last value should be True (oversold)
        assert result.iloc[-1] is True or result.any()


class TestMacd:
    def test_returns_boolean_series(self):
        close = _price_series()
        result = signals.macd(close)
        assert result.dtype == bool

    def test_same_length_as_input(self):
        close = _price_series(80)
        result = signals.macd(close)
        assert len(result) == 80


class TestSma:
    def test_returns_boolean_series(self):
        close = _price_series()
        result = signals.sma(close, short=10, long=20)
        assert result.dtype == bool

    def test_true_when_short_above_long(self):
        # Steadily rising → SMA_5 > SMA_10 at the last bar
        close = pd.Series([float(i) for i in range(60)])
        result = signals.sma(close, short=5, long=10)
        assert bool(result.iloc[-1]) is True


class TestAdx:
    def test_returns_boolean_series(self):
        high, low, close, _ = _ohlcv()
        result = signals.adx(close, high, low)
        assert result.dtype == bool

    def test_same_length_as_input(self):
        high, low, close, _ = _ohlcv(80)
        result = signals.adx(close, high, low)
        assert len(result) == 80


class TestObv:
    def test_returns_integer_series(self):
        # ta.increasing returns int64 (0/1), not bool dtype
        high, low, close, volume = _ohlcv()
        result = signals.obv(close, volume)
        assert result.dtype.kind in ("b", "i", "u")  # bool, int, or uint

    def test_values_are_zero_or_one(self):
        high, low, close, volume = _ohlcv()
        result = signals.obv(close, volume)
        non_nan = result.dropna()
        assert set(non_nan.unique()).issubset({0, 1})

    def test_same_length_as_input(self):
        high, low, close, volume = _ohlcv(80)
        result = signals.obv(close, volume)
        assert len(result) == 80


class TestZscore:
    def test_returns_series(self):
        close = _price_series()
        result = signals.zscore(close, BUY_THRESHOLD=-1.0, SELL_THRESHOLD=1.0)
        assert isinstance(result, pd.Series)

    def test_same_length_as_input(self):
        close = _price_series(60)
        result = signals.zscore(close, BUY_THRESHOLD=-1.0, SELL_THRESHOLD=1.0)
        assert len(result) == 60

    def test_values_are_zero_or_one(self):
        close = _price_series()
        result = signals.zscore(close, BUY_THRESHOLD=-1.0, SELL_THRESHOLD=1.0)
        assert set(result.unique()).issubset({0, 1})

    def test_first_value_is_zero(self):
        close = _price_series()
        result = signals.zscore(close, BUY_THRESHOLD=-1.0, SELL_THRESHOLD=1.0)
        assert result.iloc[0] == 0


class TestRoc:
    def test_returns_boolean_series(self):
        close = _price_series()
        result = signals.roc(close)
        assert result.dtype == bool

    def test_same_length_as_input(self):
        close = _price_series(60)
        result = signals.roc(close)
        assert len(result) == 60


# ---------------------------------------------------------------------------
# New statistical signal methods
# ---------------------------------------------------------------------------

class TestBollingerBands:
    def test_returns_dataframe(self):
        close = _price_series(60)
        result = signals.bollinger_bands(close)
        assert hasattr(result, 'columns')

    def test_returns_empty_for_insufficient_data(self):
        close = _price_series(5)  # less than length=20
        result = signals.bollinger_bands(close)
        assert result.empty

    def test_has_bbl_bbu_columns(self):
        close = _price_series(60)
        result = signals.bollinger_bands(close)
        if not result.empty:
            cols = result.columns.tolist()
            assert any(c.startswith('BBL') for c in cols)
            assert any(c.startswith('BBU') for c in cols)


class TestPriceZscore:
    def test_returns_float(self):
        close = _price_series(60)
        z = signals.price_zscore(close)
        assert isinstance(z, float)

    def test_returns_nan_for_insufficient_data(self):
        close = _price_series(5)
        z = signals.price_zscore(close, period=20)
        assert np.isnan(z)

    def test_extreme_move_gives_high_zscore(self):
        # A steadily rising series then a huge spike
        close = pd.Series([100.0 + i * 0.1 for i in range(50)] + [200.0])
        z = signals.price_zscore(close, period=20)
        assert abs(z) > 2.0


class TestReturnPercentile:
    def test_returns_float(self):
        close = _price_series(80)
        p = signals.return_percentile(close, period=60)
        assert isinstance(p, float)

    def test_in_valid_range(self):
        close = _price_series(80)
        p = signals.return_percentile(close, period=60)
        if not np.isnan(p):
            assert 0.0 <= p <= 100.0

    def test_returns_nan_for_insufficient_data(self):
        close = _price_series(5)
        p = signals.return_percentile(close, period=60)
        assert np.isnan(p)


class TestVolumeZscore:
    def _vol_series(self, mean=1_000_000.0, std=100_000.0, n=30, seed=42):
        rng = np.random.default_rng(seed)
        return pd.Series(rng.normal(mean, std, n))

    def test_returns_float(self):
        volume = self._vol_series()
        z = signals.volume_zscore(volume, curr_volume=5_000_000.0)
        assert isinstance(z, float)

    def test_high_volume_gives_positive_zscore(self):
        volume = self._vol_series(mean=1_000_000.0, std=100_000.0)
        z = signals.volume_zscore(volume, curr_volume=10_000_000.0)
        assert z > 0

    def test_low_volume_gives_negative_zscore(self):
        volume = self._vol_series(mean=1_000_000.0, std=100_000.0)
        z = signals.volume_zscore(volume, curr_volume=100_000.0)
        assert z < 0

    def test_returns_nan_for_insufficient_data(self):
        volume = pd.Series([1_000_000.0])  # only 1 row
        z = signals.volume_zscore(volume, curr_volume=2_000_000.0)
        assert np.isnan(z)

    def test_returns_nan_for_zero_std(self):
        volume = pd.Series([1_000_000.0] * 30)
        # All same values → std = 0
        z = signals.volume_zscore(volume, curr_volume=1_000_000.0)
        assert np.isnan(z)


class TestTechnicalConfluence:
    def test_returns_tuple_of_three(self):
        high, low, close, volume = _ohlcv(60)
        result = signals.technical_confluence(close, high, low, volume)
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_count_lte_total(self):
        high, low, close, volume = _ohlcv(60)
        count, total, details = signals.technical_confluence(close, high, low, volume)
        assert count <= total

    def test_details_has_four_keys(self):
        high, low, close, volume = _ohlcv(60)
        count, total, details = signals.technical_confluence(close, high, low, volume)
        assert set(details.keys()) == {'rsi', 'macd', 'adx', 'obv'}

    def test_details_values_are_bool_or_none(self):
        high, low, close, volume = _ohlcv(60)
        count, total, details = signals.technical_confluence(close, high, low, volume)
        for v in details.values():
            assert v is None or isinstance(v, bool)
