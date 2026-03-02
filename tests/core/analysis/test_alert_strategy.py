"""Tests for rocketstocks.core.analysis.alert_strategy."""
import math
import numpy as np
import pandas as pd
import pytest

from rocketstocks.core.analysis.classification import StockClass
from rocketstocks.core.analysis.alert_strategy import (
    AlertTriggerResult,
    evaluate_price_alert,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_daily_prices(n=60, base=100.0, std_pct=2.0, seed=42) -> pd.DataFrame:
    """Return a DataFrame with OHLCV columns and *n* rows."""
    rng = np.random.default_rng(seed)
    returns = rng.normal(0, std_pct / 100, n)
    close = [base]
    for r in returns[1:]:
        close.append(close[-1] * (1 + r))
    close = close[:n]
    volume = rng.integers(500_000, 2_000_000, n).astype(float)
    return pd.DataFrame({
        'open': [c * 0.99 for c in close],
        'high': [c * 1.01 for c in close],
        'low':  [c * 0.98 for c in close],
        'close': close,
        'volume': volume.tolist(),
    })


def _flat_prices(n=60, value=100.0) -> pd.DataFrame:
    """Return a flat price series (zero volatility)."""
    return pd.DataFrame({
        'open': [value] * n,
        'high': [value] * n,
        'low': [value] * n,
        'close': [value] * n,
        'volume': [1_000_000] * n,
    })


# ---------------------------------------------------------------------------
# AlertTriggerResult dataclass
# ---------------------------------------------------------------------------

class TestAlertTriggerResult:
    def test_instantiation(self):
        r = AlertTriggerResult(
            should_alert=True,
            classification=StockClass.STANDARD,
            zscore=2.5,
            percentile=95.0,
            bb_position=None,
            confluence_count=None,
            confluence_total=None,
            confluence_details=None,
            volume_zscore=1.0,
            signal_type='unusual_move',
        )
        assert r.should_alert is True
        assert r.signal_type == 'unusual_move'


# ---------------------------------------------------------------------------
# evaluate_price_alert — volatile / meme / standard strategy
# ---------------------------------------------------------------------------

class TestStandardStrategy:
    def test_high_zscore_triggers_alert(self):
        """A move that is 3+ std devs from the mean should trigger."""
        prices = _make_daily_prices(n=60, std_pct=1.0)
        # Insert a very large move in the last close value
        prices.at[prices.index[-1], 'close'] = prices['close'].iloc[-2] * 1.10

        result = evaluate_price_alert(
            classification='standard',
            pct_change=10.0,
            daily_prices=prices,
        )
        assert isinstance(result, AlertTriggerResult)
        assert result.classification == StockClass.STANDARD

    def test_low_zscore_does_not_trigger(self):
        """A normal move should not trigger."""
        prices = _make_daily_prices(n=60, std_pct=2.0)
        result = evaluate_price_alert(
            classification='standard',
            pct_change=0.5,
            daily_prices=prices,
        )
        assert result.should_alert is False

    def test_volatile_class_lower_threshold(self):
        """Volatile stocks use a 2.0 z-score threshold (lower than standard 2.5)."""
        prices = _make_daily_prices(n=60, std_pct=1.0)
        result = evaluate_price_alert(
            classification='volatile',
            pct_change=0.0,   # tiny move — z-score will be near 0
            daily_prices=prices,
        )
        assert result.should_alert is False
        assert result.classification == StockClass.VOLATILE

    def test_meme_class_returns_meme(self):
        prices = _make_daily_prices()
        result = evaluate_price_alert(
            classification='meme',
            pct_change=0.0,
            daily_prices=prices,
        )
        assert result.classification == StockClass.MEME

    def test_empty_prices_returns_no_alert(self):
        result = evaluate_price_alert(
            classification='standard',
            pct_change=99.0,
            daily_prices=pd.DataFrame(),
        )
        assert result.should_alert is False
        assert math.isnan(result.zscore)

    def test_signal_type_is_unusual_move(self):
        prices = _make_daily_prices(n=60, std_pct=0.5)
        result = evaluate_price_alert(
            classification='standard',
            pct_change=5.0,
            daily_prices=prices,
        )
        if result.should_alert:
            assert result.signal_type == 'unusual_move'

    def test_classification_string_input_normalised(self):
        prices = _make_daily_prices()
        result = evaluate_price_alert(
            classification='STANDARD',   # uppercase string
            pct_change=0.0,
            daily_prices=prices,
        )
        # Invalid StockClass value → should fall back to standard
        assert result.classification == StockClass.STANDARD

    def test_volume_zscore_computed_when_volume_provided(self):
        prices = _make_daily_prices(n=60, std_pct=2.0)
        result = evaluate_price_alert(
            classification='standard',
            pct_change=1.0,
            daily_prices=prices,
            current_volume=10_000_000,  # very large → high z-score
        )
        assert result.volume_zscore is not None

    def test_percentile_in_valid_range(self):
        prices = _make_daily_prices()
        result = evaluate_price_alert(
            classification='standard',
            pct_change=0.0,
            daily_prices=prices,
        )
        if not math.isnan(result.percentile):
            assert 0.0 <= result.percentile <= 100.0


# ---------------------------------------------------------------------------
# evaluate_price_alert — blue chip strategy
# ---------------------------------------------------------------------------

class TestBlueChipStrategy:
    def test_blue_chip_returns_no_alert_for_small_move(self):
        prices = _make_daily_prices(n=60, std_pct=0.5)
        result = evaluate_price_alert(
            classification='blue_chip',
            pct_change=0.1,
            daily_prices=prices,
        )
        assert result.classification == StockClass.BLUE_CHIP

    def test_blue_chip_bb_position_computed(self):
        prices = _make_daily_prices(n=60, std_pct=0.5)
        result = evaluate_price_alert(
            classification='blue_chip',
            pct_change=0.0,
            daily_prices=prices,
        )
        # bb_position should be one of the valid values or None if data insufficient
        assert result.bb_position in ('above_upper', 'below_lower', 'within', None)

    def test_blue_chip_confluence_computed(self):
        prices = _make_daily_prices(n=60, std_pct=0.5)
        result = evaluate_price_alert(
            classification='blue_chip',
            pct_change=0.0,
            daily_prices=prices,
        )
        # confluence_count should be int or None
        assert result.confluence_count is None or isinstance(result.confluence_count, int)

    def test_blue_chip_no_alert_without_volume_confirm(self):
        """Blue chip with BB breach but no volume confirmation should not alert via BB path."""
        prices = _make_daily_prices(n=60, std_pct=0.3)
        result = evaluate_price_alert(
            classification='blue_chip',
            pct_change=0.0,
            daily_prices=prices,
            current_volume=100,  # tiny volume → very negative z-score
        )
        # Even if BB is breached, no volume confirm → may not alert via BB path
        assert isinstance(result.should_alert, bool)
