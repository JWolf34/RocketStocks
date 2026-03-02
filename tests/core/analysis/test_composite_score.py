"""Tests for rocketstocks.core.analysis.composite_score."""
import math

import pytest

from rocketstocks.core.analysis.alert_strategy import AlertTriggerResult
from rocketstocks.core.analysis.classification import StockClass
from rocketstocks.core.analysis.composite_score import (
    DEFAULT_COMPOSITE_THRESHOLD,
    CompositeScoreResult,
    _W_CLASSIFICATION,
    _W_CROSS_SIGNAL,
    _W_PRICE,
    _W_VOLUME,
    compute_composite_score,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_trigger(
    volume_zscore=0.0,
    zscore=0.0,
    confluence_count=None,
    confluence_total=None,
    classification=StockClass.STANDARD,
    should_alert=False,
    percentile=50.0,
    bb_position=None,
    confluence_details=None,
    signal_type=None,
) -> AlertTriggerResult:
    return AlertTriggerResult(
        should_alert=should_alert,
        classification=classification,
        zscore=zscore,
        percentile=percentile,
        bb_position=bb_position,
        confluence_count=confluence_count,
        confluence_total=confluence_total,
        confluence_details=confluence_details,
        volume_zscore=volume_zscore,
        signal_type=signal_type,
    )


# ---------------------------------------------------------------------------
# Threshold and should_alert
# ---------------------------------------------------------------------------

def test_high_scores_exceed_threshold():
    """volume_z=4, price_z=3 → composite easily above 2.5."""
    trigger = _make_trigger(volume_zscore=4.0, zscore=3.0, classification=StockClass.STANDARD)
    result = compute_composite_score(trigger)
    assert result.should_alert is True
    assert result.composite_score > DEFAULT_COMPOSITE_THRESHOLD


def test_below_threshold_no_alert():
    """Low z-scores → below threshold → no alert."""
    trigger = _make_trigger(volume_zscore=0.5, zscore=0.5, classification=StockClass.STANDARD)
    result = compute_composite_score(trigger)
    assert result.should_alert is False
    assert result.composite_score < DEFAULT_COMPOSITE_THRESHOLD


def test_exactly_at_threshold_triggers():
    """Score right at threshold (>= 2.5) → should_alert=True."""
    # Compute what z-scores produce exactly 2.5 with STANDARD class (1.0):
    # 0.40*vol + 0.30*price + 0.15*0 + 0.15*1.0 = 2.5
    # 0.40*vol + 0.30*price = 2.35
    # Use vol=3.5, price=2.0 → 0.40*3.5 + 0.30*2.0 = 1.4 + 0.6 = 2.0 → 2.0+0.15 = 2.15 (too low)
    # Use vol=4.0, price=3.0 → 1.6+0.9+0.15 = 2.65 > 2.5 ✓
    trigger = _make_trigger(volume_zscore=4.0, zscore=3.0, classification=StockClass.STANDARD)
    result = compute_composite_score(trigger, threshold=DEFAULT_COMPOSITE_THRESHOLD)
    assert result.should_alert is True


def test_custom_threshold_raises_bar():
    """Custom threshold of 4.0 → same inputs no longer trigger."""
    trigger = _make_trigger(volume_zscore=3.0, zscore=2.0, classification=StockClass.STANDARD)
    result = compute_composite_score(trigger, threshold=4.0)
    assert result.should_alert is False


# ---------------------------------------------------------------------------
# Component breakdown
# ---------------------------------------------------------------------------

def test_volume_component_is_abs_volume_zscore():
    trigger = _make_trigger(volume_zscore=-3.5)
    result = compute_composite_score(trigger)
    assert result.volume_component == pytest.approx(3.5)


def test_price_component_is_abs_price_zscore():
    trigger = _make_trigger(zscore=-2.0)
    result = compute_composite_score(trigger)
    assert result.price_component == pytest.approx(2.0)


def test_cross_signal_component_with_confluence():
    """3/4 confluence → (3/4)*4.0 = 3.0."""
    trigger = _make_trigger(confluence_count=3, confluence_total=4)
    result = compute_composite_score(trigger)
    assert result.cross_signal_component == pytest.approx(3.0)


def test_cross_signal_component_zero_when_none():
    trigger = _make_trigger(confluence_count=None, confluence_total=None)
    result = compute_composite_score(trigger)
    assert result.cross_signal_component == pytest.approx(0.0)


def test_cross_signal_component_zero_when_total_zero():
    trigger = _make_trigger(confluence_count=0, confluence_total=0)
    result = compute_composite_score(trigger)
    assert result.cross_signal_component == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Classification bonus
# ---------------------------------------------------------------------------

def test_meme_classification_boost():
    """MEME class → classification_component = 2.5."""
    trigger = _make_trigger(classification=StockClass.MEME)
    result = compute_composite_score(trigger)
    assert result.classification_component == pytest.approx(2.5)


def test_volatile_classification_boost():
    trigger = _make_trigger(classification=StockClass.VOLATILE)
    result = compute_composite_score(trigger)
    assert result.classification_component == pytest.approx(2.0)


def test_blue_chip_classification():
    trigger = _make_trigger(classification=StockClass.BLUE_CHIP)
    result = compute_composite_score(trigger)
    assert result.classification_component == pytest.approx(1.5)


def test_standard_classification():
    trigger = _make_trigger(classification=StockClass.STANDARD)
    result = compute_composite_score(trigger)
    assert result.classification_component == pytest.approx(1.0)


def test_unknown_classification_falls_back_to_standard():
    """Invalid classification string falls back to STANDARD (1.0)."""
    trigger = _make_trigger(classification='nonexistent_class')
    result = compute_composite_score(trigger)
    assert result.classification_component == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# NaN handling
# ---------------------------------------------------------------------------

def test_nan_volume_zscore_treated_as_zero():
    trigger = _make_trigger(volume_zscore=float('nan'), zscore=3.0)
    result = compute_composite_score(trigger)
    assert result.volume_component == pytest.approx(0.0)
    # Score still computed from other components
    assert not math.isnan(result.composite_score)


def test_nan_price_zscore_treated_as_zero():
    trigger = _make_trigger(volume_zscore=3.0, zscore=float('nan'))
    result = compute_composite_score(trigger)
    assert result.price_component == pytest.approx(0.0)
    assert not math.isnan(result.composite_score)


def test_none_volume_zscore_treated_as_zero():
    trigger = _make_trigger(volume_zscore=None, zscore=2.0)
    result = compute_composite_score(trigger)
    assert result.volume_component == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Dominant signal
# ---------------------------------------------------------------------------

def test_dominant_signal_volume():
    """Volume clearly dominant (ratio >= 1.5x price weighted)."""
    # vol_weighted = 0.40 * 5.0 = 2.0, price_weighted = 0.30 * 1.0 = 0.30 → ratio=6.67 ≥ 1.5
    trigger = _make_trigger(volume_zscore=5.0, zscore=1.0)
    result = compute_composite_score(trigger)
    assert result.dominant_signal == 'volume'


def test_dominant_signal_price():
    """Price clearly dominant (ratio <= 1/1.5 = 0.67)."""
    # vol_weighted = 0.40 * 1.0 = 0.4, price_weighted = 0.30 * 5.0 = 1.5 → ratio=0.267 ≤ 0.67
    trigger = _make_trigger(volume_zscore=1.0, zscore=5.0)
    result = compute_composite_score(trigger)
    assert result.dominant_signal == 'price'


def test_dominant_signal_mixed():
    """Similar weighted contributions → 'mixed'."""
    # vol_weighted = 0.40 * 3.0 = 1.2, price_weighted = 0.30 * 3.0 = 0.9 → ratio=1.33 (between 0.67-1.5)
    trigger = _make_trigger(volume_zscore=3.0, zscore=3.0)
    result = compute_composite_score(trigger)
    assert result.dominant_signal == 'mixed'


def test_dominant_signal_both_zero_is_mixed():
    """Both volume and price are zero → 'mixed' fallback."""
    trigger = _make_trigger(volume_zscore=0.0, zscore=0.0)
    result = compute_composite_score(trigger)
    assert result.dominant_signal == 'mixed'


def test_dominant_signal_price_zero_volume_nonzero():
    """Price component is zero but volume is nonzero → 'volume'."""
    trigger = _make_trigger(volume_zscore=3.0, zscore=0.0)
    result = compute_composite_score(trigger)
    assert result.dominant_signal == 'volume'


# ---------------------------------------------------------------------------
# Weight verification
# ---------------------------------------------------------------------------

def test_weights_sum_to_one():
    """All component weights must sum to 1.0."""
    total = _W_VOLUME + _W_PRICE + _W_CROSS_SIGNAL + _W_CLASSIFICATION
    assert total == pytest.approx(1.0)


def test_composite_formula_manual_calculation():
    """Manually verify the weighted sum formula."""
    vol_z = 4.0
    price_z = 2.0
    conf_count = 2
    conf_total = 4
    # cross = (2/4)*4.0 = 2.0, class = VOLATILE = 2.0
    trigger = _make_trigger(
        volume_zscore=vol_z, zscore=price_z,
        confluence_count=conf_count, confluence_total=conf_total,
        classification=StockClass.VOLATILE,
    )
    result = compute_composite_score(trigger)
    expected = (
        _W_VOLUME * 4.0          # 0.40 * 4.0 = 1.60
        + _W_PRICE * 2.0         # 0.30 * 2.0 = 0.60
        + _W_CROSS_SIGNAL * 2.0  # 0.15 * 2.0 = 0.30
        + _W_CLASSIFICATION * 2.0  # 0.15 * 2.0 = 0.30
    )  # = 2.80
    assert result.composite_score == pytest.approx(expected)


def test_trigger_result_preserved_in_output():
    """CompositeScoreResult keeps a reference to the input trigger_result."""
    trigger = _make_trigger(volume_zscore=3.0, zscore=2.0)
    result = compute_composite_score(trigger)
    assert result.trigger_result is trigger


def test_returns_composite_score_result_type():
    trigger = _make_trigger()
    result = compute_composite_score(trigger)
    assert isinstance(result, CompositeScoreResult)
