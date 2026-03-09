"""Tests for rocketstocks.core.analysis.composite_score."""
import math

import pytest

from rocketstocks.core.analysis.alert_strategy import AlertTriggerResult
from rocketstocks.core.analysis.classification import StockClass
from rocketstocks.core.analysis.composite_score import (
    DEFAULT_COMPOSITE_THRESHOLD,
    CompositeScoreResult,
    _W_CROSS_SIGNAL,
    _W_PRICE,
    _W_VOLUME,
    _GATE_PRICE_MIN,
    _GATE_VOL_MIN,
    _GATE_VOL_EXTREME,
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
# Weights
# ---------------------------------------------------------------------------

def test_weights_sum_to_one():
    """All component weights must sum to 1.0."""
    total = _W_VOLUME + _W_PRICE + _W_CROSS_SIGNAL
    assert total == pytest.approx(1.0)


def test_volume_weight_is_50_percent():
    assert _W_VOLUME == pytest.approx(0.50)


def test_price_weight_is_35_percent():
    assert _W_PRICE == pytest.approx(0.35)


def test_cross_signal_weight_is_15_percent():
    assert _W_CROSS_SIGNAL == pytest.approx(0.15)


# ---------------------------------------------------------------------------
# Dual-gate
# ---------------------------------------------------------------------------

def test_dual_gate_fails_low_both():
    """Both z-scores below gate thresholds → no alert immediately."""
    trigger = _make_trigger(volume_zscore=1.0, zscore=1.0)
    result = compute_composite_score(trigger)
    assert result.should_alert is False
    assert result.composite_score == pytest.approx(0.0)


def test_dual_gate_passes_price_and_vol_both_at_min():
    """Both price_z and vol_z >= 1.5 → gate passes."""
    trigger = _make_trigger(volume_zscore=_GATE_VOL_MIN, zscore=_GATE_PRICE_MIN)
    result = compute_composite_score(trigger)
    # Gate passes; actual alert depends on score vs threshold
    assert result.composite_score > 0.0


def test_dual_gate_passes_extreme_vol_alone():
    """vol_z >= 4.0 alone passes gate even without price movement."""
    trigger = _make_trigger(volume_zscore=_GATE_VOL_EXTREME, zscore=0.5)
    result = compute_composite_score(trigger)
    assert result.composite_score > 0.0


def test_dual_gate_fails_only_price_high():
    """High price but low volume → gate fails."""
    trigger = _make_trigger(volume_zscore=0.5, zscore=4.0)
    result = compute_composite_score(trigger)
    assert result.should_alert is False
    assert result.composite_score == pytest.approx(0.0)


def test_dual_gate_fails_only_vol_moderate():
    """vol_z=2.0, price_z=0.5 → vol meets price-pair threshold but price doesn't → gate fails."""
    trigger = _make_trigger(volume_zscore=2.0, zscore=0.5)
    result = compute_composite_score(trigger)
    assert result.should_alert is False


# ---------------------------------------------------------------------------
# Threshold and should_alert
# ---------------------------------------------------------------------------

def test_high_scores_exceed_threshold():
    """vol_z=4, price_z=3 → gate passes and composite easily above 2.5."""
    trigger = _make_trigger(volume_zscore=4.0, zscore=3.0)
    result = compute_composite_score(trigger)
    assert result.should_alert is True
    assert result.composite_score > DEFAULT_COMPOSITE_THRESHOLD


def test_below_threshold_no_alert_after_gate_passes():
    """Gate passes but score below threshold → no alert."""
    # vol_z=1.5, price_z=1.5 → gate passes, but score = 0.50*1.5 + 0.35*1.5 = 0.75+0.525 = 1.275 < 2.5
    trigger = _make_trigger(volume_zscore=1.5, zscore=1.5)
    result = compute_composite_score(trigger)
    assert result.should_alert is False


def test_custom_threshold_raises_bar():
    """Custom threshold of 4.0 → same inputs no longer trigger."""
    trigger = _make_trigger(volume_zscore=4.0, zscore=2.0)
    result = compute_composite_score(trigger, threshold=4.0)
    assert result.should_alert is False


# ---------------------------------------------------------------------------
# Classification component always zero
# ---------------------------------------------------------------------------

def test_classification_component_always_zero():
    """Classification no longer contributes — always 0.0."""
    for cls in [StockClass.MEME, StockClass.VOLATILE, StockClass.BLUE_CHIP, StockClass.STANDARD]:
        trigger = _make_trigger(volume_zscore=2.0, zscore=2.0, classification=cls)
        result = compute_composite_score(trigger)
        assert result.classification_component == pytest.approx(0.0), f"Failed for {cls}"


def test_meme_and_standard_same_score():
    """MEME and STANDARD stocks produce the same composite score now."""
    t_meme = _make_trigger(volume_zscore=3.0, zscore=2.0, classification=StockClass.MEME)
    t_std = _make_trigger(volume_zscore=3.0, zscore=2.0, classification=StockClass.STANDARD)
    assert compute_composite_score(t_meme).composite_score == pytest.approx(
        compute_composite_score(t_std).composite_score
    )


# ---------------------------------------------------------------------------
# Component breakdown
# ---------------------------------------------------------------------------

def test_volume_component_is_abs_volume_zscore():
    trigger = _make_trigger(volume_zscore=-3.5, zscore=2.0)  # gate passes
    result = compute_composite_score(trigger)
    assert result.volume_component == pytest.approx(3.5)


def test_price_component_is_abs_price_zscore():
    trigger = _make_trigger(volume_zscore=4.0, zscore=-2.0)  # gate passes via extreme vol
    result = compute_composite_score(trigger)
    assert result.price_component == pytest.approx(2.0)


def test_cross_signal_component_with_confluence():
    """3/4 confluence → (3/4)*4.0 = 3.0."""
    trigger = _make_trigger(volume_zscore=2.0, zscore=2.0, confluence_count=3, confluence_total=4)
    result = compute_composite_score(trigger)
    assert result.cross_signal_component == pytest.approx(3.0)


def test_cross_signal_component_zero_when_none():
    trigger = _make_trigger(volume_zscore=2.0, zscore=2.0, confluence_count=None, confluence_total=None)
    result = compute_composite_score(trigger)
    assert result.cross_signal_component == pytest.approx(0.0)


def test_cross_signal_component_zero_when_total_zero():
    trigger = _make_trigger(volume_zscore=2.0, zscore=2.0, confluence_count=0, confluence_total=0)
    result = compute_composite_score(trigger)
    assert result.cross_signal_component == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# NaN handling
# ---------------------------------------------------------------------------

def test_nan_volume_zscore_treated_as_zero():
    trigger = _make_trigger(volume_zscore=float('nan'), zscore=3.0)
    result = compute_composite_score(trigger)
    assert result.volume_component == pytest.approx(0.0)
    assert not math.isnan(result.composite_score)


def test_nan_price_zscore_treated_as_zero():
    trigger = _make_trigger(volume_zscore=5.0, zscore=float('nan'))
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
    """Volume clearly dominant — gate passes via extreme vol."""
    # vol_weighted = 0.50 * 5.0 = 2.5, price_weighted = 0.35 * 1.0 = 0.35 → ratio=7.1 ≥ 1.5
    trigger = _make_trigger(volume_zscore=5.0, zscore=1.0)  # extreme vol gate
    result = compute_composite_score(trigger)
    assert result.dominant_signal == 'volume'


def test_dominant_signal_price():
    """Price clearly dominant."""
    # vol_weighted = 0.50 * 1.5 = 0.75, price_weighted = 0.35 * 5.0 = 1.75 → ratio=0.43 ≤ 0.67
    trigger = _make_trigger(volume_zscore=1.5, zscore=5.0)
    result = compute_composite_score(trigger)
    assert result.dominant_signal == 'price'


def test_dominant_signal_mixed():
    """Similar weighted contributions → 'mixed'."""
    # vol_weighted = 0.50 * 3.0 = 1.5, price_weighted = 0.35 * 3.0 = 1.05 → ratio=1.43 (between 0.67-1.5)
    trigger = _make_trigger(volume_zscore=3.0, zscore=3.0)
    result = compute_composite_score(trigger)
    assert result.dominant_signal == 'mixed'


def test_dominant_signal_both_zero_is_mixed():
    """Gate fails; returned result has dominant_signal='mixed'."""
    trigger = _make_trigger(volume_zscore=0.0, zscore=0.0)
    result = compute_composite_score(trigger)
    assert result.dominant_signal == 'mixed'


def test_dominant_signal_price_zero_volume_nonzero():
    """Price zero but volume extreme → 'volume'."""
    trigger = _make_trigger(volume_zscore=4.0, zscore=0.0)
    result = compute_composite_score(trigger)
    assert result.dominant_signal == 'volume'


# ---------------------------------------------------------------------------
# Formula verification
# ---------------------------------------------------------------------------

def test_composite_formula_manual_calculation():
    """Manually verify the weighted sum formula (no classification term)."""
    vol_z = 4.0
    price_z = 2.0
    conf_count = 2
    conf_total = 4
    # cross = (2/4)*4.0 = 2.0
    trigger = _make_trigger(
        volume_zscore=vol_z, zscore=price_z,
        confluence_count=conf_count, confluence_total=conf_total,
    )
    result = compute_composite_score(trigger)
    expected = (
        _W_VOLUME * 4.0          # 0.50 * 4.0 = 2.00
        + _W_PRICE * 2.0         # 0.35 * 2.0 = 0.70
        + _W_CROSS_SIGNAL * 2.0  # 0.15 * 2.0 = 0.30
    )  # = 3.00
    assert result.composite_score == pytest.approx(expected)


def test_trigger_result_preserved_in_output():
    """CompositeScoreResult keeps a reference to the input trigger_result."""
    trigger = _make_trigger(volume_zscore=4.0, zscore=2.0)
    result = compute_composite_score(trigger)
    assert result.trigger_result is trigger


def test_returns_composite_score_result_type():
    trigger = _make_trigger()
    result = compute_composite_score(trigger)
    assert isinstance(result, CompositeScoreResult)


# ---------------------------------------------------------------------------
# Gate edge cases with NaN
# ---------------------------------------------------------------------------

def test_gate_with_nan_vol_z_fails():
    """NaN vol_z → treated as 0.0 → gate fails when price is also insufficient."""
    trigger = _make_trigger(volume_zscore=float('nan'), zscore=3.0)
    result = compute_composite_score(trigger)
    # vol=0.0, price=3.0 → neither condition met
    assert result.should_alert is False
    assert result.composite_score == pytest.approx(0.0)


def test_gate_with_nan_price_z_passes_extreme_vol():
    """NaN price_z + extreme vol → gate passes via vol_z >= 4.0."""
    trigger = _make_trigger(volume_zscore=5.0, zscore=float('nan'))
    result = compute_composite_score(trigger)
    assert result.composite_score > 0.0
