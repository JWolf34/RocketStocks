"""Tests for rocketstocks.core.analysis.volume_divergence."""
import math
import pytest

from rocketstocks.core.analysis.volume_divergence import (
    VolumeAccumulationResult,
    evaluate_volume_accumulation,
)


class TestEvaluateVolumeAccumulation:

    def test_detects_accumulation_when_vol_high_price_flat(self):
        result = evaluate_volume_accumulation(vol_zscore=2.5, price_zscore=0.3, rvol=3.0)
        assert result.is_accumulating is True

    def test_no_accumulation_when_vol_below_threshold(self):
        result = evaluate_volume_accumulation(vol_zscore=1.5, price_zscore=0.3, rvol=2.0)
        assert result.is_accumulating is False

    def test_no_accumulation_when_price_moving(self):
        result = evaluate_volume_accumulation(vol_zscore=3.0, price_zscore=1.5, rvol=4.0)
        assert result.is_accumulating is False

    def test_no_accumulation_when_price_exactly_at_ceiling(self):
        # abs(price_z) must be strictly less than price_ceiling
        result = evaluate_volume_accumulation(vol_zscore=2.5, price_zscore=1.0, rvol=3.0)
        assert result.is_accumulating is False

    def test_accumulation_just_at_vol_threshold(self):
        result = evaluate_volume_accumulation(vol_zscore=2.5, price_zscore=0.0, rvol=2.5)
        assert result.is_accumulating is True

    def test_divergence_score_calculated_correctly(self):
        result = evaluate_volume_accumulation(vol_zscore=3.0, price_zscore=0.5, rvol=3.0)
        assert result.divergence_score == pytest.approx(3.0 - abs(0.5))

    def test_divergence_score_negative_price_z(self):
        result = evaluate_volume_accumulation(vol_zscore=2.5, price_zscore=-0.8, rvol=2.0)
        assert result.divergence_score == pytest.approx(2.5 - 0.8)

    def test_initial_signal_strength_is_volume_only(self):
        result = evaluate_volume_accumulation(vol_zscore=2.5, price_zscore=0.3, rvol=3.0)
        assert result.signal_strength == 'volume_only'

    def test_options_flow_initially_none(self):
        result = evaluate_volume_accumulation(vol_zscore=2.5, price_zscore=0.3, rvol=3.0)
        assert result.options_flow is None

    def test_nan_vol_zscore_returns_not_accumulating(self):
        result = evaluate_volume_accumulation(
            vol_zscore=float('nan'), price_zscore=0.3, rvol=3.0
        )
        assert result.is_accumulating is False
        assert math.isnan(result.divergence_score)

    def test_nan_price_zscore_returns_not_accumulating(self):
        result = evaluate_volume_accumulation(
            vol_zscore=2.5, price_zscore=float('nan'), rvol=3.0
        )
        assert result.is_accumulating is False

    def test_nan_rvol_returns_not_accumulating(self):
        result = evaluate_volume_accumulation(
            vol_zscore=2.5, price_zscore=0.3, rvol=float('nan')
        )
        assert result.is_accumulating is False

    def test_custom_thresholds_respected(self):
        # With higher vol_threshold, same z-score shouldn't trigger
        result = evaluate_volume_accumulation(
            vol_zscore=2.5, price_zscore=0.3, rvol=3.0,
            vol_threshold=3.0,
        )
        assert result.is_accumulating is False

    def test_custom_price_ceiling_respected(self):
        # With wider price_ceiling, a moderate price move can still qualify.
        # Pass min_divergence=0.0 to isolate the price_ceiling test (divergence=1.2 would
        # otherwise fail the default min_divergence=1.5 gate).
        result = evaluate_volume_accumulation(
            vol_zscore=2.5, price_zscore=1.3, rvol=3.0,
            price_ceiling=2.0, min_divergence=0.0,
        )
        assert result.is_accumulating is True

    def test_result_fields_preserved(self):
        result = evaluate_volume_accumulation(vol_zscore=2.8, price_zscore=0.4, rvol=3.5)
        assert result.vol_zscore == pytest.approx(2.8)
        assert result.price_zscore == pytest.approx(0.4)
        assert result.rvol == pytest.approx(3.5)

    def test_signal_strength_can_be_mutated_to_volume_plus_options(self):
        """Signal strength starts as volume_only and can be updated externally."""
        result = evaluate_volume_accumulation(vol_zscore=2.5, price_zscore=0.3, rvol=3.0)
        result.signal_strength = 'volume_plus_options'
        assert result.signal_strength == 'volume_plus_options'

    def test_no_accumulation_when_divergence_below_min(self):
        # vol_z=2.6, price_z=0.95 → divergence=1.65 >= default 1.5: accumulates
        result = evaluate_volume_accumulation(vol_zscore=2.6, price_zscore=0.95, rvol=3.0)
        assert result.is_accumulating is True
        # Same inputs with min_divergence=2.0: divergence=1.65 < 2.0 → does not accumulate
        result2 = evaluate_volume_accumulation(
            vol_zscore=2.6, price_zscore=0.95, rvol=3.0, min_divergence=2.0
        )
        assert result2.is_accumulating is False

    def test_custom_min_divergence_respected(self):
        # vol_z=3.0, price_z=0.8 → divergence=2.2
        # min_divergence=2.5: 2.2 < 2.5 → should not accumulate
        result = evaluate_volume_accumulation(
            vol_zscore=3.0, price_zscore=0.8, rvol=3.0, min_divergence=2.5
        )
        assert result.is_accumulating is False
        # min_divergence=2.0: 2.2 >= 2.0 → should accumulate
        result2 = evaluate_volume_accumulation(
            vol_zscore=3.0, price_zscore=0.8, rvol=3.0, min_divergence=2.0
        )
        assert result2.is_accumulating is True
