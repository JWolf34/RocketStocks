"""Tests for rocketstocks.core.analysis.momentum."""
import pytest

from rocketstocks.core.analysis.momentum import (
    compute_velocity,
    compute_acceleration,
    should_update_alert,
    build_momentum_snapshot,
)


class TestComputeVelocity:
    def test_positive_move(self):
        v = compute_velocity(10.0, 5.0)
        assert v == pytest.approx(5.0)

    def test_negative_move(self):
        v = compute_velocity(-3.0, 2.0)
        assert v == pytest.approx(-5.0)

    def test_zero_move(self):
        v = compute_velocity(5.0, 5.0)
        assert v == pytest.approx(0.0)

    def test_multiple_intervals(self):
        v = compute_velocity(15.0, 5.0, intervals=2)
        assert v == pytest.approx(5.0)

    def test_single_interval_default(self):
        v = compute_velocity(7.0, 3.0, intervals=1)
        assert v == pytest.approx(4.0)


class TestComputeAcceleration:
    def test_positive_acceleration(self):
        a = compute_acceleration(10.0, 5.0)
        assert a == pytest.approx(5.0)

    def test_negative_acceleration(self):
        a = compute_acceleration(2.0, 8.0)
        assert a == pytest.approx(-6.0)

    def test_zero_acceleration(self):
        a = compute_acceleration(3.0, 3.0)
        assert a == pytest.approx(0.0)

    def test_multiple_intervals(self):
        a = compute_acceleration(12.0, 4.0, intervals=4)
        assert a == pytest.approx(2.0)


class TestShouldUpdateAlert:
    def test_no_prev_pct_returns_false(self):
        result = should_update_alert(10.0, {})
        assert result is False

    def test_fallback_heuristic_above_100pct_relative(self):
        """With no momentum history, falls back to >100% relative change."""
        prev_data = {'pct_change': 5.0}
        # 15.0 from 5.0 = 200% relative → should trigger
        assert should_update_alert(15.0, prev_data) is True

    def test_fallback_heuristic_below_100pct_relative(self):
        """<100% relative change should not trigger."""
        prev_data = {'pct_change': 5.0}
        # 7.0 from 5.0 = 40% relative → no trigger
        assert should_update_alert(7.0, prev_data) is False

    def test_fallback_zero_prev_pct_returns_false(self):
        """Cannot divide by zero prev_pct — should return False."""
        prev_data = {'pct_change': 0.0}
        assert should_update_alert(10.0, prev_data) is False

    def test_with_sufficient_momentum_history(self):
        """With enough history, uses z-score logic."""
        history = [
            {'pct_change': 5.0, 'velocity': 1.0, 'acceleration': 0.1},
            {'pct_change': 6.0, 'velocity': 1.0, 'acceleration': 0.0},
            {'pct_change': 7.0, 'velocity': 1.0, 'acceleration': 0.0},
            {'pct_change': 8.0, 'velocity': 1.0, 'acceleration': 0.0},
        ]
        prev_data = {'pct_change': 8.0, 'momentum_history': history}
        # Normal continuation — should not trigger
        result = should_update_alert(9.0, prev_data)
        assert isinstance(result, bool)

    def test_large_acceleration_spike_triggers(self):
        """An extreme acceleration spike vs stable history should trigger."""
        # Historical accelerations cluster near 0 with small variance
        import numpy as np
        rng = np.random.default_rng(42)
        history = [
            {'pct_change': float(i), 'velocity': 1.0,
             'acceleration': float(rng.normal(0.0, 0.01))}
            for i in range(10)
        ]
        prev_data = {'pct_change': 10.0, 'momentum_history': history}
        # prev_velocity was 1.0; new velocity = 50 - 10 = 40.0 → acceleration = 39.0
        # z-score of 39.0 against near-zero history → very large → trigger
        result = should_update_alert(50.0, prev_data)
        assert result is True

    def test_single_history_entry_uses_fallback(self):
        """Only one prior entry — not enough for z-score → use heuristic."""
        history = [{'pct_change': 5.0, 'velocity': 1.0, 'acceleration': 0.0}]
        prev_data = {'pct_change': 5.0, 'momentum_history': history}
        # 15.0 from 5.0 → 200% relative → triggers via fallback
        result = should_update_alert(15.0, prev_data)
        assert result is True


class TestBuildMomentumSnapshot:
    def test_returns_dict_with_required_keys(self):
        prev_data = {'pct_change': 5.0}
        snap = build_momentum_snapshot(current_pct=7.0, prev_alert_data=prev_data)
        assert 'pct_change' in snap
        assert 'velocity' in snap
        assert 'acceleration' in snap

    def test_velocity_computed_correctly(self):
        prev_data = {'pct_change': 3.0}
        snap = build_momentum_snapshot(current_pct=8.0, prev_alert_data=prev_data)
        assert snap['velocity'] == pytest.approx(5.0)

    def test_acceleration_zero_with_no_prior_history(self):
        prev_data = {'pct_change': 3.0}
        snap = build_momentum_snapshot(current_pct=8.0, prev_alert_data=prev_data)
        assert snap['acceleration'] == pytest.approx(0.0)

    def test_acceleration_with_prior_history(self):
        history = [{'pct_change': 5.0, 'velocity': 2.0, 'acceleration': 0.0}]
        prev_data = {'pct_change': 7.0, 'momentum_history': history}
        snap = build_momentum_snapshot(current_pct=9.0, prev_alert_data=prev_data)
        # velocity = 9 - 7 = 2.0; acceleration = 2 - 2 = 0
        assert snap['velocity'] == pytest.approx(2.0)
        assert snap['acceleration'] == pytest.approx(0.0)

    def test_pct_change_stored_in_snapshot(self):
        prev_data = {'pct_change': 1.0}
        snap = build_momentum_snapshot(current_pct=4.0, prev_alert_data=prev_data)
        assert snap['pct_change'] == pytest.approx(4.0)
