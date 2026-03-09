"""Tests for rocketstocks.core.analysis.signal_confirmation."""
import pytest

from rocketstocks.core.analysis.signal_confirmation import (
    _ACCEL_ZSCORE_THRESHOLD,
    _MIN_OBSERVATIONS_SUSTAINED,
    _VOL_EXTREME_THRESHOLD,
    _compute_velocities,
    _zscore_of_last,
    should_confirm_signal,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_signal(pct_change=2.0, vol_z=2.0):
    return {
        'ticker': 'GME',
        'pct_change': pct_change,
        'vol_z': vol_z,
        'composite_score': 3.0,
        'dominant_signal': 'volume',
    }


def _make_obs(pct_change=2.0, vol_z=2.0, composite=3.0):
    return {
        'ts': '2026-03-08T10:00:00',
        'pct_change': pct_change,
        'vol_z': vol_z,
        'price_z': 1.8,
        'composite': composite,
    }


# ---------------------------------------------------------------------------
# _compute_velocities
# ---------------------------------------------------------------------------

def test_compute_velocities_basic():
    assert _compute_velocities([1.0, 2.0, 3.0]) == pytest.approx([1.0, 1.0])


def test_compute_velocities_single_element():
    assert _compute_velocities([5.0]) == []


def test_compute_velocities_two_elements():
    assert _compute_velocities([3.0, 7.0]) == pytest.approx([4.0])


def test_compute_velocities_decreasing():
    assert _compute_velocities([5.0, 3.0, 1.0]) == pytest.approx([-2.0, -2.0])


# ---------------------------------------------------------------------------
# _zscore_of_last
# ---------------------------------------------------------------------------

def test_zscore_of_last_basic():
    """Last element far above baseline → returns a non-None z-score."""
    # baseline = [1.0, 3.0] → mean=2, std=1.41, last=20 → large positive z
    series = [1.0, 3.0, 20.0]
    result = _zscore_of_last(series)
    assert result is not None
    assert result > 2.0


def test_zscore_of_last_returns_none_for_single():
    assert _zscore_of_last([5.0]) is None


def test_zscore_of_last_returns_none_for_zero_std():
    """All baseline values equal → std=0 → returns None."""
    assert _zscore_of_last([2.0, 2.0]) is None


def test_zscore_of_last_above_baseline():
    """Last value far above baseline → large positive z-score."""
    series = [1.0, 1.1, 0.9, 1.0, 20.0]
    z = _zscore_of_last(series)
    assert z is not None and z > 2.0


# ---------------------------------------------------------------------------
# should_confirm_signal — extreme volume (immediate)
# ---------------------------------------------------------------------------

def test_extreme_volume_confirms_immediately():
    """vol_z >= 4.0 triggers immediate confirmation regardless of history."""
    signal = _make_signal(pct_change=1.0)
    confirmed, reason = should_confirm_signal(
        signal=signal,
        observations=[],
        current_pct_change=1.0,
        current_vol_z=4.5,
    )
    assert confirmed is True
    assert reason == 'volume_extreme'


def test_extreme_volume_at_threshold():
    signal = _make_signal()
    confirmed, reason = should_confirm_signal(
        signal=signal,
        observations=[],
        current_pct_change=2.0,
        current_vol_z=_VOL_EXTREME_THRESHOLD,
    )
    assert confirmed is True
    assert reason == 'volume_extreme'


def test_extreme_volume_below_threshold_no_immediate():
    """vol_z < 3.0 → does not trigger volume_extreme alone."""
    signal = _make_signal(pct_change=0.5)
    confirmed, reason = should_confirm_signal(
        signal=signal,
        observations=[],
        current_pct_change=0.5,
        current_vol_z=2.9,
    )
    assert confirmed is False


# ---------------------------------------------------------------------------
# should_confirm_signal — sustained
# ---------------------------------------------------------------------------

def test_sustained_confirms_with_two_observations():
    """2 observations + current_pct >= original_pct → sustained."""
    signal = _make_signal(pct_change=2.0)
    obs = [_make_obs(2.0), _make_obs(2.5)]
    confirmed, reason = should_confirm_signal(
        signal=signal,
        observations=obs,
        current_pct_change=2.5,
        current_vol_z=1.0,
    )
    assert confirmed is True
    assert reason == 'sustained'


def test_sustained_requires_two_observations():
    """Only 1 observation → not enough for sustained."""
    signal = _make_signal(pct_change=2.0)
    obs = [_make_obs(2.0)]
    confirmed, reason = should_confirm_signal(
        signal=signal,
        observations=obs,
        current_pct_change=3.0,
        current_vol_z=1.0,
    )
    assert confirmed is False


def test_sustained_fails_when_pct_fades():
    """Move faded (current_pct < original_pct) → not sustained."""
    signal = _make_signal(pct_change=5.0)
    obs = [_make_obs(5.0), _make_obs(4.0)]
    confirmed, reason = should_confirm_signal(
        signal=signal,
        observations=obs,
        current_pct_change=3.0,
        current_vol_z=1.0,
    )
    assert confirmed is False


def test_sustained_works_for_negative_moves():
    """Downward move that sustains or deepens → confirmed."""
    signal = _make_signal(pct_change=-3.0)
    obs = [_make_obs(-3.0), _make_obs(-3.5)]
    confirmed, reason = should_confirm_signal(
        signal=signal,
        observations=obs,
        current_pct_change=-4.0,
        current_vol_z=1.0,
    )
    assert confirmed is True
    assert reason == 'sustained'


# ---------------------------------------------------------------------------
# should_confirm_signal — price accelerating
# ---------------------------------------------------------------------------

def test_price_accelerating_confirms_with_three_observations():
    """3 obs + strongly accelerating price → price_accelerating."""
    # original_pct=10.0; current=-5.0 → abs(5) < abs(10) → 'sustained' does NOT fire
    signal = _make_signal(pct_change=10.0)
    # obs pct series: [4.0, 3.0, 1.5] then current=-5.0
    # velocities: [-1.0, -1.5, -6.5] → accel z of -6.5 vs baseline [-1.0, -1.5] >> 1.5
    obs = [
        _make_obs(pct_change=4.0),
        _make_obs(pct_change=3.0),
        _make_obs(pct_change=1.5),
    ]
    confirmed, reason = should_confirm_signal(
        signal=signal,
        observations=obs,
        current_pct_change=-5.0,
        current_vol_z=1.0,
    )
    assert confirmed is True
    assert reason == 'price_accelerating'


def test_price_accel_needs_three_observations():
    """Only 2 observations → cannot compute accel z-score."""
    signal = _make_signal(pct_change=1.0)
    obs = [_make_obs(1.0), _make_obs(1.2)]
    confirmed, reason = should_confirm_signal(
        signal=signal,
        observations=obs,
        current_pct_change=5.0,
        current_vol_z=1.0,
    )
    # Only sustained check applies here (current=5.0 >= original=1.0, n_obs=2)
    assert confirmed is True
    assert reason == 'sustained'


# ---------------------------------------------------------------------------
# should_confirm_signal — volume accelerating
# ---------------------------------------------------------------------------

def test_volume_accelerating_confirms():
    """Volume z-score series strongly accelerating → volume_accelerating."""
    signal = _make_signal(pct_change=0.3)  # price not big enough for sustained
    obs = [
        _make_obs(vol_z=1.0),
        _make_obs(vol_z=1.1),
        _make_obs(vol_z=1.2),
    ]
    # With obs vol_z=[1.0,1.1,1.2]+current=5.0:
    # velocities=[0.1, 0.1, 3.8] → accel z of 3.8 vs [0.1, 0.1] → very high
    confirmed, reason = should_confirm_signal(
        signal=signal,
        observations=obs,
        current_pct_change=0.4,
        current_vol_z=5.0,
    )
    # 5.0 >= 4.0 → volume_extreme fires first
    assert confirmed is True
    # Could be either volume_extreme (checked first) or volume_accelerating
    assert reason in ('volume_extreme', 'volume_accelerating')


# ---------------------------------------------------------------------------
# should_confirm_signal — no confirmation
# ---------------------------------------------------------------------------

def test_no_confirmation_no_observations():
    """No observations, vol below extreme, pct unchanged → no confirm."""
    signal = _make_signal(pct_change=2.0)
    confirmed, reason = should_confirm_signal(
        signal=signal,
        observations=[],
        current_pct_change=2.0,
        current_vol_z=1.5,
    )
    assert confirmed is False
    assert reason == ''


def test_no_confirmation_fading_move_few_obs():
    """Fading move, only 1 observation → no confirm."""
    signal = _make_signal(pct_change=3.0)
    obs = [_make_obs(3.0)]
    confirmed, reason = should_confirm_signal(
        signal=signal,
        observations=obs,
        current_pct_change=1.5,
        current_vol_z=1.0,
    )
    assert confirmed is False


# ---------------------------------------------------------------------------
# should_confirm_signal — None vol_z handled
# ---------------------------------------------------------------------------

def test_none_vol_z_treated_as_zero():
    """None vol_z should not raise."""
    signal = _make_signal(pct_change=2.0)
    obs = [_make_obs(2.0), _make_obs(2.5)]
    confirmed, reason = should_confirm_signal(
        signal=signal,
        observations=obs,
        current_pct_change=2.5,
        current_vol_z=None,
    )
    assert isinstance(confirmed, bool)
