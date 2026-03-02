"""Momentum acceleration module for alert update decisions.

Replaces the arbitrary ">100% relative change" logic with z-score-based
acceleration detection. No discord or data imports.
"""
from __future__ import annotations

import logging
import math

import numpy as np

logger = logging.getLogger(__name__)

_MOMENTUM_HISTORY_KEY = 'momentum_history'


def compute_velocity(
    current_pct: float,
    prev_pct: float,
    intervals: int = 1,
) -> float:
    """Return the rate of change of pct_change between two observations.

    Args:
        current_pct: Current percentage change reading.
        prev_pct: Previous percentage change reading.
        intervals: Number of time intervals between observations (default 1).

    Returns:
        Velocity (change in pct_change per interval).
    """
    return (current_pct - prev_pct) / max(intervals, 1)


def compute_acceleration(
    current_velocity: float,
    prev_velocity: float,
    intervals: int = 1,
) -> float:
    """Return the rate of change of velocity between two observations.

    Args:
        current_velocity: Most-recent velocity value.
        prev_velocity: Previous velocity value.
        intervals: Number of time intervals between observations (default 1).

    Returns:
        Acceleration (change in velocity per interval).
    """
    return (current_velocity - prev_velocity) / max(intervals, 1)


def should_update_alert(
    current_pct: float,
    prev_alert_data: dict,
    accel_zscore_threshold: float = 2.0,
) -> bool:
    """Decide whether to post an alert update based on momentum acceleration.

    The function inspects ``prev_alert_data['momentum_history']`` — a list of
    snapshots recorded by :func:`record_momentum_snapshot`.  Each snapshot has::

        {'pct_change': float, 'velocity': float, 'acceleration': float}

    If fewer than 2 historical snapshots exist (not enough to compute a z-score
    baseline), falls back to the original ">100% relative change" heuristic.

    Args:
        current_pct: Current intraday percentage change.
        prev_alert_data: The ``alert_data`` dict from the most-recently stored alert.
        accel_zscore_threshold: Trigger when abs(accel z-score) >= this value (default 2.0).

    Returns:
        True if the alert should be updated, False otherwise.
    """
    history: list[dict] = prev_alert_data.get(_MOMENTUM_HISTORY_KEY, [])
    prev_pct = prev_alert_data.get('pct_change')

    if prev_pct is None:
        return False

    # Need at least one prior velocity to compute current velocity
    if not history:
        # Fall back: trigger when pct_change moved >100% relative
        if prev_pct == 0:
            return False
        pct_diff = ((current_pct - prev_pct) / abs(prev_pct)) * 100.0
        return pct_diff > 100.0

    prev_velocity = history[-1].get('velocity', 0.0)
    current_velocity = compute_velocity(current_pct, prev_pct)
    current_accel = compute_acceleration(current_velocity, prev_velocity)

    # Need at least 2 historical acceleration readings for z-score baseline
    accels = [snap.get('acceleration', 0.0) for snap in history if 'acceleration' in snap]
    if len(accels) < 2:
        # Fall back heuristic
        if prev_pct == 0:
            return False
        pct_diff = ((current_pct - prev_pct) / abs(prev_pct)) * 100.0
        return pct_diff > 100.0

    mean_a = np.mean(accels)
    std_a = np.std(accels, ddof=1)
    if std_a == 0 or math.isnan(std_a):
        return False

    accel_zscore = (current_accel - mean_a) / std_a
    logger.debug(
        f"Momentum acceleration z-score: {accel_zscore:.2f} "
        f"(current_accel={current_accel:.4f}, mean={mean_a:.4f}, std={std_a:.4f})"
    )
    return abs(accel_zscore) >= accel_zscore_threshold


def build_momentum_snapshot(
    current_pct: float,
    prev_alert_data: dict,
) -> dict:
    """Build a momentum snapshot dict to append to ``momentum_history``.

    Args:
        current_pct: Current intraday percentage change.
        prev_alert_data: Previously stored ``alert_data`` dict.

    Returns:
        Dict with keys ``pct_change``, ``velocity``, ``acceleration``.
    """
    history: list[dict] = prev_alert_data.get(_MOMENTUM_HISTORY_KEY, [])
    prev_pct = prev_alert_data.get('pct_change', current_pct)

    velocity = compute_velocity(current_pct, prev_pct)

    if history:
        prev_velocity = history[-1].get('velocity', 0.0)
        acceleration = compute_acceleration(velocity, prev_velocity)
    else:
        acceleration = 0.0

    return {
        'pct_change': current_pct,
        'velocity': velocity,
        'acceleration': acceleration,
    }
