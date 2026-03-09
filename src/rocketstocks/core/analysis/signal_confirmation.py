"""Signal confirmation module for Market Mover alerts.

Determines whether a silently-recorded market signal should be promoted
to a visible alert based on sustained or accelerating price/volume activity.

No discord or data imports.
"""
from __future__ import annotations

import logging
import math

import numpy as np

logger = logging.getLogger(__name__)

# Confirmation thresholds
_MIN_OBSERVATIONS_SUSTAINED = 2
_MIN_OBSERVATIONS_ACCEL = 3
_ACCEL_ZSCORE_THRESHOLD = 1.5
_VOL_EXTREME_THRESHOLD = 4.0


def _compute_velocities(series: list[float]) -> list[float]:
    """Return first-differences (velocities) for a numeric series."""
    return [series[i] - series[i - 1] for i in range(1, len(series))]


def _zscore_of_last(series: list[float]) -> float | None:
    """Compute z-score of the last element relative to the rest.

    Returns None if there are fewer than 2 elements or std is zero.
    """
    if len(series) < 2:
        return None
    baseline = series[:-1]
    mean = float(np.mean(baseline))
    std = float(np.std(baseline, ddof=1)) if len(baseline) > 1 else 0.0
    if std == 0 or math.isnan(std):
        return None
    return (series[-1] - mean) / std


def should_confirm_signal(
    signal: dict,
    observations: list[dict],
    current_pct_change: float,
    current_vol_z: float | None,
    min_observations: int = _MIN_OBSERVATIONS_SUSTAINED,
) -> tuple[bool, str]:
    """Determine whether a market signal should be confirmed as a Market Mover alert.

    Args:
        signal: Signal row dict from MarketSignalRepository.get_active_signals().
                Must include 'pct_change' (original detection pct_change).
        observations: List of observation snapshot dicts from
                      MarketSignalRepository.get_signal_history().
                      Each has keys: 'ts', 'pct_change', 'vol_z', 'price_z', 'composite'.
        current_pct_change: Current intraday percentage change.
        current_vol_z: Current volume z-score.
        min_observations: Minimum number of observations for 'sustained' confirmation.

    Returns:
        (confirmed: bool, reason: str)
        reason is one of: 'sustained', 'price_accelerating', 'volume_accelerating',
        'volume_extreme', '' (when not confirmed).
    """
    n_obs = len(observations)
    original_pct = signal.get('pct_change', 0.0) or 0.0
    vol_z = current_vol_z if current_vol_z is not None else 0.0

    # Criterion 4: extreme volume — immediate confirmation regardless of history
    if abs(vol_z) >= _VOL_EXTREME_THRESHOLD:
        logger.debug(f"Signal confirmed: volume_extreme (vol_z={vol_z:.2f})")
        return True, 'volume_extreme'

    # Criterion 1: sustained — move hasn't faded
    if n_obs >= min_observations and abs(current_pct_change) >= abs(original_pct):
        logger.debug(
            f"Signal confirmed: sustained "
            f"(current={current_pct_change:.2f}%, original={original_pct:.2f}%)"
        )
        return True, 'sustained'

    if n_obs >= _MIN_OBSERVATIONS_ACCEL:
        pct_series = [obs.get('pct_change', 0.0) or 0.0 for obs in observations]
        pct_series.append(current_pct_change)

        vol_series = [obs.get('vol_z', 0.0) or 0.0 for obs in observations]
        vol_series.append(vol_z)

        # Criterion 2: price accelerating
        pct_velocities = _compute_velocities(pct_series)
        if len(pct_velocities) >= 2:
            price_accel_z = _zscore_of_last(pct_velocities)
            if price_accel_z is not None and abs(price_accel_z) >= _ACCEL_ZSCORE_THRESHOLD:
                logger.debug(
                    f"Signal confirmed: price_accelerating (accel_z={price_accel_z:.2f})"
                )
                return True, 'price_accelerating'

        # Criterion 3: volume accelerating
        vol_velocities = _compute_velocities(vol_series)
        if len(vol_velocities) >= 2:
            vol_accel_z = _zscore_of_last(vol_velocities)
            if vol_accel_z is not None and abs(vol_accel_z) >= _ACCEL_ZSCORE_THRESHOLD:
                logger.debug(
                    f"Signal confirmed: volume_accelerating (accel_z={vol_accel_z:.2f})"
                )
                return True, 'volume_accelerating'

    return False, ''
