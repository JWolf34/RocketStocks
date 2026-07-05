"""Volume-price divergence detection.

Detects the "volume without price" pattern — institutional accumulation/distribution
before price moves. When volume is unusually high but price has not moved significantly,
it signals that smart money may be positioning in advance of a move.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class VolumeAccumulationResult:
    is_accumulating: bool
    vol_zscore: float
    price_zscore: float
    rvol: float
    divergence_score: float      # vol_z - abs(price_z): higher = more divergent
    signal_strength: str         # 'volume_only' or 'volume_plus_options'
    options_flow: object | None = None  # OptionsFlowResult attached after options check


def evaluate_volume_accumulation(
    vol_zscore: float,
    price_zscore: float,
    rvol: float,
    vol_threshold: float = 2.5,
    price_ceiling: float = 1.0,
    min_divergence: float = 1.5,
) -> VolumeAccumulationResult:
    """Evaluate whether a ticker shows volume accumulation without significant price movement.

    Args:
        vol_zscore: Volume z-score (current volume vs 20-day average).
        price_zscore: Intraday price z-score (current pct_change vs 20-day distribution).
        rvol: Relative volume (current volume / average volume).
        vol_threshold: Minimum vol_zscore to qualify (default 2.5).
        price_ceiling: Maximum abs(price_zscore) to qualify (default 1.0).
        min_divergence: Minimum divergence score (vol_z - abs(price_z)) to qualify (default 1.5).

    Returns:
        VolumeAccumulationResult with is_accumulating=True when the pattern is detected.
    """
    is_valid = (
        not math.isnan(vol_zscore)
        and not math.isnan(price_zscore)
        and not math.isnan(rvol)
    )

    if not is_valid:
        return VolumeAccumulationResult(
            is_accumulating=False,
            vol_zscore=vol_zscore,
            price_zscore=price_zscore,
            rvol=rvol,
            divergence_score=float('nan'),
            signal_strength='volume_only',
        )

    divergence_score = vol_zscore - abs(price_zscore)
    is_accumulating = (
        vol_zscore >= vol_threshold
        and abs(price_zscore) < price_ceiling
        and divergence_score >= min_divergence
    )

    return VolumeAccumulationResult(
        is_accumulating=is_accumulating,
        vol_zscore=vol_zscore,
        price_zscore=price_zscore,
        rvol=rvol,
        divergence_score=divergence_score,
        signal_strength='volume_only',
    )

