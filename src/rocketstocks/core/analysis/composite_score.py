"""Composite activity scoring module — pure analysis, no discord or data imports.

Combines volume, price, cross-signal, and classification factors into a
single score to determine whether to fire a Market alert.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass

from rocketstocks.core.analysis.alert_strategy import AlertTriggerResult
from rocketstocks.core.analysis.classification import StockClass

logger = logging.getLogger(__name__)

# Component weights (must sum to 1.0)
_W_VOLUME = 0.40
_W_PRICE = 0.30
_W_CROSS_SIGNAL = 0.15
_W_CLASSIFICATION = 0.15

DEFAULT_COMPOSITE_THRESHOLD = 2.5  # ~10-20 market alerts/day

# Classification bonus scores
_CLASS_SCORE: dict[StockClass, float] = {
    StockClass.VOLATILE: 2.0,
    StockClass.MEME: 2.5,
    StockClass.STANDARD: 1.0,
    StockClass.BLUE_CHIP: 1.5,
}


@dataclass
class CompositeScoreResult:
    composite_score: float
    should_alert: bool
    volume_component: float        # abs(volume_zscore)
    price_component: float         # abs(price_zscore)
    cross_signal_component: float  # (confluence_count / total) * 4.0
    classification_component: float
    trigger_result: AlertTriggerResult
    dominant_signal: str           # 'volume', 'price', or 'mixed'


def compute_composite_score(
    trigger_result: AlertTriggerResult,
    threshold: float = DEFAULT_COMPOSITE_THRESHOLD,
) -> CompositeScoreResult:
    """Compute a weighted composite score for market alert detection.

    Args:
        trigger_result: AlertTriggerResult from evaluate_price_alert().
        threshold: Score threshold to fire an alert (default 2.5).

    Returns:
        CompositeScoreResult with weighted breakdown and alert decision.
    """
    # Volume component: abs(volume_zscore)
    vol_z = trigger_result.volume_zscore
    if vol_z is None or (isinstance(vol_z, float) and math.isnan(vol_z)):
        volume_component = 0.0
    else:
        volume_component = abs(float(vol_z))

    # Price component: abs(price_zscore)
    price_z = trigger_result.zscore
    if price_z is None or (isinstance(price_z, float) and math.isnan(price_z)):
        price_component = 0.0
    else:
        price_component = abs(float(price_z))

    # Cross-signal component: (confluence_count / total) * 4.0
    c_count = trigger_result.confluence_count
    c_total = trigger_result.confluence_total
    if c_count is not None and c_total is not None and c_total > 0:
        cross_signal_component = (c_count / c_total) * 4.0
    else:
        cross_signal_component = 0.0

    # Classification component
    classification = trigger_result.classification
    if not isinstance(classification, StockClass):
        try:
            classification = StockClass(classification)
        except (ValueError, KeyError):
            classification = StockClass.STANDARD
    classification_component = _CLASS_SCORE.get(classification, 1.0)

    # Weighted composite score
    composite_score = (
        _W_VOLUME * volume_component
        + _W_PRICE * price_component
        + _W_CROSS_SIGNAL * cross_signal_component
        + _W_CLASSIFICATION * classification_component
    )

    should_alert = composite_score >= threshold

    # Determine dominant signal by comparing weighted contributions
    vol_weighted = _W_VOLUME * volume_component
    price_weighted = _W_PRICE * price_component

    if vol_weighted == 0 and price_weighted == 0:
        dominant_signal = 'mixed'
    elif price_weighted == 0:
        dominant_signal = 'volume'
    else:
        ratio = vol_weighted / price_weighted
        if ratio >= 1.5:
            dominant_signal = 'volume'
        elif ratio <= (1 / 1.5):
            dominant_signal = 'price'
        else:
            dominant_signal = 'mixed'

    logger.debug(
        f"Composite score: {composite_score:.2f} "
        f"(vol={volume_component:.2f}, price={price_component:.2f}, "
        f"cross={cross_signal_component:.2f}, class={classification_component:.2f}) "
        f"→ should_alert={should_alert}, dominant={dominant_signal}"
    )

    return CompositeScoreResult(
        composite_score=composite_score,
        should_alert=should_alert,
        volume_component=volume_component,
        price_component=price_component,
        cross_signal_component=cross_signal_component,
        classification_component=classification_component,
        trigger_result=trigger_result,
        dominant_signal=dominant_signal,
    )
