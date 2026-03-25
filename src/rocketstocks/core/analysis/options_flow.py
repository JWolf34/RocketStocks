"""Options flow evaluation — leading indicator combining unusual activity, IV skew,
and put/call ratio into a unified signal for use alongside volume divergence detection.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class OptionsFlowResult:
    has_unusual_activity: bool
    unusual_contracts: list         # list of dicts from detect_unusual_activity()
    put_call_ratio: float | None    # total put_vol / call_vol; None if call_vol == 0
    iv_skew_direction: str | None   # 'put_skew', 'call_skew', or 'neutral'
    max_pain: float | None
    iv_rank: float | None           # 0-100; None if insufficient IV history
    flow_score: float               # composite 0-10


def evaluate_options_flow(
    options_chain: dict,
    underlying_price: float,
    iv_history=None,  # pd.DataFrame | None
) -> OptionsFlowResult:
    """Evaluate options flow as a core leading indicator.

    Combines unusual volume/OI activity, IV skew, and put/call ratio into a
    composite flow_score (0-10). Used alongside volume divergence to upgrade
    VolumeAccumulationResult.signal_strength to 'volume_plus_options'.

    Args:
        options_chain: Schwab options chain response dict.
        underlying_price: Current underlying price for IV skew calculation.
        iv_history: Optional DataFrame with 'iv' column for IV rank computation.

    Returns:
        OptionsFlowResult with composite flow_score.
    """
    from rocketstocks.core.analysis.options import (
        detect_unusual_activity,
        compute_put_call_stats,
        compute_iv_skew,
        compute_max_pain,
        compute_iv_rank,
    )

    unusual_contracts = detect_unusual_activity(options_chain)
    has_unusual = len(unusual_contracts) > 0

    pc_stats = compute_put_call_stats(options_chain)
    total_call_vol = pc_stats.get('call_volume', 0) or 0
    total_put_vol = pc_stats.get('put_volume', 0) or 0
    put_call_ratio = (total_put_vol / total_call_vol) if total_call_vol > 0 else None

    iv_skew = compute_iv_skew(options_chain, underlying_price) if underlying_price > 0 else None
    iv_skew_direction = iv_skew['direction'] if iv_skew else None

    max_pain = compute_max_pain(options_chain)

    iv_rank = None
    if iv_history is not None and unusual_contracts:
        atm_iv = unusual_contracts[0].get('iv')
        if atm_iv and atm_iv > 0:
            try:
                if hasattr(iv_history, 'empty') and not iv_history.empty:
                    iv_rank = compute_iv_rank(float(atm_iv), iv_history)
            except Exception:
                logger.debug("IV rank computation failed", exc_info=True)

    # --- Composite flow_score (0-10) ---
    # Up to 4 pts for unusual contracts (capped at 3 contracts)
    score = min(len(unusual_contracts), 3) * (4.0 / 3.0)
    # +2 for clear IV skew direction
    if iv_skew_direction and iv_skew_direction != 'neutral':
        score += 2.0
    # +2 for extreme put/call ratio
    if put_call_ratio is not None and (put_call_ratio > 1.5 or put_call_ratio < 0.5):
        score += 2.0
    # +2 for high IV rank
    if iv_rank is not None and iv_rank > 80:
        score += 2.0
    flow_score = min(score, 10.0)

    return OptionsFlowResult(
        has_unusual_activity=has_unusual,
        unusual_contracts=unusual_contracts,
        put_call_ratio=put_call_ratio,
        iv_skew_direction=iv_skew_direction,
        max_pain=max_pain,
        iv_rank=iv_rank,
        flow_score=flow_score,
    )
