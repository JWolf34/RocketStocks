"""Popularity signal detection module — pure analysis, no discord or data imports."""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from enum import Enum

import pandas as pd

from rocketstocks.core.analysis.indicators import indicators

logger = logging.getLogger(__name__)


class SurgeType(str, Enum):
    MENTION_SURGE = "mention_surge"    # mentions >= 3x vs 24h ago
    RANK_JUMP = "rank_jump"            # rank improved 100+ spots
    NEW_ENTRANT = "new_entrant"        # newly entered top 200 with no prior rank
    VELOCITY_SPIKE = "velocity_spike"  # rank_velocity_zscore <= -2.5 (gaining popularity)


@dataclass
class PopularitySurgeResult:
    ticker: str
    is_surging: bool
    surge_types: list[SurgeType]
    current_rank: int | None
    rank_24h_ago: int | None
    rank_change: int | None          # positive = gained spots (rank number decreased)
    mentions: int | None
    mentions_24h_ago: int | None
    mention_ratio: float | None      # mentions / mentions_24h_ago
    rank_velocity: float | None
    rank_velocity_zscore: float | None


def evaluate_popularity_surge(
    ticker: str,
    current_rank: int | None,
    rank_24h_ago: int | None,
    mentions: int | None,
    mentions_24h_ago: int | None,
    popularity_history: pd.DataFrame | None = None,
    mention_surge_threshold: float = 3.0,
    mention_surge_min_base: int = 15,
    rank_jump_ratio_threshold: float = 1.5,
    rank_jump_min_spots: int = 50,
    new_entrant_cutoff: int = 200,
    velocity_zscore_threshold: float = 2.5,
    min_mentions: int = 15,
) -> PopularitySurgeResult:
    """Evaluate whether a ticker is experiencing a popularity surge.

    Args:
        ticker: Stock ticker symbol.
        current_rank: Current popularity rank (lower = more popular).
        rank_24h_ago: Popularity rank 24 hours ago.
        mentions: Current mention count.
        mentions_24h_ago: Mention count 24 hours ago.
        popularity_history: DataFrame with 'rank' and 'datetime' columns.
        mention_surge_threshold: Ratio threshold for MENTION_SURGE (default 3.0x).
        mention_surge_min_base: Minimum 24h-ago mentions required for MENTION_SURGE (default 15).
        rank_jump_ratio_threshold: rank_change/current_rank threshold for RANK_JUMP (default 1.5).
        rank_jump_min_spots: Minimum spots gained for RANK_JUMP (default 50).
        new_entrant_cutoff: Rank cutoff for NEW_ENTRANT detection (default 200).
        velocity_zscore_threshold: Z-score threshold for VELOCITY_SPIKE (default 2.5).
        min_mentions: Minimum mention count required to trigger any surge (default 5).

    Returns:
        A PopularitySurgeResult describing whether and why a surge was detected.
    """
    # Rank change: positive means gained spots (rank number decreased = more popular)
    rank_change: int | None = None
    if current_rank is not None and rank_24h_ago is not None:
        rank_change = rank_24h_ago - current_rank

    # Minimum mention filter — low-mention stocks produce noisy rank fluctuations
    if mentions is None or mentions < min_mentions:
        return PopularitySurgeResult(
            ticker=ticker,
            is_surging=False,
            surge_types=[],
            current_rank=current_rank,
            rank_24h_ago=rank_24h_ago,
            rank_change=rank_change,
            mentions=mentions,
            mentions_24h_ago=mentions_24h_ago,
            mention_ratio=None,
            rank_velocity=None,
            rank_velocity_zscore=None,
        )

    surge_types: list[SurgeType] = []

    # Mention ratio
    mention_ratio: float | None = None
    if (mentions is not None
            and mentions_24h_ago is not None
            and mentions_24h_ago > 0):
        mention_ratio = mentions / mentions_24h_ago

    # Velocity stats from history
    rank_velocity: float | None = None
    rank_velocity_zscore: float | None = None
    if popularity_history is not None and not popularity_history.empty:
        rank_velocity = indicators.popularity.rank_velocity(
            popularity_df=popularity_history,
            periods=5,
        )
        rank_velocity_zscore = indicators.popularity.rank_velocity_zscore(
            popularity_df=popularity_history,
            lookback=30,
            velocity_window=5,
        )

    # --- MENTION_SURGE ---
    if (mention_ratio is not None
            and mention_ratio >= mention_surge_threshold
            and mentions_24h_ago is not None
            and mentions_24h_ago >= mention_surge_min_base):
        surge_types.append(SurgeType.MENTION_SURGE)

    # --- RANK_JUMP ---
    if (rank_change is not None
            and current_rank is not None
            and current_rank > 0
            and rank_change >= rank_jump_min_spots
            and rank_change / current_rank >= rank_jump_ratio_threshold):
        surge_types.append(SurgeType.RANK_JUMP)

    # --- NEW_ENTRANT ---
    # Ticker just appeared in top N, had no prior rank 24h ago
    if (current_rank is not None
            and current_rank <= new_entrant_cutoff
            and rank_24h_ago is None):
        surge_types.append(SurgeType.NEW_ENTRANT)

    # --- VELOCITY_SPIKE ---
    # Negative z-score = rank number dropping = gaining popularity.
    # Only alert on upward popularity movement (zscore <= -threshold).
    if (rank_velocity_zscore is not None
            and not math.isnan(rank_velocity_zscore)
            and rank_velocity_zscore <= -velocity_zscore_threshold):
        surge_types.append(SurgeType.VELOCITY_SPIKE)

    is_surging = len(surge_types) > 0

    logger.debug(
        f"[{ticker}] popularity surge: is_surging={is_surging}, "
        f"types={[st.value for st in surge_types]}, rank_change={rank_change}, "
        f"mention_ratio={mention_ratio}, rv_zscore={rank_velocity_zscore}"
    )

    return PopularitySurgeResult(
        ticker=ticker,
        is_surging=is_surging,
        surge_types=surge_types,
        current_rank=current_rank,
        rank_24h_ago=rank_24h_ago,
        rank_change=rank_change,
        mentions=mentions,
        mentions_24h_ago=mentions_24h_ago,
        mention_ratio=mention_ratio,
        rank_velocity=rank_velocity,
        rank_velocity_zscore=rank_velocity_zscore,
    )
