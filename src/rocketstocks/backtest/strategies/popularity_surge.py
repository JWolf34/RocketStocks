"""PopularitySurgeStrategy — test popularity surges as a leading indicator.

Detects when a ticker is experiencing an unusual surge in social media
popularity (mentions, rank momentum) and tests whether it predicts forward
price movement. Reuses evaluate_popularity_surge() from the production bot's
alert pipeline.

Requires popularity data (requires_popularity = True). The runner fetches
popularity data via PopularityRepository and merges it onto the price bars
using merge_popularity(), adding: Rank, Mentions, Rank_24h_ago, Mentions_24h_ago.

Popularity data is collected every ~30 minutes by the bot, so each reading
covers many price bars (forward-filled). On daily timeframes, the most recent
popularity observation for each day is used.

Bars where Rank is NaN (no popularity data yet) are skipped silently.
"""
from __future__ import annotations

import logging
import math

import pandas as pd

from rocketstocks.backtest.registry import register
from rocketstocks.backtest.strategies.base import LeadingIndicatorStrategy
from rocketstocks.core.analysis.popularity_signals import evaluate_popularity_surge

logger = logging.getLogger(__name__)

_POPULARITY_HISTORY_WINDOW = 30   # bars of popularity history for velocity/acceleration


@register('popularity_surge')
class PopularitySurgeStrategy(LeadingIndicatorStrategy):
    """Entry on popularity surge signal; exit via configurable z-score or bar-hold.

    Optimizable parameters (in addition to base exit params)::

        bt.optimize(min_mentions=[10, 15, 20, 30])

    Attributes:
        min_mentions: Minimum current mention count to consider a surge valid.
    """

    requires_popularity: bool = True

    min_mentions: int = 15

    def _detect_signal(self) -> bool:
        if 'Rank' not in self.data.df.columns:
            return False

        row = self.data.df.iloc[-1]
        current_rank = row.get('Rank')
        if current_rank is None or (isinstance(current_rank, float) and math.isnan(current_rank)):
            return False

        current_rank = int(current_rank)
        mentions = row.get('Mentions')
        rank_24h_ago_val = row.get('Rank_24h_ago')
        mentions_24h_ago = row.get('Mentions_24h_ago')

        mentions = int(mentions) if mentions is not None and not _isnan(mentions) else None
        rank_24h_ago = (
            int(rank_24h_ago_val)
            if rank_24h_ago_val is not None and not _isnan(rank_24h_ago_val)
            else None
        )
        mentions_24h_ago = (
            int(mentions_24h_ago)
            if mentions_24h_ago is not None and not _isnan(mentions_24h_ago)
            else None
        )

        # Build popularity history from backward-looking bars (deduplicated by rank value)
        pop_history = self._build_popularity_history()

        ticker = getattr(self.data, '_name', 'UNKNOWN')

        result = evaluate_popularity_surge(
            ticker=ticker,
            current_rank=current_rank,
            rank_24h_ago=rank_24h_ago,
            mentions=mentions,
            mentions_24h_ago=mentions_24h_ago,
            popularity_history=pop_history,
            min_mentions=self.min_mentions,
        )
        return result.is_surging

    def _build_popularity_history(self) -> pd.DataFrame | None:
        """Build a deduplicated popularity history DataFrame from prior bars.

        Since popularity is forward-filled every 30 min onto 5m bars, we
        deduplicate consecutive identical rank values to avoid inflating
        the velocity/acceleration calculations.
        """
        df = self.data.df
        if 'Rank' not in df.columns:
            return None

        window = df.iloc[max(0, len(df) - _POPULARITY_HISTORY_WINDOW):len(df)]
        pop_cols = [c for c in ('Rank', 'Mentions', 'Rank_24h_ago', 'Mentions_24h_ago')
                    if c in window.columns]
        if not pop_cols:
            return None

        pop_slice = window[pop_cols].copy()
        pop_slice = pop_slice.dropna(subset=['Rank'])

        # Deduplicate: keep only rows where Rank changed from the previous row
        rank_changed = pop_slice['Rank'].ne(pop_slice['Rank'].shift())
        pop_slice = pop_slice[rank_changed].copy()

        if pop_slice.empty:
            return None

        pop_slice = pop_slice.rename(columns={
            'Rank': 'rank',
            'Mentions': 'mentions',
            'Rank_24h_ago': 'rank_24h_ago',
            'Mentions_24h_ago': 'mentions_24h_ago',
        })
        pop_slice['datetime'] = pop_slice.index
        pop_slice = pop_slice.reset_index(drop=True)
        return pop_slice


def _isnan(v) -> bool:
    try:
        return math.isnan(float(v))
    except (TypeError, ValueError):
        return False
