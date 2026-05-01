"""Sentiment event detector — detects mention spikes and rank jumps.

Uses raw popularity data from the `popularity` table.  Intentionally does NOT
reuse evaluate_popularity_surge() so that we start analysis without the bias
of existing surge-detection logic (thresholds, tier adjustments, etc.).
"""
import logging

import pandas as pd

from rocketstocks.eda.data_loader import load_popularity_raw
from rocketstocks.eda.events.base import _to_naive_utc

logger = logging.getLogger(__name__)

_DEFAULT_MENTION_THRESHOLDS = [2.0, 3.0, 5.0]
_DEFAULT_RANK_CHANGE_THRESHOLDS = [50, 100, 200]
_DEFAULT_MIN_MENTIONS = 10


class SentimentDetector:
    """Detects mention spikes and rank improvements from ApeWisdom data.

    Produces one event per ticker per detected condition, de-duplicated at
    the snapshot level.  Event datetime is the popularity snapshot timestamp.

    For *daily* timeframe events are further reduced to one per ticker per
    calendar day (highest signal_value wins; ties broken by earliest datetime).

    Args:
        mention_thresholds: mention_ratio values that trigger an event.
            A separate event set is produced for each threshold.
        rank_change_thresholds: rank_change values that trigger an event.
        min_mentions: Minimum raw mention count; filters low-activity noise.
        mode: 'mention_ratio' | 'rank_change' | 'both'.  Controls which
            condition(s) to detect.

    Attributes:
        n_below_min_mentions: Number of snapshots filtered by min_mentions
            during the most recent detect() call.
    """

    def __init__(
        self,
        mention_thresholds: list[float] | None = None,
        rank_change_thresholds: list[int] | None = None,
        min_mentions: int = _DEFAULT_MIN_MENTIONS,
        mode: str = 'both',
    ):
        self.mention_thresholds = mention_thresholds or _DEFAULT_MENTION_THRESHOLDS
        self.rank_change_thresholds = rank_change_thresholds or _DEFAULT_RANK_CHANGE_THRESHOLDS
        self.min_mentions = min_mentions
        self.mode = mode
        self.n_below_min_mentions: int = 0

    async def detect(
        self,
        stock_data,
        timeframe: str = 'daily',
        start_date=None,
        end_date=None,
        tickers: list[str] | None = None,
    ) -> pd.DataFrame:
        """Return events DataFrame in standard format.

        Returns one row per event.  'signal_value' holds the mention_ratio
        for mention-spike events, or the rank_change for rank-jump events.
        'source_detail' describes which condition triggered the event
        (e.g. 'mention_ratio>=3.0').

        The returned DataFrame is NOT yet de-duplicated within a window —
        callers should apply deduplicate_events() from events/base.py if
        they want to avoid clustering bias.
        """
        self.n_below_min_mentions = 0

        # Normalise explicit tickers to uppercase
        if tickers is not None:
            tickers = [t.upper() for t in tickers]

        pop = await load_popularity_raw(stock_data, start_date, end_date)
        if pop.empty:
            return _empty_events()

        pop = pop.copy()
        pop['mention_ratio'] = _safe_ratio(pop['mentions'], pop['mentions_24h_ago'])
        pop['rank_change'] = pop['rank_24h_ago'] - pop['rank']

        # Track how many snapshots are filtered by min_mentions
        self.n_below_min_mentions = int((pop['mentions'] < self.min_mentions).sum())

        # Base filter: minimum mentions
        pop = pop[pop['mentions'] >= self.min_mentions]

        # Ticker filter
        if tickers:
            pop = pop[pop['ticker'].isin(tickers)]

        if pop.empty:
            return _empty_events()

        events_list: list[pd.DataFrame] = []

        # -- Mention ratio events --
        if self.mode in ('mention_ratio', 'both'):
            for threshold in self.mention_thresholds:
                mask = pop['mention_ratio'] >= threshold
                subset = pop[mask].copy()
                if subset.empty:
                    continue
                subset['signal_value'] = subset['mention_ratio']
                subset['source'] = 'sentiment'
                subset['source_detail'] = f'mention_ratio>={threshold}'
                events_list.append(
                    subset[['ticker', 'datetime', 'signal_value', 'source', 'source_detail',
                            'mentions', 'rank', 'mention_ratio', 'rank_change']]
                )

        # -- Rank change events --
        if self.mode in ('rank_change', 'both'):
            for threshold in self.rank_change_thresholds:
                mask = pop['rank_change'] >= threshold
                subset = pop[mask].copy()
                if subset.empty:
                    continue
                subset['signal_value'] = subset['rank_change'].astype(float)
                subset['source'] = 'sentiment'
                subset['source_detail'] = f'rank_change>={threshold}'
                events_list.append(
                    subset[['ticker', 'datetime', 'signal_value', 'source', 'source_detail',
                            'mentions', 'rank', 'mention_ratio', 'rank_change']]
                )

        if not events_list:
            logger.info("SentimentDetector: no events found")
            return _empty_events()

        events = pd.concat(events_list, ignore_index=True)

        # Normalize datetime to tz-naive and uppercase tickers
        events['datetime'] = _to_naive_utc(events['datetime'])
        events['ticker'] = events['ticker'].str.upper()

        # For daily timeframe: reduce to one event per ticker per day.
        # Sort descending by signal_value, then ascending by datetime so the
        # highest-value (earliest-in-case-of-tie) snapshot wins.
        if timeframe == 'daily':
            events['_date'] = events['datetime'].dt.date
            events = (
                events.sort_values(
                    ['signal_value', 'datetime'],
                    ascending=[False, True],
                )
                .groupby(['ticker', '_date', 'source_detail'], sort=False)
                .first()
                .reset_index()
            )
            # Snap datetime to midnight of the event date for daily alignment
            events['datetime'] = pd.to_datetime(events['_date'].astype(str))
            events = events.drop(columns=['_date'])

        events = events.sort_values(['ticker', 'datetime']).reset_index(drop=True)
        logger.info(
            f"SentimentDetector: {len(events)} events detected "
            f"({events['ticker'].nunique()} tickers)"
        )
        return events


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _empty_events() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        'ticker', 'datetime', 'signal_value', 'source', 'source_detail',
        'mentions', 'rank', 'mention_ratio', 'rank_change',
    ])


def _safe_ratio(num: pd.Series, denom: pd.Series) -> pd.Series:
    import numpy as np
    d = denom.replace(0, float('nan'))
    return num / d
