"""LeadingIndicatorComboStrategy — combined popularity surge + volume accumulation.

Tests whether the convergence of both leading indicators (a popularity surge AND
simultaneous volume accumulation without price movement) produces a stronger
predictive edge than either signal alone.

This strategy fires less frequently but should show stronger results if the
hypothesis is correct: social momentum building while smart money positions ahead
of a move is a higher-conviction setup than either signal in isolation.

Compare against the individual strategies using the compare command to measure
whether convergence adds value.
"""
from __future__ import annotations

import logging
import math

import pandas as pd

from rocketstocks.backtest.data_prep import prep_for_signals
from rocketstocks.backtest.registry import register
from rocketstocks.backtest.strategies.base import LeadingIndicatorStrategy
from rocketstocks.core.analysis.popularity_signals import evaluate_popularity_surge
from rocketstocks.core.analysis.volume_divergence import evaluate_volume_accumulation

try:
    from rocketstocks.core.analysis.signals import signals as _signals
except ImportError:
    _signals = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

_POPULARITY_HISTORY_WINDOW = 30


@register('leading_indicator_combo')
class LeadingIndicatorComboStrategy(LeadingIndicatorStrategy):
    """Entry only when BOTH popularity surge AND volume accumulation fire simultaneously.

    Optimizable parameters (in addition to base exit params)::

        bt.optimize(
            min_mentions=[10, 15, 20],
            vol_threshold=[1.5, 2.0, 2.5],
            price_ceiling=[0.5, 1.0, 1.5],
        )

    Attributes:
        min_mentions: Minimum current mention count for popularity surge.
        vol_threshold: Minimum volume z-score for accumulation signal.
        price_ceiling: Maximum abs(price_zscore) for accumulation signal.
    """

    requires_popularity: bool = True
    requires_daily: bool = True

    min_mentions: int = 15
    vol_threshold: float = 2.0
    price_ceiling: float = 1.0

    def _detect_signal(self) -> bool:
        return self._popularity_surging() and self._volume_accumulating()

    # ------------------------------------------------------------------
    # Popularity surge (same logic as PopularitySurgeStrategy)
    # ------------------------------------------------------------------

    def _popularity_surging(self) -> bool:
        if 'Rank' not in self.data.df.columns:
            return False

        row = self.data.df.iloc[-1]
        current_rank = row.get('Rank')
        if current_rank is None or _isnan(current_rank):
            return False

        current_rank = int(current_rank)
        mentions_val = row.get('Mentions')
        rank_24h_val = row.get('Rank_24h_ago')
        mentions_24h_val = row.get('Mentions_24h_ago')

        mentions = int(mentions_val) if mentions_val is not None and not _isnan(mentions_val) else None
        rank_24h_ago = int(rank_24h_val) if rank_24h_val is not None and not _isnan(rank_24h_val) else None
        mentions_24h_ago = (
            int(mentions_24h_val)
            if mentions_24h_val is not None and not _isnan(mentions_24h_val)
            else None
        )

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
        return pop_slice.reset_index(drop=True)

    # ------------------------------------------------------------------
    # Volume accumulation (same logic as VolumeAccumulationStrategy)
    # ------------------------------------------------------------------

    def _volume_accumulating(self) -> bool:
        if self._has_daily_enrichment():
            return self._detect_volume_5m()
        return self._detect_volume_daily()

    def _has_daily_enrichment(self) -> bool:
        return 'Daily_Vol_Mean' in self.data.df.columns

    def _detect_volume_daily(self) -> bool:
        if len(self.data) < 21:
            return False
        if _signals is None:
            return False

        lookback = self.data.df.iloc[max(0, len(self.data) - 20):len(self.data)]
        lookback_lower = prep_for_signals(lookback)
        volume_series = lookback_lower['volume']
        curr_volume = float(volume_series.iloc[-1])

        vol_zscore = _signals.volume_zscore(volume_series=volume_series,
                                             curr_volume=curr_volume, period=20)
        price_zscore = _signals.price_zscore(lookback_lower['close'], period=20)
        avg_vol = float(volume_series.mean()) if len(volume_series) > 0 else 1.0
        rvol = curr_volume / avg_vol if avg_vol > 0 else float('nan')

        result = evaluate_volume_accumulation(
            vol_zscore=vol_zscore,
            price_zscore=price_zscore,
            rvol=rvol,
            vol_threshold=self.vol_threshold,
            price_ceiling=self.price_ceiling,
        )
        return result.is_accumulating

    def _detect_volume_5m(self) -> bool:
        row = self.data.df.iloc[-1]

        cumvol = row.get('Cumulative_Volume')
        vol_mean = row.get('Daily_Vol_Mean')
        vol_std = row.get('Daily_Vol_Std')
        intraday_pct = row.get('Intraday_Pct_Change')
        ret_mean = row.get('Daily_Return_Mean')
        ret_std = row.get('Daily_Return_Std')

        if any(
            v is None or (isinstance(v, float) and math.isnan(v))
            for v in (cumvol, vol_mean, vol_std, intraday_pct, ret_mean, ret_std)
        ):
            return False

        if vol_std == 0:
            return False

        vol_zscore = (cumvol - vol_mean) / vol_std
        price_zscore = (
            (intraday_pct - ret_mean) / ret_std
            if ret_std and ret_std != 0
            else float('nan')
        )
        rvol = cumvol / vol_mean if vol_mean and vol_mean > 0 else float('nan')

        result = evaluate_volume_accumulation(
            vol_zscore=vol_zscore,
            price_zscore=price_zscore,
            rvol=rvol,
            vol_threshold=self.vol_threshold,
            price_ceiling=self.price_ceiling,
        )
        return result.is_accumulating


def _isnan(v) -> bool:
    try:
        return math.isnan(float(v))
    except (TypeError, ValueError):
        return False
