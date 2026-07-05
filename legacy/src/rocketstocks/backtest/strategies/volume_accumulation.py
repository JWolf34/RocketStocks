"""VolumeAccumulationStrategy — test volume-without-price as a leading indicator.

Detects the "high volume, no price move yet" pattern and tests whether it
predicts forward price movement — the core accumulation/distribution hypothesis
behind the RocketStocks alert pipeline.

Daily mode
----------
Evaluates directly from the strategy's own lookback window.
Volume z-score and price z-score are computed from the 20-bar rolling history.

5m mode (requires_daily = True)
--------------------------------
Uses pre-computed daily enrichment columns (added by enrich_5m_with_daily_context):
    - vol_zscore  = (Cumulative_Volume - Daily_Vol_Mean) / Daily_Vol_Std
    - price_zscore = (Intraday_Pct_Change - Daily_Return_Mean) / Daily_Return_Std
    - rvol        = Cumulative_Volume / Daily_Vol_Mean

This matches exactly how the production bot evaluates volume accumulation.
"""
from __future__ import annotations

import logging
import math

from rocketstocks.backtest.data_prep import prep_for_signals
from rocketstocks.backtest.registry import register
from rocketstocks.backtest.strategies.base import LeadingIndicatorStrategy
from rocketstocks.core.analysis.volume_divergence import evaluate_volume_accumulation

try:
    from rocketstocks.core.analysis.signals import signals as _signals
except ImportError:
    _signals = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

_MIN_DAILY_BARS = 21  # need at least 20 prior bars for z-score
_MIN_5M_BARS = 2


@register('volume_accumulation')
class VolumeAccumulationStrategy(LeadingIndicatorStrategy):
    """Entry on volume accumulation signal; exit via configurable z-score or bar-hold.

    Optimizable parameters (in addition to base exit params)::

        bt.optimize(vol_threshold=[1.5, 2.0, 2.5], price_ceiling=[0.5, 1.0, 1.5])

    Attributes:
        vol_threshold: Minimum volume z-score to qualify (default 2.0).
        price_ceiling: Maximum abs(price_zscore) to qualify (default 1.0).
    """

    requires_daily: bool = True

    vol_threshold: float = 2.0
    price_ceiling: float = 1.0

    def _detect_signal(self) -> bool:
        if self._has_daily_enrichment():
            return self._detect_5m()
        return self._detect_daily()

    def _has_daily_enrichment(self) -> bool:
        return 'Daily_Vol_Mean' in self.data.df.columns

    def _detect_daily(self) -> bool:
        if len(self.data) < _MIN_DAILY_BARS:
            return False
        if _signals is None:
            return False

        lookback = self.data.df.iloc[max(0, len(self.data) - 20):len(self.data)]
        lookback_lower = prep_for_signals(lookback)
        volume_series = lookback_lower['volume']
        curr_volume = float(volume_series.iloc[-1])

        vol_zscore = _signals.volume_zscore(
            volume_series=volume_series,
            curr_volume=curr_volume,
            period=20,
        )
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

    def _detect_5m(self) -> bool:
        row = self.data.df.iloc[-1]

        cumvol = row.get('Cumulative_Volume')
        vol_mean = row.get('Daily_Vol_Mean')
        vol_std = row.get('Daily_Vol_Std')
        intraday_pct = row.get('Intraday_Pct_Change')
        ret_mean = row.get('Daily_Return_Mean')
        ret_std = row.get('Daily_Return_Std')

        # Skip if enrichment data is missing for this bar
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
