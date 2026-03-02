"""Alert trigger strategy module — replaces scattered hardcoded thresholds.

No discord or data imports. All functions are pure analysis logic.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

from rocketstocks.core.analysis.classification import StockClass
from rocketstocks.core.analysis.indicators import indicators
from rocketstocks.core.analysis.signals import signals

logger = logging.getLogger(__name__)

# Z-score thresholds per stock class
_ZSCORE_THRESHOLDS = {
    StockClass.VOLATILE: 2.0,
    StockClass.MEME: 2.0,
    StockClass.STANDARD: 2.5,
    StockClass.BLUE_CHIP: 2.0,  # used in fallback only
}

# Minimum volume z-score required to confirm blue-chip breakouts
_BLUE_CHIP_VOLUME_ZSCORE_MIN = 2.0

# Minimum confluence count for blue-chip fallback trigger
_BLUE_CHIP_CONFLUENCE_MIN = 3


@dataclass
class AlertTriggerResult:
    should_alert: bool
    classification: StockClass
    zscore: float
    percentile: float
    bb_position: str | None          # 'above_upper', 'below_lower', 'within'
    confluence_count: int | None
    confluence_total: int | None
    confluence_details: dict | None
    volume_zscore: float | None
    signal_type: str | None          # 'mean_reversion', 'trend_breakout', 'unusual_move'


def evaluate_price_alert(
    classification: str | StockClass,
    pct_change: float,
    daily_prices: pd.DataFrame,
    current_volume: float | None = None,
) -> AlertTriggerResult:
    """Decide whether a price movement warrants an alert.

    Args:
        classification: Stock classification string or ``StockClass`` enum.
        pct_change: Current intraday percentage change (e.g. 5.0 = 5%).
        daily_prices: Daily OHLCV DataFrame with 'open', 'high', 'low', 'close', 'volume'.
        current_volume: Today's current volume (used for volume z-score).

    Returns:
        An :class:`AlertTriggerResult` describing whether to alert and why.
    """
    if not isinstance(classification, StockClass):
        try:
            classification = StockClass(classification)
        except ValueError:
            classification = StockClass.STANDARD

    # --- Compute common statistics ---
    zscore = float('nan')
    percentile = float('nan')
    vol_zscore = None

    if not daily_prices.empty and 'close' in daily_prices.columns:
        zscore = indicators.price.intraday_zscore(daily_prices, pct_change, period=20)
        percentile = indicators.price.return_percentile(daily_prices, pct_change, period=60)

    if current_volume is not None and not daily_prices.empty and 'volume' in daily_prices.columns:
        vol_zscore = signals.volume_zscore(
            volume_series=daily_prices['volume'],
            curr_volume=current_volume,
            period=20,
        )

    # --- Strategy dispatch ---
    if classification == StockClass.BLUE_CHIP:
        return _blue_chip_strategy(
            classification=classification,
            pct_change=pct_change,
            daily_prices=daily_prices,
            zscore=zscore,
            percentile=percentile,
            vol_zscore=vol_zscore,
        )
    else:
        return _standard_strategy(
            classification=classification,
            pct_change=pct_change,
            zscore=zscore,
            percentile=percentile,
            vol_zscore=vol_zscore,
        )


def _standard_strategy(
    classification: StockClass,
    pct_change: float,
    zscore: float,
    percentile: float,
    vol_zscore: float | None,
) -> AlertTriggerResult:
    """Volatile, Meme, and Standard strategy: trigger on abs(z-score) >= threshold."""
    import math
    threshold = _ZSCORE_THRESHOLDS.get(classification, 2.5)
    should_alert = (not math.isnan(zscore)) and abs(zscore) >= threshold

    return AlertTriggerResult(
        should_alert=should_alert,
        classification=classification,
        zscore=zscore,
        percentile=percentile,
        bb_position=None,
        confluence_count=None,
        confluence_total=None,
        confluence_details=None,
        volume_zscore=vol_zscore,
        signal_type='unusual_move' if should_alert else None,
    )


def _blue_chip_strategy(
    classification: StockClass,
    pct_change: float,
    daily_prices: pd.DataFrame,
    zscore: float,
    percentile: float,
    vol_zscore: float | None,
) -> AlertTriggerResult:
    """Blue chip strategy: BB breach + volume confirmation, with confluence fallback."""
    import math

    bb_position = None
    confluence_count = None
    confluence_total = None
    confluence_details = None
    signal_type = None
    should_alert = False

    has_vol_confirm = (
        vol_zscore is not None
        and not math.isnan(vol_zscore)
        and vol_zscore >= _BLUE_CHIP_VOLUME_ZSCORE_MIN
    )

    # Compute Bollinger Bands
    if not daily_prices.empty and 'close' in daily_prices.columns:
        bb_df = signals.bollinger_bands(daily_prices['close'])
        current_price = daily_prices['close'].iloc[-1] if not daily_prices.empty else None

        if not bb_df.empty and current_price is not None:
            # Pick upper and lower band columns (first and last of the four/five columns)
            try:
                cols = bb_df.columns.tolist()
                # pandas_ta names: BBL_20_2.0, BBM_20_2.0, BBU_20_2.0, ...
                bb_lower_col = next((c for c in cols if c.startswith('BBL')), None)
                bb_upper_col = next((c for c in cols if c.startswith('BBU')), None)

                if bb_lower_col and bb_upper_col:
                    bb_lower_val = bb_df[bb_lower_col].iloc[-1]
                    bb_upper_val = bb_df[bb_upper_col].iloc[-1]

                    if current_price >= bb_upper_val:
                        bb_position = 'above_upper'
                    elif current_price <= bb_lower_val:
                        bb_position = 'below_lower'
                    else:
                        bb_position = 'within'
            except Exception as exc:
                logger.debug(f"BB position determination failed: {exc}")

        # Compute technical confluence
        if all(col in daily_prices.columns for col in ['close', 'high', 'low', 'volume']):
            confluence_count, confluence_total, confluence_details = signals.technical_confluence(
                close=daily_prices['close'],
                high=daily_prices['high'],
                low=daily_prices['low'],
                volume=daily_prices['volume'],
            )

        # Primary trigger: BB breach + volume confirmation
        if bb_position in ('above_upper', 'below_lower') and has_vol_confirm:
            # Mean reversion: at lower BB + (RSI oversold OR OBV increasing)
            if bb_position == 'below_lower':
                rsi_bullish = (confluence_details or {}).get('rsi', False)
                obv_bullish = (confluence_details or {}).get('obv', False)
                if rsi_bullish or obv_bullish:
                    should_alert = True
                    signal_type = 'mean_reversion'
            # Trend breakout: at upper BB + (ADX strong OR MACD bullish)
            elif bb_position == 'above_upper':
                adx_bullish = (confluence_details or {}).get('adx', False)
                macd_bullish = (confluence_details or {}).get('macd', False)
                if adx_bullish or macd_bullish:
                    should_alert = True
                    signal_type = 'trend_breakout'

        # Fallback: 3+ confluence signals + volume confirm + z-score >= 2.0
        if not should_alert:
            valid_count = confluence_count if confluence_count is not None else 0
            zs_ok = not math.isnan(zscore) and abs(zscore) >= 2.0
            if valid_count >= _BLUE_CHIP_CONFLUENCE_MIN and has_vol_confirm and zs_ok:
                should_alert = True
                signal_type = 'unusual_move'

    return AlertTriggerResult(
        should_alert=should_alert,
        classification=classification,
        zscore=zscore,
        percentile=percentile,
        bb_position=bb_position,
        confluence_count=confluence_count,
        confluence_total=confluence_total,
        confluence_details=confluence_details,
        volume_zscore=vol_zscore,
        signal_type=signal_type,
    )
