"""Alert trigger strategy module — replaces scattered hardcoded thresholds.

No discord or data imports. All functions are pure analysis logic.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass

import pandas as pd

from rocketstocks.core.analysis.classification import (
    StockClass,
    compute_volatility,
    dynamic_zscore_threshold,
)
from rocketstocks.core.analysis.indicators import indicators
from rocketstocks.core.analysis.signals import signals

logger = logging.getLogger(__name__)

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


@dataclass
class ConfirmationResult:
    """Result of evaluating whether price movement since a leading indicator warrants confirmation."""
    should_confirm: bool
    pct_since_flag: float | None    # % price change since leading indicator was flagged
    zscore_since_flag: float        # z-scored against the ticker's own intraday distribution
    is_sustained: bool | None       # True if price is consistently moving in the same direction


def evaluate_confirmation(
    price_at_flag: float,
    current_price: float,
    mean_return: float,
    std_return: float,
    zscore_threshold: float = 1.5,
    observations: list[dict] | None = None,
) -> ConfirmationResult:
    """Evaluate whether price has moved significantly since a leading indicator was flagged.

    Uses the ticker's own historical return distribution for z-scoring, so "significant"
    is relative to how the ticker normally moves — not an absolute threshold.

    Args:
        price_at_flag: Price at the time the leading indicator fired.
        current_price: Current price.
        mean_return: Mean daily return (%) for the ticker over the lookback period.
        std_return: Standard deviation of daily returns (%) over the lookback period.
        zscore_threshold: Minimum abs(z-score) to confirm (default 1.5).
        observations: Optional list of prior observation dicts, each with a
            'pct_since_flag' key, for sustained-direction check.

    Returns:
        A :class:`ConfirmationResult` describing whether to confirm and why.
    """
    pct_since_flag: float | None = None
    zscore_since_flag: float = float('nan')
    is_sustained: bool | None = None

    if price_at_flag and price_at_flag != 0 and current_price:
        pct_since_flag = (current_price - price_at_flag) / price_at_flag * 100

    if pct_since_flag is not None and std_return and std_return != 0 and not math.isnan(std_return):
        zscore_since_flag = (pct_since_flag - mean_return) / std_return

    # Sustained-direction check: if multiple prior observations exist, price should
    # be consistently moving away from the flag price (not spiking and reversing).
    if (observations and len(observations) >= 2
            and pct_since_flag is not None):
        prior_pcts = [
            obs.get('pct_since_flag')
            for obs in observations
            if obs.get('pct_since_flag') is not None
        ]
        if len(prior_pcts) >= 2:
            direction = 1 if pct_since_flag > 0 else -1
            is_sustained = all(p * direction > 0 for p in prior_pcts)

    should_confirm = (
        pct_since_flag is not None
        and not math.isnan(zscore_since_flag)
        and abs(zscore_since_flag) >= zscore_threshold
    )

    return ConfirmationResult(
        should_confirm=should_confirm,
        pct_since_flag=pct_since_flag,
        zscore_since_flag=zscore_since_flag,
        is_sustained=is_sustained,
    )


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
    volatility_20d = float('nan')

    if not daily_prices.empty and 'close' in daily_prices.columns:
        volatility_20d = compute_volatility(daily_prices)
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
            volatility_20d=volatility_20d,
        )
    else:
        return _standard_strategy(
            classification=classification,
            pct_change=pct_change,
            zscore=zscore,
            percentile=percentile,
            vol_zscore=vol_zscore,
            volatility_20d=volatility_20d,
        )


def _standard_strategy(
    classification: StockClass,
    pct_change: float,
    zscore: float,
    percentile: float,
    vol_zscore: float | None,
    volatility_20d: float,
) -> AlertTriggerResult:
    """Volatile, Meme, and Standard strategy: trigger on abs(z-score) >= dynamic threshold."""
    import math
    threshold = dynamic_zscore_threshold(volatility_20d)
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
    volatility_20d: float,
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

        # Fallback: 3+ confluence signals + volume confirm + z-score >= dynamic threshold
        if not should_alert:
            valid_count = confluence_count if confluence_count is not None else 0
            threshold = dynamic_zscore_threshold(volatility_20d)
            zs_ok = not math.isnan(zscore) and abs(zscore) >= threshold
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
