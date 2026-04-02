"""Money Flow Index (MFI) — continuous buy/sell volume pressure indicator.

MFI is a 0-100 oscillator that combines price direction with volume magnitude
to measure whether money is flowing into (buying pressure) or out of (selling
pressure) a security.  Unlike the existing ``obv()`` and ``ad()`` indicators,
which return booleans ("is it increasing?"), MFI returns a continuous score:

    MFI > 50  — net buying pressure  (money flowing in)
    MFI < 50  — net selling pressure (money flowing out)
    MFI > 80  — overbought territory
    MFI < 20  — oversold territory

This granularity is required by the direction regression model, which needs
continuous features to distinguish strong buying pressure from mild pressure.
"""
from __future__ import annotations

import logging
import math

import pandas as pd
import pandas_ta_classic as ta

logger = logging.getLogger(__name__)

_MIN_BARS = 2  # ta.mfi() requires at least period + 1 bars; we guard at call site


def compute_mfi(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    volume: pd.Series,
    period: int = 14,
) -> pd.Series:
    """Compute the Money Flow Index over a price/volume Series.

    Wraps ``ta.mfi()``.  The first *period* values in the returned Series
    will be NaN because MFI requires a full lookback window.

    Args:
        high:   Daily high prices.
        low:    Daily low prices.
        close:  Daily closing prices.
        volume: Daily volumes.
        period: Lookback window (default 14).

    Returns:
        Series of MFI values in the range [0, 100].  Returns an all-NaN
        Series of the same length if computation fails or data is insufficient.
    """
    if len(close) < period + 1:
        logger.debug(
            f'compute_mfi: insufficient data ({len(close)} bars, need {period + 1})'
        )
        return pd.Series(float('nan'), index=close.index)

    try:
        result = ta.mfi(high=high, low=low, close=close, volume=volume, length=period)
        if result is None or result.empty:
            return pd.Series(float('nan'), index=close.index)
        return result
    except Exception as exc:
        logger.warning(f'compute_mfi failed: {exc}')
        return pd.Series(float('nan'), index=close.index)


def mfi_signal(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    volume: pd.Series,
    period: int = 14,
) -> float:
    """Return the latest MFI value as a single float.

    Convenience wrapper around ``compute_mfi()`` for use in per-bar signal
    evaluation (live alerting, backtest ``next()`` loops).

    Args:
        high:   Daily high prices.
        low:    Daily low prices.
        close:  Daily closing prices.
        volume: Daily volumes.
        period: Lookback window (default 14).

    Returns:
        Latest MFI value in [0, 100], or NaN if data is insufficient.
    """
    mfi = compute_mfi(high=high, low=low, close=close, volume=volume, period=period)
    if mfi.empty:
        return float('nan')
    val = mfi.iloc[-1]
    return float(val) if not (isinstance(val, float) and math.isnan(val)) else float('nan')
