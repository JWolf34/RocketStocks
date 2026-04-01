"""Market regime classification for backtesting analysis.

Classifies each trading day into a market regime based on SPY price data.
Regimes are used to tag trade entries so aggregate stats can be broken down
by market conditions (e.g., "does this strategy only work in bull markets?").

Regime definitions
------------------
BULL        SPY above its 200-day SMA, and 50-day SMA slope is positive.
BEAR        SPY below its 200-day SMA, and 50-day SMA slope is negative.
CORRECTION  SPY above 200-day SMA but drawdown from 52-week high exceeds 7%.
RECOVERY    SPY below 200-day SMA but 50-day SMA slope has turned positive.

The UNKNOWN regime is assigned when data is insufficient or ambiguous.
"""
import datetime
import logging
from enum import Enum

import pandas as pd

logger = logging.getLogger(__name__)

# Minimum bars required for the 200-day SMA to be meaningful
_MIN_BARS_200 = 150

# Drawdown threshold that triggers CORRECTION even in a bull trend
_CORRECTION_DRAWDOWN_PCT = -7.0


class MarketRegime(str, Enum):
    """Market regime classification."""
    BULL = 'bull'
    BEAR = 'bear'
    CORRECTION = 'correction'
    RECOVERY = 'recovery'
    UNKNOWN = 'unknown'


def classify_regimes(spy_daily_df: pd.DataFrame) -> dict[datetime.date, MarketRegime]:
    """Classify each trading day into a market regime.

    Args:
        spy_daily_df: Daily OHLCV DataFrame for SPY (or any broad-market ETF)
            in backtesting.py format — DatetimeIndex, columns Open/High/Low/Close/Volume.
            Should cover at least 200 trading days before the target date range so
            SMAs are fully seeded.

    Returns:
        Dict mapping each date to a MarketRegime. Dates before the SMA warm-up
        period receive MarketRegime.UNKNOWN.
    """
    if spy_daily_df is None or spy_daily_df.empty:
        return {}

    df = spy_daily_df.copy()
    close = df['Close']

    # Rolling SMAs
    sma50 = close.rolling(50, min_periods=1).mean()
    sma200 = close.rolling(200, min_periods=1).mean()

    # 50-day SMA slope: positive if today's SMA > 10-day-ago SMA
    sma50_slope = sma50 - sma50.shift(10)

    # Rolling 52-week high (252 trading days)
    high_52w = close.rolling(252, min_periods=1).max()
    drawdown_from_peak = (close / high_52w - 1) * 100

    regimes: dict[datetime.date, MarketRegime] = {}

    for i, (idx, row_close) in enumerate(close.items()):
        date = idx.date() if hasattr(idx, 'date') else idx

        if i < _MIN_BARS_200:
            regimes[date] = MarketRegime.UNKNOWN
            continue

        above_200 = row_close > sma200.iloc[i]
        slope_pos = sma50_slope.iloc[i] > 0
        dd = drawdown_from_peak.iloc[i]

        if above_200 and dd < _CORRECTION_DRAWDOWN_PCT:
            regime = MarketRegime.CORRECTION
        elif above_200 and slope_pos:
            regime = MarketRegime.BULL
        elif not above_200 and slope_pos:
            regime = MarketRegime.RECOVERY
        else:
            regime = MarketRegime.BEAR

        regimes[date] = regime

    logger.debug(
        f"classify_regimes: {len(regimes)} dates classified. "
        f"Distribution: "
        + ", ".join(
            f"{r.value}={sum(1 for v in regimes.values() if v == r)}"
            for r in MarketRegime
        )
    )
    return regimes


def tag_trades_with_regime(
    trades: list[dict],
    regime_map: dict[datetime.date, MarketRegime],
) -> list[dict]:
    """Add a 'regime' field to each trade based on its entry date.

    Args:
        trades: List of trade dicts from ``_extract_trades()``. Each must have
            an ``entry_time`` field (datetime or date).
        regime_map: Dict from ``classify_regimes()``.

    Returns:
        Same list with 'regime' set to the MarketRegime value string (or 'unknown').
    """
    for trade in trades:
        entry = trade.get('entry_time')
        if entry is None:
            trade['regime'] = MarketRegime.UNKNOWN.value
            continue
        entry_date = entry.date() if hasattr(entry, 'date') else entry
        regime = regime_map.get(entry_date, MarketRegime.UNKNOWN)
        trade['regime'] = regime.value
    return trades
