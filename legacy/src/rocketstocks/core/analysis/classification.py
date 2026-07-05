"""Stock classification module — pure analysis logic, no discord or data imports."""
import logging
from enum import Enum

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_MEME_VOLATILITY_THRESHOLD = 4.0        # % daily std dev
_MEME_POPULARITY_RANK_THRESHOLD = 50    # rank <= this (lower = more popular)
_VOLATILE_MARKET_CAP_THRESHOLD = 2e9    # $2B
_VOLATILE_VOLATILITY_THRESHOLD = 4.0   # % daily std dev
_BLUE_CHIP_MARKET_CAP_THRESHOLD = 10e9  # $10B
_BLUE_CHIP_VOLATILITY_THRESHOLD = 1.5  # % daily std dev


class StockClass(str, Enum):
    VOLATILE = "volatile"    # penny stocks, high-vol small caps
    MEME = "meme"            # high popularity + high volatility
    BLUE_CHIP = "blue_chip"  # large cap, low volatility
    STANDARD = "standard"    # everything else


def compute_volatility(daily_prices_df: pd.DataFrame, period: int = 20) -> float:
    """Return the annualised-daily std-dev of returns (as a percentage) over *period* days.

    Args:
        daily_prices_df: DataFrame with a 'close' column.
        period: Number of most-recent trading days to use.

    Returns:
        Volatility as a percentage (e.g. 3.5 means 3.5%).  Returns NaN if
        there is insufficient data.
    """
    if daily_prices_df.empty or 'close' not in daily_prices_df.columns:
        return float('nan')
    close = daily_prices_df['close'].tail(period + 1)
    if len(close) < 2:
        return float('nan')
    pct_returns = close.pct_change().dropna() * 100.0
    return float(pct_returns.std())


def compute_return_stats(daily_prices_df: pd.DataFrame, period: int = 60) -> tuple[float, float]:
    """Return (mean, std) of daily percentage returns over *period* days.

    Returns (NaN, NaN) if there is insufficient data.
    """
    if daily_prices_df.empty or 'close' not in daily_prices_df.columns:
        return float('nan'), float('nan')
    close = daily_prices_df['close'].tail(period + 1)
    if len(close) < 2:
        return float('nan'), float('nan')
    pct_returns = close.pct_change().dropna() * 100.0
    return float(pct_returns.mean()), float(pct_returns.std())


def classify_ticker(
    ticker: str,
    market_cap: float | None,
    volatility_20d: float | None,
    popularity_rank: int | None = None,
    watchlist_override: str | None = None,
) -> StockClass:
    """Classify a ticker into a StockClass.

    Priority order:
    1. Watchlist override (e.g. from watchlists named ``class:volatile``)
    2. Meme: popularity rank <= 50 AND volatility > 4%
    3. Volatile: market_cap < $2B AND volatility > 4%
    4. Blue chip: market_cap >= $10B AND volatility < 1.5%
    5. Default: standard

    Args:
        ticker: Ticker symbol (used only for logging).
        market_cap: Market capitalisation in USD, or None if unknown.
        volatility_20d: 20-day daily return std-dev in percent, or None if unknown.
        popularity_rank: Current WallStreetBets-style popularity rank (lower = more popular).
        watchlist_override: Classification string if the ticker is on a classification watchlist.

    Returns:
        A :class:`StockClass` value.
    """
    # 1. Watchlist override takes priority
    if watchlist_override is not None:
        try:
            cls = StockClass(watchlist_override.lower())
            logger.debug(f"Ticker '{ticker}' classification overridden to '{cls}' via watchlist")
            return cls
        except ValueError:
            logger.warning(f"Unknown watchlist override '{watchlist_override}' for '{ticker}'; ignoring")

    vol = volatility_20d if (volatility_20d is not None and not _isnan(volatility_20d)) else None
    cap = market_cap if market_cap is not None else None

    # 2. Meme: high popularity + high volatility
    if (
        popularity_rank is not None
        and popularity_rank <= _MEME_POPULARITY_RANK_THRESHOLD
        and vol is not None
        and vol > _MEME_VOLATILITY_THRESHOLD
    ):
        logger.debug(f"Ticker '{ticker}' classified as MEME (rank={popularity_rank}, vol={vol:.2f}%)")
        return StockClass.MEME

    # 3. Volatile: small cap + high volatility
    if (
        cap is not None
        and cap < _VOLATILE_MARKET_CAP_THRESHOLD
        and vol is not None
        and vol > _VOLATILE_VOLATILITY_THRESHOLD
    ):
        logger.debug(f"Ticker '{ticker}' classified as VOLATILE (cap={cap:.0f}, vol={vol:.2f}%)")
        return StockClass.VOLATILE

    # 4. Blue chip: large cap + low volatility
    if (
        cap is not None
        and cap >= _BLUE_CHIP_MARKET_CAP_THRESHOLD
        and vol is not None
        and vol < _BLUE_CHIP_VOLATILITY_THRESHOLD
    ):
        logger.debug(f"Ticker '{ticker}' classified as BLUE_CHIP (cap={cap:.0f}, vol={vol:.2f}%)")
        return StockClass.BLUE_CHIP

    logger.debug(f"Ticker '{ticker}' classified as STANDARD")
    return StockClass.STANDARD


def dynamic_zscore_threshold(volatility_20d: float, max_volatility: float = 8.0) -> float:
    """Return a z-score threshold scaled continuously by volatility.

    Higher volatility → lower threshold (alerts trigger more easily).
    Lower volatility → higher threshold (alerts are harder to trigger).

    Range: 1.5 (at max_volatility or above) to 3.0 (at zero volatility).
    Falls back to 2.5 if volatility is unknown (NaN or negative).

    Eliminates cliff effects: adjacent volatility values produce similar thresholds
    rather than jumping between discrete per-class constants.

    Args:
        volatility_20d: 20-day daily return std-dev in percent.
        max_volatility: Volatility at which the threshold reaches its floor (1.5).

    Returns:
        A z-score threshold in the range [1.5, 3.0].
    """
    if _isnan(volatility_20d) or volatility_20d < 0:
        return 2.5  # neutral default for unknown volatility
    normalized = min(volatility_20d / max_volatility, 1.0)
    return 3.0 - (normalized * 1.5)


def _isnan(v) -> bool:
    try:
        return np.isnan(v)
    except (TypeError, ValueError):
        return False
