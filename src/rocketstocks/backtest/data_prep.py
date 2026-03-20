"""DataFrame preparation for backtesting.py compatibility."""
import datetime
import logging
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_market_calendars as mcal

logger = logging.getLogger(__name__)

_ET = ZoneInfo('America/New_York')
_MARKET_OPEN = datetime.time(9, 30)
_MARKET_CLOSE = datetime.time(16, 0)

_RENAME_MAP = {
    'open': 'Open',
    'high': 'High',
    'low': 'Low',
    'close': 'Close',
    'volume': 'Volume',
}
_REVERSE_RENAME_MAP = {v: k for k, v in _RENAME_MAP.items()}

# NYSE calendar instance (module-level, loaded once)
try:
    _NYSE = mcal.get_calendar('NYSE')
except Exception:
    _NYSE = None
    logger.warning('pandas_market_calendars NYSE calendar unavailable; using fixed-time fallback')


def prep_daily(df: pd.DataFrame) -> pd.DataFrame:
    """Convert a DB-format daily OHLCV DataFrame to backtesting.py format.

    Args:
        df: DataFrame from PriceHistoryRepository with columns:
            ticker, open, high, low, close, volume, date

    Returns:
        DataFrame with columns Open, High, Low, Close, Volume and a
        DatetimeIndex named 'Date', sorted ascending. Empty DataFrame
        if input is empty or has insufficient columns.
    """
    if df.empty:
        return pd.DataFrame()

    result = df.copy()
    result = result.drop(columns=['ticker'], errors='ignore')
    result = result.rename(columns=_RENAME_MAP)

    if 'date' in result.columns:
        result['Date'] = pd.to_datetime(result['date'])
        result = result.drop(columns=['date'])
    elif 'Date' not in result.columns:
        logger.warning('prep_daily: no date column found')
        return pd.DataFrame()

    result = result.set_index('Date')
    result = result.sort_index()

    for col in ('Open', 'High', 'Low', 'Close', 'Volume'):
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors='coerce')

    return result


def prep_5m(df: pd.DataFrame) -> pd.DataFrame:
    """Convert a DB-format 5-minute OHLCV DataFrame to backtesting.py format.

    Args:
        df: DataFrame from PriceHistoryRepository with columns:
            ticker, open, high, low, close, volume, datetime

    Returns:
        DataFrame with columns Open, High, Low, Close, Volume and a
        DatetimeIndex named 'Datetime', sorted ascending.
    """
    if df.empty:
        return pd.DataFrame()

    result = df.copy()
    result = result.drop(columns=['ticker'], errors='ignore')
    result = result.rename(columns=_RENAME_MAP)

    if 'datetime' in result.columns:
        result['Datetime'] = pd.to_datetime(result['datetime'])
        result = result.drop(columns=['datetime'])
    elif 'Datetime' not in result.columns:
        logger.warning('prep_5m: no datetime column found')
        return pd.DataFrame()

    result = result.set_index('Datetime')
    result = result.sort_index()

    for col in ('Open', 'High', 'Low', 'Close', 'Volume'):
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors='coerce')

    return result


def filter_regular_hours(df: pd.DataFrame) -> pd.DataFrame:
    """Filter a 5-minute DataFrame to regular market hours bars only.

    Uses the NYSE calendar to handle early close days correctly.
    Falls back to fixed 9:30–16:00 ET if the calendar is unavailable.

    Extended hours bars remain in the full DataFrame for signal detection,
    but this function produces the subset where orders can fill.

    Args:
        df: DataFrame with DatetimeIndex (from prep_5m).

    Returns:
        Filtered DataFrame containing only regular-session bars.
    """
    if df.empty:
        return df

    idx = df.index
    if idx.tz is None:
        idx = idx.tz_localize(_ET)
    else:
        idx = idx.tz_convert(_ET)

    if _NYSE is not None:
        try:
            start_date = idx.date.min()
            end_date = idx.date.max()
            schedule = _NYSE.schedule(start_date=start_date, end_date=end_date)
            if not schedule.empty:
                # Build a mask: bar is in regular hours for its date
                mask = pd.Series(False, index=df.index)
                for _, row in schedule.iterrows():
                    day_open = row['market_open'].tz_convert(_ET)
                    day_close = row['market_close'].tz_convert(_ET)
                    day_mask = (idx >= day_open) & (idx < day_close)
                    mask |= day_mask.values
                return df.loc[mask]
        except Exception as exc:
            logger.warning(f'NYSE calendar schedule failed, using fixed hours: {exc}')

    # Fallback: fixed 9:30–16:00 ET
    mask = (idx.time >= _MARKET_OPEN) & (idx.time < _MARKET_CLOSE)
    return df.loc[mask]


def prep_for_signals(df: pd.DataFrame) -> pd.DataFrame:
    """Reverse the column rename for passing data to core/analysis signal functions.

    Signal functions in core/analysis/ expect lowercase column names
    (open, high, low, close, volume). This converts back from the
    capitalized format used by backtesting.py.

    Args:
        df: DataFrame with capitalized columns (Open, High, Low, Close, Volume).

    Returns:
        DataFrame with lowercase column names.
    """
    return df.rename(columns=_REVERSE_RENAME_MAP)
