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


def mark_regular_hours(df: pd.DataFrame) -> pd.DataFrame:
    """Add an Is_Regular_Hours boolean column to a 5-minute DataFrame.

    Unlike filter_regular_hours(), this keeps all bars (including extended hours)
    and marks which bars fall within regular market hours. Strategies can detect
    signals on any bar while restricting trade execution to regular-hours bars.

    Uses the NYSE calendar to handle early close days correctly.
    Falls back to fixed 9:30–16:00 ET if the calendar is unavailable.

    Args:
        df: DataFrame with DatetimeIndex (from prep_5m).

    Returns:
        DataFrame with an added 'Is_Regular_Hours' boolean column.
    """
    if df.empty:
        return df.copy()

    result = df.copy()
    idx = df.index
    if idx.tz is None:
        idx = idx.tz_localize(_ET)
    else:
        idx = idx.tz_convert(_ET)

    mask = pd.Series(False, index=df.index)

    if _NYSE is not None:
        try:
            start_date = idx.date.min()
            end_date = idx.date.max()
            schedule = _NYSE.schedule(start_date=start_date, end_date=end_date)
            if not schedule.empty:
                for _, row in schedule.iterrows():
                    day_open = row['market_open'].tz_convert(_ET)
                    day_close = row['market_close'].tz_convert(_ET)
                    day_mask = (idx >= day_open) & (idx < day_close)
                    mask |= day_mask.values
                result['Is_Regular_Hours'] = mask.values
                return result
        except Exception as exc:
            logger.warning(f'NYSE calendar schedule failed, using fixed hours: {exc}')

    # Fallback: fixed 9:30–16:00 ET
    mask = (idx.time >= _MARKET_OPEN) & (idx.time < _MARKET_CLOSE)
    result['Is_Regular_Hours'] = mask
    return result


def enrich_5m_with_daily_context(df_5m: pd.DataFrame, df_daily: pd.DataFrame) -> pd.DataFrame:
    """Enrich a 5-minute price DataFrame with daily context statistics.

    Pre-computes rolling 20-day daily statistics and merges them onto each 5-minute
    bar, so strategies can evaluate signals the same way the production bot does:
    using daily baselines for volume and return z-scores rather than short intraday
    lookbacks.

    Added columns:
        Prev_Close: previous trading day's closing price
        Daily_Vol_Mean: rolling 20-day mean daily volume (as of prior day)
        Daily_Vol_Std: rolling 20-day std of daily volume (as of prior day)
        Daily_Return_Mean: rolling 20-day mean of daily pct returns (as of prior day)
        Daily_Return_Std: rolling 20-day std of daily pct returns (as of prior day)
        Cumulative_Volume: running volume sum from start of each trading day
        Intraday_Pct_Change: (current_close / prev_close - 1) * 100

    Args:
        df_5m: 5-minute DataFrame (from prep_5m — DatetimeIndex, capitalised columns).
        df_daily: Daily DataFrame (from prep_daily — DatetimeIndex, capitalised columns).
            Should include at least 20 extra days before the 5m start date so rolling
            stats are fully populated from the first 5m bar.

    Returns:
        Enriched 5-minute DataFrame with the additional columns.
    """
    if df_5m.empty or df_daily.empty:
        return df_5m.copy()

    result = df_5m.copy()

    # --- Rolling daily statistics (shift(1) = no lookahead) ---
    daily = df_daily.copy()
    pct_returns = daily['Close'].pct_change() * 100

    daily_stats = pd.DataFrame(
        {
            'Prev_Close': daily['Close'].shift(1),
            'Daily_Vol_Mean': daily['Volume'].rolling(20, min_periods=1).mean().shift(1),
            'Daily_Vol_Std': daily['Volume'].rolling(20, min_periods=1).std().shift(1),
            'Daily_Return_Mean': pct_returns.rolling(20, min_periods=1).mean().shift(1),
            'Daily_Return_Std': pct_returns.rolling(20, min_periods=1).std().shift(1),
        },
        index=daily.index.normalize(),
    )

    # --- Map each 5m bar to its ET trading date ---
    idx_5m = result.index
    if idx_5m.tz is None:
        idx_et = idx_5m.tz_localize(_ET)
    else:
        idx_et = idx_5m.tz_convert(_ET)

    # DatetimeIndex of midnight timestamps (one per 5m bar, many duplicates per day)
    trading_dates = pd.DatetimeIndex(pd.to_datetime(idx_et.date))

    # Align daily stats to 5m bars; ffill handles dates missing from daily (e.g. no-trade-day gaps)
    aligned = daily_stats.reindex(trading_dates, method='ffill')

    for col in ('Prev_Close', 'Daily_Vol_Mean', 'Daily_Vol_Std',
                'Daily_Return_Mean', 'Daily_Return_Std'):
        result[col] = aligned[col].values

    # --- Cumulative volume: running sum from start of each trading day ---
    date_labels = pd.Series(idx_et.strftime('%Y-%m-%d'), index=result.index)
    result['Cumulative_Volume'] = result.groupby(date_labels)['Volume'].cumsum()

    # --- Intraday pct change from previous close ---
    result['Intraday_Pct_Change'] = (result['Close'] / result['Prev_Close'] - 1) * 100

    return result


def merge_popularity(price_df: pd.DataFrame, popularity_df: pd.DataFrame) -> pd.DataFrame:
    """Merge popularity data onto a price DataFrame using forward-fill alignment.

    Popularity data is collected every ~30 minutes. Each reading is forward-filled
    onto all price bars until the next reading arrives. Bars before the first
    popularity reading receive NaN.

    Adds columns: Rank, Mentions, Rank_24h_ago, Mentions_24h_ago.

    Args:
        price_df: Price DataFrame with DatetimeIndex (from prep_5m or prep_daily).
        popularity_df: DataFrame from PopularityRepository.fetch_popularity() with
            columns: datetime, rank, mentions, rank_24h_ago, mentions_24h_ago.

    Returns:
        Price DataFrame with the four popularity columns added.
    """
    result = price_df.copy()
    _pop_cols = ('Rank', 'Mentions', 'Rank_24h_ago', 'Mentions_24h_ago')

    if popularity_df is None or popularity_df.empty or 'datetime' not in popularity_df.columns:
        for col in _pop_cols:
            result[col] = float('nan')
        return result

    pop = popularity_df.copy()
    pop['datetime'] = pd.to_datetime(pop['datetime'])
    pop = pop.sort_values('datetime').drop_duplicates('datetime')

    # Normalise both sides to UTC for timestamp alignment
    price_idx = result.index
    if price_idx.tz is None:
        price_idx_utc = price_idx.tz_localize('UTC')
    else:
        price_idx_utc = price_idx.tz_convert('UTC')

    pop_ts = pop['datetime']
    if pop_ts.dt.tz is None:
        pop_ts = pop_ts.dt.tz_localize('UTC')
    else:
        pop_ts = pop_ts.dt.tz_convert('UTC')

    pop_indexed = pd.DataFrame(
        {
            'Rank': pop['rank'].values,
            'Mentions': pop['mentions'].values,
            'Rank_24h_ago': pop['rank_24h_ago'].values,
            'Mentions_24h_ago': pop['mentions_24h_ago'].values,
        },
        index=pop_ts,
    )

    # Union the two indexes, ffill popularity readings across price timestamps,
    # then select only the price bar timestamps.
    combined_idx = price_idx_utc.union(pop_ts).sort_values()
    pop_aligned = pop_indexed.reindex(combined_idx).ffill().reindex(price_idx_utc)

    result['Rank'] = pop_aligned['Rank'].values
    result['Mentions'] = pop_aligned['Mentions'].values
    result['Rank_24h_ago'] = pop_aligned['Rank_24h_ago'].values
    result['Mentions_24h_ago'] = pop_aligned['Mentions_24h_ago'].values

    return result


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
