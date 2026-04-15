"""Data loading and panel construction for the EDA framework.

Provides aligned panel DataFrames and per-ticker close Series for use
by event detectors and analysis engines.  Handles both daily and intraday
(5-minute) timeframes.
"""
import datetime
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

async def load_popularity_raw(
    stock_data,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> pd.DataFrame:
    """Fetch all popularity snapshots and filter to the requested date window.

    Returns a DataFrame with all _POPULARITY_COLS columns, datetime parsed
    as Timestamp.  Rows ordered by (ticker, datetime) ascending.
    """
    pop = await stock_data.popularity.fetch_popularity(start_date=start_date, end_date=end_date)
    if pop.empty:
        return pop

    pop['datetime'] = pd.to_datetime(pop['datetime'])
    pop = pop.sort_values(['ticker', 'datetime'])
    return pop.reset_index(drop=True)


async def load_daily_panel(
    stock_data,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
    tickers: list[str] | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.Series]]:
    """Build an aligned daily panel of sentiment + price data.

    For each ticker×day, takes the intraday popularity snapshot with the
    highest mention_ratio.  Joins with daily close prices.

    Returns:
        panel_df: DataFrame indexed by integer with columns:
            ticker, date, close, volume, daily_return,
            mentions, rank, mentions_24h_ago, rank_24h_ago,
            mention_ratio, rank_change, mention_delta
        close_dict: {ticker: pd.Series(close, DatetimeIndex)} for forward
            return lookups.  Index is UTC-naive.
    """
    pop = await load_popularity_raw(stock_data, start_date, end_date)
    if pop.empty:
        logger.warning("No popularity data found for requested date range")
        return pd.DataFrame(), {}

    # Determine tickers from popularity universe (intersect with explicit list)
    pop_tickers = pop['ticker'].unique().tolist()
    if tickers:
        pop_tickers = [t for t in tickers if t in set(pop_tickers)]
    if not pop_tickers:
        logger.warning("No matching tickers found in popularity data")
        return pd.DataFrame(), {}

    print(f"Loading daily price data for {len(pop_tickers)} tickers...")

    # Aggregate popularity to daily — keep snapshot with highest mention_ratio
    pop = pop.copy()
    pop['date'] = pop['datetime'].dt.date
    pop['mention_ratio'] = _safe_ratio(pop['mentions'], pop['mentions_24h_ago'])
    pop['rank_change'] = pop['rank_24h_ago'] - pop['rank']  # positive = rank improved

    daily_pop = (
        pop.sort_values('mention_ratio', ascending=False)
        .groupby(['ticker', 'date'], sort=False)
        .first()
        .reset_index()
    )

    # Fetch daily price history
    price_dict = await stock_data.price_history.fetch_daily_price_history_batch(
        pop_tickers,
        start_date=start_date,
        end_date=end_date,
    )

    # Build close_dict for forward return lookups (DatetimeIndex, UTC-naive)
    close_dict: dict[str, pd.Series] = {}
    price_dfs: list[pd.DataFrame] = []

    for ticker, price_df in price_dict.items():
        if price_df.empty or 'close' not in price_df.columns:
            continue

        price_df = price_df.copy()
        price_df = price_df.sort_values('date')
        price_df['daily_return'] = price_df['close'].pct_change() * 100.0

        price_dfs.append(price_df)

        # Build close Series for this ticker
        idx = pd.to_datetime(price_df['date'].astype(str))
        close_dict[ticker] = pd.Series(price_df['close'].values, index=idx)

    if not price_dfs:
        logger.warning("No price data found for the resolved ticker set")
        return pd.DataFrame(), {}

    all_prices = pd.concat(price_dfs, ignore_index=True)

    # Join popularity onto price (left join to keep all price rows)
    all_prices['date'] = pd.to_datetime(all_prices['date']).dt.date
    daily_pop['date'] = pd.to_datetime(daily_pop['date']).dt.date

    panel = all_prices.merge(
        daily_pop[['ticker', 'date', 'mentions', 'rank',
                   'mentions_24h_ago', 'rank_24h_ago',
                   'mention_ratio', 'rank_change']],
        on=['ticker', 'date'],
        how='left',
    )

    # mention_delta: day-over-day change in mentions per ticker
    panel = panel.sort_values(['ticker', 'date'])
    panel['mention_delta'] = panel.groupby('ticker')['mentions'].diff()

    logger.info(
        f"Daily panel: {len(panel)} rows, {panel['ticker'].nunique()} tickers, "
        f"{panel['date'].min()} → {panel['date'].max()}"
    )
    return panel.reset_index(drop=True), close_dict


async def load_intraday_panel(
    stock_data,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
    tickers: list[str] | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.Series]]:
    """Build an aligned intraday (5-minute) panel of sentiment + price data.

    Popularity snapshots (30-min frequency) are forward-filled onto 5-minute
    price bars using the same alignment pattern as backtest/data_prep.py.

    Returns:
        panel_df: DataFrame with columns:
            ticker, datetime, close, volume, bar_return,
            mentions, rank, mentions_24h_ago, rank_24h_ago,
            mention_ratio, rank_change
        close_dict: {ticker: pd.Series(close, DatetimeIndex)} for forward
            return lookups.
    """
    pop = await load_popularity_raw(stock_data, start_date, end_date)
    if pop.empty:
        logger.warning("No popularity data found for requested date range")
        return pd.DataFrame(), {}

    pop['mention_ratio'] = _safe_ratio(pop['mentions'], pop['mentions_24h_ago'])
    pop['rank_change'] = pop['rank_24h_ago'] - pop['rank']

    pop_tickers = pop['ticker'].unique().tolist()
    if tickers:
        pop_tickers = [t for t in tickers if t in set(pop_tickers)]
    if not pop_tickers:
        logger.warning("No matching tickers found in popularity data")
        return pd.DataFrame(), {}

    start_dt = datetime.datetime.combine(start_date, datetime.time.min) if start_date else None
    end_dt = datetime.datetime.combine(end_date, datetime.time.max) if end_date else None

    n_total = len(pop_tickers)
    print(f"Fetching 5m price data for {n_total} tickers...")

    close_dict: dict[str, pd.Series] = {}
    panel_parts: list[pd.DataFrame] = []

    for i, ticker in enumerate(pop_tickers, 1):
        if i == 1 or i % 10 == 0 or i == n_total:
            print(f"  [{i}/{n_total}] {ticker}")
        price_df = await stock_data.price_history.fetch_5m_price_history(
            ticker,
            start_datetime=start_dt,
            end_datetime=end_dt,
        )
        if price_df.empty or 'close' not in price_df.columns:
            continue

        price_df = price_df.copy()
        price_df['datetime'] = pd.to_datetime(price_df['datetime'])
        price_df = price_df.sort_values('datetime')
        price_df['bar_return'] = price_df['close'].pct_change() * 100.0

        # Forward-fill popularity onto price bars
        ticker_pop = pop[pop['ticker'] == ticker].copy()
        price_with_pop = _merge_popularity_intraday(price_df, ticker_pop)

        panel_parts.append(price_with_pop)

        # Build close Series for forward return lookups
        close_dict[ticker] = pd.Series(
            price_df['close'].values,
            index=price_df['datetime'],
        )

    if not panel_parts:
        logger.warning("No 5m price data found for the resolved ticker set")
        return pd.DataFrame(), {}

    panel = pd.concat(panel_parts, ignore_index=True)

    # mention_delta: change in mentions at each snapshot boundary (zero between snapshots)
    panel = panel.sort_values(['ticker', 'datetime'])
    panel['mention_delta'] = panel.groupby('ticker')['mentions'].diff()

    logger.info(
        f"Intraday panel: {len(panel)} bars, {panel['ticker'].nunique()} tickers"
    )
    return panel, close_dict


async def load_spy_daily(
    stock_data,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> pd.DataFrame:
    """Fetch SPY daily price history for market regime classification.

    Returns a DataFrame with columns: ticker, open, high, low, close, volume, date.
    Returns an empty DataFrame if SPY data is unavailable.
    """
    spy = await stock_data.price_history.fetch_daily_price_history(
        'SPY',
        start_date=start_date,
        end_date=end_date,
    )
    if spy.empty:
        logger.warning("SPY price data unavailable — regime analysis will be skipped")
    return spy


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Compute numerator/denominator, returning NaN where denominator <= 0."""
    denom = denominator.replace(0, np.nan)
    return numerator / denom


def _merge_popularity_intraday(
    price_df: pd.DataFrame,
    pop_df: pd.DataFrame,
) -> pd.DataFrame:
    """Forward-fill popularity snapshots onto 5-minute price bars.

    Each popularity reading is carried forward until the next reading arrives.
    Bars before the first popularity reading receive NaN.

    Adds columns: mentions, rank, mentions_24h_ago, rank_24h_ago,
                  mention_ratio, rank_change.
    """
    result = price_df.copy()
    pop_cols = ('mentions', 'rank', 'mentions_24h_ago', 'rank_24h_ago',
                'mention_ratio', 'rank_change')

    if pop_df.empty:
        for col in pop_cols:
            result[col] = float('nan')
        return result

    pop = pop_df.copy()
    pop['datetime'] = pd.to_datetime(pop['datetime'])
    pop = pop.sort_values('datetime').drop_duplicates('datetime')

    # Build a mini DataFrame indexed by popularity snapshot timestamps
    pop_indexed = pop.set_index('datetime')[list(pop_cols)]

    # Align and forward-fill onto price bar index
    price_idx = result['datetime']
    combined_idx = price_idx.tolist() + pop_indexed.index.tolist()
    combined_idx_sorted = sorted(set(combined_idx))

    merged = pop_indexed.reindex(combined_idx_sorted).ffill()
    merged = merged.reindex(price_idx.values)

    for col in pop_cols:
        if col in merged.columns:
            result[col] = merged[col].values

    return result
