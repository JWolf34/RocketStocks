"""Per-ticker streaming generator for EDA pipelines.

Yields one (ticker, price_df, pop_df) triple at a time so callers maintain
an O(1) peak-memory working set rather than materialising the full
universe × history panel.
"""
import datetime
import logging

import pandas as pd

logger = logging.getLogger(__name__)

_DAILY_PRICE_TABLE = 'daily_price_history'
_5M_PRICE_TABLE = 'five_minute_price_history'


async def stream_tickers(
    stock_data,
    tickers: list[str],
    timeframe: str,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
):
    """Async generator yielding (ticker, price_df, pop_df) one ticker at a time.

    Price and popularity DataFrames for the previous ticker are deleted before
    fetching the next one, keeping peak RSS bounded to roughly one ticker's data.

    Args:
        stock_data: StockData singleton with DB access.
        tickers: Ordered sequence of tickers to stream.
        timeframe: 'daily' or '5m'.
        start_date: Earliest date to include (inclusive).
        end_date: Latest date to include (inclusive).

    Yields:
        (ticker, price_df, pop_df) — either DataFrame may be empty if the DB
        has no data for that ticker/range.
    """
    for ticker in tickers:
        price_df = await _fetch_price(stock_data, ticker, timeframe, start_date, end_date)
        pop_df = await stock_data.popularity.fetch_popularity(
            ticker=ticker, start_date=start_date, end_date=end_date,
        )
        yield ticker, price_df, pop_df
        del price_df, pop_df


async def fetch_bar_counts(
    stock_data,
    tickers: list[str],
    timeframe: str,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> dict[str, int]:
    """Return {ticker: bar_count} via a single SQL COUNT(*) GROUP BY query.

    Issues one lightweight query instead of pulling any price data.
    Used by build_control_group to sample random (ticker, bar-offset) pairs.

    Args:
        stock_data: StockData singleton.
        tickers: Tickers to count bars for.
        timeframe: 'daily' or '5m'.
        start_date: Earliest date to include.
        end_date: Latest date to include.

    Returns:
        Dict mapping ticker → bar count in the requested window.
        Tickers with zero bars are omitted.
    """
    if not tickers:
        return {}

    if timeframe == 'daily':
        table = _DAILY_PRICE_TABLE
        date_col = 'date'
        start_val = start_date
        end_val = end_date
    else:
        table = _5M_PRICE_TABLE
        date_col = 'datetime'
        start_val = (
            datetime.datetime.combine(start_date, datetime.time.min) if start_date else None
        )
        end_val = (
            datetime.datetime.combine(end_date, datetime.time.max) if end_date else None
        )

    conditions = ["ticker = ANY(%s)"]
    params: list = [tickers]

    if start_val is not None:
        conditions.append(f"{date_col} >= %s")
        params.append(start_val)
    if end_val is not None:
        conditions.append(f"{date_col} <= %s")
        params.append(end_val)

    where = " AND ".join(conditions)
    query = f"SELECT ticker, COUNT(*) FROM {table} WHERE {where} GROUP BY ticker"
    rows = await stock_data.db.execute(query, params)
    if not rows:
        return {}
    return {ticker: int(count) for ticker, count in rows}


async def fetch_distinct_tickers(
    stock_data,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> list[str]:
    """Return distinct tickers from the popularity table within the date window.

    Used by the cross-correlation engine when the caller does not restrict
    analysis to a specific ticker list.

    Args:
        stock_data: StockData singleton.
        start_date: Earliest popularity snapshot date to include.
        end_date: Latest popularity snapshot date to include.

    Returns:
        Sorted list of ticker strings.
    """
    import datetime as _dt
    conditions: list[str] = []
    params: list = []

    if start_date is not None:
        conditions.append("datetime >= %s")
        params.append(start_date)
    if end_date is not None:
        end_cutoff = (
            end_date + _dt.timedelta(days=1)
            if isinstance(end_date, _dt.date)
            else end_date
        )
        conditions.append("datetime < %s")
        params.append(end_cutoff)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    query = f"SELECT DISTINCT ticker FROM popularity {where} ORDER BY ticker"
    rows = await stock_data.db.execute(query, params or None)
    return [row[0] for row in rows] if rows else []


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _fetch_price(
    stock_data,
    ticker: str,
    timeframe: str,
    start_date: datetime.date | None,
    end_date: datetime.date | None,
) -> pd.DataFrame:
    """Fetch price data for a single ticker using the existing per-ticker SQL path."""
    if timeframe == 'daily':
        return await stock_data.price_history.fetch_daily_price_history(
            ticker,
            start_date=start_date,
            end_date=end_date,
        )
    else:
        start_dt = (
            datetime.datetime.combine(start_date, datetime.time.min) if start_date else None
        )
        end_dt = (
            datetime.datetime.combine(end_date, datetime.time.max) if end_date else None
        )
        return await stock_data.price_history.fetch_5m_price_history(
            ticker,
            start_datetime=start_dt,
            end_datetime=end_dt,
        )
