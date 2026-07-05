"""Tests for rocketstocks.eda.streaming."""
import datetime
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from rocketstocks.eda.streaming import fetch_bar_counts, fetch_distinct_tickers, stream_tickers


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_stock_data(bar_count_rows=None, price_rows=None, pop_rows=None):
    """Build a minimal mock StockData for streaming tests."""
    stock_data = MagicMock()

    # db.execute returns raw rows for COUNT(*) queries
    stock_data.db.execute = AsyncMock(return_value=bar_count_rows or [])

    # price_history per-ticker fetchers
    price_df = pd.DataFrame(price_rows or [], columns=['ticker', 'open', 'high', 'low', 'close', 'volume', 'date'])
    stock_data.price_history.fetch_daily_price_history = AsyncMock(return_value=price_df)
    stock_data.price_history.fetch_5m_price_history = AsyncMock(return_value=pd.DataFrame())

    # popularity per-ticker fetcher
    pop_df = pd.DataFrame(pop_rows or [], columns=[
        'datetime', 'rank', 'ticker', 'name',
        'mentions', 'upvotes', 'rank_24h_ago', 'mentions_24h_ago',
    ])
    stock_data.popularity.fetch_popularity = AsyncMock(return_value=pop_df)

    return stock_data


# ---------------------------------------------------------------------------
# fetch_bar_counts
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_bar_counts_returns_dict():
    rows = [('AAPL', 100), ('GOOG', 50)]
    sd = _make_stock_data(bar_count_rows=rows)
    result = await fetch_bar_counts(sd, ['AAPL', 'GOOG'], 'daily')
    assert result == {'AAPL': 100, 'GOOG': 50}


@pytest.mark.asyncio
async def test_fetch_bar_counts_empty_tickers():
    sd = _make_stock_data()
    result = await fetch_bar_counts(sd, [], 'daily')
    assert result == {}


@pytest.mark.asyncio
async def test_fetch_bar_counts_no_rows():
    sd = _make_stock_data(bar_count_rows=[])
    result = await fetch_bar_counts(sd, ['AAPL'], 'daily')
    assert result == {}


@pytest.mark.asyncio
async def test_fetch_bar_counts_passes_date_filters():
    """Verify date params are included in the DB call."""
    rows = [('AAPL', 10)]
    sd = _make_stock_data(bar_count_rows=rows)
    start = datetime.date(2024, 1, 1)
    end = datetime.date(2024, 6, 1)

    await fetch_bar_counts(sd, ['AAPL'], 'daily', start_date=start, end_date=end)

    call_args = sd.db.execute.call_args
    query, params = call_args[0]
    assert 'daily_price_history' in query
    assert start in params
    assert end in params


@pytest.mark.asyncio
async def test_fetch_bar_counts_5m_uses_correct_table():
    rows = [('AAPL', 200)]
    sd = _make_stock_data(bar_count_rows=rows)
    await fetch_bar_counts(sd, ['AAPL'], '5m')
    query = sd.db.execute.call_args[0][0]
    assert 'five_minute_price_history' in query


@pytest.mark.asyncio
async def test_fetch_bar_counts_values_are_int():
    rows = [('AAPL', 42)]
    sd = _make_stock_data(bar_count_rows=rows)
    result = await fetch_bar_counts(sd, ['AAPL'], 'daily')
    assert isinstance(result['AAPL'], int)


# ---------------------------------------------------------------------------
# stream_tickers
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stream_tickers_yields_correct_count():
    tickers = ['AAPL', 'GOOG', 'MSFT']
    sd = _make_stock_data()
    results = []
    async for item in stream_tickers(sd, tickers, 'daily'):
        results.append(item)
    assert len(results) == 3


@pytest.mark.asyncio
async def test_stream_tickers_yields_tuples():
    sd = _make_stock_data()
    async for ticker, price_df, pop_df in stream_tickers(sd, ['AAPL'], 'daily'):
        assert isinstance(ticker, str)
        assert isinstance(price_df, pd.DataFrame)
        assert isinstance(pop_df, pd.DataFrame)


@pytest.mark.asyncio
async def test_stream_tickers_calls_price_fetch_per_ticker():
    tickers = ['AAPL', 'GOOG']
    sd = _make_stock_data()
    async for _ in stream_tickers(sd, tickers, 'daily'):
        pass
    assert sd.price_history.fetch_daily_price_history.call_count == 2


@pytest.mark.asyncio
async def test_stream_tickers_calls_pop_fetch_per_ticker():
    tickers = ['AAPL', 'GOOG']
    sd = _make_stock_data()
    async for _ in stream_tickers(sd, tickers, 'daily'):
        pass
    assert sd.popularity.fetch_popularity.call_count == 2


@pytest.mark.asyncio
async def test_stream_tickers_5m_uses_5m_fetcher():
    sd = _make_stock_data()
    async for _ in stream_tickers(sd, ['AAPL'], '5m'):
        pass
    sd.price_history.fetch_5m_price_history.assert_called_once()
    sd.price_history.fetch_daily_price_history.assert_not_called()


@pytest.mark.asyncio
async def test_stream_tickers_passes_date_bounds():
    sd = _make_stock_data()
    start = datetime.date(2024, 1, 1)
    end = datetime.date(2024, 6, 1)
    async for _ in stream_tickers(sd, ['AAPL'], 'daily', start_date=start, end_date=end):
        pass
    call_kwargs = sd.price_history.fetch_daily_price_history.call_args[1]
    assert call_kwargs['start_date'] == start
    assert call_kwargs['end_date'] == end


# ---------------------------------------------------------------------------
# fetch_distinct_tickers
# ---------------------------------------------------------------------------

def _make_sd_for_distinct(ticker_rows):
    sd = MagicMock()
    sd.db.execute = AsyncMock(return_value=ticker_rows)
    return sd


@pytest.mark.asyncio
async def test_fetch_distinct_tickers_returns_sorted_list():
    sd = _make_sd_for_distinct([('MSFT',), ('AAPL',), ('GOOG',)])
    result = await fetch_distinct_tickers(sd)
    assert result == ['MSFT', 'AAPL', 'GOOG']


@pytest.mark.asyncio
async def test_fetch_distinct_tickers_empty():
    sd = _make_sd_for_distinct([])
    result = await fetch_distinct_tickers(sd)
    assert result == []


@pytest.mark.asyncio
async def test_fetch_distinct_tickers_none_rows():
    sd = _make_sd_for_distinct(None)
    result = await fetch_distinct_tickers(sd)
    assert result == []


@pytest.mark.asyncio
async def test_fetch_distinct_tickers_no_date_filter():
    sd = _make_sd_for_distinct([('AAPL',)])
    await fetch_distinct_tickers(sd)
    query, params = sd.db.execute.call_args[0]
    assert 'WHERE' not in query
    assert params is None


@pytest.mark.asyncio
async def test_fetch_distinct_tickers_with_start_date():
    sd = _make_sd_for_distinct([('AAPL',)])
    start = datetime.date(2024, 1, 1)
    await fetch_distinct_tickers(sd, start_date=start)
    query, params = sd.db.execute.call_args[0]
    assert 'WHERE' in query
    assert start in params


@pytest.mark.asyncio
async def test_fetch_distinct_tickers_with_end_date():
    sd = _make_sd_for_distinct([('AAPL',)])
    end = datetime.date(2024, 6, 1)
    await fetch_distinct_tickers(sd, end_date=end)
    query, params = sd.db.execute.call_args[0]
    assert 'WHERE' in query
    # end_date becomes end_date + 1 day for exclusive upper bound
    assert any(isinstance(p, datetime.date) and p > end for p in params)
