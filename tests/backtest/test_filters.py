"""Tests for rocketstocks.backtest.filters.TickerFilter."""
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from rocketstocks.backtest.filters import TickerFilter


def _make_stock_data(
    tickers=None,
    ticker_info=None,
    all_stats=None,
    pop_df=None,
):
    """Build a minimal mock StockData for filter tests."""
    tickers = tickers or ['AAPL', 'MSFT', 'GME']
    ticker_info = ticker_info if ticker_info is not None else pd.DataFrame({
        'ticker': tickers,
        'sector': ['Technology', 'Technology', 'Consumer Cyclical'],
        'industry': ['Consumer Electronics', 'Software', 'Specialty Retail'],
        'delist_date': [None, None, None],
    })
    all_stats = all_stats if all_stats is not None else [
        {'ticker': 'AAPL', 'classification': 'blue_chip', 'market_cap': 3_000_000_000_000},
        {'ticker': 'MSFT', 'classification': 'blue_chip', 'market_cap': 2_500_000_000_000},
        {'ticker': 'GME', 'classification': 'meme', 'market_cap': 10_000_000_000},
    ]
    pop_df = pop_df if pop_df is not None else pd.DataFrame(
        columns=['datetime', 'rank', 'ticker', 'name', 'mentions', 'upvotes',
                 'rank_24h_ago', 'mentions_24h_ago']
    )

    sd = MagicMock(name='StockData')
    sd.tickers = MagicMock()
    sd.tickers.get_all_tickers = AsyncMock(return_value=tickers)
    sd.tickers.get_all_ticker_info = AsyncMock(return_value=ticker_info)
    sd.ticker_stats = MagicMock()
    sd.ticker_stats.get_all_stats = AsyncMock(return_value=all_stats)
    sd.popularity = MagicMock()
    sd.popularity.fetch_popularity = AsyncMock(return_value=pop_df)
    return sd


# ---------------------------------------------------------------------------
# Explicit ticker list bypasses everything
# ---------------------------------------------------------------------------

async def test_explicit_tickers_bypasses_all_filters():
    sd = _make_stock_data()
    tf = TickerFilter(tickers=['TSLA', 'NVDA'])
    result = await tf.apply(sd)
    assert result == ['NVDA', 'TSLA']
    sd.tickers.get_all_tickers.assert_not_called()


# ---------------------------------------------------------------------------
# Empty filter — all non-delisted tickers
# ---------------------------------------------------------------------------

async def test_empty_filter_returns_all_tickers():
    sd = _make_stock_data()
    tf = TickerFilter()
    result = await tf.apply(sd)
    assert set(result) == {'AAPL', 'MSFT', 'GME'}


# ---------------------------------------------------------------------------
# Delist exclusion
# ---------------------------------------------------------------------------

async def test_exclude_delisted_removes_delisted_tickers():
    info = pd.DataFrame({
        'ticker': ['AAPL', 'MSFT', 'DEAD'],
        'sector': ['Technology', 'Technology', 'Finance'],
        'industry': ['x', 'x', 'x'],
        'delist_date': [None, None, '2024-01-01'],
    })
    sd = _make_stock_data(tickers=['AAPL', 'MSFT', 'DEAD'], ticker_info=info)
    tf = TickerFilter(exclude_delisted=True)
    result = await tf.apply(sd)
    assert 'DEAD' not in result
    assert 'AAPL' in result


async def test_include_delisted_when_disabled():
    info = pd.DataFrame({
        'ticker': ['AAPL', 'DEAD'],
        'sector': ['Technology', 'Finance'],
        'industry': ['x', 'x'],
        'delist_date': [None, '2024-01-01'],
    })
    sd = _make_stock_data(tickers=['AAPL', 'DEAD'], ticker_info=info)
    tf = TickerFilter(exclude_delisted=False)
    result = await tf.apply(sd)
    assert 'DEAD' in result


# ---------------------------------------------------------------------------
# Sector filter
# ---------------------------------------------------------------------------

async def test_sector_filter():
    sd = _make_stock_data()
    tf = TickerFilter(sectors=['Technology'])
    result = await tf.apply(sd)
    assert 'AAPL' in result
    assert 'MSFT' in result
    assert 'GME' not in result


async def test_industry_filter():
    sd = _make_stock_data()
    tf = TickerFilter(industries=['Software'])
    result = await tf.apply(sd)
    assert result == ['MSFT']


# ---------------------------------------------------------------------------
# Classification filter
# ---------------------------------------------------------------------------

async def test_classification_filter():
    sd = _make_stock_data()
    tf = TickerFilter(classifications=['meme'])
    result = await tf.apply(sd)
    assert result == ['GME']


async def test_classification_filter_multiple():
    sd = _make_stock_data()
    tf = TickerFilter(classifications=['meme', 'blue_chip'])
    result = await tf.apply(sd)
    assert set(result) == {'AAPL', 'MSFT', 'GME'}


# ---------------------------------------------------------------------------
# Market cap filter
# ---------------------------------------------------------------------------

async def test_min_market_cap_filter():
    sd = _make_stock_data()
    tf = TickerFilter(min_market_cap=1_000_000_000_000)  # 1T
    result = await tf.apply(sd)
    assert 'AAPL' in result
    assert 'MSFT' in result
    assert 'GME' not in result


async def test_max_market_cap_filter():
    sd = _make_stock_data()
    tf = TickerFilter(max_market_cap=100_000_000_000)  # 100B
    result = await tf.apply(sd)
    assert result == ['GME']


# ---------------------------------------------------------------------------
# Popularity filter
# ---------------------------------------------------------------------------

async def test_popularity_rank_filter():
    import datetime
    pop = pd.DataFrame({
        'datetime': [datetime.datetime(2025, 1, 1)] * 3,
        'rank': [1, 5, 50],
        'ticker': ['AAPL', 'MSFT', 'GME'],
        'name': ['Apple', 'Microsoft', 'GameStop'],
        'mentions': [100, 80, 30],
        'upvotes': [50, 40, 15],
        'rank_24h_ago': [2, 4, 60],
        'mentions_24h_ago': [80, 70, 20],
    })
    sd = _make_stock_data(pop_df=pop)
    tf = TickerFilter(max_popularity_rank=10)
    result = await tf.apply(sd)
    assert 'AAPL' in result
    assert 'MSFT' in result
    assert 'GME' not in result


# ---------------------------------------------------------------------------
# Composable: classification + sector
# ---------------------------------------------------------------------------

async def test_composable_filters_intersection():
    sd = _make_stock_data()
    tf = TickerFilter(classifications=['blue_chip'], sectors=['Technology'])
    result = await tf.apply(sd)
    # Both blue_chip AND Technology
    assert set(result) == {'AAPL', 'MSFT'}
    assert 'GME' not in result


# ---------------------------------------------------------------------------
# to_dict serialization
# ---------------------------------------------------------------------------

def test_to_dict_excludes_none_fields():
    tf = TickerFilter(classifications=['volatile'], min_market_cap=1e9)
    d = tf.to_dict()
    assert 'classifications' in d
    assert 'min_market_cap' in d
    assert 'sectors' not in d
    assert 'tickers' not in d


def test_to_dict_empty_filter():
    tf = TickerFilter()
    d = tf.to_dict()
    # exclude_delisted=True is the only non-None default
    assert d == {'exclude_delisted': True}
