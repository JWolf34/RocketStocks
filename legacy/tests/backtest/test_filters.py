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
    watchlist_tickers=None,
    tv_market_caps=None,
):
    """Build a minimal mock StockData for filter tests."""
    tickers = tickers or ['AAPL', 'MSFT', 'GME']
    ticker_info = ticker_info if ticker_info is not None else pd.DataFrame({
        'ticker': tickers,
        'sector': ['Technology', 'Technology', 'Consumer Cyclical'],
        'industry': ['Consumer Electronics', 'Software', 'Specialty Retail'],
        'exchange': ['NASDAQ', 'NASDAQ', 'NYSE'],
        'delist_date': [None, None, None],
    })
    all_stats = all_stats if all_stats is not None else [
        {'ticker': 'AAPL', 'classification': 'blue_chip', 'market_cap': 3_000_000_000_000, 'volatility_20d': 1.2},
        {'ticker': 'MSFT', 'classification': 'blue_chip', 'market_cap': 2_500_000_000_000, 'volatility_20d': 1.1},
        {'ticker': 'GME', 'classification': 'meme', 'market_cap': 10_000_000_000, 'volatility_20d': 6.5},
    ]
    pop_df = pop_df if pop_df is not None else pd.DataFrame(
        columns=['datetime', 'rank', 'ticker', 'name', 'mentions', 'upvotes',
                 'rank_24h_ago', 'mentions_24h_ago']
    )
    tv_df = tv_market_caps if tv_market_caps is not None else pd.DataFrame(
        columns=['ticker', 'market_cap']
    )

    sd = MagicMock(name='StockData')
    sd.tickers = MagicMock()
    sd.tickers.get_all_tickers = AsyncMock(return_value=tickers)
    sd.tickers.get_all_ticker_info = AsyncMock(return_value=ticker_info)
    sd.ticker_stats = MagicMock()
    sd.ticker_stats.get_all_stats = AsyncMock(return_value=all_stats)
    sd.popularity = MagicMock()
    sd.popularity.fetch_popularity = AsyncMock(return_value=pop_df)
    sd.watchlists = MagicMock()
    sd.watchlists.get_watchlist_tickers = AsyncMock(
        return_value=watchlist_tickers if watchlist_tickers is not None else []
    )
    sd.trading_view = MagicMock()
    sd.trading_view.get_market_caps = MagicMock(return_value=tv_df)
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


# ---------------------------------------------------------------------------
# Classification filter: unclassified tickers default to 'standard'
# ---------------------------------------------------------------------------

async def test_classification_filter_includes_unclassified_as_standard():
    # NVDA has no ticker_stats row — should match classifications=['standard']
    stats = [
        {'ticker': 'AAPL', 'classification': 'blue_chip', 'market_cap': 3e12, 'volatility_20d': 1.2},
        {'ticker': 'GME', 'classification': 'meme', 'market_cap': 10e9, 'volatility_20d': 6.5},
    ]
    sd = _make_stock_data(tickers=['AAPL', 'GME', 'NVDA'], all_stats=stats)
    # Provide matching ticker_info for 3 tickers
    sd.tickers.get_all_ticker_info = AsyncMock(return_value=pd.DataFrame({
        'ticker': ['AAPL', 'GME', 'NVDA'],
        'sector': ['Technology', 'Consumer Cyclical', 'Technology'],
        'industry': ['x', 'x', 'x'],
        'exchange': ['NASDAQ', 'NYSE', 'NASDAQ'],
        'delist_date': [None, None, None],
    }))
    tf = TickerFilter(classifications=['standard'])
    result = await tf.apply(sd)
    assert 'NVDA' in result      # no stats row → defaults to 'standard'
    assert 'AAPL' not in result  # blue_chip
    assert 'GME' not in result   # meme


# ---------------------------------------------------------------------------
# Market cap filter: TradingView fallback
# ---------------------------------------------------------------------------

async def test_market_cap_filter_uses_tradingview_fallback():
    # Only AAPL has a ticker_stats row — coverage is 1/3 (< 50%), so TV fallback fires
    stats = [
        {'ticker': 'AAPL', 'classification': 'blue_chip', 'market_cap': 3e12, 'volatility_20d': 1.2},
    ]
    tv_caps = pd.DataFrame({
        'ticker': ['MSFT', 'GME'],
        'market_cap': [2.5e12, 10e9],
    })
    sd = _make_stock_data(all_stats=stats, tv_market_caps=tv_caps)
    # min_market_cap=1T → should match AAPL and MSFT (both > 1T via different sources)
    tf = TickerFilter(min_market_cap=1_000_000_000_000)
    result = await tf.apply(sd)
    assert 'AAPL' in result
    assert 'MSFT' in result
    assert 'GME' not in result  # 10B < 1T
    sd.trading_view.get_market_caps.assert_called_once()


async def test_market_cap_filter_no_fallback_when_coverage_sufficient():
    # All 3 tickers have stats rows — coverage is 100%, TV should NOT be called
    sd = _make_stock_data()
    tf = TickerFilter(min_market_cap=1_000_000_000_000)
    result = await tf.apply(sd)
    assert 'AAPL' in result
    assert 'MSFT' in result
    assert 'GME' not in result
    sd.trading_view.get_market_caps.assert_not_called()


async def test_market_cap_filter_prefers_ticker_stats_over_tradingview():
    # AAPL in both sources with different values — ticker_stats value wins
    stats = [
        {'ticker': 'AAPL', 'classification': 'blue_chip', 'market_cap': 3e12, 'volatility_20d': 1.2},
    ]
    # TV says AAPL has only 5B — should be overridden by ticker_stats (3T)
    tv_caps = pd.DataFrame({
        'ticker': ['AAPL', 'MSFT'],
        'market_cap': [5e9, 2.5e12],
    })
    sd = _make_stock_data(all_stats=stats, tv_market_caps=tv_caps)
    tf = TickerFilter(min_market_cap=1_000_000_000_000)
    result = await tf.apply(sd)
    assert 'AAPL' in result   # 3T from ticker_stats wins over 5B from TV
    assert 'MSFT' in result   # 2.5T from TV


# ---------------------------------------------------------------------------
# Popularity filter: empty data returns empty set
# ---------------------------------------------------------------------------

async def test_popularity_filter_empty_data_returns_empty():
    sd = _make_stock_data()  # default pop_df is empty
    tf = TickerFilter(max_popularity_rank=10)
    result = await tf.apply(sd)
    assert result == []


# ---------------------------------------------------------------------------
# Redundant get_all_ticker_info call eliminated
# ---------------------------------------------------------------------------

async def test_get_all_ticker_info_called_once_for_delist_and_sector():
    sd = _make_stock_data()
    tf = TickerFilter(exclude_delisted=True, sectors=['Technology'])
    await tf.apply(sd)
    sd.tickers.get_all_ticker_info.assert_called_once()


# ---------------------------------------------------------------------------
# Exchange filter
# ---------------------------------------------------------------------------

async def test_exchange_filter():
    sd = _make_stock_data()
    tf = TickerFilter(exchanges=['NYSE'])
    result = await tf.apply(sd)
    assert result == ['GME']
    assert 'AAPL' not in result
    assert 'MSFT' not in result


async def test_exchange_filter_multiple():
    sd = _make_stock_data()
    tf = TickerFilter(exchanges=['NASDAQ', 'NYSE'])
    result = await tf.apply(sd)
    assert set(result) == {'AAPL', 'MSFT', 'GME'}


async def test_composable_exchange_and_sector():
    sd = _make_stock_data()
    tf = TickerFilter(exchanges=['NASDAQ'], sectors=['Technology'])
    result = await tf.apply(sd)
    assert set(result) == {'AAPL', 'MSFT'}
    assert 'GME' not in result


# ---------------------------------------------------------------------------
# Watchlist filter
# ---------------------------------------------------------------------------

async def test_watchlist_filter():
    sd = _make_stock_data()
    sd.watchlists.get_watchlist_tickers = AsyncMock(return_value=['AAPL', 'MSFT'])
    tf = TickerFilter(watchlists=['mag7'])
    result = await tf.apply(sd)
    assert set(result) == {'AAPL', 'MSFT'}
    assert 'GME' not in result
    sd.watchlists.get_watchlist_tickers.assert_called_once_with('mag7')


async def test_watchlist_filter_union_multiple():
    sd = _make_stock_data()
    # Return different tickers for different watchlist names
    async def _wl_tickers(name):
        return {'mag7': ['AAPL'], 'big-banks': ['GME']}.get(name, [])
    sd.watchlists.get_watchlist_tickers = _wl_tickers
    tf = TickerFilter(watchlists=['mag7', 'big-banks'])
    result = await tf.apply(sd)
    assert set(result) == {'AAPL', 'GME'}
    assert 'MSFT' not in result


# ---------------------------------------------------------------------------
# Volatility filter
# ---------------------------------------------------------------------------

async def test_volatility_filter_min():
    sd = _make_stock_data()
    tf = TickerFilter(min_volatility=5.0)
    result = await tf.apply(sd)
    assert result == ['GME']  # only GME has volatility_20d=6.5 >= 5.0


async def test_volatility_filter_max():
    sd = _make_stock_data()
    tf = TickerFilter(max_volatility=2.0)
    result = await tf.apply(sd)
    assert set(result) == {'AAPL', 'MSFT'}
    assert 'GME' not in result


# ---------------------------------------------------------------------------
# to_dict includes new fields
# ---------------------------------------------------------------------------

def test_to_dict_includes_new_fields():
    tf = TickerFilter(
        exchanges=['NYSE'],
        watchlists=['mag7'],
        min_volatility=1.0,
        max_volatility=5.0,
    )
    d = tf.to_dict()
    assert d['exchanges'] == ['NYSE']
    assert d['watchlists'] == ['mag7']
    assert d['min_volatility'] == 1.0
    assert d['max_volatility'] == 5.0
    assert 'sectors' not in d
    assert 'tickers' not in d
