"""Tests for rocketstocks.core.content.models typed dataclasses."""
import datetime

import pandas as pd
import pytest

from rocketstocks.core.content.models import (
    EarningsMoverData,
    EarningsSpotlightData,
    GainerScreenerData,
    NewsReportData,
    PoliticianReportData,
    PoliticianTradeAlertData,
    PopularityAlertData,
    PopularityReportData,
    PopularityScreenerData,
    SECFilingData,
    StockReportData,
    TickerData,
    VolumeMoverData,
    VolumeSpikeData,
    WatchlistMoverData,
    WeeklyEarningsData,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def minimal_quote():
    return {
        'quote': {'netPercentChange': 3.5, 'openPrice': 100.0, 'highPrice': 105.0,
                  'lowPrice': 99.0, 'totalVolume': 1_000_000},
        'regular': {'regularMarketLastPrice': 103.0},
        'assetSubType': 'CS',
        'reference': {'exchangeName': 'NASDAQ', 'isShortable': True, 'isHardToBorrow': False},
    }


@pytest.fixture
def minimal_ticker_info():
    return {'ticker': 'AAPL', 'name': 'Apple Inc.', 'sector': 'Technology',
            'industry': 'Consumer Electronics', 'country': 'US'}


@pytest.fixture
def empty_df():
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# TickerData
# ---------------------------------------------------------------------------

def test_ticker_data_fields(minimal_ticker_info, minimal_quote):
    td = TickerData(ticker='AAPL', ticker_info=minimal_ticker_info, quote=minimal_quote)
    assert td.ticker == 'AAPL'
    assert td.ticker_info is minimal_ticker_info
    assert td.quote is minimal_quote


# ---------------------------------------------------------------------------
# Report models
# ---------------------------------------------------------------------------

def test_stock_report_data_construction(minimal_ticker_info, minimal_quote, empty_df):
    data = StockReportData(
        ticker='AAPL',
        ticker_info=minimal_ticker_info,
        quote=minimal_quote,
        fundamentals={'instruments': []},
        daily_price_history=empty_df,
        popularity=empty_df,
        historical_earnings=empty_df,
        next_earnings_info=None,
        recent_sec_filings=empty_df,
    )
    assert data.ticker == 'AAPL'
    assert data.fundamentals == {'instruments': []}
    assert data.next_earnings_info is None


def test_stock_report_data_missing_field_raises(minimal_ticker_info, minimal_quote, empty_df):
    with pytest.raises(TypeError):
        StockReportData(
            ticker='AAPL',
            ticker_info=minimal_ticker_info,
            quote=minimal_quote,
            # missing fundamentals and other required fields
        )


def test_news_report_data_construction():
    data = NewsReportData(query='AAPL earnings', news={'articles': []})
    assert data.query == 'AAPL earnings'
    assert data.news == {'articles': []}


def test_earnings_spotlight_data_construction(minimal_ticker_info, minimal_quote, empty_df):
    data = EarningsSpotlightData(
        ticker='TSLA',
        ticker_info=minimal_ticker_info,
        quote=minimal_quote,
        fundamentals={},
        daily_price_history=empty_df,
        historical_earnings=empty_df,
        next_earnings_info={'date': datetime.date.today()},
    )
    assert data.ticker == 'TSLA'


def test_popularity_report_data_construction(empty_df):
    data = PopularityReportData(popular_stocks=empty_df, filter='all')
    assert data.filter == 'all'


def test_politician_report_data_construction(empty_df):
    politician = {'name': 'Nancy Pelosi', 'politician_id': 'nancy-pelosi'}
    data = PoliticianReportData(politician=politician, trades=empty_df, politician_facts={})
    assert data.politician['name'] == 'Nancy Pelosi'


# ---------------------------------------------------------------------------
# Screener models
# ---------------------------------------------------------------------------

def test_gainer_screener_data_construction(empty_df):
    data = GainerScreenerData(market_period='premarket', gainers=empty_df)
    assert data.market_period == 'premarket'
    assert data.gainers is empty_df


def test_volume_screener_data_construction(empty_df):
    from rocketstocks.core.content.models import VolumeScreenerData
    data = VolumeScreenerData(unusual_volume=empty_df)
    assert data.unusual_volume is empty_df


def test_popularity_screener_data_construction(empty_df):
    data = PopularityScreenerData(popular_stocks=empty_df)
    assert data.popular_stocks is empty_df


def test_weekly_earnings_data_construction(empty_df):
    data = WeeklyEarningsData(upcoming_earnings=empty_df, watchlist_tickers=['AAPL', 'TSLA'])
    assert data.watchlist_tickers == ['AAPL', 'TSLA']


# ---------------------------------------------------------------------------
# Alert models
# ---------------------------------------------------------------------------

def test_earnings_mover_data_construction(minimal_ticker_info, minimal_quote, empty_df):
    data = EarningsMoverData(
        ticker='NVDA',
        ticker_info=minimal_ticker_info,
        quote=minimal_quote,
        next_earnings_info={},
        historical_earnings=empty_df,
    )
    assert data.ticker == 'NVDA'


def test_volume_mover_data_construction(minimal_ticker_info, minimal_quote, empty_df):
    data = VolumeMoverData(
        ticker='AMD',
        ticker_info=minimal_ticker_info,
        quote=minimal_quote,
        rvol=15.7,
        daily_price_history=empty_df,
    )
    assert data.rvol == 15.7


def test_volume_spike_data_construction(minimal_ticker_info, minimal_quote):
    data = VolumeSpikeData(
        ticker='GME',
        ticker_info=minimal_ticker_info,
        quote=minimal_quote,
        rvol_at_time=75.3,
        avg_vol_at_time=500_000.0,
        time='10:30 AM',
    )
    assert data.rvol_at_time == 75.3
    assert data.time == '10:30 AM'


def test_watchlist_mover_data_construction(minimal_ticker_info, minimal_quote):
    data = WatchlistMoverData(
        ticker='AAPL',
        ticker_info=minimal_ticker_info,
        quote=minimal_quote,
        watchlist='my-watchlist',
    )
    assert data.watchlist == 'my-watchlist'


def test_sec_filing_data_construction(minimal_ticker_info, minimal_quote, empty_df):
    data = SECFilingData(
        ticker='AAPL',
        ticker_info=minimal_ticker_info,
        quote=minimal_quote,
        recent_sec_filings=empty_df,
    )
    assert data.ticker == 'AAPL'


def test_popularity_alert_data_construction(minimal_ticker_info, minimal_quote, empty_df):
    data = PopularityAlertData(
        ticker='GME',
        ticker_info=minimal_ticker_info,
        quote=minimal_quote,
        popularity=empty_df,
    )
    assert data.ticker == 'GME'


def test_politician_trade_alert_data_construction(empty_df):
    politician = {'name': 'Nancy Pelosi', 'politician_id': 'nancy-pelosi'}
    data = PoliticianTradeAlertData(politician=politician, trades=empty_df)
    assert data.politician['politician_id'] == 'nancy-pelosi'
