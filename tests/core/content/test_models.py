"""Tests for rocketstocks.core.content.models typed dataclasses."""
import datetime

import pandas as pd
import pytest

from rocketstocks.core.content.models import (
    EarningsMoverData,
    EarningsSpotlightData,
    GainerScreenerData,
    MarketAlertData,
    MomentumConfirmationData,
    NewsReportData,
    PoliticianReportData,
    PopularityReportData,
    PopularityScreenerData,
    PopularitySurgeData,
    StockReportData,
    TickerData,
    VolumeScreenerData,
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


def test_watchlist_mover_data_construction(minimal_ticker_info, minimal_quote):
    data = WatchlistMoverData(
        ticker='AAPL',
        ticker_info=minimal_ticker_info,
        quote=minimal_quote,
        watchlist='my-watchlist',
    )
    assert data.watchlist == 'my-watchlist'


def test_popularity_surge_data_construction(minimal_ticker_info, minimal_quote, empty_df):
    from rocketstocks.core.analysis.popularity_signals import PopularitySurgeResult, SurgeType
    surge_result = PopularitySurgeResult(
        ticker='GME',
        is_surging=True,
        surge_types=[SurgeType.MENTION_SURGE],
        current_rank=50,
        rank_24h_ago=200,
        rank_change=150,
        mentions=3000,
        mentions_24h_ago=800,
        mention_ratio=3.75,
        rank_velocity=-10.0,
        rank_velocity_zscore=-2.5,
    )
    data = PopularitySurgeData(
        ticker='GME',
        ticker_info=minimal_ticker_info,
        quote=minimal_quote,
        surge_result=surge_result,
    )
    assert data.ticker == 'GME'
    assert data.surge_result is surge_result
    assert data.popularity_history.empty  # default


def test_momentum_confirmation_data_construction(minimal_ticker_info, minimal_quote, empty_df):
    flagged_at = datetime.datetime(2026, 3, 2, 10, 0)
    data = MomentumConfirmationData(
        ticker='GME',
        ticker_info=minimal_ticker_info,
        quote=minimal_quote,
        surge_flagged_at=flagged_at,
        surge_types=['mention_surge'],
        price_at_flag=50.0,
        price_change_since_flag=8.5,
        surge_alert_message_id=123456789,
    )
    assert data.ticker == 'GME'
    assert data.price_change_since_flag == pytest.approx(8.5)
    assert data.surge_alert_message_id == 123456789
    assert data.daily_price_history.empty  # default


def test_market_alert_data_construction(minimal_ticker_info, minimal_quote, empty_df):
    from rocketstocks.core.analysis.alert_strategy import AlertTriggerResult
    from rocketstocks.core.analysis.classification import StockClass
    from rocketstocks.core.analysis.composite_score import CompositeScoreResult
    trigger = AlertTriggerResult(
        should_alert=True,
        classification=StockClass.VOLATILE,
        zscore=3.5,
        percentile=98.5,
        bb_position=None,
        confluence_count=None,
        confluence_total=None,
        confluence_details=None,
        volume_zscore=4.2,
        signal_type='unusual_move',
    )
    composite = CompositeScoreResult(
        composite_score=3.1,
        should_alert=True,
        volume_component=4.2,
        price_component=3.5,
        cross_signal_component=0.0,
        classification_component=2.0,
        trigger_result=trigger,
        dominant_signal='volume',
    )
    data = MarketAlertData(
        ticker='GME',
        ticker_info=minimal_ticker_info,
        quote=minimal_quote,
        composite_result=composite,
        rvol=4.5,
    )
    assert data.ticker == 'GME'
    assert data.rvol == pytest.approx(4.5)
    assert data.composite_result is composite
