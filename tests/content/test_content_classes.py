"""Tests for concrete content classes: reports, screeners, alerts."""
import datetime

import pandas as pd
import pytest

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.alerts.earnings_alert import EarningsMoverAlert
from rocketstocks.core.content.alerts.popularity_alert import PopularityAlert
from rocketstocks.core.content.alerts.volume_alert import VolumeMoverAlert
from rocketstocks.core.content.alerts.volume_spike_alert import VolumeSpikeAlert
from rocketstocks.core.content.alerts.watchlist_alert import WatchlistMoverAlert
from rocketstocks.core.content.models import (
    EarningsMoverData,
    GainerScreenerData,
    NewsReportData,
    PopularityAlertData,
    PopularityScreenerData,
    VolumeScreenerData,
    VolumeMoverData,
    VolumeSpikeData,
    WatchlistMoverData,
)
from rocketstocks.core.content.reports.news_report import NewsReport
from rocketstocks.core.content.screeners.gainer_screener import GainerScreener
from rocketstocks.core.content.screeners.popularity_screener import PopularityScreener
from rocketstocks.core.content.screeners.volume_screener import VolumeScreener


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def quote():
    return {
        'quote': {
            'netPercentChange': 7.5,
            'openPrice': 50.0, 'highPrice': 55.0, 'lowPrice': 49.0,
            'totalVolume': 5_000_000,
        },
        'regular': {'regularMarketLastPrice': 54.0},
        'assetSubType': 'CS',
        'reference': {'exchangeName': 'NYSE', 'isShortable': True, 'isHardToBorrow': False},
    }


@pytest.fixture
def ticker_info():
    return {'ticker': 'GME', 'name': 'GameStop Corp', 'sector': 'Consumer',
            'industry': 'Retail', 'country': 'US'}


@pytest.fixture
def price_history():
    dates = [datetime.date.today() - datetime.timedelta(days=i) for i in range(100)]
    return pd.DataFrame({'date': dates, 'close': [10.0] * 100, 'volume': [500_000] * 100})


# ---------------------------------------------------------------------------
# Alert.override_and_edit base logic
# ---------------------------------------------------------------------------

class ConcreteAlert(Alert):
    alert_type = 'TEST'

    def __init__(self, pct_change: float):
        super().__init__()
        self.ticker = 'TEST'
        self.alert_data['pct_change'] = pct_change

    def build_alert(self) -> str:
        return 'test alert'


def test_alert_base_override_triggers_on_100pct_move():
    alert = ConcreteAlert(pct_change=20.0)
    prev = {'pct_change': 5.0}
    # (20 - 5) / 5 * 100 = 300% > 100 → should override
    assert alert.override_and_edit(prev) is True


def test_alert_base_override_no_trigger_small_move():
    alert = ConcreteAlert(pct_change=6.0)
    prev = {'pct_change': 5.0}
    # (6 - 5) / 5 * 100 = 20% → should NOT override
    assert alert.override_and_edit(prev) is False


def test_alert_base_override_missing_prev_pct_returns_false():
    alert = ConcreteAlert(pct_change=10.0)
    assert alert.override_and_edit({}) is False


# ---------------------------------------------------------------------------
# VolumeMoverAlert
# ---------------------------------------------------------------------------

def test_volume_mover_alert_build_alert_contains_ticker(quote, ticker_info, price_history):
    data = VolumeMoverData(ticker='GME', ticker_info=ticker_info, quote=quote,
                           rvol=30.0, daily_price_history=price_history)
    alert = VolumeMoverAlert(data=data)
    result = alert.build_alert()
    assert 'GME' in result
    assert '30.00 times' in result


def test_volume_mover_alert_data_contains_pct_and_rvol(quote, ticker_info, price_history):
    data = VolumeMoverData(ticker='GME', ticker_info=ticker_info, quote=quote,
                           rvol=30.0, daily_price_history=price_history)
    alert = VolumeMoverAlert(data=data)
    assert alert.alert_data['pct_change'] == 7.5
    assert alert.alert_data['rvol'] == 30.0


def test_volume_mover_override_rvol_doubling(quote, ticker_info, price_history):
    data = VolumeMoverData(ticker='GME', ticker_info=ticker_info, quote=quote,
                           rvol=60.0, daily_price_history=price_history)
    alert = VolumeMoverAlert(data=data)
    prev = {'pct_change': 7.5, 'rvol': 25.0}  # rvol doubled: 60 > 2*25 → override
    assert alert.override_and_edit(prev) is True


def test_volume_mover_override_no_trigger(quote, ticker_info, price_history):
    data = VolumeMoverData(ticker='GME', ticker_info=ticker_info, quote=quote,
                           rvol=30.0, daily_price_history=price_history)
    alert = VolumeMoverAlert(data=data)
    prev = {'pct_change': 7.5, 'rvol': 20.0}  # 30 < 2*20=40 → no override
    assert alert.override_and_edit(prev) is False


# ---------------------------------------------------------------------------
# VolumeSpikeAlert
# ---------------------------------------------------------------------------

def test_volume_spike_alert_contains_rvol_at_time(quote, ticker_info):
    data = VolumeSpikeData(ticker='NVDA', ticker_info=ticker_info, quote=quote,
                           rvol_at_time=75.0, avg_vol_at_time=200_000.0, time='10:30 AM')
    alert = VolumeSpikeAlert(data=data)
    result = alert.build_alert()
    assert '75.00 times' in result
    assert 'NVDA' in result


def test_volume_spike_override_on_rvol_at_time_increase(quote, ticker_info):
    data = VolumeSpikeData(ticker='NVDA', ticker_info=ticker_info, quote=quote,
                           rvol_at_time=80.0, avg_vol_at_time=200_000.0, time='11:00 AM')
    alert = VolumeSpikeAlert(data=data)
    prev = {'pct_change': 7.5, 'rvol_at_time': 50.0}  # 80 > 1.5*50=75 → override
    assert alert.override_and_edit(prev) is True


# ---------------------------------------------------------------------------
# WatchlistMoverAlert
# ---------------------------------------------------------------------------

def test_watchlist_mover_alert_build_alert(quote, ticker_info):
    data = WatchlistMoverData(ticker='AAPL', ticker_info=ticker_info, quote=quote,
                              watchlist='my-portfolio')
    alert = WatchlistMoverAlert(data=data)
    result = alert.build_alert()
    assert 'AAPL' in result
    assert 'my-portfolio' in result


def test_watchlist_mover_alert_type():
    quote = {'quote': {'netPercentChange': 5.0}, 'regular': {}, 'assetSubType': '', 'reference': {}}
    info = {'ticker': 'X', 'name': '', 'sector': '', 'industry': '', 'country': ''}
    data = WatchlistMoverData(ticker='X', ticker_info=info, quote=quote, watchlist='wl')
    alert = WatchlistMoverAlert(data=data)
    assert alert.alert_type == 'WATCHLIST_MOVER'


# ---------------------------------------------------------------------------
# EarningsMoverAlert
# ---------------------------------------------------------------------------

def test_earnings_mover_alert_build_alert(quote, ticker_info):
    data = EarningsMoverData(
        ticker='TSLA', ticker_info=ticker_info, quote=quote,
        next_earnings_info={'date': datetime.date.today(), 'time': ['pre-market']},
        historical_earnings=pd.DataFrame(),
    )
    alert = EarningsMoverAlert(data=data)
    result = alert.build_alert()
    assert 'TSLA' in result
    assert 'earnings today' in result


# ---------------------------------------------------------------------------
# PopularityAlert.override_and_edit
# ---------------------------------------------------------------------------

def test_popularity_alert_override_on_high_rank_improvement(quote, ticker_info):
    now = datetime.datetime.now()
    rounded = now.replace(minute=(now.minute // 30) * 30, second=0, microsecond=0)
    # Create 5 days of data
    pop_data = []
    for i in range(6):
        d = rounded - datetime.timedelta(days=i)
        pop_data.append({'datetime': d, 'ticker': 'GME', 'rank': 50 - i * 2})
    pop_df = pd.DataFrame(pop_data)

    data = PopularityAlertData(ticker='GME', ticker_info=ticker_info, quote=quote,
                               popularity=pop_df)
    alert = PopularityAlert(data=data)
    # alert high_rank (50) < 0.5 * prev high_rank (200 = 100) → override
    prev = {'pct_change': 7.5, 'high_rank': 200, 'high_rank_date': '2024-01-01',
            'low_rank': 40, 'low_rank_date': '2024-01-05'}
    assert alert.override_and_edit(prev) is True


def test_popularity_alert_no_override(quote, ticker_info):
    now = datetime.datetime.now()
    rounded = now.replace(minute=(now.minute // 30) * 30, second=0, microsecond=0)
    pop_data = [{'datetime': rounded - datetime.timedelta(days=i), 'ticker': 'GME', 'rank': 90 - i}
                for i in range(6)]
    pop_df = pd.DataFrame(pop_data)

    data = PopularityAlertData(ticker='GME', ticker_info=ticker_info, quote=quote,
                               popularity=pop_df)
    alert = PopularityAlert(data=data)
    prev = {'pct_change': 7.5, 'high_rank': 80, 'high_rank_date': '2024-01-01',
            'low_rank': 50, 'low_rank_date': '2024-01-05'}
    # high_rank >= 0.5 * 80 = 40 → no override
    assert alert.override_and_edit(prev) is False


# ---------------------------------------------------------------------------
# NewsReport
# ---------------------------------------------------------------------------

def test_news_report_build_report_contains_query():
    news_data = {'articles': []}
    data = NewsReportData(query='semiconductor stocks', news=news_data)
    report = NewsReport(data=data)
    result = report.build_report()
    assert 'semiconductor stocks' in result


def test_news_report_build_report_with_articles():
    news_data = {
        'articles': [{
            'title': 'Big Tech Earnings',
            'source': {'name': 'Bloomberg'},
            'publishedAt': '2024-01-30T10:00:00Z',
            'url': 'https://bloomberg.com/article1',
        }]
    }
    data = NewsReportData(query='tech', news=news_data)
    report = NewsReport(data=data)
    result = report.build_report()
    assert 'Big Tech Earnings' in result
    assert 'Bloomberg' in result


# ---------------------------------------------------------------------------
# Screener classes
# ---------------------------------------------------------------------------

def test_popularity_screener_get_tickers():
    df = pd.DataFrame({
        'rank': [1, 2, 3],
        'ticker': ['GME', 'AMC', 'AAPL'],
        'mentions': [1000, 800, 600],
        'rank_24h_ago': [2, 1, 3],
        'mentions_24h_ago': [900, 850, 550],
    })
    data = PopularityScreenerData(popular_stocks=df)
    screener = PopularityScreener(data=data)
    tickers = screener.get_tickers()
    assert 'GME' in tickers
    assert 'AMC' in tickers
    assert 'AAPL' in tickers


def test_popularity_screener_screener_type():
    df = pd.DataFrame({
        'rank': [1], 'ticker': ['AAPL'], 'mentions': [100],
        'rank_24h_ago': [2], 'mentions_24h_ago': [90],
    })
    screener = PopularityScreener(data=PopularityScreenerData(popular_stocks=df))
    assert screener.screener_type == 'popular-stocks'


def test_popularity_screener_build_report_contains_header():
    df = pd.DataFrame({
        'rank': [1, 2], 'ticker': ['AAPL', 'TSLA'],
        'mentions': [500, 400], 'rank_24h_ago': [2, 1], 'mentions_24h_ago': [450, 420],
    })
    screener = PopularityScreener(data=PopularityScreenerData(popular_stocks=df))
    result = screener.build_report()
    assert 'Popular Stocks' in result


def test_volume_screener_formats_change_pct():
    df = pd.DataFrame({
        'name': ['AAPL'],
        'close': [150.0],
        'change': [5.25],
        'relative_volume_10d_calc': [12.5],
        'volume': [2_000_000],
        'average_volume_10d_calc': [1_000_000],
        'market_cap_basic': [3_000_000_000_000],
    })
    screener = VolumeScreener(data=VolumeScreenerData(unusual_volume=df))
    assert '5.25%' in screener.data['Change (%)'].iloc[0]


def test_volume_screener_formats_relative_volume():
    df = pd.DataFrame({
        'name': ['TSLA'],
        'close': [200.0],
        'change': [3.0],
        'relative_volume_10d_calc': [8.5],
        'volume': [3_000_000],
        'average_volume_10d_calc': [500_000],
        'market_cap_basic': [700_000_000_000],
    })
    screener = VolumeScreener(data=VolumeScreenerData(unusual_volume=df))
    assert '8.5x' in screener.data['Relative Volume (10 Day)'].iloc[0]


def test_gainer_screener_premarket_column_map():
    df = pd.DataFrame({
        'name': ['NVDA'],
        'premarket_change': [8.0],
        'premarket_close': [500.0],
        'close': [462.0],
        'premarket_volume': [200_000],
        'market_cap_basic': [1_200_000_000_000],
    })
    data = GainerScreenerData(market_period='premarket', gainers=df)
    screener = GainerScreener(data=data)
    assert 'Ticker' in screener.data.columns
    assert 'Change (%)' in screener.data.columns
    assert 'Pre Market Volume' in screener.data.columns


def test_gainer_screener_intraday_column_map():
    df = pd.DataFrame({
        'name': ['AMD'],
        'change': [5.0],
        'close': [120.0],
        'volume': [4_000_000],
        'market_cap_basic': [200_000_000_000],
    })
    data = GainerScreenerData(market_period='intraday', gainers=df)
    screener = GainerScreener(data=data)
    assert 'Volume' in screener.data.columns


def test_gainer_screener_unknown_period_empty_columns():
    df = pd.DataFrame({'x': [1], 'y': [2]})
    data = GainerScreenerData(market_period='unknown', gainers=df)
    screener = GainerScreener(data=data)
    # No columns match the empty column_map → data has no columns
    assert screener.data.columns.tolist() == []


def test_gainer_screener_build_report_contains_header():
    df = pd.DataFrame({
        'name': ['GME'],
        'change': [15.0],
        'close': [20.0],
        'volume': [50_000_000],
        'market_cap_basic': [5_000_000_000],
    })
    screener = GainerScreener(data=GainerScreenerData(market_period='intraday', gainers=df))
    result = screener.build_report()
    assert 'Intraday' in result
    assert 'Gainers' in result
