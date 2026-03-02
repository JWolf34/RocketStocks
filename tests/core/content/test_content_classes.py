"""Tests for concrete content classes: reports, screeners, alerts."""
import datetime

import pandas as pd
import pytest

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.alerts.volume_alert import VolumeMoverAlert
from rocketstocks.core.content.alerts.volume_spike_alert import VolumeSpikeAlert
from rocketstocks.core.content.alerts.watchlist_alert import WatchlistMoverAlert
from rocketstocks.core.content.alerts.popularity_alert import PopularityAlert
from rocketstocks.core.content.models import (
    GainerScreenerData,
    PopularityAlertData,
    PopularityScreenerData,
    VolumeScreenerData,
    VolumeMoverData,
    VolumeSpikeData,
    WatchlistMoverData,
)
from rocketstocks.core.content.screeners.gainer_screener import GainerScreener
from rocketstocks.core.content.screeners.popularity_screener import PopularityScreener
from rocketstocks.core.content.screeners.volume_screener import VolumeScreener


# ---------------------------------------------------------------------------
# Alert.override_and_edit base logic
# ---------------------------------------------------------------------------

class ConcreteAlert(Alert):
    alert_type = 'TEST'

    def __init__(self, pct_change: float):
        super().__init__()
        self.ticker = 'TEST'
        self.alert_data['pct_change'] = pct_change


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

def test_volume_mover_alert_data_contains_pct_and_rvol(quote_up, ticker_info, price_history):
    data = VolumeMoverData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                           rvol=30.0, daily_price_history=price_history)
    alert = VolumeMoverAlert(data=data)
    assert alert.alert_data['pct_change'] == 7.5
    assert alert.alert_data['rvol'] == 30.0


def test_volume_mover_override_triggers_on_large_pct_move(quote_up, ticker_info, price_history):
    """Large pct_change movement triggers override via momentum fallback (>100% relative)."""
    # quote_up has pct_change=7.5; prev_pct=1.0 → (7.5-1)/1 * 100 = 650% > 100 → True
    data = VolumeMoverData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                           rvol=60.0, daily_price_history=price_history)
    alert = VolumeMoverAlert(data=data)
    prev = {'pct_change': 1.0, 'rvol': 60.0}
    assert alert.override_and_edit(prev) is True


def test_volume_mover_override_no_trigger_small_move(quote_up, ticker_info, price_history):
    """Small relative pct_change does not trigger override."""
    data = VolumeMoverData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                           rvol=30.0, daily_price_history=price_history)
    alert = VolumeMoverAlert(data=data)
    prev = {'pct_change': 7.0, 'rvol': 30.0}  # (7.5 - 7.0) / 7.0 = 7.1% → no trigger
    assert alert.override_and_edit(prev) is False


# ---------------------------------------------------------------------------
# VolumeSpikeAlert
# ---------------------------------------------------------------------------

def test_volume_spike_override_triggers_on_large_pct_move(quote_up, ticker_info):
    """Volume spike alert uses base class momentum logic for override."""
    data = VolumeSpikeData(ticker='NVDA', ticker_info=ticker_info, quote=quote_up,
                           rvol_at_time=80.0, avg_vol_at_time=200_000.0, time='11:00 AM')
    alert = VolumeSpikeAlert(data=data)
    # quote_up pct_change=7.5; prev_pct=1.0 → 650% relative change → trigger
    prev = {'pct_change': 1.0, 'rvol_at_time': 80.0}
    assert alert.override_and_edit(prev) is True


# ---------------------------------------------------------------------------
# WatchlistMoverAlert
# ---------------------------------------------------------------------------

def test_watchlist_mover_alert_type():
    quote = {'quote': {'netPercentChange': 5.0}, 'regular': {}, 'assetSubType': '', 'reference': {}}
    info = {'ticker': 'X', 'name': '', 'sector': '', 'industry': '', 'country': ''}
    data = WatchlistMoverData(ticker='X', ticker_info=info, quote=quote, watchlist='wl')
    alert = WatchlistMoverAlert(data=data)
    assert alert.alert_type == 'WATCHLIST_MOVER'


# ---------------------------------------------------------------------------
# PopularityAlert.override_and_edit
# ---------------------------------------------------------------------------

def test_popularity_alert_override_on_high_velocity_zscore(quote_up, ticker_info):
    """PopularityAlert overrides when rank_velocity_zscore is high."""
    now = datetime.datetime.now()
    rounded = now.replace(minute=(now.minute // 30) * 30, second=0, microsecond=0)
    pop_data = [{'datetime': rounded - datetime.timedelta(days=i), 'ticker': 'GME', 'rank': 50 - i}
                for i in range(6)]
    pop_df = pd.DataFrame(pop_data)

    data = PopularityAlertData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                               popularity=pop_df, rank_velocity=-5.0, rank_velocity_zscore=3.0)
    alert = PopularityAlert(data=data)
    # rv_zscore=3.0 >= 2.0, and 3.0 > 0.5*1.5=0.75 → override via velocity path
    prev = {'pct_change': 7.5, 'rank_velocity_zscore': 1.0}
    assert alert.override_and_edit(prev) is True


def test_popularity_alert_no_override_low_velocity(quote_up, ticker_info):
    """PopularityAlert does not override with low rank velocity z-score."""
    now = datetime.datetime.now()
    rounded = now.replace(minute=(now.minute // 30) * 30, second=0, microsecond=0)
    pop_data = [{'datetime': rounded - datetime.timedelta(days=i), 'ticker': 'GME', 'rank': 90 - i}
                for i in range(6)]
    pop_df = pd.DataFrame(pop_data)

    data = PopularityAlertData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                               popularity=pop_df, rank_velocity=-0.5, rank_velocity_zscore=0.5)
    alert = PopularityAlert(data=data)
    # rv_zscore < 2.0 → velocity path doesn't trigger; pct_change similar → momentum fallback also no
    prev = {'pct_change': 7.0, 'rank_velocity_zscore': 0.4}
    assert alert.override_and_edit(prev) is False


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
