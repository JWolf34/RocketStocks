"""Tests for build() on all alert classes."""
import datetime

import pandas as pd
import pytest

from rocketstocks.core.content.alerts.earnings_alert import EarningsMoverAlert
from rocketstocks.core.content.alerts.volume_alert import VolumeMoverAlert
from rocketstocks.core.content.alerts.volume_spike_alert import VolumeSpikeAlert
from rocketstocks.core.content.alerts.watchlist_alert import WatchlistMoverAlert
from rocketstocks.core.content.alerts.sec_filing_alert import SECFilingMoverAlert
from rocketstocks.core.content.alerts.popularity_alert import PopularityAlert
from rocketstocks.core.content.alerts.politician_alert import PoliticianTradeAlert
from rocketstocks.core.content.models import (
    COLOR_GREEN, COLOR_RED, COLOR_ORANGE, COLOR_PURPLE,
    EarningsMoverData, VolumeMoverData, VolumeSpikeData,
    WatchlistMoverData, SECFilingData, PopularityAlertData,
    PoliticianTradeAlertData, EmbedSpec, EmbedField,
)


# ---------------------------------------------------------------------------
# EmbedSpec / EmbedField dataclasses
# ---------------------------------------------------------------------------

def test_embed_spec_defaults():
    spec = EmbedSpec(title="T", description="D", color=COLOR_GREEN)
    assert spec.fields == []
    assert spec.footer is None
    assert spec.timestamp is False
    assert spec.url is None
    assert spec.thumbnail_url is None


def test_embed_field_defaults():
    f = EmbedField(name="N", value="V")
    assert f.inline is False


# ---------------------------------------------------------------------------
# EarningsMoverAlert
# ---------------------------------------------------------------------------

def test_earnings_mover_embed_spec_positive(quote_up, ticker_info):
    data = EarningsMoverData(
        ticker='GME', ticker_info=ticker_info, quote=quote_up,
        next_earnings_info={'date': datetime.date.today(), 'time': ['pre-market'],
                            'eps_forecast': '1.50', 'fiscal_quarter_ending': 'Dec 2024',
                            'no_of_ests': 10, 'last_year_rpt_dt': '2024-01-01', 'last_year_eps': '1.20'},
        historical_earnings=pd.DataFrame(),
    )
    alert = EarningsMoverAlert(data=data)
    spec = alert.build()
    assert isinstance(spec, EmbedSpec)
    assert spec.color == COLOR_GREEN
    assert 'GME' in spec.title
    assert '7.50%' in spec.description
    assert spec.timestamp is True
    assert spec.url is not None and 'finviz' in spec.url


def test_earnings_mover_embed_spec_negative(quote_down, ticker_info):
    data = EarningsMoverData(
        ticker='GME', ticker_info=ticker_info, quote=quote_down,
        next_earnings_info=None,
        historical_earnings=pd.DataFrame(),
    )
    alert = EarningsMoverAlert(data=data)
    spec = alert.build()
    assert spec.color == COLOR_RED


def test_earnings_mover_embed_spec_has_eps_and_time_fields(quote_up, ticker_info):
    data = EarningsMoverData(
        ticker='GME', ticker_info=ticker_info, quote=quote_up,
        next_earnings_info={'date': datetime.date.today(), 'time': ['after-hours'],
                            'eps_forecast': '2.00', 'fiscal_quarter_ending': 'Sep 2024',
                            'no_of_ests': 5, 'last_year_rpt_dt': '2024-01-01', 'last_year_eps': '1.80'},
        historical_earnings=pd.DataFrame(),
    )
    alert = EarningsMoverAlert(data=data)
    spec = alert.build()
    field_names = [f.name for f in spec.fields]
    assert 'EPS Forecast' in field_names
    assert 'Time' in field_names


# ---------------------------------------------------------------------------
# VolumeMoverAlert
# ---------------------------------------------------------------------------

def test_volume_mover_embed_spec_positive(quote_up, ticker_info, price_history):
    data = VolumeMoverData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                           rvol=15.0, daily_price_history=price_history)
    alert = VolumeMoverAlert(data=data)
    spec = alert.build()
    assert spec.color == COLOR_ORANGE
    assert 'GME' in spec.title
    assert '15.00x' in spec.description


def test_volume_mover_embed_spec_negative(quote_down, ticker_info, price_history):
    data = VolumeMoverData(ticker='GME', ticker_info=ticker_info, quote=quote_down,
                           rvol=8.0, daily_price_history=price_history)
    alert = VolumeMoverAlert(data=data)
    spec = alert.build()
    assert spec.color == COLOR_RED


def test_volume_mover_embed_inline_fields(quote_up, ticker_info, price_history):
    data = VolumeMoverData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                           rvol=20.0, daily_price_history=price_history)
    alert = VolumeMoverAlert(data=data)
    spec = alert.build()
    field_names = [f.name for f in spec.fields]
    assert 'RVOL (10D)' in field_names
    assert 'Volume' in field_names


# ---------------------------------------------------------------------------
# VolumeSpikeAlert
# ---------------------------------------------------------------------------

def test_volume_spike_embed_spec_positive(quote_up, ticker_info):
    data = VolumeSpikeData(ticker='NVDA', ticker_info=ticker_info, quote=quote_up,
                           rvol_at_time=50.0, avg_vol_at_time=200_000.0, time='10:30 AM')
    alert = VolumeSpikeAlert(data=data)
    spec = alert.build()
    assert spec.color == COLOR_ORANGE
    assert 'NVDA' in spec.title
    assert '10:30 AM' in spec.description


def test_volume_spike_embed_spec_negative(quote_down, ticker_info):
    data = VolumeSpikeData(ticker='NVDA', ticker_info=ticker_info, quote=quote_down,
                           rvol_at_time=30.0, avg_vol_at_time=100_000.0, time='11:00 AM')
    alert = VolumeSpikeAlert(data=data)
    spec = alert.build()
    assert spec.color == COLOR_RED


# ---------------------------------------------------------------------------
# WatchlistMoverAlert
# ---------------------------------------------------------------------------

def test_watchlist_mover_embed_spec_positive(quote_up, ticker_info):
    data = WatchlistMoverData(ticker='AAPL', ticker_info=ticker_info, quote=quote_up,
                              watchlist='my-portfolio')
    alert = WatchlistMoverAlert(data=data)
    spec = alert.build()
    assert spec.color == COLOR_GREEN
    assert 'AAPL' in spec.title
    assert 'my-portfolio' in spec.description


def test_watchlist_mover_embed_spec_negative(quote_down, ticker_info):
    data = WatchlistMoverData(ticker='AAPL', ticker_info=ticker_info, quote=quote_down,
                              watchlist='watchlist-a')
    alert = WatchlistMoverAlert(data=data)
    spec = alert.build()
    assert spec.color == COLOR_RED


def test_watchlist_mover_embed_has_watchlist_field(quote_up, ticker_info):
    data = WatchlistMoverData(ticker='AAPL', ticker_info=ticker_info, quote=quote_up,
                              watchlist='specials')
    alert = WatchlistMoverAlert(data=data)
    spec = alert.build()
    field_values = {f.name: f.value for f in spec.fields}
    assert 'Watchlist' in field_values
    assert field_values['Watchlist'] == 'specials'


# ---------------------------------------------------------------------------
# SECFilingMoverAlert
# ---------------------------------------------------------------------------

def test_sec_filing_embed_spec_positive(quote_up, ticker_info):
    today = datetime.datetime.today().strftime("%Y-%m-%d")
    filings = pd.DataFrame({
        'form': ['8-K', '10-Q'],
        'filingDate': [today, today],
        'link': ['https://sec.gov/1', 'https://sec.gov/2'],
    })
    data = SECFilingData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                         recent_sec_filings=filings)
    alert = SECFilingMoverAlert(data=data)
    spec = alert.build()
    assert spec.color == COLOR_GREEN
    assert 'GME' in spec.title
    assert '8-K' in spec.description or '8-K' in str([f.value for f in spec.fields])


def test_sec_filing_embed_spec_negative(quote_down, ticker_info):
    data = SECFilingData(ticker='GME', ticker_info=ticker_info, quote=quote_down,
                         recent_sec_filings=pd.DataFrame())
    alert = SECFilingMoverAlert(data=data)
    spec = alert.build()
    assert spec.color == COLOR_RED


# ---------------------------------------------------------------------------
# PopularityAlert
# ---------------------------------------------------------------------------

@pytest.fixture
def pop_df():
    now = datetime.datetime.now()
    rounded = now.replace(minute=(now.minute // 30) * 30, second=0, microsecond=0)
    rows = []
    for i in range(6):
        rows.append({'datetime': rounded - datetime.timedelta(days=i), 'ticker': 'GME', 'rank': 50 - i * 3})
    return pd.DataFrame(rows)


def test_popularity_embed_spec_returns_embed_spec(quote_up, ticker_info, pop_df):
    data = PopularityAlertData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                               popularity=pop_df)
    alert = PopularityAlert(data=data)
    spec = alert.build()
    assert isinstance(spec, EmbedSpec)
    assert 'GME' in spec.title


def test_popularity_embed_spec_has_inline_fields(quote_up, ticker_info, pop_df):
    data = PopularityAlertData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                               popularity=pop_df)
    alert = PopularityAlert(data=data)
    spec = alert.build()
    field_names = [f.name for f in spec.fields]
    assert 'Current Rank' in field_names
    assert '5D Best Rank' in field_names


# ---------------------------------------------------------------------------
# PoliticianTradeAlert
# ---------------------------------------------------------------------------

@pytest.fixture
def politician():
    return {'name': 'Nancy Pelosi', 'party': 'Democrat', 'state': 'CA',
            'politician_id': 'nancy-pelosi'}


@pytest.fixture
def trades_df():
    return pd.DataFrame({
        'Stock': ['AAPL', 'MSFT'],
        'Amount': ['$10K-$50K', '$50K-$100K'],
        'Date': ['2024-01-01', '2024-01-01'],
        'Type': ['Purchase', 'Purchase'],
    })


def test_politician_embed_spec_color_is_purple(politician, trades_df):
    data = PoliticianTradeAlertData(politician=politician, trades=trades_df)
    alert = PoliticianTradeAlert(data=data)
    spec = alert.build()
    assert spec.color == COLOR_PURPLE


def test_politician_embed_spec_no_url(politician, trades_df):
    data = PoliticianTradeAlertData(politician=politician, trades=trades_df)
    alert = PoliticianTradeAlert(data=data)
    spec = alert.build()
    assert spec.url is None


def test_politician_embed_spec_has_party_and_state_fields(politician, trades_df):
    data = PoliticianTradeAlertData(politician=politician, trades=trades_df)
    alert = PoliticianTradeAlert(data=data)
    spec = alert.build()
    field_names = [f.name for f in spec.fields]
    assert 'Party' in field_names
    assert 'State' in field_names
    assert 'Trades Today' in field_names


def test_politician_embed_spec_name_in_title(politician, trades_df):
    data = PoliticianTradeAlertData(politician=politician, trades=trades_df)
    alert = PoliticianTradeAlert(data=data)
    spec = alert.build()
    assert 'Nancy Pelosi' in spec.title


# ---------------------------------------------------------------------------
# ticker_info=None guard — all TickerData-based alerts must not raise
# ---------------------------------------------------------------------------

def test_earnings_mover_embed_spec_none_ticker_info(quote_up):
    data = EarningsMoverData(
        ticker='GME', ticker_info=None, quote=quote_up,
        next_earnings_info=None,
        historical_earnings=pd.DataFrame(),
    )
    alert = EarningsMoverAlert(data=data)
    spec = alert.build()
    assert 'GME' in spec.description


def test_volume_mover_embed_spec_none_ticker_info(quote_up, price_history):
    data = VolumeMoverData(ticker='GME', ticker_info=None, quote=quote_up,
                           rvol=10.0, daily_price_history=price_history)
    spec = VolumeMoverAlert(data=data).build()
    assert 'GME' in spec.description


def test_volume_spike_embed_spec_none_ticker_info(quote_up):
    data = VolumeSpikeData(ticker='NVDA', ticker_info=None, quote=quote_up,
                           rvol_at_time=20.0, avg_vol_at_time=100_000.0, time='10:30 AM')
    spec = VolumeSpikeAlert(data=data).build()
    assert 'NVDA' in spec.description


def test_watchlist_mover_embed_spec_none_ticker_info(quote_up):
    data = WatchlistMoverData(ticker='AAPL', ticker_info=None, quote=quote_up,
                              watchlist='my-list')
    spec = WatchlistMoverAlert(data=data).build()
    assert 'AAPL' in spec.description


def test_sec_filing_embed_spec_none_ticker_info(quote_up):
    data = SECFilingData(ticker='GME', ticker_info=None, quote=quote_up,
                         recent_sec_filings=pd.DataFrame())
    spec = SECFilingMoverAlert(data=data).build()
    assert 'GME' in spec.description


def test_popularity_embed_spec_none_ticker_info(quote_up, pop_df):
    data = PopularityAlertData(ticker='GME', ticker_info=None, quote=quote_up,
                               popularity=pop_df)
    spec = PopularityAlert(data=data).build()
    assert 'GME' in spec.description


# ---------------------------------------------------------------------------
# Base Alert — build() raises NotImplementedError by default
# ---------------------------------------------------------------------------

from rocketstocks.core.content.alerts.base import Alert


class MinimalAlert(Alert):
    alert_type = 'MINIMAL'

    def __init__(self):
        super().__init__()
        self.ticker = 'X'
        self.alert_data['pct_change'] = 1.0


def test_base_alert_build_raises():
    alert = MinimalAlert()
    with pytest.raises(NotImplementedError):
        alert.build()
