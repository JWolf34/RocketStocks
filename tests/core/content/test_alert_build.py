"""Tests for build() on all alert classes."""
import datetime

import pandas as pd
import pytest

from rocketstocks.core.analysis.alert_strategy import AlertTriggerResult
from rocketstocks.core.analysis.classification import StockClass
from rocketstocks.core.analysis.composite_score import CompositeScoreResult
from rocketstocks.core.analysis.popularity_signals import PopularitySurgeResult, SurgeType
from rocketstocks.core.content.alerts.earnings_alert import EarningsMoverAlert
from rocketstocks.core.content.alerts.watchlist_alert import WatchlistMoverAlert
from rocketstocks.core.content.alerts.popularity_surge_alert import PopularitySurgeAlert
from rocketstocks.core.content.alerts.momentum_confirmation_alert import MomentumConfirmationAlert
from rocketstocks.core.content.alerts.market_alert import MarketAlert
from rocketstocks.core.content.models import (
    COLOR_GREEN, COLOR_RED, COLOR_PURPLE, COLOR_GOLD, COLOR_CYAN,
    EarningsMoverData, WatchlistMoverData,
    PopularitySurgeData, MomentumConfirmationData, MarketAlertData,
    EmbedSpec, EmbedField,
)


def _make_trigger_result(classification=StockClass.STANDARD, should_alert=True):
    return AlertTriggerResult(
        should_alert=should_alert,
        classification=classification,
        zscore=2.8,
        percentile=97.5,
        bb_position=None,
        confluence_count=None,
        confluence_total=None,
        confluence_details=None,
        volume_zscore=3.1,
        signal_type='unusual_move',
    )


def _make_blue_chip_trigger():
    return AlertTriggerResult(
        should_alert=True,
        classification=StockClass.BLUE_CHIP,
        zscore=2.2,
        percentile=92.0,
        bb_position='above_upper',
        confluence_count=3,
        confluence_total=4,
        confluence_details={'rsi': False, 'macd': True, 'adx': True, 'obv': True},
        volume_zscore=2.5,
        signal_type='trend_breakout',
    )


def _make_surge_result(ticker='GME', surge_types=None):
    if surge_types is None:
        surge_types = [SurgeType.MENTION_SURGE, SurgeType.RANK_JUMP]
    return PopularitySurgeResult(
        ticker=ticker,
        is_surging=True,
        surge_types=surge_types,
        current_rank=45,
        rank_24h_ago=180,
        rank_change=135,
        mentions=3200,
        mentions_24h_ago=900,
        mention_ratio=3.56,
        rank_velocity=-12.0,
        rank_velocity_zscore=-2.8,
    )


def _make_composite_result(trigger_result=None, dominant='volume'):
    if trigger_result is None:
        trigger_result = _make_trigger_result()
    return CompositeScoreResult(
        composite_score=3.1,
        should_alert=True,
        volume_component=4.2,
        price_component=2.8,
        cross_signal_component=0.0,
        classification_component=2.0,
        trigger_result=trigger_result,
        dominant_signal=dominant,
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
# PopularitySurgeAlert
# ---------------------------------------------------------------------------

def test_popularity_surge_embed_returns_spec(quote_up, ticker_info):
    surge_result = _make_surge_result()
    data = PopularitySurgeData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                               surge_result=surge_result)
    alert = PopularitySurgeAlert(data=data)
    spec = alert.build()
    assert isinstance(spec, EmbedSpec)
    assert 'GME' in spec.title
    assert spec.color == COLOR_PURPLE
    assert spec.footer == "RocketStocks · popularity-surge"
    assert spec.timestamp is True


def test_popularity_surge_embed_shows_rank_fields(quote_up, ticker_info):
    surge_result = _make_surge_result()
    data = PopularitySurgeData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                               surge_result=surge_result)
    spec = PopularitySurgeAlert(data=data).build()
    field_names = [f.name for f in spec.fields]
    assert 'Current Rank' in field_names
    assert 'Rank 24h Ago' in field_names
    assert 'Rank Change' in field_names


def test_popularity_surge_embed_shows_mention_ratio(quote_up, ticker_info):
    surge_result = _make_surge_result()
    data = PopularitySurgeData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                               surge_result=surge_result)
    spec = PopularitySurgeAlert(data=data).build()
    field_names = [f.name for f in spec.fields]
    assert 'Mention Surge' in field_names


def test_popularity_surge_embed_description_contains_reasons(quote_up, ticker_info):
    surge_result = _make_surge_result(surge_types=[SurgeType.MENTION_SURGE])
    data = PopularitySurgeData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                               surge_result=surge_result)
    spec = PopularitySurgeAlert(data=data).build()
    assert 'Mentions surged' in spec.description


def test_popularity_surge_alert_data_stored(quote_up, ticker_info):
    surge_result = _make_surge_result()
    data = PopularitySurgeData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                               surge_result=surge_result)
    alert = PopularitySurgeAlert(data=data)
    assert alert.alert_data['current_rank'] == 45
    assert alert.alert_data['mention_ratio'] == pytest.approx(3.56)
    assert 'mention_surge' in alert.alert_data['surge_types']


def test_popularity_surge_override_on_intensifying(quote_up, ticker_info):
    """override_and_edit returns True when mention_ratio is 1.5x previous."""
    surge_result = _make_surge_result()
    data = PopularitySurgeData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                               surge_result=surge_result)
    alert = PopularitySurgeAlert(data=data)
    alert.alert_data['mention_ratio'] = 4.5

    prev_data = {'mention_ratio': 2.0, 'pct_change': 7.5}
    # 4.5 >= 1.5 and 4.5 > 2.0 * 1.5 = 3.0 → True
    assert alert.override_and_edit(prev_data) is True


def test_popularity_surge_override_not_triggered_small_ratio(quote_up, ticker_info):
    """override_and_edit stays False when mention_ratio hasn't grown enough."""
    surge_result = _make_surge_result()
    data = PopularitySurgeData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                               surge_result=surge_result)
    alert = PopularitySurgeAlert(data=data)
    alert.alert_data['mention_ratio'] = 2.0

    prev_data = {'mention_ratio': 2.5, 'pct_change': 7.5}
    # 2.0 < 2.5 * 1.5 = 3.75 → falls through to base logic
    # base: pct_change same → no override
    assert alert.override_and_edit(prev_data) is False


# ---------------------------------------------------------------------------
# MomentumConfirmationAlert
# ---------------------------------------------------------------------------

def test_momentum_confirmation_embed_returns_spec(quote_up, ticker_info):
    tr = _make_trigger_result()
    data = MomentumConfirmationData(
        ticker='GME', ticker_info=ticker_info, quote=quote_up,
        surge_flagged_at=datetime.datetime.now() - datetime.timedelta(hours=2),
        surge_types=['mention_surge', 'rank_jump'],
        price_at_flag=50.0,
        price_change_since_flag=8.0,
        trigger_result=tr,
    )
    alert = MomentumConfirmationAlert(data=data)
    spec = alert.build()
    assert isinstance(spec, EmbedSpec)
    assert 'GME' in spec.title
    assert spec.color == COLOR_GOLD
    assert spec.footer == "RocketStocks · momentum-confirmation"
    assert spec.timestamp is True


def test_momentum_confirmation_embed_shows_price_delta(quote_up, ticker_info):
    tr = _make_trigger_result()
    data = MomentumConfirmationData(
        ticker='GME', ticker_info=ticker_info, quote=quote_up,
        surge_flagged_at=datetime.datetime.now(),
        surge_types=['rank_jump'],
        price_at_flag=50.0,
        price_change_since_flag=7.5,
        trigger_result=tr,
    )
    spec = MomentumConfirmationAlert(data=data).build()
    field_names = [f.name for f in spec.fields]
    assert 'Change Since Flag' in field_names


def test_momentum_confirmation_embed_shows_surge_types(quote_up, ticker_info):
    tr = _make_trigger_result()
    data = MomentumConfirmationData(
        ticker='GME', ticker_info=ticker_info, quote=quote_up,
        surge_flagged_at=datetime.datetime.now(),
        surge_types=['mention_surge', 'velocity_spike'],
        trigger_result=tr,
    )
    spec = MomentumConfirmationAlert(data=data).build()
    field_names = [f.name for f in spec.fields]
    assert 'Original Surge Types' in field_names


def test_momentum_confirmation_alert_data_stored(quote_up, ticker_info):
    tr = _make_trigger_result()
    flagged_at = datetime.datetime(2026, 3, 2, 10, 0, 0)
    data = MomentumConfirmationData(
        ticker='GME', ticker_info=ticker_info, quote=quote_up,
        surge_flagged_at=flagged_at,
        surge_types=['rank_jump'],
        price_change_since_flag=5.0,
        trigger_result=tr,
    )
    alert = MomentumConfirmationAlert(data=data)
    assert alert.alert_data['price_change_since_flag'] == pytest.approx(5.0)
    assert alert.alert_data['zscore'] == pytest.approx(2.8)


# ---------------------------------------------------------------------------
# MarketAlert
# ---------------------------------------------------------------------------

def test_market_alert_embed_returns_spec(quote_up, ticker_info):
    tr = _make_trigger_result()
    cr = _make_composite_result(trigger_result=tr, dominant='volume')
    data = MarketAlertData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                           composite_result=cr, rvol=4.5)
    alert = MarketAlert(data=data)
    spec = alert.build()
    assert isinstance(spec, EmbedSpec)
    assert 'GME' in spec.title
    assert spec.timestamp is True
    assert 'market-alert' in spec.footer


def test_market_alert_embed_positive_color(quote_up, ticker_info):
    tr = _make_trigger_result()
    cr = _make_composite_result(trigger_result=tr, dominant='volume')
    data = MarketAlertData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                           composite_result=cr)
    spec = MarketAlert(data=data).build()
    assert spec.color == COLOR_CYAN


def test_market_alert_embed_negative_color(quote_down, ticker_info):
    tr = _make_trigger_result()
    cr = _make_composite_result(trigger_result=tr, dominant='price')
    data = MarketAlertData(ticker='GME', ticker_info=ticker_info, quote=quote_down,
                           composite_result=cr)
    spec = MarketAlert(data=data).build()
    assert spec.color == COLOR_RED


def test_market_alert_embed_shows_composite_score(quote_up, ticker_info):
    tr = _make_trigger_result()
    cr = _make_composite_result(trigger_result=tr)
    data = MarketAlertData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                           composite_result=cr)
    spec = MarketAlert(data=data).build()
    field_names = [f.name for f in spec.fields]
    assert 'Composite Score' in field_names
    assert 'Score Breakdown' in field_names


def test_market_alert_narrative_volume_driven(quote_up, ticker_info):
    tr = _make_trigger_result()
    cr = _make_composite_result(trigger_result=tr, dominant='volume')
    data = MarketAlertData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                           composite_result=cr)
    spec = MarketAlert(data=data).build()
    assert 'volume activity' in spec.description


def test_market_alert_narrative_price_driven(quote_up, ticker_info):
    tr = _make_trigger_result()
    cr = _make_composite_result(trigger_result=tr, dominant='price')
    data = MarketAlertData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                           composite_result=cr)
    spec = MarketAlert(data=data).build()
    assert 'price move' in spec.description


def test_market_alert_narrative_mixed(quote_up, ticker_info):
    tr = _make_trigger_result()
    cr = _make_composite_result(trigger_result=tr, dominant='mixed')
    data = MarketAlertData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                           composite_result=cr)
    spec = MarketAlert(data=data).build()
    assert 'mixed' in spec.description


def test_market_alert_shows_rvol_when_available(quote_up, ticker_info):
    tr = _make_trigger_result()
    cr = _make_composite_result(trigger_result=tr)
    data = MarketAlertData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                           composite_result=cr, rvol=4.5)
    spec = MarketAlert(data=data).build()
    field_names = [f.name for f in spec.fields]
    assert 'RVOL' in field_names


def test_market_alert_no_rvol_field_when_none(quote_up, ticker_info):
    tr = _make_trigger_result()
    cr = _make_composite_result(trigger_result=tr)
    data = MarketAlertData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                           composite_result=cr, rvol=None)
    spec = MarketAlert(data=data).build()
    field_names = [f.name for f in spec.fields]
    assert 'RVOL' not in field_names


def test_market_alert_dominant_in_footer(quote_up, ticker_info):
    tr = _make_trigger_result()
    cr = _make_composite_result(trigger_result=tr, dominant='volume')
    data = MarketAlertData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                           composite_result=cr)
    spec = MarketAlert(data=data).build()
    assert 'volume' in spec.footer


def test_market_alert_stores_composite_score_in_alert_data(quote_up, ticker_info):
    tr = _make_trigger_result()
    cr = _make_composite_result(trigger_result=tr)
    data = MarketAlertData(ticker='GME', ticker_info=ticker_info, quote=quote_up,
                           composite_result=cr)
    alert = MarketAlert(data=data)
    assert alert.alert_data['composite_score'] == pytest.approx(3.1)
    assert alert.alert_data['dominant_signal'] == 'volume'


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


def test_watchlist_mover_embed_spec_none_ticker_info(quote_up):
    data = WatchlistMoverData(ticker='AAPL', ticker_info=None, quote=quote_up,
                              watchlist='my-list')
    spec = WatchlistMoverAlert(data=data).build()
    assert 'AAPL' in spec.description


def test_popularity_surge_embed_none_ticker_info(quote_up):
    surge_result = _make_surge_result()
    data = PopularitySurgeData(ticker='GME', ticker_info=None, quote=quote_up,
                               surge_result=surge_result)
    spec = PopularitySurgeAlert(data=data).build()
    assert 'GME' in spec.description


def test_momentum_confirmation_embed_none_ticker_info(quote_up):
    tr = _make_trigger_result()
    data = MomentumConfirmationData(
        ticker='GME', ticker_info=None, quote=quote_up,
        surge_flagged_at=None,
        trigger_result=tr,
    )
    spec = MomentumConfirmationAlert(data=data).build()
    assert 'GME' in spec.description


def test_market_alert_embed_none_ticker_info(quote_up):
    tr = _make_trigger_result()
    cr = _make_composite_result(trigger_result=tr)
    data = MarketAlertData(ticker='GME', ticker_info=None, quote=quote_up,
                           composite_result=cr)
    spec = MarketAlert(data=data).build()
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


# ---------------------------------------------------------------------------
# Trigger result stored in alert_data
# ---------------------------------------------------------------------------

def test_earnings_mover_stores_zscore_in_alert_data(quote_up, ticker_info):
    tr = _make_trigger_result()
    data = EarningsMoverData(
        ticker='GME', ticker_info=ticker_info, quote=quote_up,
        next_earnings_info=None, historical_earnings=pd.DataFrame(),
        trigger_result=tr,
    )
    alert = EarningsMoverAlert(data=data)
    assert alert.alert_data.get('zscore') == pytest.approx(2.8)
    assert alert.alert_data.get('classification') == 'standard'


def test_watchlist_mover_stores_zscore_in_alert_data(quote_up, ticker_info):
    tr = _make_trigger_result()
    data = WatchlistMoverData(ticker='AAPL', ticker_info=ticker_info, quote=quote_up,
                              watchlist='my-list', trigger_result=tr)
    alert = WatchlistMoverAlert(data=data)
    assert alert.alert_data.get('zscore') == pytest.approx(2.8)


def test_earnings_mover_embed_has_zscore_field(quote_up, ticker_info):
    tr = _make_trigger_result()
    data = EarningsMoverData(
        ticker='GME', ticker_info=ticker_info, quote=quote_up,
        next_earnings_info=None, historical_earnings=pd.DataFrame(),
        trigger_result=tr,
    )
    spec = EarningsMoverAlert(data=data).build()
    field_names = [f.name for f in spec.fields]
    assert 'Z-Score' in field_names


def test_blue_chip_alert_shows_bb_position(quote_up, ticker_info, price_history):
    tr = _make_blue_chip_trigger()
    data = WatchlistMoverData(ticker='AAPL', ticker_info=ticker_info, quote=quote_up,
                              watchlist='portfolio', trigger_result=tr)
    spec = WatchlistMoverAlert(data=data).build()
    field_names = [f.name for f in spec.fields]
    assert 'BB Position' in field_names
    assert 'Confluence' in field_names


# ---------------------------------------------------------------------------
# Base Alert — record_momentum
# ---------------------------------------------------------------------------

def test_record_momentum_appends_to_history():
    alert = MinimalAlert()
    alert.alert_data['pct_change'] = 10.0
    prev_data = {'pct_change': 5.0}
    alert.record_momentum(prev_alert_data=prev_data)
    history = alert.alert_data.get('momentum_history', [])
    assert len(history) == 1
    assert 'velocity' in history[0]
    assert 'acceleration' in history[0]


def test_record_momentum_no_pct_change_does_nothing():
    alert = MinimalAlert()
    del alert.alert_data['pct_change']
    alert.record_momentum(prev_alert_data={'pct_change': 5.0})
    assert 'momentum_history' not in alert.alert_data


def test_override_and_edit_uses_momentum_logic():
    alert = MinimalAlert()
    alert.alert_data['pct_change'] = 50.0
    result = alert.override_and_edit({'pct_change': 5.0})
    assert result is True


# ---------------------------------------------------------------------------
# EarningsMoverAlert — earnings result enrichment
# ---------------------------------------------------------------------------

def test_earnings_mover_with_results_shows_result_field(quote_up, ticker_info):
    data = EarningsMoverData(
        ticker='GME', ticker_info=ticker_info, quote=quote_up,
        next_earnings_info=None, historical_earnings=pd.DataFrame(),
        eps_actual=1.52, eps_estimate=1.45, surprise_pct=4.83,
    )
    spec = EarningsMoverAlert(data=data).build()
    field_names = [f.name for f in spec.fields]
    assert 'Earnings Result' in field_names


def test_earnings_mover_without_results_omits_result_field(quote_up, ticker_info):
    data = EarningsMoverData(
        ticker='GME', ticker_info=ticker_info, quote=quote_up,
        next_earnings_info=None, historical_earnings=pd.DataFrame(),
    )
    spec = EarningsMoverAlert(data=data).build()
    field_names = [f.name for f in spec.fields]
    assert 'Earnings Result' not in field_names


def test_earnings_mover_result_field_contains_eps_value(quote_up, ticker_info):
    data = EarningsMoverData(
        ticker='GME', ticker_info=ticker_info, quote=quote_up,
        next_earnings_info=None, historical_earnings=pd.DataFrame(),
        eps_actual=2.34, eps_estimate=2.10, surprise_pct=11.4,
    )
    spec = EarningsMoverAlert(data=data).build()
    result_field = next(f for f in spec.fields if f.name == 'Earnings Result')
    assert '2.34' in result_field.value


def test_earnings_mover_result_beat_shows_checkmark(quote_up, ticker_info):
    data = EarningsMoverData(
        ticker='GME', ticker_info=ticker_info, quote=quote_up,
        next_earnings_info=None, historical_earnings=pd.DataFrame(),
        eps_actual=1.52, eps_estimate=1.45, surprise_pct=4.83,
    )
    spec = EarningsMoverAlert(data=data).build()
    result_field = next(f for f in spec.fields if f.name == 'Earnings Result')
    assert '✅' in result_field.value


def test_earnings_mover_result_miss_shows_x(quote_up, ticker_info):
    data = EarningsMoverData(
        ticker='GME', ticker_info=ticker_info, quote=quote_up,
        next_earnings_info=None, historical_earnings=pd.DataFrame(),
        eps_actual=1.30, eps_estimate=1.45, surprise_pct=-10.3,
    )
    spec = EarningsMoverAlert(data=data).build()
    result_field = next(f for f in spec.fields if f.name == 'Earnings Result')
    assert '❌' in result_field.value
