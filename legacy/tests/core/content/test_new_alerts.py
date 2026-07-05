"""Tests for VolumeAccumulationAlert and BreakoutAlert build()."""
import datetime

import pytest

from rocketstocks.core.analysis.alert_strategy import ConfirmationResult
from rocketstocks.core.analysis.options_flow import OptionsFlowResult
from rocketstocks.core.content.alerts.volume_accumulation_alert import VolumeAccumulationAlert
from rocketstocks.core.content.alerts.breakout_alert import BreakoutAlert
from rocketstocks.core.content.models import (
    COLOR_BLUE, COLOR_GREEN, COLOR_RED,
    VolumeAccumulationAlertData, BreakoutAlertData,
    EmbedSpec,
)


def _make_quote_up():
    return {
        'quote': {'netPercentChange': 2.5, 'totalVolume': 3_000_000},
        'regular': {'regularMarketLastPrice': 55.0},
    }


def _make_quote_down():
    return {
        'quote': {'netPercentChange': -1.2, 'totalVolume': 2_000_000},
        'regular': {'regularMarketLastPrice': 48.0},
    }


def _make_ticker_info():
    return {'description': 'Test Corp', 'fundamental': {}}


def _make_options_flow(has_unusual=True):
    contracts = []
    if has_unusual:
        contracts = [{'strike': 110.0, 'type': 'call', 'ratio': 5.2, 'iv': 28.0}]
    return OptionsFlowResult(
        has_unusual_activity=has_unusual,
        unusual_contracts=contracts,
        put_call_ratio=0.4,
        iv_skew_direction='call_skew',
        max_pain=105.0,
        iv_rank=75.0,
        flow_score=6.0,
    )


def _make_confirmation_result(pct=3.0, zscore=2.0):
    return ConfirmationResult(
        should_confirm=True,
        pct_since_flag=pct,
        zscore_since_flag=zscore,
        is_sustained=True,
    )


# ---------------------------------------------------------------------------
# VolumeAccumulationAlert
# ---------------------------------------------------------------------------

class TestVolumeAccumulationAlert:

    def test_build_returns_embed_spec(self):
        data = VolumeAccumulationAlertData(
            ticker='AAPL', ticker_info=_make_ticker_info(), quote=_make_quote_up(),
            vol_zscore=3.2, price_zscore=0.4, rvol=4.1,
            divergence_score=2.8, signal_strength='volume_only',
        )
        spec = VolumeAccumulationAlert(data=data).build()
        assert isinstance(spec, EmbedSpec)

    def test_title_contains_ticker(self):
        data = VolumeAccumulationAlertData(
            ticker='GME', ticker_info=_make_ticker_info(), quote=_make_quote_up(),
            vol_zscore=3.2, price_zscore=0.4, rvol=4.1,
            divergence_score=2.8, signal_strength='volume_only',
        )
        spec = VolumeAccumulationAlert(data=data).build()
        assert 'GME' in spec.title

    def test_color_is_blue(self):
        data = VolumeAccumulationAlertData(
            ticker='AAPL', ticker_info=_make_ticker_info(), quote=_make_quote_up(),
            vol_zscore=3.2, price_zscore=0.4, rvol=4.1,
            divergence_score=2.8, signal_strength='volume_only',
        )
        spec = VolumeAccumulationAlert(data=data).build()
        assert spec.color == COLOR_BLUE

    def test_footer_is_correct(self):
        data = VolumeAccumulationAlertData(
            ticker='AAPL', ticker_info=_make_ticker_info(), quote=_make_quote_up(),
            vol_zscore=3.2, price_zscore=0.4, rvol=4.1,
            divergence_score=2.8, signal_strength='volume_only',
        )
        spec = VolumeAccumulationAlert(data=data).build()
        assert 'volume-accumulation' in spec.footer

    def test_shows_rvol_vol_z_price_z_fields(self):
        data = VolumeAccumulationAlertData(
            ticker='AAPL', ticker_info=_make_ticker_info(), quote=_make_quote_up(),
            vol_zscore=3.2, price_zscore=0.4, rvol=4.1,
            divergence_score=2.8, signal_strength='volume_only',
        )
        spec = VolumeAccumulationAlert(data=data).build()
        field_names = [f.name for f in spec.fields]
        assert 'RVOL' in field_names
        assert 'Volume Z-Score' in field_names
        assert 'Price Z-Score' in field_names

    def test_shows_divergence_score_and_signal_strength(self):
        data = VolumeAccumulationAlertData(
            ticker='AAPL', ticker_info=_make_ticker_info(), quote=_make_quote_up(),
            vol_zscore=3.2, price_zscore=0.4, rvol=4.1,
            divergence_score=2.8, signal_strength='volume_only',
        )
        spec = VolumeAccumulationAlert(data=data).build()
        field_names = [f.name for f in spec.fields]
        assert 'Divergence Score' in field_names
        assert 'Signal Strength' in field_names

    def test_options_flow_section_shown_when_present(self):
        options_flow = _make_options_flow(has_unusual=True)
        data = VolumeAccumulationAlertData(
            ticker='AAPL', ticker_info=_make_ticker_info(), quote=_make_quote_up(),
            vol_zscore=3.2, price_zscore=0.4, rvol=4.1,
            divergence_score=2.8, signal_strength='volume_plus_options',
            options_flow=options_flow,
        )
        spec = VolumeAccumulationAlert(data=data).build()
        field_names = [f.name for f in spec.fields]
        assert 'Unusual Options' in field_names

    def test_no_options_section_when_flow_is_none(self):
        data = VolumeAccumulationAlertData(
            ticker='AAPL', ticker_info=_make_ticker_info(), quote=_make_quote_up(),
            vol_zscore=3.2, price_zscore=0.4, rvol=4.1,
            divergence_score=2.8, signal_strength='volume_only',
            options_flow=None,
        )
        spec = VolumeAccumulationAlert(data=data).build()
        field_names = [f.name for f in spec.fields]
        assert 'Unusual Options' not in field_names

    def test_options_mention_in_description_when_unusual_activity(self):
        options_flow = _make_options_flow(has_unusual=True)
        data = VolumeAccumulationAlertData(
            ticker='AAPL', ticker_info=_make_ticker_info(), quote=_make_quote_up(),
            vol_zscore=3.2, price_zscore=0.4, rvol=4.1,
            divergence_score=2.8, signal_strength='volume_plus_options',
            options_flow=options_flow,
        )
        spec = VolumeAccumulationAlert(data=data).build()
        assert 'call' in spec.description or 'put' in spec.description

    def test_alert_data_stored(self):
        data = VolumeAccumulationAlertData(
            ticker='AAPL', ticker_info=_make_ticker_info(), quote=_make_quote_up(),
            vol_zscore=3.2, price_zscore=0.4, rvol=4.1,
            divergence_score=2.8, signal_strength='volume_only',
        )
        alert = VolumeAccumulationAlert(data=data)
        assert alert.alert_data['vol_zscore'] == pytest.approx(3.2)
        assert alert.alert_data['price_zscore'] == pytest.approx(0.4)
        assert alert.alert_data['divergence_score'] == pytest.approx(2.8)

    def test_alert_type_and_role_key(self):
        assert VolumeAccumulationAlert.alert_type == "VOLUME_ACCUMULATION"
        assert VolumeAccumulationAlert.role_key == "volume_accumulation"


# ---------------------------------------------------------------------------
# BreakoutAlert
# ---------------------------------------------------------------------------

class TestBreakoutAlert:

    def _make_data(self, quote=None, signal_strength='volume_only', options_flow=None,
                   trigger_result=None, confidence_pct=None, price_change_since_flag=2.0):
        return BreakoutAlertData(
            ticker='GME',
            ticker_info=_make_ticker_info(),
            quote=quote or _make_quote_up(),
            signal_detected_at=datetime.datetime.utcnow() - datetime.timedelta(minutes=25),
            signal_alert_message_id=123456789,
            price_at_flag=50.0,
            price_change_since_flag=price_change_since_flag,
            vol_z_at_signal=3.2,
            current_vol_z=2.1,
            price_zscore=1.8,
            divergence_score=2.8,
            rvol=3.5,
            signal_strength=signal_strength,
            options_flow=options_flow,
            trigger_result=trigger_result,
            confidence_pct=confidence_pct,
        )

    def test_build_returns_embed_spec(self):
        spec = BreakoutAlert(data=self._make_data()).build()
        assert isinstance(spec, EmbedSpec)

    def test_title_contains_ticker(self):
        spec = BreakoutAlert(data=self._make_data()).build()
        assert 'GME' in spec.title

    def test_footer_is_correct(self):
        spec = BreakoutAlert(data=self._make_data()).build()
        assert 'breakout' in spec.footer

    def test_color_green_when_positive_since_flag(self):
        spec = BreakoutAlert(data=self._make_data(price_change_since_flag=2.0)).build()
        assert spec.color == COLOR_GREEN

    def test_color_red_when_negative_since_flag(self):
        spec = BreakoutAlert(data=self._make_data(price_change_since_flag=-1.5)).build()
        assert spec.color == COLOR_RED

    def test_shows_price_and_change_since_flag(self):
        spec = BreakoutAlert(data=self._make_data()).build()
        field_names = [f.name for f in spec.fields]
        assert 'Price Now' in field_names
        assert 'Change Since Flag' in field_names

    def test_shows_time_since_signal(self):
        spec = BreakoutAlert(data=self._make_data()).build()
        field_names = [f.name for f in spec.fields]
        assert 'Time Since Signal' in field_names

    def test_description_mentions_duration(self):
        spec = BreakoutAlert(data=self._make_data()).build()
        # Duration should be ~25 min
        assert 'm' in spec.description or 'h' in spec.description

    def test_shows_vol_z_at_signal(self):
        spec = BreakoutAlert(data=self._make_data()).build()
        field_names = [f.name for f in spec.fields]
        assert 'Vol Z-Score at Signal' in field_names

    def test_shows_confidence_when_available(self):
        spec = BreakoutAlert(data=self._make_data(confidence_pct=62.5)).build()
        field_names = [f.name for f in spec.fields]
        assert 'Signal Confidence (30d)' in field_names

    def test_no_confidence_when_none(self):
        spec = BreakoutAlert(data=self._make_data(confidence_pct=None)).build()
        field_names = [f.name for f in spec.fields]
        assert 'Signal Confidence (30d)' not in field_names

    def test_options_flow_section_shown_when_present(self):
        options_flow = _make_options_flow(has_unusual=True)
        spec = BreakoutAlert(data=self._make_data(options_flow=options_flow)).build()
        field_names = [f.name for f in spec.fields]
        assert 'Unusual Options' in field_names

    def test_no_options_section_without_flow(self):
        spec = BreakoutAlert(data=self._make_data(options_flow=None)).build()
        field_names = [f.name for f in spec.fields]
        assert 'Unusual Options' not in field_names

    def test_alert_data_stored(self):
        trigger = _make_confirmation_result(pct=2.5, zscore=1.8)
        data = self._make_data(trigger_result=trigger)
        alert = BreakoutAlert(data=data)
        assert alert.alert_data['price_change_since_flag'] == pytest.approx(2.0)
        assert alert.alert_data['zscore_since_flag'] == pytest.approx(1.8)

    def test_alert_type_and_role_key(self):
        assert BreakoutAlert.alert_type == "BREAKOUT"
        assert BreakoutAlert.role_key == "breakout"

    def test_none_ticker_info_does_not_raise(self):
        data = BreakoutAlertData(
            ticker='GME', ticker_info=None, quote=_make_quote_up(),
            signal_detected_at=datetime.datetime.utcnow() - datetime.timedelta(minutes=15),
            signal_alert_message_id=None,
            price_at_flag=None,
            price_change_since_flag=None,
            vol_z_at_signal=None,
            current_vol_z=None,
            price_zscore=None,
            divergence_score=None,
            rvol=None,
            signal_strength='volume_only',
        )
        spec = BreakoutAlert(data=data).build()
        assert isinstance(spec, EmbedSpec)
