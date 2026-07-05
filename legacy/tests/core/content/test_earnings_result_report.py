"""Tests for EarningsResultReport.build()."""
import datetime
import pandas as pd
import pytest

from rocketstocks.core.content.models import (
    COLOR_GREEN, COLOR_RED,
    EarningsResultData, EmbedSpec,
)
from rocketstocks.core.content.reports.earnings_result_report import EarningsResultReport


def _minimal_quote(pct: float = 7.4, price: float = 895.50) -> dict:
    return {
        'symbol': 'NVDA',
        'quote': {
            'openPrice': price - 10,
            'highPrice': price + 15,
            'lowPrice': price - 12,
            'totalVolume': 45_000_000,
            'netPercentChange': pct,
        },
        'regular': {'regularMarketLastPrice': price},
        'reference': {'exchangeName': 'NASDAQ', 'isShortable': True, 'isHardToBorrow': False},
        'assetSubType': 'CS',
    }


def _minimal_ticker_info() -> dict:
    return {'name': 'NVIDIA Corp', 'sector': 'Technology', 'industry': 'Semiconductors', 'country': 'US'}


def _minimal_earnings_df() -> pd.DataFrame:
    today = datetime.date.today()
    return pd.DataFrame({
        'date': [today - datetime.timedelta(days=90)],
        'eps': [4.94],
        'epsforecast': [4.80],
        'surprise': [2.9],
        'fiscalquarterending': ['Oct 2025'],
    })


def _make_report(eps_actual=5.89, eps_estimate=5.56, surprise_pct=5.9, pct=7.4):
    data = EarningsResultData(
        ticker='NVDA',
        ticker_info=_minimal_ticker_info(),
        quote=_minimal_quote(pct=pct),
        eps_actual=eps_actual,
        eps_estimate=eps_estimate,
        surprise_pct=surprise_pct,
        historical_earnings=_minimal_earnings_df(),
        next_earnings_info={},
        daily_price_history=pd.DataFrame(),
    )
    return EarningsResultReport(data=data)


class TestEarningsResultReportBuild:
    def test_returns_embed_spec(self):
        spec = _make_report().build()
        assert isinstance(spec, EmbedSpec)

    def test_beat_uses_green_color(self):
        spec = _make_report(surprise_pct=5.9).build()
        assert spec.color == COLOR_GREEN

    def test_miss_uses_red_color(self):
        spec = _make_report(surprise_pct=-3.2).build()
        assert spec.color == COLOR_RED

    def test_title_contains_ticker(self):
        spec = _make_report().build()
        assert 'NVDA' in spec.title

    def test_title_contains_beat_indicator_for_positive_surprise(self):
        spec = _make_report(surprise_pct=5.9).build()
        assert 'Beat' in spec.title or '✅' in spec.title

    def test_title_contains_miss_indicator_for_negative_surprise(self):
        spec = _make_report(surprise_pct=-3.2).build()
        assert 'Missed' in spec.title or '❌' in spec.title

    def test_footer_is_earnings_result(self):
        spec = _make_report().build()
        assert spec.footer == 'RocketStocks · earnings-result'

    def test_timestamp_is_true(self):
        spec = _make_report().build()
        assert spec.timestamp is True

    def test_description_contains_eps_actual(self):
        spec = _make_report(eps_actual=5.89).build()
        assert '5.89' in spec.description

    def test_description_contains_surprise_pct(self):
        spec = _make_report(surprise_pct=5.9).build()
        assert '5.9' in spec.description

    def test_description_within_discord_limit(self):
        spec = _make_report().build()
        assert len(spec.description) <= 4096

    def test_zero_surprise_treated_as_beat(self):
        spec = _make_report(surprise_pct=0.0).build()
        assert spec.color == COLOR_GREEN

    def test_none_eps_estimate_handled(self):
        spec = _make_report(eps_estimate=None, surprise_pct=None).build()
        assert isinstance(spec, EmbedSpec)
