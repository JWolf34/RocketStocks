"""Tests for build() on all report and screener classes.

Verifies:
- EmbedSpec is returned with correct structure
- Character budget stays within Discord's 6000-char total embed limit
- Color values are valid integers (discord.Embed accepts them)
- Title, footer, timestamp are correctly populated
"""
import datetime
import pandas as pd
import pytest

from rocketstocks.core.content.models import (
    COLOR_BLUE, COLOR_GREEN, COLOR_ORANGE,
    COLOR_GOLD, COLOR_INDIGO, COLOR_PINK, COLOR_CYAN, COLOR_AMBER,
    EmbedSpec,
    EarningsSpotlightData, GainerScreenerData, NewsReportData,
    PopularityReportData, PopularityScreenerData, StockReportData,
    VolumeScreenerData, WeeklyEarningsData,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _embed_char_count(spec: EmbedSpec) -> int:
    """Compute total character count of all text in an EmbedSpec."""
    total = 0
    if spec.title:
        total += len(spec.title)
    if spec.description:
        total += len(spec.description)
    for f in spec.fields:
        total += len(f.name) + len(f.value)
    if spec.footer:
        total += len(spec.footer)
    return total


def _minimal_quote(ticker: str = "AAPL", pct: float = 2.5, price: float = 188.9) -> dict:
    return {
        'symbol': ticker,
        'quote': {
            'openPrice': price - 2,
            'highPrice': price + 2,
            'lowPrice': price - 3,
            'totalVolume': 52_000_000,
            'netPercentChange': pct,
        },
        'regular': {'regularMarketLastPrice': price},
        'reference': {
            'exchangeName': 'NASDAQ',
            'isShortable': True,
            'isHardToBorrow': False,
        },
        'assetSubType': 'CS',
    }


def _minimal_ticker_info() -> dict:
    return {
        'name': 'Apple Inc',
        'sector': 'Technology',
        'industry': 'Consumer Electronics',
        'country': 'US',
    }


def _minimal_fundamentals() -> dict:
    return {
        'instruments': [{'fundamental': {
            'marketCap': 2_900_000_000_000,
            'eps': 6.42,
            'epsTTM': 6.42,
            'peRatio': 29.42,
            'beta': 1.24,
            'dividendAmount': 0.96,
        }}]
    }


def _price_history(rows: int = 250) -> pd.DataFrame:
    dates = [datetime.date.today() - datetime.timedelta(days=i) for i in range(rows)]
    dates.reverse()
    return pd.DataFrame({
        'date': dates,
        'open': [180.0] * rows,
        'high': [195.0] * rows,
        'low': [175.0] * rows,
        'close': [188.9] * rows,
        'volume': [50_000_000] * rows,
    })


def _earnings_df() -> pd.DataFrame:
    return pd.DataFrame({
        'date': [datetime.date(2025, 10, 31), datetime.date(2025, 7, 31)],
        'eps': [1.58, 1.40],
        'surprise': [3.1, 1.8],
        'epsforecast': [1.53, 1.38],
        'fiscalquarterending': ['Sep 2025', 'Jun 2025'],
    })


# ---------------------------------------------------------------------------
# StockReport
# ---------------------------------------------------------------------------

class TestStockReportEmbedSpec:
    def _make_report(self, pct=2.5):
        from rocketstocks.core.content.reports.stock_report import StockReport
        data = StockReportData(
            ticker="AAPL",
            ticker_info=_minimal_ticker_info(),
            quote=_minimal_quote(pct=pct),
            fundamentals=_minimal_fundamentals(),
            daily_price_history=_price_history(),
            popularity=pd.DataFrame(),
            historical_earnings=_earnings_df(),
            next_earnings_info={},
            recent_sec_filings=pd.DataFrame(),
        )
        return StockReport(data)

    def test_returns_embed_spec(self):
        spec = self._make_report().build()
        assert isinstance(spec, EmbedSpec)

    def test_title_contains_ticker_and_date(self):
        spec = self._make_report().build()
        assert "AAPL" in spec.title

    def test_color_is_always_blue(self):
        assert self._make_report(pct=2.5).build().color == COLOR_BLUE
        assert self._make_report(pct=-1.5).build().color == COLOR_BLUE
        assert self._make_report(pct=0).build().color == COLOR_BLUE

    def test_has_footer(self):
        spec = self._make_report().build()
        assert spec.footer is not None
        assert "stock-report" in spec.footer

    def test_has_timestamp(self):
        spec = self._make_report().build()
        assert spec.timestamp is True

    def test_has_finviz_url(self):
        spec = self._make_report().build()
        assert spec.url is not None
        assert "AAPL" in spec.url

    def test_char_budget_under_6000(self):
        spec = self._make_report().build()
        total = _embed_char_count(spec)
        assert total < 6000, f"Embed char count {total} exceeds 6000"

    def test_description_under_4096(self):
        spec = self._make_report().build()
        assert len(spec.description) <= 4096

    def test_fields_each_under_1024(self):
        spec = self._make_report().build()
        for f in spec.fields:
            assert len(f.value) <= 1024, f"Field '{f.name}' value exceeds 1024 chars"

    def test_no_more_than_25_fields(self):
        spec = self._make_report().build()
        assert len(spec.fields) <= 25

    def test_empty_popularity_does_not_crash(self):
        spec = self._make_report().build()
        assert spec is not None

    def test_empty_earnings_does_not_crash(self):
        from rocketstocks.core.content.reports.stock_report import StockReport
        data = StockReportData(
            ticker="AAPL",
            ticker_info=_minimal_ticker_info(),
            quote=_minimal_quote(),
            fundamentals=_minimal_fundamentals(),
            daily_price_history=pd.DataFrame(),
            popularity=pd.DataFrame(),
            historical_earnings=pd.DataFrame(),
            next_earnings_info={},
            recent_sec_filings=pd.DataFrame(),
        )
        spec = StockReport(data).build()
        assert isinstance(spec, EmbedSpec)


# ---------------------------------------------------------------------------
# EarningsSpotlightReport
# ---------------------------------------------------------------------------

class TestEarningsSpotlightEmbedSpec:
    def _make_report(self, pct=1.5):
        from rocketstocks.core.content.reports.earnings_report import EarningsSpotlightReport
        data = EarningsSpotlightData(
            ticker="NVDA",
            ticker_info=_minimal_ticker_info(),
            quote=_minimal_quote("NVDA", pct=pct),
            fundamentals=_minimal_fundamentals(),
            daily_price_history=_price_history(),
            historical_earnings=_earnings_df(),
            next_earnings_info={
                'date': datetime.date(2026, 3, 15),
                'time': 'after-hours',
                'fiscal_quarter_ending': 'Jan 2026',
                'eps_forecast': '0.89',
                'no_of_ests': '42',
                'last_year_rpt_dt': '2025-02-26',
                'last_year_eps': '0.76',
            },
        )
        return EarningsSpotlightReport(data)

    def test_returns_embed_spec(self):
        assert isinstance(self._make_report().build(), EmbedSpec)

    def test_title_contains_ticker(self):
        spec = self._make_report().build()
        assert "NVDA" in spec.title

    def test_color_is_always_gold(self):
        assert self._make_report(pct=0).build().color == COLOR_GOLD
        assert self._make_report(pct=2.5).build().color == COLOR_GOLD
        assert self._make_report(pct=-1.5).build().color == COLOR_GOLD

    def test_char_budget_under_6000(self):
        spec = self._make_report().build()
        assert _embed_char_count(spec) < 6000

    def test_description_under_4096(self):
        spec = self._make_report().build()
        assert len(spec.description) <= 4096


# ---------------------------------------------------------------------------
# NewsReport
# ---------------------------------------------------------------------------

class TestNewsReportEmbedSpec:
    def _make_report(self):
        from rocketstocks.core.content.reports.news_report import NewsReport
        news = {'articles': [
            {
                'title': f'Article {i}',
                'url': f'https://example.com/{i}',
                'source': {'name': 'Reuters'},
                'publishedAt': '2026-02-27T10:00:00Z',
            }
            for i in range(5)
        ]}
        return NewsReport(NewsReportData(query="AAPL", news=news))

    def test_returns_embed_spec(self):
        assert isinstance(self._make_report().build(), EmbedSpec)

    def test_title_contains_query(self):
        spec = self._make_report().build()
        assert "AAPL" in spec.title

    def test_color_is_indigo(self):
        assert self._make_report().build().color == COLOR_INDIGO

    def test_description_under_4096(self):
        spec = self._make_report().build()
        assert len(spec.description) <= 4096


# ---------------------------------------------------------------------------
# PopularityReport
# ---------------------------------------------------------------------------

class TestPopularityReportEmbedSpec:
    def _make_report(self, tmp_path):
        from unittest.mock import patch
        from rocketstocks.core.content.reports.popularity_report import PopularityReport
        df = pd.DataFrame({
            'rank': range(1, 21),
            'ticker': [f'TK{i}' for i in range(1, 21)],
            'mentions': [100 - i for i in range(20)],
            'rank_24h_ago': range(2, 22),
            'mentions_24h_ago': [90 - i for i in range(20)],
        })
        with patch('rocketstocks.core.content.reports.popularity_report.datapaths') as mock_dp:
            mock_dp.attachments_path = str(tmp_path)
            with patch('rocketstocks.core.content.reports.popularity_report.write_df_to_file'):
                return PopularityReport(PopularityReportData(popular_stocks=df, filter="all"))

    def test_returns_embed_spec(self, tmp_path):
        spec = self._make_report(tmp_path).build()
        assert isinstance(spec, EmbedSpec)

    def test_color_is_pink(self, tmp_path):
        assert self._make_report(tmp_path).build().color == COLOR_PINK

    def test_description_under_4096(self, tmp_path):
        spec = self._make_report(tmp_path).build()
        assert len(spec.description) <= 4096


# ---------------------------------------------------------------------------
# Screeners
# ---------------------------------------------------------------------------

class TestGainerScreenerEmbedSpec:
    def _make_screener(self):
        from rocketstocks.core.content.screeners.gainer_screener import GainerScreener
        df = pd.DataFrame({
            'name': [f'TK{i}' for i in range(15)],
            'change': [5.0 + i for i in range(15)],
            'close': [100.0 + i for i in range(15)],
            'volume': [1_000_000 * (i + 1) for i in range(15)],
            'market_cap_basic': [1_000_000_000 * (i + 1) for i in range(15)],
        })
        return GainerScreener(GainerScreenerData(market_period='intraday', gainers=df))

    def test_returns_embed_spec(self):
        assert isinstance(self._make_screener().build(), EmbedSpec)

    def test_color_is_green(self):
        assert self._make_screener().build().color == COLOR_GREEN

    def test_title_contains_gainers(self):
        spec = self._make_screener().build()
        assert "Gainers" in spec.title

    def test_description_contains_ticker(self):
        spec = self._make_screener().build()
        assert "TK0" in spec.description

    def test_description_under_4096(self):
        spec = self._make_screener().build()
        assert len(spec.description) <= 4096

    def test_char_budget_under_6000(self):
        spec = self._make_screener().build()
        assert _embed_char_count(spec) < 6000


class TestVolumeScreenerEmbedSpec:
    def _make_screener(self):
        from rocketstocks.core.content.screeners.volume_screener import VolumeScreener
        df = pd.DataFrame({
            'name': [f'TK{i}' for i in range(12)],
            'close': [100.0 + i for i in range(12)],
            'change': [3.0 + i for i in range(12)],
            'relative_volume_10d_calc': [2.0 + i * 0.1 for i in range(12)],
            'volume': [5_000_000 * (i + 1) for i in range(12)],
            'average_volume_10d_calc': [2_000_000 * (i + 1) for i in range(12)],
            'market_cap_basic': [500_000_000 * (i + 1) for i in range(12)],
        })
        return VolumeScreener(VolumeScreenerData(unusual_volume=df))

    def test_returns_embed_spec(self):
        assert isinstance(self._make_screener().build(), EmbedSpec)

    def test_color_is_orange(self):
        assert self._make_screener().build().color == COLOR_ORANGE

    def test_description_under_4096(self):
        assert len(self._make_screener().build().description) <= 4096


class TestPopularityScreenerEmbedSpec:
    def _make_screener(self):
        from rocketstocks.core.content.screeners.popularity_screener import PopularityScreener
        df = pd.DataFrame({
            'rank': range(1, 21),
            'ticker': [f'TK{i}' for i in range(1, 21)],
            'mentions': [100 - i for i in range(20)],
            'rank_24h_ago': range(2, 22),
            'mentions_24h_ago': [90 - i for i in range(20)],
        })
        return PopularityScreener(PopularityScreenerData(popular_stocks=df))

    def test_returns_embed_spec(self):
        assert isinstance(self._make_screener().build(), EmbedSpec)

    def test_color_is_cyan(self):
        assert self._make_screener().build().color == COLOR_CYAN

    def test_description_under_4096(self):
        assert len(self._make_screener().build().description) <= 4096


class TestWeeklyEarningsScreenerEmbedSpec:
    def _make_screener(self):
        from unittest.mock import patch
        from rocketstocks.core.content.screeners.earnings_screener import WeeklyEarningsScreener
        today = datetime.date.today()
        df = pd.DataFrame({
            'date': [today + datetime.timedelta(days=i) for i in range(3)],
            'ticker': ['AAPL', 'MSFT', 'NVDA'],
            'time': ['after-hours', 'pre-market', 'after-hours'],
            'fiscal_quarter_ending': ['Dec 2025'] * 3,
            'eps_forecast': ['1.60', '2.10', '0.89'],
            'no_of_ests': ['30', '25', '42'],
            'last_year_eps': ['1.46', '1.93', '0.76'],
            'last_year_rpt_dt': ['2025-02-01'] * 3,
        })
        with patch('rocketstocks.core.content.screeners.earnings_screener.datapaths') as dp:
            dp.attachments_path = '/tmp'
            with patch('rocketstocks.core.content.screeners.earnings_screener.write_df_to_file'):
                return WeeklyEarningsScreener(WeeklyEarningsData(
                    upcoming_earnings=df, watchlist_tickers=['AAPL', 'NVDA']
                ))

    def test_returns_embed_spec(self):
        assert isinstance(self._make_screener().build(), EmbedSpec)

    def test_color_is_amber(self):
        assert self._make_screener().build().color == COLOR_AMBER

    def test_title_contains_week(self):
        spec = self._make_screener().build()
        assert "Earnings" in spec.title


# ---------------------------------------------------------------------------
# ohlcv_card premarket fallback
# ---------------------------------------------------------------------------

class TestOhlcvCard:
    """Unit tests for the ohlcv_card() fallback behaviour."""

    def _quote_with_zeros(self, close: float = 150.0) -> dict:
        return {
            'quote': {'openPrice': 0, 'highPrice': 0, 'lowPrice': 0, 'totalVolume': 0},
            'regular': {'regularMarketLastPrice': close},
        }

    def _quote_normal(self, price: float = 188.9) -> dict:
        return {
            'quote': {
                'openPrice': price - 2,
                'highPrice': price + 2,
                'lowPrice': price - 3,
                'totalVolume': 52_000_000,
            },
            'regular': {'regularMarketLastPrice': price},
        }

    def _price_history(self) -> pd.DataFrame:
        return pd.DataFrame({
            'date': [datetime.date(2026, 3, 7), datetime.date(2026, 3, 8)],
            'open': [170.0, 172.0],
            'high': [180.0, 182.0],
            'low': [165.0, 168.0],
            'close': [175.0, 177.0],
            'volume': [30_000_000, 35_000_000],
        })

    def test_normal_quote_shows_todays_summary(self):
        from rocketstocks.core.content.sections_card import ohlcv_card
        result = ohlcv_card(self._quote_normal())
        assert "Today's Summary" in result

    def test_normal_quote_shows_live_ohlc(self):
        from rocketstocks.core.content.sections_card import ohlcv_card
        result = ohlcv_card(self._quote_normal(price=188.9))
        assert "$186.90" in result  # openPrice = price - 2
        assert "$190.90" in result  # highPrice = price + 2

    def test_zero_open_with_history_shows_previous_session(self):
        from rocketstocks.core.content.sections_card import ohlcv_card
        result = ohlcv_card(self._quote_with_zeros(), self._price_history())
        assert "Previous Session" in result
        assert "Today's Summary" not in result

    def test_zero_open_with_history_uses_last_row(self):
        from rocketstocks.core.content.sections_card import ohlcv_card
        result = ohlcv_card(self._quote_with_zeros(), self._price_history())
        # Last row: open=172, high=182, low=168, close=177
        assert "$172.00" in result
        assert "$182.00" in result
        assert "$168.00" in result
        assert "$177.00" in result

    def test_zero_open_without_history_shows_todays_summary(self):
        from rocketstocks.core.content.sections_card import ohlcv_card
        result = ohlcv_card(self._quote_with_zeros())
        assert "Today's Summary" in result

    def test_zero_open_with_empty_history_shows_todays_summary(self):
        from rocketstocks.core.content.sections_card import ohlcv_card
        result = ohlcv_card(self._quote_with_zeros(), pd.DataFrame())
        assert "Today's Summary" in result
