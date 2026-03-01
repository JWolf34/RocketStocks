"""Tests for card-format section builder functions in sections_card.py."""
import datetime

import pandas as pd
import pytest

from rocketstocks.core.content.sections_card import (
    performance_card, fundamentals_card, technical_signals_card,
    popularity_card, upcoming_earnings_card, politician_info_card, sec_filings_card,
    ticker_info_card, todays_change_card, earnings_date_card, news_card,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Card function tests
# ---------------------------------------------------------------------------

class TestCardFunctions:
    def test_performance_card_returns_nonempty(self):
        result = performance_card(_price_history(), _minimal_quote())
        assert isinstance(result, str) and len(result) > 0

    def test_performance_card_no_hash_headers(self):
        result = performance_card(_price_history(), _minimal_quote())
        assert '##' not in result

    def test_performance_card_empty_df(self):
        result = performance_card(pd.DataFrame(), _minimal_quote())
        assert 'No price data' in result

    def test_fundamentals_card_returns_nonempty(self):
        result = fundamentals_card(_minimal_fundamentals(), _minimal_quote(), _price_history())
        assert isinstance(result, str) and len(result) > 0

    def test_fundamentals_card_no_hash_headers(self):
        result = fundamentals_card(_minimal_fundamentals(), _minimal_quote())
        assert '##' not in result

    def test_fundamentals_card_no_fundamentals(self):
        result = fundamentals_card(None, _minimal_quote())
        assert 'No fundamentals' in result

    def test_fundamentals_card_includes_52w_when_history_provided(self):
        result = fundamentals_card(_minimal_fundamentals(), _minimal_quote(), _price_history())
        assert '52W' in result

    def test_technical_signals_card_returns_nonempty(self):
        result = technical_signals_card(_price_history())
        assert isinstance(result, str) and len(result) > 0

    def test_technical_signals_card_no_hash_headers(self):
        result = technical_signals_card(_price_history())
        assert '##' not in result

    def test_technical_signals_card_empty_df(self):
        result = technical_signals_card(pd.DataFrame())
        assert 'No price data' in result

    def test_popularity_card_returns_nonempty(self):
        result = popularity_card(pd.DataFrame())
        assert isinstance(result, str) and len(result) > 0

    def test_popularity_card_no_hash_headers(self):
        result = popularity_card(pd.DataFrame())
        assert '##' not in result

    def test_popularity_card_empty_df(self):
        result = popularity_card(pd.DataFrame())
        assert 'No popularity data' in result

    def test_upcoming_earnings_card_returns_nonempty(self):
        info = {
            'date': datetime.date(2026, 3, 15),
            'time': 'after-hours',
            'fiscal_quarter_ending': 'Jan 2026',
            'eps_forecast': '0.89',
            'no_of_ests': '42',
            'last_year_rpt_dt': '2025-02-26',
            'last_year_eps': '0.76',
        }
        result = upcoming_earnings_card(info)
        assert isinstance(result, str) and len(result) > 0

    def test_upcoming_earnings_card_no_hash_headers(self):
        result = upcoming_earnings_card({'date': datetime.date(2026, 3, 15), 'time': 'after-hours',
                                         'fiscal_quarter_ending': 'Q1', 'eps_forecast': '1.0',
                                         'no_of_ests': '10', 'last_year_rpt_dt': '2025-01-01',
                                         'last_year_eps': '0.9'})
        assert '##' not in result

    def test_upcoming_earnings_card_empty(self):
        result = upcoming_earnings_card(None)
        assert 'No upcoming earnings' in result

    def test_upcoming_earnings_card_falsy_empty_dict(self):
        result = upcoming_earnings_card({})
        assert 'No upcoming earnings' in result

    def test_politician_info_card_returns_nonempty(self):
        result = politician_info_card(
            {'name': 'Nancy Pelosi', 'party': 'Democrat', 'state': 'California'},
            {'Net Worth': '$120M'},
        )
        assert isinstance(result, str) and len(result) > 0

    def test_politician_info_card_no_hash_headers(self):
        result = politician_info_card({'party': 'Democrat', 'state': 'CA'}, {})
        assert '##' not in result

    def test_politician_info_card_empty_facts(self):
        result = politician_info_card({'party': 'Republican', 'state': 'TX'}, None)
        assert 'Republican' in result

    def test_sec_filings_card_returns_nonempty(self):
        df = pd.DataFrame({
            'form': ['10-K', '8-K'],
            'filingDate': ['2026-01-15', '2026-02-03'],
            'link': ['https://example.com/1', 'https://example.com/2'],
        })
        result = sec_filings_card(df)
        assert isinstance(result, str) and len(result) > 0

    def test_sec_filings_card_no_hash_headers(self):
        df = pd.DataFrame({
            'form': ['10-K'],
            'filingDate': ['2026-01-15'],
            'link': ['https://example.com/1'],
        })
        result = sec_filings_card(df)
        assert '##' not in result

    def test_sec_filings_card_empty_df(self):
        result = sec_filings_card(pd.DataFrame())
        assert 'No recent SEC filings' in result

    def test_ticker_info_card_returns_str(self):
        result = ticker_info_card({'name': 'Apple Inc', 'sector': 'Tech', 'industry': 'Software'}, _minimal_quote())
        assert isinstance(result, str)
        assert 'Apple Inc' in result

    def test_ticker_info_card_none_ticker_info(self):
        result = ticker_info_card(None, _minimal_quote())
        assert isinstance(result, str)

    def test_todays_change_card_positive(self):
        result = todays_change_card(_minimal_quote(pct=2.5))
        assert '+2.50%' in result
        assert '🟢' in result

    def test_todays_change_card_negative(self):
        result = todays_change_card(_minimal_quote(pct=-1.5))
        assert '-1.50%' in result
        assert '🔻' in result

    def test_earnings_date_card_pre_market(self):
        info = {'date': datetime.date(2026, 3, 15), 'time': 'pre-market'}
        result = earnings_date_card('AAPL', info)
        assert 'AAPL' in result
        assert 'before market open' in result

    def test_earnings_date_card_after_hours(self):
        info = {'date': datetime.date(2026, 3, 15), 'time': 'after-hours'}
        result = earnings_date_card('NVDA', info)
        assert 'after market close' in result

    def test_earnings_date_card_no_info(self):
        result = earnings_date_card('AAPL', None)
        assert result == ''

    def test_news_card_returns_articles(self):
        news = {'articles': [
            {'title': 'Headline', 'url': 'https://example.com/1',
             'source': {'name': 'Reuters'}, 'publishedAt': '2026-02-27T10:00:00Z'},
        ]}
        result = news_card(news)
        assert 'Headline' in result

    def test_news_card_empty_articles(self):
        result = news_card({'articles': []})
        assert result == ''
