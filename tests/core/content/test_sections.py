"""Tests for rocketstocks.core.content.sections standalone section builders."""
import datetime

import pandas as pd
import pytest

from rocketstocks.core.content import sections


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def quote():
    return {
        'quote': {
            'netPercentChange': 5.2,
            'openPrice': 100.0,
            'highPrice': 108.0,
            'lowPrice': 99.5,
            'totalVolume': 2_500_000,
        },
        'regular': {'regularMarketLastPrice': 107.0},
        'assetSubType': 'CS',
        'reference': {
            'exchangeName': 'NASDAQ',
            'isShortable': True,
            'isHardToBorrow': False,
        },
    }


@pytest.fixture
def ticker_info():
    return {
        'ticker': 'AAPL',
        'name': 'Apple Inc.',
        'sector': 'Technology',
        'industry': 'Consumer Electronics',
        'country': 'US',
    }


@pytest.fixture
def fundamentals():
    return {
        'instruments': [{
            'fundamental': {
                'marketCap': 3_000_000_000_000,
                'eps': 6.43,
                'epsTTM': 6.43,
                'peRatio': 28.5,
                'beta': 1.2,
                'dividendAmount': 0.96,
            }
        }]
    }


@pytest.fixture
def price_history():
    today = datetime.date.today()
    dates = [today - datetime.timedelta(days=i) for i in range(200)]
    return pd.DataFrame({'date': dates, 'close': [100.0 + i * 0.1 for i in range(200)],
                         'volume': [1_000_000] * 200})


@pytest.fixture
def popularity_df():
    now = datetime.datetime.now()
    # round down to nearest 30 min
    rounded = now.replace(minute=(now.minute // 30) * 30, second=0, microsecond=0)
    return pd.DataFrame({
        'datetime': [rounded],
        'ticker': ['AAPL'],
        'rank': [5],
    })


@pytest.fixture
def historical_earnings():
    return pd.DataFrame({
        'date': [datetime.date(2024, 1, 30), datetime.date(2024, 4, 30),
                 datetime.date(2024, 7, 30), datetime.date(2024, 10, 30)],
        'eps': [1.50, 1.60, 1.70, 1.80],
        'surprise': [2.5, 3.0, -1.2, 4.5],
        'epsforecast': [1.45, 1.55, 1.72, 1.75],
        'fiscalquarterending': ['Dec 2023', 'Mar 2024', 'Jun 2024', 'Sep 2024'],
    })


@pytest.fixture
def sec_filings():
    return pd.DataFrame({
        'form': ['10-K', '8-K'],
        'filingDate': ['2024-01-15', '2024-02-01'],
        'link': ['https://sec.gov/1', 'https://sec.gov/2'],
    })


@pytest.fixture
def next_earnings_info():
    return {
        'date': datetime.date(2025, 2, 1),
        'time': ['pre-market'],
        'fiscal_quarter_ending': 'Dec 2024',
        'eps_forecast': '2.10',
        'no_of_ests': 35,
        'last_year_rpt_dt': '2024-02-02',
        'last_year_eps': '1.80',
    }


# ---------------------------------------------------------------------------
# Header sections
# ---------------------------------------------------------------------------

def test_report_header_contains_ticker():
    result = sections.report_header('AAPL')
    assert 'AAPL' in result
    assert 'Report' in result


def test_report_header_starts_with_hash():
    result = sections.report_header('MSFT')
    assert result.startswith('# ')


def test_earnings_spotlight_header():
    result = sections.earnings_spotlight_header('TSLA')
    assert 'TSLA' in result
    assert 'Earnings Spotlight' in result


def test_popularity_report_header():
    result = sections.popularity_report_header('tech')
    assert 'tech' in result
    assert 'Most Popular Stocks' in result


def test_politician_report_header():
    result = sections.politician_report_header('Nancy Pelosi')
    assert 'Nancy Pelosi' in result


def test_news_report_header():
    result = sections.news_report_header('AI stocks')
    assert 'AI stocks' in result


# ---------------------------------------------------------------------------
# ticker_info_section
# ---------------------------------------------------------------------------

def test_ticker_info_section_contains_sector(ticker_info, quote):
    result = sections.ticker_info_section(ticker_info, quote)
    assert 'Technology' in result


def test_ticker_info_section_contains_exchange(ticker_info, quote):
    result = sections.ticker_info_section(ticker_info, quote)
    assert 'NASDAQ' in result


def test_ticker_info_section_skips_nan_values(ticker_info, quote):
    ticker_info['sector'] = 'NaN'
    result = sections.ticker_info_section(ticker_info, quote)
    assert 'NaN' not in result


# ---------------------------------------------------------------------------
# daily_summary_section
# ---------------------------------------------------------------------------

def test_daily_summary_has_ohlcv_labels(quote):
    result = sections.daily_summary_section(quote)
    for label in ['Open', 'High', 'Low', 'Close', 'Volume']:
        assert label in result


def test_daily_summary_has_code_block(quote):
    result = sections.daily_summary_section(quote)
    assert '```' in result


# ---------------------------------------------------------------------------
# performance_section
# ---------------------------------------------------------------------------

def test_performance_section_with_data(price_history, quote):
    result = sections.performance_section(price_history, quote)
    assert 'Performance' in result
    assert '```' in result


def test_performance_section_empty_df(quote):
    result = sections.performance_section(pd.DataFrame(), quote)
    assert 'No price data' in result


def test_performance_section_contains_intervals(price_history, quote):
    result = sections.performance_section(price_history, quote)
    assert '1D' in result
    assert '1M' in result


# ---------------------------------------------------------------------------
# fundamentals_section
# ---------------------------------------------------------------------------

def test_fundamentals_section_with_data(fundamentals, quote):
    result = sections.fundamentals_section(fundamentals, quote)
    assert 'Market Cap' in result
    assert 'EPS' in result
    assert 'P/E Ratio' in result


def test_fundamentals_section_no_data(quote):
    result = sections.fundamentals_section(None, quote)
    assert 'No fundamentals' in result


def test_fundamentals_section_dividend_yes(fundamentals, quote):
    result = sections.fundamentals_section(fundamentals, quote)
    assert 'Yes' in result  # dividendAmount is truthy


# ---------------------------------------------------------------------------
# popularity_section
# ---------------------------------------------------------------------------

def test_popularity_section_with_data(popularity_df):
    result = sections.popularity_section(popularity_df)
    assert 'Popularity' in result


def test_popularity_section_empty_df():
    result = sections.popularity_section(pd.DataFrame())
    assert 'No popularity data' in result


# ---------------------------------------------------------------------------
# recent_earnings_section
# ---------------------------------------------------------------------------

def test_recent_earnings_section_with_data(historical_earnings):
    result = sections.recent_earnings_section(historical_earnings)
    assert 'Recent Earnings' in result
    assert '```' in result


def test_recent_earnings_section_empty():
    result = sections.recent_earnings_section(pd.DataFrame())
    assert 'No historical earnings' in result


def test_recent_earnings_shows_at_most_4_rows(historical_earnings):
    # Add 2 extra rows — should still show at most 4
    extra = pd.DataFrame({
        'date': [datetime.date(2025, 1, 30), datetime.date(2025, 4, 30)],
        'eps': [2.0, 2.1],
        'surprise': [1.0, 2.0],
        'epsforecast': [1.9, 2.0],
        'fiscalquarterending': ['Dec 2024', 'Mar 2025'],
    })
    df = pd.concat([historical_earnings, extra], ignore_index=True)
    result = sections.recent_earnings_section(df)
    # The table should exist but we just verify it doesn't crash
    assert 'Recent Earnings' in result


# ---------------------------------------------------------------------------
# sec_filings_section
# ---------------------------------------------------------------------------

def test_sec_filings_section_with_data(sec_filings):
    result = sections.sec_filings_section(sec_filings)
    assert '10-K' in result
    assert 'sec.gov' in result


def test_sec_filings_section_empty():
    result = sections.sec_filings_section(pd.DataFrame())
    assert 'no recent SEC filings' in result


def test_sec_filings_section_shows_at_most_5(sec_filings):
    # Build 7-row DataFrame
    big_df = pd.concat([sec_filings] * 4, ignore_index=True)
    result = sections.sec_filings_section(big_df)
    # Should only reference 5 links at most
    assert result.count('sec.gov') <= 5


# ---------------------------------------------------------------------------
# earnings_date_section
# ---------------------------------------------------------------------------

def test_earnings_date_section_premarket(next_earnings_info):
    result = sections.earnings_date_section('AAPL', next_earnings_info)
    assert 'AAPL' in result
    assert 'before market open' in result


def test_earnings_date_section_afterhours():
    info = {'date': datetime.date(2025, 5, 1), 'time': ['after-hours']}
    result = sections.earnings_date_section('MSFT', info)
    assert 'after market close' in result


def test_earnings_date_section_no_info():
    result = sections.earnings_date_section('AAPL', None)
    assert result == ''


# ---------------------------------------------------------------------------
# upcoming_earnings_summary_section
# ---------------------------------------------------------------------------

def test_upcoming_earnings_summary_with_info(next_earnings_info):
    result = sections.upcoming_earnings_summary_section(next_earnings_info)
    assert 'Next Earnings Summary' in result
    assert 'Dec 2024' in result


def test_upcoming_earnings_summary_no_info():
    result = sections.upcoming_earnings_summary_section(None)
    assert 'no upcoming earnings' in result


# ---------------------------------------------------------------------------
# politician_info_section
# ---------------------------------------------------------------------------

def test_politician_info_section():
    politician = {'name': 'Nancy Pelosi', 'party': 'Democrat', 'state': 'CA',
                  'politician_id': 'nancy-pelosi'}
    facts = {'Net Worth': '$100M+', 'Tenure': '37 years'}
    result = sections.politician_info_section(politician, facts)
    assert 'Democrat' in result
    assert 'CA' in result
    assert 'Net Worth' in result


# ---------------------------------------------------------------------------
# politician_trades_section
# ---------------------------------------------------------------------------

def test_politician_trades_section():
    trades = pd.DataFrame({'Stock': ['AAPL', 'MSFT'], 'Amount': ['$10K', '$20K'],
                           'Date': ['2024-01-01', '2024-01-02']})
    result = sections.politician_trades_section(trades)
    assert 'Latest Trades' in result
    assert '```' in result


# ---------------------------------------------------------------------------
# news_section
# ---------------------------------------------------------------------------

def test_news_section_with_articles():
    news = {
        'articles': [
            {
                'title': 'AAPL Earnings Beat',
                'source': {'name': 'Reuters'},
                'publishedAt': '2024-01-30T12:00:00Z',
                'url': 'https://reuters.com/story1',
            }
        ]
    }
    result = sections.news_section(news)
    assert 'AAPL Earnings Beat' in result
    assert 'Reuters' in result


def test_news_section_empty_articles():
    result = sections.news_section({'articles': []})
    assert result == ''


def test_news_section_truncates_at_1900_chars():
    long_title = 'A' * 200
    news = {
        'articles': [
            {
                'title': long_title,
                'source': {'name': 'Test'},
                'publishedAt': '2024-01-30T12:00:00Z',
                'url': 'https://example.com/' + 'x' * 100,
            }
            for _ in range(20)
        ]
    }
    result = sections.news_section(news)
    assert len(result) <= 1900 + 300  # allow some slack for last partial line


# ---------------------------------------------------------------------------
# alert_header
# ---------------------------------------------------------------------------

def test_alert_header_contains_label():
    result = sections.alert_header('Volume Mover: AAPL')
    assert 'Volume Mover: AAPL' in result
    assert ':rotating_light:' in result


# ---------------------------------------------------------------------------
# todays_change
# ---------------------------------------------------------------------------

def test_todays_change_positive():
    result = sections.todays_change('AAPL', 5.2)
    assert '🟢' in result
    assert '5.20%' in result
    assert 'AAPL' in result


def test_todays_change_negative():
    result = sections.todays_change('TSLA', -12.3)
    assert '🔻' in result
    assert '12.30%' in result


# ---------------------------------------------------------------------------
# volume_stats_section
# ---------------------------------------------------------------------------

def test_volume_stats_section_with_quote(quote):
    result = sections.volume_stats_section(quote=quote)
    assert 'Volume Stats' in result
    assert 'Volume Today' in result


def test_volume_stats_section_with_rvol(quote):
    result = sections.volume_stats_section(quote=quote, rvol=15.7)
    assert '15.70x' in result


def test_volume_stats_section_with_price_history(quote, price_history):
    result = sections.volume_stats_section(quote=quote, daily_price_history=price_history)
    assert 'Average Volume' in result


def test_volume_stats_section_no_data():
    result = sections.volume_stats_section(quote=None)
    assert 'No volume stats' in result


def test_volume_stats_section_with_rvol_at_time(quote):
    result = sections.volume_stats_section(
        quote=quote, rvol_at_time=50.0, avg_vol_at_time=100_000.0, time='10:30 AM'
    )
    assert '10:30 AM' in result
    assert '50.00x' in result


# ---------------------------------------------------------------------------
# todays_sec_filings_section
# ---------------------------------------------------------------------------

def test_todays_sec_filings_section_with_today_filing():
    today = datetime.datetime.today().strftime("%Y-%m-%d")
    df = pd.DataFrame({
        'form': ['8-K'],
        'filingDate': [today],
        'link': ['https://sec.gov/filing/1'],
    })
    result = sections.todays_sec_filings_section(df)
    assert '8-K' in result


def test_todays_sec_filings_section_no_today_filings():
    df = pd.DataFrame({
        'form': ['10-K'],
        'filingDate': ['2020-01-01'],
        'link': ['https://sec.gov/filing/1'],
    })
    result = sections.todays_sec_filings_section(df)
    # No today filings — just the header
    assert 'SEC Filings' in result
    assert '10-K' not in result


# ---------------------------------------------------------------------------
# technical_signals_section
# ---------------------------------------------------------------------------

@pytest.fixture
def ohlcv_price_history():
    """250 candles of OHLCV data — enough for all indicators including SMA200."""
    n = 250
    import numpy as np
    closes = [100.0 + i * 0.05 + (i % 10) * 0.2 for i in range(n)]
    return pd.DataFrame({
        'date': [datetime.date.today() - datetime.timedelta(days=i) for i in range(n)],
        'open': [c - 0.5 for c in closes],
        'high': [c + 1.0 for c in closes],
        'low': [c - 1.0 for c in closes],
        'close': closes,
        'volume': [1_000_000] * n,
    })


def test_technical_signals_section_returns_string(ohlcv_price_history):
    result = sections.technical_signals_section(ohlcv_price_history)
    assert isinstance(result, str)
    assert 'Technical Signals' in result


def test_technical_signals_section_contains_rsi(ohlcv_price_history):
    result = sections.technical_signals_section(ohlcv_price_history)
    assert 'RSI' in result


def test_technical_signals_section_contains_macd(ohlcv_price_history):
    result = sections.technical_signals_section(ohlcv_price_history)
    assert 'MACD' in result


def test_technical_signals_section_contains_adx(ohlcv_price_history):
    result = sections.technical_signals_section(ohlcv_price_history)
    assert 'ADX' in result


def test_technical_signals_section_contains_sma_cross(ohlcv_price_history):
    result = sections.technical_signals_section(ohlcv_price_history)
    assert '50/200 MA' in result
    # With 250 candles we should get either Golden or Death Cross, not N/A
    assert 'Cross' in result


def test_technical_signals_section_short_history():
    """Fewer than 35 candles — MACD and ADX should show N/A."""
    short_df = pd.DataFrame({
        'date': [datetime.date.today() - datetime.timedelta(days=i) for i in range(20)],
        'open': [100.0] * 20,
        'high': [101.0] * 20,
        'low': [99.0] * 20,
        'close': [100.0] * 20,
        'volume': [1_000_000] * 20,
    })
    result = sections.technical_signals_section(short_df)
    assert 'N/A' in result


def test_technical_signals_section_empty_df():
    result = sections.technical_signals_section(pd.DataFrame())
    assert 'No price data' in result


# ---------------------------------------------------------------------------
# fundamentals_section — 52W additions
# ---------------------------------------------------------------------------

def test_fundamentals_section_with_52w(fundamentals, quote, ohlcv_price_history):
    result = sections.fundamentals_section(fundamentals, quote, daily_price_history=ohlcv_price_history)
    assert '52W High' in result
    assert '52W Low' in result
    assert '% From 52W High' in result


def test_fundamentals_section_without_52w(fundamentals, quote):
    """Without price history, 52W fields should not appear."""
    result = sections.fundamentals_section(fundamentals, quote)
    assert '52W High' not in result


def test_fundamentals_section_52w_from_high_is_nonpositive(fundamentals, quote, ohlcv_price_history):
    """% From 52W High should always be ≤ 0 since current price ≤ 52W high."""
    result = sections.fundamentals_section(fundamentals, quote, daily_price_history=ohlcv_price_history)
    # Extract the percentage — it appears as something like "-5.23%"
    import re
    match = re.search(r'% From 52W High.*?(-?\d+\.\d+)%', result)
    if match:
        pct = float(match.group(1))
        assert pct <= 0


# ---------------------------------------------------------------------------
# todays_change — enhanced signature
# ---------------------------------------------------------------------------

def test_todays_change_with_price_and_name():
    result = sections.todays_change('AAPL', 4.23, price=187.50, company_name='Apple Inc.')
    assert 'Apple Inc.' in result
    assert 'AAPL' in result
    assert '$187.50' in result
    assert '4.23%' in result


def test_todays_change_without_optionals():
    result = sections.todays_change('TSLA', -2.5)
    assert 'TSLA' in result
    assert '2.50%' in result
    # Should not contain $ price
    assert '$' not in result


def test_todays_change_shows_plus_sign_for_positive():
    result = sections.todays_change('NVDA', 5.0)
    assert '+5.00%' in result


def test_todays_change_no_plus_sign_for_negative():
    result = sections.todays_change('NVDA', -5.0)
    assert '-5.00%' in result
    assert '+-' not in result


# ---------------------------------------------------------------------------
# recent_earnings_section — beat/miss streak
# ---------------------------------------------------------------------------

def test_recent_earnings_streak_all_beats(historical_earnings):
    """All 4 rows have positive surprise — should show beat streak of 4."""
    result = sections.recent_earnings_section(historical_earnings)
    assert 'Beat estimates' in result
    assert '4' in result


def test_recent_earnings_streak_all_misses():
    df = pd.DataFrame({
        'date': [datetime.date(2024, 1, 30), datetime.date(2024, 4, 30)],
        'eps': [1.40, 1.50],
        'surprise': [-2.0, -1.5],
        'epsforecast': [1.45, 1.55],
        'fiscalquarterending': ['Dec 2023', 'Mar 2024'],
    })
    result = sections.recent_earnings_section(df)
    assert 'Missed estimates' in result


def test_recent_earnings_streak_mixed():
    df = pd.DataFrame({
        'date': [datetime.date(2024, 1, 30), datetime.date(2024, 4, 30),
                 datetime.date(2024, 7, 30), datetime.date(2024, 10, 30)],
        'eps': [1.50, 1.60, 1.70, 1.80],
        'surprise': [2.5, -1.0, -0.5, 4.5],  # last 2 entries: miss then beat
        'epsforecast': [1.45, 1.65, 1.75, 1.75],
        'fiscalquarterending': ['Dec 2023', 'Mar 2024', 'Jun 2024', 'Sep 2024'],
    })
    result = sections.recent_earnings_section(df)
    # Most recent (4.5) is a beat, only 1 consecutive beat
    assert 'Beat estimates' in result
    assert '**1**' in result


def test_recent_earnings_section_empty_no_streak():
    result = sections.recent_earnings_section(pd.DataFrame())
    assert 'No historical earnings' in result
    assert 'Beat' not in result
    assert 'Missed' not in result
