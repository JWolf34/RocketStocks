"""Tests for core/content/data/ content classes."""
import pytest
import pandas as pd
import discord

from rocketstocks.core.content.models import (
    COLOR_BLUE, COLOR_GREEN, COLOR_GOLD, COLOR_PURPLE, COLOR_RED,
    COLOR_TEAL, COLOR_ORANGE, COLOR_PINK, COLOR_INDIGO, COLOR_AMBER, COLOR_CYAN,
    EmbedSpec,
    QuoteData, UpcomingEarningsData, TickerStatsData, MoverData,
    PriceSnapshotData, FinancialHighlightsData, FundamentalsSnapshotData,
    OptionsSummaryData, PopularitySnapshotData, TickersSummaryData,
    EarningsTableData, SecFilingData,
)
from rocketstocks.core.content.data.quote_card import QuoteCard
from rocketstocks.core.content.data.upcoming_earnings_card import UpcomingEarningsCard
from rocketstocks.core.content.data.stats_card import StatsCard
from rocketstocks.core.content.data.movers_card import MoversCard
from rocketstocks.core.content.data.price_snapshot import PriceSnapshot
from rocketstocks.core.content.data.financial_highlights import FinancialHighlights
from rocketstocks.core.content.data.fundamentals_snapshot import FundamentalsSnapshot
from rocketstocks.core.content.data.options_summary import OptionsSummary
from rocketstocks.core.content.data.popularity_snapshot import PopularitySnapshot
from rocketstocks.core.content.data.tickers_summary import TickersSummary
from rocketstocks.core.content.data.earnings_card import EarningsCard
from rocketstocks.core.content.data.sec_filing_card import SecFilingCard
from rocketstocks.bot.senders.embed_utils import spec_to_embed


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _schwab_quote(last=185.45, change=1.5, change_pct=0.82, bid=185.4, ask=185.5,
                  volume=50_000_000, open_=183.0, high=186.0, low=182.5):
    return {
        'quote': {
            'lastPrice': last,
            'netChange': change,
            'netPercentChange': change_pct,
            'bidPrice': bid,
            'askPrice': ask,
            'totalVolume': volume,
            'openPrice': open_,
            'highPrice': high,
            'lowPrice': low,
        },
        'regular': {'regularMarketLastPrice': last},
    }


def _earnings_info(date='2026-04-01', time='after', eps='1.50', ests=20, last_year='1.29'):
    return {
        'date': date,
        'time': time,
        'eps_forecast': eps,
        'no_of_ests': ests,
        'last_year_eps': last_year,
    }


def _stats(classification='mega_cap', market_cap=3_000_000_000_000,
           vol=0.015, mean_20=0.001, std_20=0.012,
           mean_60=0.0008, std_60=0.013, avg_rvol=1.2,
           bb_upper=192.0, bb_mid=185.0, bb_lower=178.0):
    return {
        'classification': classification,
        'market_cap': market_cap,
        'volatility_20d': vol,
        'mean_return_20d': mean_20,
        'std_return_20d': std_20,
        'mean_return_60d': mean_60,
        'std_return_60d': std_60,
        'avg_rvol_20d': avg_rvol,
        'bb_upper': bb_upper,
        'bb_mid': bb_mid,
        'bb_lower': bb_lower,
        'updated_at': '2026-03-15',
    }


def _mover(symbol='GME', last_price=25.5, change_pct=15.0, volume=10_000_000):
    return {'symbol': symbol, 'lastPrice': last_price, 'percentChange': change_pct, 'totalVolume': volume}


# ---------------------------------------------------------------------------
# QuoteCard
# ---------------------------------------------------------------------------

class TestQuoteCard:
    def test_build_returns_embedspec(self):
        data = QuoteData(tickers=['AAPL'], quotes={'AAPL': _schwab_quote()})
        spec = QuoteCard(data).build()
        assert isinstance(spec, EmbedSpec)

    def test_color_is_blue(self):
        data = QuoteData(tickers=['AAPL'], quotes={'AAPL': _schwab_quote()})
        spec = QuoteCard(data).build()
        assert spec.color == COLOR_BLUE

    def test_title(self):
        data = QuoteData(tickers=['AAPL'], quotes={'AAPL': _schwab_quote()})
        spec = QuoteCard(data).build()
        assert spec.title == "Real-Time Quotes"

    def test_one_field_per_ticker(self):
        data = QuoteData(
            tickers=['AAPL', 'MSFT'],
            quotes={'AAPL': _schwab_quote(), 'MSFT': _schwab_quote(last=400.0)},
        )
        spec = QuoteCard(data).build()
        assert len(spec.fields) == 2
        assert spec.fields[0].name == 'AAPL'
        assert spec.fields[1].name == 'MSFT'

    def test_field_contains_price_and_change(self):
        data = QuoteData(tickers=['AAPL'], quotes={'AAPL': _schwab_quote(last=185.45, change=1.5, change_pct=0.82)})
        spec = QuoteCard(data).build()
        value = spec.fields[0].value
        assert '185.45' in value
        assert '+1.50' in value
        assert '+0.82%' in value

    def test_invalid_tickers_in_footer(self):
        data = QuoteData(tickers=['AAPL'], quotes={'AAPL': _schwab_quote()}, invalid_tickers=['FAKE'])
        spec = QuoteCard(data).build()
        assert spec.footer is not None
        assert 'FAKE' in spec.footer

    def test_no_invalid_tickers_no_footer(self):
        data = QuoteData(tickers=['AAPL'], quotes={'AAPL': _schwab_quote()})
        spec = QuoteCard(data).build()
        assert spec.footer is None

    def test_spec_to_embed_produces_discord_embed(self):
        data = QuoteData(tickers=['AAPL'], quotes={'AAPL': _schwab_quote()})
        spec = QuoteCard(data).build()
        embed = spec_to_embed(spec)
        assert isinstance(embed, discord.Embed)
        assert len(embed.fields) == 1

    def test_missing_quote_data_does_not_raise(self):
        data = QuoteData(tickers=['AAPL'], quotes={})
        spec = QuoteCard(data).build()
        assert len(spec.fields) == 1
        assert 'N/A' in spec.fields[0].value

    def test_non_numeric_change_shows_na(self):
        quote = {'quote': {'netChange': 'ERR', 'netPercentChange': 'ERR'}, 'regular': {}}
        data = QuoteData(tickers=['AAPL'], quotes={'AAPL': quote})
        spec = QuoteCard(data).build()
        assert 'N/A' in spec.fields[0].value


# ---------------------------------------------------------------------------
# UpcomingEarningsCard
# ---------------------------------------------------------------------------

class TestUpcomingEarningsCard:
    def test_build_returns_embedspec(self):
        data = UpcomingEarningsData(tickers=['AAPL'], earnings_info={'AAPL': _earnings_info()})
        spec = UpcomingEarningsCard(data).build()
        assert isinstance(spec, EmbedSpec)

    def test_color_is_green(self):
        data = UpcomingEarningsData(tickers=['AAPL'], earnings_info={'AAPL': _earnings_info()})
        spec = UpcomingEarningsCard(data).build()
        assert spec.color == COLOR_GREEN

    def test_title(self):
        data = UpcomingEarningsData(tickers=['AAPL'], earnings_info={'AAPL': _earnings_info()})
        spec = UpcomingEarningsCard(data).build()
        assert spec.title == "Upcoming Earnings"

    def test_field_shows_date_and_eps(self):
        data = UpcomingEarningsData(
            tickers=['AAPL'],
            earnings_info={'AAPL': _earnings_info(date='2026-04-01', eps='1.50')},
        )
        spec = UpcomingEarningsCard(data).build()
        value = spec.fields[0].value
        assert '2026-04-01' in value
        assert '1.50' in value

    def test_after_market_label(self):
        data = UpcomingEarningsData(tickers=['AAPL'], earnings_info={'AAPL': _earnings_info(time='after')})
        spec = UpcomingEarningsCard(data).build()
        assert 'After Market' in spec.fields[0].value

    def test_pre_market_label(self):
        data = UpcomingEarningsData(tickers=['AAPL'], earnings_info={'AAPL': _earnings_info(time='pre')})
        spec = UpcomingEarningsCard(data).build()
        assert 'Before Market' in spec.fields[0].value

    def test_none_earnings_shows_no_upcoming(self):
        data = UpcomingEarningsData(tickers=['AAPL'], earnings_info={'AAPL': None})
        spec = UpcomingEarningsCard(data).build()
        assert 'No upcoming earnings found.' in spec.fields[0].value

    def test_invalid_tickers_in_footer(self):
        data = UpcomingEarningsData(
            tickers=['AAPL'], earnings_info={'AAPL': None}, invalid_tickers=['ZZZ']
        )
        spec = UpcomingEarningsCard(data).build()
        assert spec.footer is not None
        assert 'ZZZ' in spec.footer

    def test_multiple_tickers(self):
        data = UpcomingEarningsData(
            tickers=['AAPL', 'MSFT'],
            earnings_info={'AAPL': _earnings_info(), 'MSFT': None},
        )
        spec = UpcomingEarningsCard(data).build()
        assert len(spec.fields) == 2

    def test_spec_to_embed_produces_discord_embed(self):
        data = UpcomingEarningsData(tickers=['AAPL'], earnings_info={'AAPL': _earnings_info()})
        spec = UpcomingEarningsCard(data).build()
        embed = spec_to_embed(spec)
        assert isinstance(embed, discord.Embed)


# ---------------------------------------------------------------------------
# StatsCard
# ---------------------------------------------------------------------------

class TestStatsCard:
    def test_build_returns_embedspec(self):
        data = TickerStatsData(tickers=['AAPL'], stats={'AAPL': _stats()})
        spec = StatsCard(data).build()
        assert isinstance(spec, EmbedSpec)

    def test_color_is_purple(self):
        data = TickerStatsData(tickers=['AAPL'], stats={'AAPL': _stats()})
        spec = StatsCard(data).build()
        assert spec.color == COLOR_PURPLE

    def test_title(self):
        data = TickerStatsData(tickers=['AAPL'], stats={'AAPL': _stats()})
        spec = StatsCard(data).build()
        assert spec.title == "Ticker Stats"

    def test_field_contains_classification_and_market_cap(self):
        data = TickerStatsData(tickers=['AAPL'], stats={'AAPL': _stats(classification='mega_cap', market_cap=3e12)})
        spec = StatsCard(data).build()
        value = spec.fields[0].value
        assert 'mega_cap' in value
        assert '$3000.0B' in value

    def test_none_stats_shows_no_stats_message(self):
        data = TickerStatsData(tickers=['AAPL'], stats={'AAPL': None})
        spec = StatsCard(data).build()
        assert 'No stats available' in spec.fields[0].value

    def test_missing_market_cap_shows_na(self):
        s = _stats()
        s['market_cap'] = None
        data = TickerStatsData(tickers=['AAPL'], stats={'AAPL': s})
        spec = StatsCard(data).build()
        assert 'N/A' in spec.fields[0].value

    def test_invalid_tickers_in_footer(self):
        data = TickerStatsData(tickers=['AAPL'], stats={'AAPL': _stats()}, invalid_tickers=['BAD'])
        spec = StatsCard(data).build()
        assert spec.footer is not None
        assert 'BAD' in spec.footer

    def test_multiple_tickers(self):
        data = TickerStatsData(
            tickers=['AAPL', 'MSFT'],
            stats={'AAPL': _stats(), 'MSFT': None},
        )
        spec = StatsCard(data).build()
        assert len(spec.fields) == 2

    def test_spec_to_embed_produces_discord_embed(self):
        data = TickerStatsData(tickers=['AAPL'], stats={'AAPL': _stats()})
        spec = StatsCard(data).build()
        embed = spec_to_embed(spec)
        assert isinstance(embed, discord.Embed)


# ---------------------------------------------------------------------------
# MoversCard
# ---------------------------------------------------------------------------

class TestMoversCard:
    def test_build_returns_embedspec(self):
        data = MoverData(direction='gainers', screeners=[_mover()])
        spec = MoversCard(data).build()
        assert isinstance(spec, EmbedSpec)

    def test_gainers_title_and_color(self):
        data = MoverData(direction='gainers', screeners=[_mover()])
        spec = MoversCard(data).build()
        assert spec.title == "Top 10 Daily Movers"
        assert spec.color == COLOR_GOLD

    def test_losers_title_and_color(self):
        data = MoverData(direction='losers', screeners=[_mover(change_pct=-8.0)])
        spec = MoversCard(data).build()
        assert spec.title == "Top 10 Daily Losers"
        assert spec.color == COLOR_RED

    def test_empty_screeners_shows_no_data_description(self):
        data = MoverData(direction='gainers', screeners=[])
        spec = MoversCard(data).build()
        assert 'No mover data available' in spec.description
        assert len(spec.fields) == 0

    def test_one_field_per_mover(self):
        screeners = [_mover('GME', change_pct=15.0), _mover('AMC', change_pct=8.5)]
        data = MoverData(direction='gainers', screeners=screeners)
        spec = MoversCard(data).build()
        assert len(spec.fields) == 2

    def test_capped_at_ten_movers(self):
        screeners = [_mover(f'TK{i}', change_pct=float(i)) for i in range(15)]
        data = MoverData(direction='gainers', screeners=screeners)
        spec = MoversCard(data).build()
        assert len(spec.fields) == 10

    def test_field_name_contains_ticker_and_change(self):
        data = MoverData(direction='gainers', screeners=[_mover('GME', change_pct=15.0)])
        spec = MoversCard(data).build()
        assert 'GME' in spec.fields[0].name
        assert '+15.00%' in spec.fields[0].name

    def test_field_value_contains_price_and_volume(self):
        data = MoverData(direction='gainers', screeners=[_mover('GME', last_price=25.5, volume=10_000_000)])
        spec = MoversCard(data).build()
        assert '25.5' in spec.fields[0].value
        assert '10,000,000' in spec.fields[0].value

    def test_spec_to_embed_produces_discord_embed(self):
        data = MoverData(direction='gainers', screeners=[_mover()])
        spec = MoversCard(data).build()
        embed = spec_to_embed(spec)
        assert isinstance(embed, discord.Embed)


# ---------------------------------------------------------------------------
# Phase 2 helpers
# ---------------------------------------------------------------------------

def _daily_history(n=60):
    import datetime
    base = datetime.date(2026, 1, 1)
    rows = []
    price = 150.0
    for i in range(n):
        rows.append({
            'date': base + datetime.timedelta(days=i),
            'open': price, 'high': price + 2, 'low': price - 2,
            'close': price + 0.5, 'volume': 1_000_000,
        })
        price += 0.1
    return pd.DataFrame(rows)


def _schwab_fundamentals(pe=28.5, pb=5.2, pcf=22.1, eps=6.13, beta=1.22,
                          div=0.96, div_yield=0.52, gross_m=46.0, op_m=30.0,
                          net_m=26.0, roe=162.0, roa=30.0, si_ratio=1.5,
                          si_shares=81_000_000, shares_out=15_000_000_000):
    return {
        'instruments': [{
            'fundamental': {
                'peRatio': pe, 'pbRatio': pb, 'pcfRatio': pcf,
                'eps': eps, 'epsTTM': eps, 'beta': beta,
                'dividendAmount': div, 'dividendYield': div_yield,
                'grossMarginTTM': gross_m, 'operatingMarginTTM': op_m,
                'netProfitMarginTTM': net_m,
                'returnOnEquity': roe, 'returnOnAssets': roa,
                'shortInterestRatio': si_ratio, 'shortInterestShares': si_shares,
                'sharesOutstanding': shares_out,
            }
        }]
    }


def _yfinance_financials():
    """Minimal yfinance-style financials dict with DataFrames."""
    idx = ['Total Revenue', 'Net Income', 'Gross Profit', 'Operating Income']
    data = {'2025-12-31': [1_000_000_000, 200_000_000, 400_000_000, 300_000_000]}
    income = pd.DataFrame(data, index=idx)
    cf_idx = ['Operating Cash Flow', 'Free Cash Flow']
    cf_data = {'2025-12-31': [250_000_000, 180_000_000]}
    cash_flow = pd.DataFrame(cf_data, index=cf_idx)
    return {
        'income_statement': income,
        'quarterly_income_statement': income,
        'balance_sheet': pd.DataFrame(),
        'quarterly_balance_sheet': pd.DataFrame(),
        'cash_flow': cash_flow,
        'quarterly_cash_flow': cash_flow,
    }


def _options_chain(underlying=185.45, iv=32.5, pcr=0.85):
    return {
        'status': 'SUCCESS',
        'underlyingPrice': underlying,
        'volatility': iv,
        'putCallRatio': pcr,
        'callExpDateMap': {
            '2026-04-04:2': {
                '185.0': [{'totalVolume': 5000, 'openInterest': 10000, 'volatility': 33.0}],
                '190.0': [{'totalVolume': 1000, 'openInterest': 3000, 'volatility': 30.0}],
            }
        },
        'putExpDateMap': {
            '2026-04-04:2': {
                '185.0': [{'totalVolume': 4500, 'openInterest': 9000, 'volatility': 34.0}],
            }
        },
    }


def _popularity_df():
    import datetime
    now = datetime.datetime.now()
    rows = [
        {'datetime': now - datetime.timedelta(hours=i), 'rank': 10 + i, 'mentions': 500 - i * 5, 'mentions_24h_ago': 300}
        for i in range(10)
    ]
    return pd.DataFrame(rows)


def _tickers_df():
    return pd.DataFrame([
        {'ticker': 'AAPL', 'sector': 'Technology', 'exchange': 'NASDAQ'},
        {'ticker': 'MSFT', 'sector': 'Technology', 'exchange': 'NASDAQ'},
        {'ticker': 'JPM', 'sector': 'Financial Services', 'exchange': 'NYSE'},
        {'ticker': 'XOM', 'sector': 'Energy', 'exchange': 'NYSE'},
    ])


def _historical_earnings():
    return pd.DataFrame([
        {'date': '2025-10-15', 'eps': 1.50, 'epsforecast': 1.40, 'surprise': 7.1, 'fiscalquarterending': 'Sep 2025'},
        {'date': '2025-07-15', 'eps': 1.28, 'epsforecast': 1.25, 'surprise': 2.4, 'fiscalquarterending': 'Jun 2025'},
        {'date': '2025-04-15', 'eps': 1.20, 'epsforecast': 1.22, 'surprise': -1.6, 'fiscalquarterending': 'Mar 2025'},
        {'date': '2025-01-15', 'eps': 1.45, 'epsforecast': 1.40, 'surprise': 3.6, 'fiscalquarterending': 'Dec 2024'},
    ])


# ---------------------------------------------------------------------------
# PriceSnapshot
# ---------------------------------------------------------------------------

class TestPriceSnapshot:
    def test_build_returns_embedspec(self):
        data = PriceSnapshotData(ticker='AAPL', daily_price_history=_daily_history(), frequency='daily')
        spec = PriceSnapshot(data).build()
        assert isinstance(spec, EmbedSpec)

    def test_title_includes_ticker_and_frequency(self):
        data = PriceSnapshotData(ticker='AAPL', daily_price_history=_daily_history(), frequency='daily')
        spec = PriceSnapshot(data).build()
        assert 'AAPL' in spec.title
        assert 'Daily' in spec.title

    def test_5m_title(self):
        data = PriceSnapshotData(ticker='AAPL', daily_price_history=_daily_history(), frequency='5m')
        spec = PriceSnapshot(data).build()
        assert '5-Minute' in spec.title

    def test_color_is_blue(self):
        data = PriceSnapshotData(ticker='AAPL', daily_price_history=_daily_history(), frequency='daily')
        spec = PriceSnapshot(data).build()
        assert spec.color == COLOR_BLUE

    def test_no_quote_uses_history_fallback(self):
        data = PriceSnapshotData(ticker='AAPL', daily_price_history=_daily_history(), frequency='daily', quote=None)
        spec = PriceSnapshot(data).build()
        assert 'Last Session' in spec.description

    def test_with_quote_shows_ohlcv(self):
        data = PriceSnapshotData(
            ticker='AAPL',
            daily_price_history=_daily_history(),
            frequency='daily',
            quote=_schwab_quote(),
        )
        spec = PriceSnapshot(data).build()
        assert "Today's Summary" in spec.description or "Previous Session" in spec.description

    def test_empty_history_no_quote_shows_no_data(self):
        data = PriceSnapshotData(ticker='AAPL', daily_price_history=pd.DataFrame(), frequency='daily', quote=None)
        spec = PriceSnapshot(data).build()
        assert 'No price data' in spec.description

    def test_technical_signals_present_with_sufficient_history(self):
        data = PriceSnapshotData(ticker='AAPL', daily_price_history=_daily_history(n=60), frequency='daily')
        spec = PriceSnapshot(data).build()
        assert 'Technical Signals' in spec.description

    def test_spec_to_embed_produces_discord_embed(self):
        data = PriceSnapshotData(ticker='AAPL', daily_price_history=_daily_history(), frequency='daily')
        spec = PriceSnapshot(data).build()
        assert isinstance(spec_to_embed(spec), discord.Embed)


# ---------------------------------------------------------------------------
# FinancialHighlights
# ---------------------------------------------------------------------------

class TestFinancialHighlights:
    def test_build_returns_embedspec(self):
        data = FinancialHighlightsData(ticker='AAPL', financials=_yfinance_financials())
        spec = FinancialHighlights(data).build()
        assert isinstance(spec, EmbedSpec)

    def test_title_includes_ticker(self):
        data = FinancialHighlightsData(ticker='AAPL', financials=_yfinance_financials())
        spec = FinancialHighlights(data).build()
        assert 'AAPL' in spec.title

    def test_color_is_teal(self):
        data = FinancialHighlightsData(ticker='AAPL', financials=_yfinance_financials())
        spec = FinancialHighlights(data).build()
        assert spec.color == COLOR_TEAL

    def test_shows_revenue_and_margins(self):
        data = FinancialHighlightsData(ticker='AAPL', financials=_yfinance_financials())
        spec = FinancialHighlights(data).build()
        assert 'Revenue' in spec.description
        assert 'Margins' in spec.description

    def test_empty_financials_shows_no_data(self):
        data = FinancialHighlightsData(ticker='AAPL', financials={})
        spec = FinancialHighlights(data).build()
        assert 'No financial data' in spec.description

    def test_spec_to_embed_produces_discord_embed(self):
        data = FinancialHighlightsData(ticker='AAPL', financials=_yfinance_financials())
        assert isinstance(spec_to_embed(FinancialHighlights(data).build()), discord.Embed)


# ---------------------------------------------------------------------------
# FundamentalsSnapshot
# ---------------------------------------------------------------------------

class TestFundamentalsSnapshot:
    def test_build_returns_embedspec(self):
        data = FundamentalsSnapshotData(ticker='AAPL', fundamentals=_schwab_fundamentals())
        spec = FundamentalsSnapshot(data).build()
        assert isinstance(spec, EmbedSpec)

    def test_title_includes_ticker(self):
        data = FundamentalsSnapshotData(ticker='AAPL', fundamentals=_schwab_fundamentals())
        spec = FundamentalsSnapshot(data).build()
        assert 'AAPL' in spec.title

    def test_color_is_orange(self):
        data = FundamentalsSnapshotData(ticker='AAPL', fundamentals=_schwab_fundamentals())
        spec = FundamentalsSnapshot(data).build()
        assert spec.color == COLOR_ORANGE

    def test_shows_ratios_and_margins(self):
        data = FundamentalsSnapshotData(ticker='AAPL', fundamentals=_schwab_fundamentals())
        spec = FundamentalsSnapshot(data).build()
        assert 'P/E' in spec.description
        assert 'Margins' in spec.description

    def test_shows_short_interest(self):
        data = FundamentalsSnapshotData(ticker='AAPL', fundamentals=_schwab_fundamentals())
        spec = FundamentalsSnapshot(data).build()
        assert 'Short Interest' in spec.description

    def test_empty_fundamentals_shows_no_data(self):
        data = FundamentalsSnapshotData(ticker='AAPL', fundamentals={})
        spec = FundamentalsSnapshot(data).build()
        assert 'No fundamentals' in spec.description

    def test_spec_to_embed_produces_discord_embed(self):
        data = FundamentalsSnapshotData(ticker='AAPL', fundamentals=_schwab_fundamentals())
        assert isinstance(spec_to_embed(FundamentalsSnapshot(data).build()), discord.Embed)


# ---------------------------------------------------------------------------
# OptionsSummary
# ---------------------------------------------------------------------------

class TestOptionsSummary:
    def test_build_returns_embedspec(self):
        data = OptionsSummaryData(ticker='AAPL', options_chain=_options_chain())
        spec = OptionsSummary(data).build()
        assert isinstance(spec, EmbedSpec)

    def test_title_includes_ticker(self):
        data = OptionsSummaryData(ticker='AAPL', options_chain=_options_chain())
        spec = OptionsSummary(data).build()
        assert 'AAPL' in spec.title

    def test_color_is_gold(self):
        data = OptionsSummaryData(ticker='AAPL', options_chain=_options_chain())
        spec = OptionsSummary(data).build()
        assert spec.color == COLOR_GOLD

    def test_shows_iv_and_pcr(self):
        data = OptionsSummaryData(ticker='AAPL', options_chain=_options_chain(iv=32.5, pcr=0.85))
        spec = OptionsSummary(data).build()
        assert '32.5' in spec.description
        assert '0.85' in spec.description

    def test_shows_nearest_expiration(self):
        data = OptionsSummaryData(ticker='AAPL', options_chain=_options_chain(), current_price=185.0)
        spec = OptionsSummary(data).build()
        assert '2026-04-04' in spec.description

    def test_empty_chain_shows_no_data(self):
        data = OptionsSummaryData(ticker='AAPL', options_chain={})
        spec = OptionsSummary(data).build()
        assert 'No options data' in spec.description

    def test_failed_status_shows_no_data(self):
        data = OptionsSummaryData(ticker='AAPL', options_chain={'status': 'FAILED'})
        spec = OptionsSummary(data).build()
        assert 'No options data' in spec.description

    def test_spec_to_embed_produces_discord_embed(self):
        data = OptionsSummaryData(ticker='AAPL', options_chain=_options_chain())
        assert isinstance(spec_to_embed(OptionsSummary(data).build()), discord.Embed)


# ---------------------------------------------------------------------------
# PopularitySnapshot
# ---------------------------------------------------------------------------

class TestPopularitySnapshot:
    def test_build_returns_embedspec(self):
        data = PopularitySnapshotData(ticker='GME', popularity=_popularity_df())
        spec = PopularitySnapshot(data).build()
        assert isinstance(spec, EmbedSpec)

    def test_title_includes_ticker(self):
        data = PopularitySnapshotData(ticker='GME', popularity=_popularity_df())
        spec = PopularitySnapshot(data).build()
        assert 'GME' in spec.title

    def test_color_is_pink(self):
        data = PopularitySnapshotData(ticker='GME', popularity=_popularity_df())
        spec = PopularitySnapshot(data).build()
        assert spec.color == COLOR_PINK

    def test_empty_popularity_shows_no_data(self):
        data = PopularitySnapshotData(ticker='GME', popularity=pd.DataFrame())
        spec = PopularitySnapshot(data).build()
        assert 'No popularity data' in spec.description

    def test_spec_to_embed_produces_discord_embed(self):
        data = PopularitySnapshotData(ticker='GME', popularity=_popularity_df())
        assert isinstance(spec_to_embed(PopularitySnapshot(data).build()), discord.Embed)


# ---------------------------------------------------------------------------
# TickersSummary
# ---------------------------------------------------------------------------

class TestTickersSummary:
    def test_build_returns_embedspec(self):
        data = TickersSummaryData(tickers_df=_tickers_df())
        spec = TickersSummary(data).build()
        assert isinstance(spec, EmbedSpec)

    def test_title(self):
        data = TickersSummaryData(tickers_df=_tickers_df())
        spec = TickersSummary(data).build()
        assert 'Tracked Tickers' in spec.title

    def test_color_is_indigo(self):
        data = TickersSummaryData(tickers_df=_tickers_df())
        spec = TickersSummary(data).build()
        assert spec.color == COLOR_INDIGO

    def test_shows_total_count(self):
        data = TickersSummaryData(tickers_df=_tickers_df())
        spec = TickersSummary(data).build()
        assert '4' in spec.description

    def test_shows_sector_breakdown(self):
        data = TickersSummaryData(tickers_df=_tickers_df())
        spec = TickersSummary(data).build()
        assert 'Technology' in spec.description

    def test_empty_df_shows_no_data(self):
        data = TickersSummaryData(tickers_df=pd.DataFrame())
        spec = TickersSummary(data).build()
        assert 'No ticker data' in spec.description

    def test_spec_to_embed_produces_discord_embed(self):
        data = TickersSummaryData(tickers_df=_tickers_df())
        assert isinstance(spec_to_embed(TickersSummary(data).build()), discord.Embed)


# ---------------------------------------------------------------------------
# EarningsCard
# ---------------------------------------------------------------------------

class TestEarningsCard:
    def test_build_returns_embedspec(self):
        data = EarningsTableData(ticker='AAPL', historical_earnings=_historical_earnings())
        spec = EarningsCard(data).build()
        assert isinstance(spec, EmbedSpec)

    def test_title_includes_ticker(self):
        data = EarningsTableData(ticker='AAPL', historical_earnings=_historical_earnings())
        spec = EarningsCard(data).build()
        assert 'AAPL' in spec.title

    def test_color_is_amber(self):
        data = EarningsTableData(ticker='AAPL', historical_earnings=_historical_earnings())
        spec = EarningsCard(data).build()
        assert spec.color == COLOR_AMBER

    def test_shows_recent_earnings_content(self):
        data = EarningsTableData(ticker='AAPL', historical_earnings=_historical_earnings())
        spec = EarningsCard(data).build()
        assert 'Recent Earnings' in spec.description

    def test_empty_earnings_shows_no_data(self):
        data = EarningsTableData(ticker='AAPL', historical_earnings=pd.DataFrame())
        spec = EarningsCard(data).build()
        assert 'No historical earnings' in spec.description

    def test_spec_to_embed_produces_discord_embed(self):
        data = EarningsTableData(ticker='AAPL', historical_earnings=_historical_earnings())
        assert isinstance(spec_to_embed(EarningsCard(data).build()), discord.Embed)


# ---------------------------------------------------------------------------
# SecFilingCard
# ---------------------------------------------------------------------------

class TestSecFilingCard:
    def test_build_returns_embedspec(self):
        data = SecFilingData(
            tickers=['AAPL'], form='10-K',
            filings={'AAPL': {'filingDate': '2025-11-01', 'link': 'https://sec.gov/1'}},
        )
        spec = SecFilingCard(data).build()
        assert isinstance(spec, EmbedSpec)

    def test_title_includes_form(self):
        data = SecFilingData(tickers=['AAPL'], form='10-K', filings={'AAPL': None})
        spec = SecFilingCard(data).build()
        assert '10-K' in spec.title

    def test_color_is_cyan(self):
        data = SecFilingData(tickers=['AAPL'], form='10-K', filings={'AAPL': None})
        spec = SecFilingCard(data).build()
        assert spec.color == COLOR_CYAN

    def test_none_filing_shows_not_found(self):
        data = SecFilingData(tickers=['AAPL'], form='10-K', filings={'AAPL': None})
        spec = SecFilingCard(data).build()
        assert 'No Form 10-K found' in spec.fields[0].value

    def test_filing_link_in_field(self):
        data = SecFilingData(
            tickers=['AAPL'], form='10-K',
            filings={'AAPL': {'filingDate': '2025-11-01', 'link': 'https://sec.gov/1'}},
        )
        spec = SecFilingCard(data).build()
        assert 'https://sec.gov/1' in spec.fields[0].value

    def test_multiple_tickers(self):
        data = SecFilingData(
            tickers=['AAPL', 'MSFT'], form='10-K',
            filings={'AAPL': None, 'MSFT': {'filingDate': '2025-11-05', 'link': 'https://sec.gov/2'}},
        )
        spec = SecFilingCard(data).build()
        assert len(spec.fields) == 2

    def test_spec_to_embed_produces_discord_embed(self):
        data = SecFilingData(tickers=['AAPL'], form='10-K', filings={'AAPL': None})
        assert isinstance(spec_to_embed(SecFilingCard(data).build()), discord.Embed)
