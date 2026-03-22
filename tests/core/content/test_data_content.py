"""Tests for core/content/data/ content classes."""
import pytest
import discord

from rocketstocks.core.content.models import (
    COLOR_BLUE, COLOR_GREEN, COLOR_GOLD, COLOR_PURPLE, COLOR_RED,
    EmbedSpec,
    QuoteData, UpcomingEarningsData, TickerStatsData, MoverData,
)
from rocketstocks.core.content.data.quote_card import QuoteCard
from rocketstocks.core.content.data.upcoming_earnings_card import UpcomingEarningsCard
from rocketstocks.core.content.data.stats_card import StatsCard
from rocketstocks.core.content.data.movers_card import MoversCard
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
