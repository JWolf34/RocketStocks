"""Tests for card-format section builder functions in sections_card.py."""
import datetime

import pandas as pd
import pytest

from rocketstocks.core.content.sections_card import (
    performance_card, fundamentals_card, technical_signals_card,
    popularity_card, upcoming_earnings_card, politician_info_card, sec_filings_card,
    ticker_info_card, todays_change_card, earnings_date_card, news_card,
    recent_alerts_card, earnings_result_card, recent_earnings_card,
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

    def test_popularity_card_empty_df(self):
        result = popularity_card(pd.DataFrame())
        assert isinstance(result, str) and len(result) > 0
        assert 'No popularity data' in result

    def test_popularity_card_none(self):
        result = popularity_card(None)
        assert 'No popularity data' in result

    def test_popularity_card_no_hash_headers(self):
        result = popularity_card(pd.DataFrame())
        assert '##' not in result


class TestPopularityCard:
    """Tests for the rank-change delta popularity card."""

    def _make_df(self, now: datetime.datetime) -> pd.DataFrame:
        """Full DataFrame with data at every expected interval offset."""
        rows = [
            # Most recent (current)
            {'datetime': now - datetime.timedelta(minutes=5),
             'rank': 15, 'mentions': 127, 'mentions_24h_ago': 38},
            # ~2H ago
            {'datetime': now - datetime.timedelta(hours=2),
             'rank': 28, 'mentions': 100, 'mentions_24h_ago': 30},
            # ~4H ago
            {'datetime': now - datetime.timedelta(hours=4),
             'rank': 35, 'mentions': 90, 'mentions_24h_ago': 28},
            # ~8H ago
            {'datetime': now - datetime.timedelta(hours=8),
             'rank': 50, 'mentions': 80, 'mentions_24h_ago': 25},
            # 1D ago daily row
            {'datetime': now - datetime.timedelta(days=1, hours=2),
             'rank': 18, 'mentions': 70, 'mentions_24h_ago': 20},
            # 3D ago daily row
            {'datetime': now - datetime.timedelta(days=3, hours=2),
             'rank': 42, 'mentions': 60, 'mentions_24h_ago': 15},
            # 7D ago daily row
            {'datetime': now - datetime.timedelta(days=7, hours=2),
             'rank': 65, 'mentions': 50, 'mentions_24h_ago': 10},
        ]
        df = pd.DataFrame(rows)
        return df.sort_values('datetime', ascending=False).reset_index(drop=True)

    def test_normal_header_shows_rank_and_mentions(self):
        now = datetime.datetime.now()
        result = popularity_card(self._make_df(now))
        assert 'Rank **#15**' in result
        assert '127' in result

    def test_normal_intraday_2h_delta(self):
        now = datetime.datetime.now()
        result = popularity_card(self._make_df(now))
        # rank 28 vs current 15 → delta = 28-15 = 13 gained → ↑13
        assert '2H' in result
        assert '↑13' in result

    def test_normal_intraday_4h_delta(self):
        now = datetime.datetime.now()
        result = popularity_card(self._make_df(now))
        # rank 35 vs current 15 → delta = 20
        assert '4H' in result
        assert '↑20' in result

    def test_normal_intraday_8h_delta(self):
        now = datetime.datetime.now()
        result = popularity_card(self._make_df(now))
        # rank 50 vs current 15 → delta = 35
        assert '8H' in result
        assert '↑35' in result

    def test_normal_daily_best_1d(self):
        now = datetime.datetime.now()
        result = popularity_card(self._make_df(now))
        # best rank on 1D ago date = 18, current 15 → delta = 3
        assert '1D best' in result
        assert '↑3' in result

    def test_normal_daily_best_3d(self):
        now = datetime.datetime.now()
        result = popularity_card(self._make_df(now))
        # best rank on 3D ago date = 42, current 15 → delta = 27
        assert '3D best' in result
        assert '↑27' in result

    def test_normal_daily_best_7d(self):
        now = datetime.datetime.now()
        result = popularity_card(self._make_df(now))
        # best rank on 7D ago date = 65, current 15 → delta = 50
        assert '7D best' in result
        assert '↑50' in result

    def test_normal_mentions_line(self):
        now = datetime.datetime.now()
        result = popularity_card(self._make_df(now))
        # mentions=127, mentions_24h_ago=38, delta=89, pct=~234%
        assert '24H Mentions' in result
        assert '+89' in result

    def test_rank_worse_shows_down_arrow(self):
        now = datetime.datetime.now()
        rows = [
            {'datetime': now - datetime.timedelta(minutes=5),
             'rank': 50, 'mentions': 80, 'mentions_24h_ago': 100},
            {'datetime': now - datetime.timedelta(hours=2),
             'rank': 20, 'mentions': 120, 'mentions_24h_ago': 80},
        ]
        df = pd.DataFrame(rows).sort_values('datetime', ascending=False).reset_index(drop=True)
        result = popularity_card(df)
        # past=20, current=50 → delta = 20-50 = -30 → ↓30
        assert '↓30' in result

    def test_missing_intraday_shows_na(self):
        now = datetime.datetime.now()
        # Only current row + 4H row — no row near 2H target
        rows = [
            {'datetime': now - datetime.timedelta(minutes=5),
             'rank': 15, 'mentions': 127, 'mentions_24h_ago': 38},
            {'datetime': now - datetime.timedelta(hours=4),
             'rank': 35, 'mentions': 90, 'mentions_24h_ago': 28},
        ]
        df = pd.DataFrame(rows).sort_values('datetime', ascending=False).reset_index(drop=True)
        result = popularity_card(df)
        # Nearest to 2H target: 5min row (diff=115min) or 4H row (diff=120min) — both >35min
        assert '2H: N/A' in result

    def test_missing_daily_best_shows_na(self):
        now = datetime.datetime.now()
        rows = [
            {'datetime': now - datetime.timedelta(minutes=5),
             'rank': 15, 'mentions': 127, 'mentions_24h_ago': 38},
            {'datetime': now - datetime.timedelta(days=1, hours=2),
             'rank': 18, 'mentions': 70, 'mentions_24h_ago': 20},
            # No 3D or 7D data
        ]
        df = pd.DataFrame(rows).sort_values('datetime', ascending=False).reset_index(drop=True)
        result = popularity_card(df)
        assert '3D best: N/A' in result
        assert '7D best: N/A' in result

    def test_zero_mentions_24h_ago_skips_mentions_line(self):
        now = datetime.datetime.now()
        rows = [{'datetime': now - datetime.timedelta(minutes=5),
                 'rank': 15, 'mentions': 127, 'mentions_24h_ago': 0}]
        df = pd.DataFrame(rows)
        result = popularity_card(df)
        assert '24H Mentions' not in result

    def test_none_mentions_24h_ago_skips_mentions_line(self):
        now = datetime.datetime.now()
        rows = [{'datetime': now - datetime.timedelta(minutes=5),
                 'rank': 15, 'mentions': 127, 'mentions_24h_ago': None}]
        df = pd.DataFrame(rows)
        result = popularity_card(df)
        assert '24H Mentions' not in result

    def test_returns_string(self):
        now = datetime.datetime.now()
        result = popularity_card(self._make_df(now))
        assert isinstance(result, str)

    def test_no_hash_headers(self):
        now = datetime.datetime.now()
        result = popularity_card(self._make_df(now))
        assert '##' not in result

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


class TestRecentAlertsCard:
    def test_empty_list_returns_empty_string(self):
        assert recent_alerts_card([]) == ''

    def test_with_url_includes_view_link(self):
        alerts = [{'date': datetime.date(2026, 3, 5), 'alert_type': 'EARNINGS_MOVER',
                   'url': 'https://discord.com/channels/1/2/3'}]
        result = recent_alerts_card(alerts)
        assert '[View](<https://discord.com/channels/1/2/3>)' in result

    def test_without_url_no_link(self):
        alerts = [{'date': datetime.date(2026, 3, 5), 'alert_type': 'EARNINGS_MOVER', 'url': None}]
        result = recent_alerts_card(alerts)
        assert '[View]' not in result
        assert '🚨 Earnings Mover' in result
        assert '03/05' in result

    def test_header_present(self):
        alerts = [{'date': datetime.date(2026, 3, 5), 'alert_type': 'WATCHLIST_MOVER', 'url': None}]
        result = recent_alerts_card(alerts)
        assert '__**Recent Alerts**__' in result

    def test_all_alert_type_labels(self):
        types = {
            'EARNINGS_MOVER': '🚨 Earnings Mover',
            'WATCHLIST_MOVER': '👀 Watchlist Mover',
            'MARKET_ALERT': '📈 Market Alert',
            'POPULARITY_SURGE': '🔥 Popularity Surge',
            'MOMENTUM_CONFIRMATION': '⚡ Momentum Confirmation',
        }
        for alert_type, expected_label in types.items():
            alerts = [{'date': datetime.date(2026, 3, 5), 'alert_type': alert_type, 'url': None}]
            result = recent_alerts_card(alerts)
            assert expected_label in result, f"Expected '{expected_label}' for alert_type '{alert_type}'"

    def test_multiple_entries_all_rendered(self):
        alerts = [
            {'date': datetime.date(2026, 3, 5), 'alert_type': 'EARNINGS_MOVER',
             'url': 'https://discord.com/channels/1/2/10'},
            {'date': datetime.date(2026, 3, 4), 'alert_type': 'POPULARITY_SURGE', 'url': None},
        ]
        result = recent_alerts_card(alerts)
        assert '🚨 Earnings Mover' in result
        assert '🔥 Popularity Surge' in result
        assert '03/05' in result
        assert '03/04' in result

    def test_unknown_alert_type_uses_raw_value(self):
        alerts = [{'date': datetime.date(2026, 3, 5), 'alert_type': 'CUSTOM_ALERT', 'url': None}]
        result = recent_alerts_card(alerts)
        assert 'CUSTOM_ALERT' in result


# ---------------------------------------------------------------------------
# earnings_result_card
# ---------------------------------------------------------------------------

class TestEarningsResultCard:
    def test_beat_shows_checkmark(self):
        result = earnings_result_card(1.52, 1.45, 4.83)
        assert '✅' in result
        assert 'Beat' in result

    def test_miss_shows_x(self):
        result = earnings_result_card(1.30, 1.45, -10.3)
        assert '❌' in result
        assert 'Missed' in result

    def test_contains_eps_actual(self):
        result = earnings_result_card(1.52, 1.45, 4.83)
        assert '1.52' in result

    def test_contains_eps_estimate(self):
        result = earnings_result_card(1.52, 1.45, 4.83)
        assert '1.45' in result

    def test_contains_surprise_pct(self):
        result = earnings_result_card(1.52, 1.45, 4.83)
        assert '4.8' in result

    def test_none_estimate_handled(self):
        result = earnings_result_card(1.52, None, None)
        assert '1.52' in result
        assert 'Result available' in result

    def test_zero_surprise_treated_as_beat(self):
        result = earnings_result_card(1.50, 1.50, 0.0)
        assert '✅' in result

    def test_shows_earnings_result_header(self):
        result = earnings_result_card(1.52, 1.45, 4.83)
        assert 'Earnings Result' in result


# ---------------------------------------------------------------------------
# recent_earnings_card — None/NaN safety
# ---------------------------------------------------------------------------

def _earnings_df(**overrides) -> pd.DataFrame:
    """One-row historical earnings DataFrame with sensible defaults."""
    row = {
        'date': datetime.date(2025, 11, 1),
        'fiscalquarterending': 'Sep 2025',
        'eps': 1.52,
        'epsforecast': 1.45,
        'surprise': 4.83,
    }
    row.update(overrides)
    return pd.DataFrame([row])


class TestRecentEarningsCard:
    def test_fully_populated_shows_beat_emoji(self):
        result = recent_earnings_card(_earnings_df(surprise=4.83))
        assert '✅' in result

    def test_fully_populated_shows_miss_emoji(self):
        result = recent_earnings_card(_earnings_df(surprise=-3.5))
        assert '❌' in result

    def test_fully_populated_formats_eps(self):
        result = recent_earnings_card(_earnings_df(eps=1.52))
        assert '$1.52' in result

    def test_fully_populated_formats_estimate(self):
        result = recent_earnings_card(_earnings_df(epsforecast=1.45))
        assert '$1.45' in result

    def test_surprise_none_no_type_error(self):
        result = recent_earnings_card(_earnings_df(surprise=None))
        assert 'N/A' in result
        assert '❓' in result

    def test_surprise_nan_no_type_error(self):
        result = recent_earnings_card(_earnings_df(surprise=float('nan')))
        assert 'N/A' in result
        assert '❓' in result

    def test_eps_none_no_type_error(self):
        result = recent_earnings_card(_earnings_df(eps=None))
        assert 'N/A' in result

    def test_eps_nan_no_type_error(self):
        result = recent_earnings_card(_earnings_df(eps=float('nan')))
        assert 'N/A' in result

    def test_epsforecast_none_no_type_error(self):
        result = recent_earnings_card(_earnings_df(epsforecast=None))
        assert 'N/A' in result

    def test_epsforecast_nan_no_type_error(self):
        result = recent_earnings_card(_earnings_df(epsforecast=float('nan')))
        assert 'N/A' in result

    def test_all_nullable_fields_none(self):
        result = recent_earnings_card(_earnings_df(eps=None, epsforecast=None, surprise=None))
        assert result.count('N/A') >= 3

    def test_empty_df_returns_no_historical(self):
        result = recent_earnings_card(pd.DataFrame())
        assert 'No historical earnings found' in result

    def test_none_df_returns_no_historical(self):
        result = recent_earnings_card(None)
        assert 'No historical earnings found' in result

    def test_no_header_when_show_header_false(self):
        result = recent_earnings_card(_earnings_df(), show_header=False)
        assert '__**Recent Earnings**__' not in result

    def test_returns_string(self):
        result = recent_earnings_card(_earnings_df())
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# fundamentals_card — None safety for eps/peRatio
# ---------------------------------------------------------------------------

class TestFundamentalsCardNoneSafety:
    def _fund_with(self, eps, pe_ratio) -> dict:
        return {
            'instruments': [{'fundamental': {
                'marketCap': 2_900_000_000_000,
                'eps': eps,
                'peRatio': pe_ratio,
                'beta': 1.24,
                'dividendAmount': 0.96,
            }}]
        }

    def test_eps_none_shows_na(self):
        result = fundamentals_card(self._fund_with(None, 29.42), _minimal_quote())
        assert 'EPS **N/A**' in result

    def test_pe_none_shows_na(self):
        result = fundamentals_card(self._fund_with(6.42, None), _minimal_quote())
        assert 'P/E **N/A**' in result

    def test_both_none_no_type_error(self):
        result = fundamentals_card(self._fund_with(None, None), _minimal_quote())
        assert result.count('N/A') >= 2

    def test_normal_values_formatted(self):
        result = fundamentals_card(self._fund_with(6.42, 29.42), _minimal_quote())
        assert 'EPS **6.42**' in result
        assert 'P/E **29.42**' in result
