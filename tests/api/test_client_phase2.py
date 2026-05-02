"""Tests for DataAPI Phase 2 methods — full single-ticker surface."""
import datetime
from unittest.mock import AsyncMock, MagicMock, call

import pandas as pd
import pytest

from rocketstocks.api.client import DataAPI


# ---------------------------------------------------------------------------
# Reference & stats
# ---------------------------------------------------------------------------

class TestGetTickerInfos:
    async def test_fans_out_concurrently(self, api, mock_stock_data):
        mock_stock_data.tickers.get_ticker_info = AsyncMock(
            side_effect=lambda t: {"ticker": t}
        )
        results = await api.get_ticker_infos(["AAPL", "MSFT", "NVDA"])
        assert len(results) == 3
        assert mock_stock_data.tickers.get_ticker_info.call_count == 3

    async def test_preserves_order(self, api, mock_stock_data):
        mock_stock_data.tickers.get_ticker_info = AsyncMock(
            side_effect=lambda t: {"ticker": t}
        )
        results = await api.get_ticker_infos(["NVDA", "AAPL"])
        assert results[0]["ticker"] == "NVDA"
        assert results[1]["ticker"] == "AAPL"

    async def test_propagates_none_for_missing(self, api, mock_stock_data):
        mock_stock_data.tickers.get_ticker_info = AsyncMock(return_value=None)
        results = await api.get_ticker_infos(["ZZZZ"])
        assert results == [None]


class TestValidateTicker:
    async def test_returns_true_when_found(self, api, mock_stock_data):
        result = await api.validate_ticker("AAPL")
        mock_stock_data.tickers.validate_ticker.assert_called_once_with("AAPL")
        assert result is True

    async def test_returns_false_when_not_found(self, api, mock_stock_data):
        mock_stock_data.tickers.validate_ticker = AsyncMock(return_value=False)
        result = await api.validate_ticker("ZZZZ")
        assert result is False


class TestGetTickerStats:
    async def test_returns_stats_dict(self, api, mock_stock_data):
        result = await api.get_ticker_stats("AAPL")
        mock_stock_data.ticker_stats.get_stats.assert_called_once_with("AAPL")
        assert result["ticker"] == "AAPL"

    async def test_returns_none_when_absent(self, api, mock_stock_data):
        mock_stock_data.ticker_stats.get_stats = AsyncMock(return_value=None)
        result = await api.get_ticker_stats("ZZZZ")
        assert result is None


class TestGetTickerStatsBatch:
    async def test_returns_dict_keyed_by_ticker(self, api, mock_stock_data):
        mock_stock_data.ticker_stats.get_stats = AsyncMock(side_effect=lambda t: {"ticker": t})
        result = await api.get_ticker_stats_batch(["AAPL", "MSFT"])
        assert set(result.keys()) == {"AAPL", "MSFT"}
        assert mock_stock_data.ticker_stats.get_stats.call_count == 2

    async def test_gather_parallelism(self, api, mock_stock_data):
        mock_stock_data.ticker_stats.get_stats = AsyncMock(side_effect=lambda t: {"ticker": t})
        tickers = ["A", "B", "C", "D"]
        result = await api.get_ticker_stats_batch(tickers)
        assert mock_stock_data.ticker_stats.get_stats.call_count == len(tickers)


# ---------------------------------------------------------------------------
# Quotes
# ---------------------------------------------------------------------------

class TestGetQuotes:
    async def test_delegates_to_schwab_batch(self, api, mock_stock_data):
        result = await api.get_quotes(["AAPL", "MSFT"])
        mock_stock_data.schwab.get_quotes.assert_called_once_with(["AAPL", "MSFT"], fields=None)
        assert "AAPL" in result

    async def test_passes_fields(self, api, mock_stock_data):
        await api.get_quotes(["AAPL"], fields=["fundamental"])
        mock_stock_data.schwab.get_quotes.assert_called_once_with(["AAPL"], fields=["fundamental"])


# ---------------------------------------------------------------------------
# Price history
# ---------------------------------------------------------------------------

class TestGetDailyHistories:
    async def test_uses_batch_query(self, api, mock_stock_data, sample_daily_df):
        mock_stock_data.price_history.fetch_daily_price_history_batch = AsyncMock(
            return_value={"AAPL": sample_daily_df, "MSFT": sample_daily_df}
        )
        result = await api.get_daily_histories(["AAPL", "MSFT"], "2026-01-01", "2026-01-31")
        mock_stock_data.price_history.fetch_daily_price_history_batch.assert_called_once()
        assert set(result.keys()) == {"AAPL", "MSFT"}

    async def test_empty_tickers_returns_empty_dict(self, api, mock_stock_data):
        mock_stock_data.price_history.fetch_daily_price_history_batch = AsyncMock(return_value={})
        result = await api.get_daily_histories([], "2026-01-01", "2026-01-31")
        assert result == {}


class TestGet5mHistory:
    async def test_returns_dataframe(self, api, mock_stock_data, sample_5m_df):
        mock_stock_data.price_history.fetch_5m_price_history = AsyncMock(return_value=sample_5m_df)
        result = await api.get_5m_history("AAPL", "2026-01-02", "2026-01-03")
        mock_stock_data.price_history.fetch_5m_price_history.assert_called_once()
        assert not result.empty

    async def test_accepts_datetime_objects(self, api, mock_stock_data, sample_5m_df):
        mock_stock_data.price_history.fetch_5m_price_history = AsyncMock(return_value=sample_5m_df)
        start = datetime.datetime(2026, 1, 2, 9, 30)
        end = datetime.datetime(2026, 1, 2, 16, 0)
        result = await api.get_5m_history("AAPL", start, end)
        assert not result.empty


class TestGet5mHistories:
    async def test_fans_out_per_ticker(self, api, mock_stock_data, sample_5m_df):
        mock_stock_data.price_history.fetch_5m_price_history = AsyncMock(return_value=sample_5m_df)
        result = await api.get_5m_histories(["AAPL", "MSFT"], "2026-01-02", "2026-01-03")
        assert set(result.keys()) == {"AAPL", "MSFT"}
        assert mock_stock_data.price_history.fetch_5m_price_history.call_count == 2


# ---------------------------------------------------------------------------
# Fundamentals & financials
# ---------------------------------------------------------------------------

class TestGetSchwabFundamentals:
    async def test_single_ticker_wraps_in_list(self, api, mock_stock_data):
        await api.get_schwab_fundamentals("AAPL")
        mock_stock_data.schwab.get_fundamentals.assert_called_once_with(["AAPL"])

    async def test_propagates_error(self, api, mock_stock_data):
        mock_stock_data.schwab.get_fundamentals = AsyncMock(side_effect=RuntimeError("err"))
        with pytest.raises(RuntimeError):
            await api.get_schwab_fundamentals("AAPL")


class TestGetSchwabFundamentalsBatch:
    async def test_passes_list_directly(self, api, mock_stock_data):
        await api.get_schwab_fundamentals_batch(["AAPL", "MSFT"])
        mock_stock_data.schwab.get_fundamentals.assert_called_once_with(["AAPL", "MSFT"])


class TestGetFinancials:
    def test_returns_dict_of_dataframes(self, api, mock_stock_data):
        result = api.get_financials("AAPL")
        mock_stock_data.yfinance.get_financials.assert_called_once_with("AAPL")
        assert "income_statement" in result

    def test_propagates_error(self, api, mock_stock_data):
        mock_stock_data.yfinance.get_financials = MagicMock(side_effect=RuntimeError("yf down"))
        with pytest.raises(RuntimeError):
            api.get_financials("AAPL")


class TestGetEpsHistory:
    async def test_returns_dataframe(self, api, mock_stock_data):
        result = await api.get_eps_history("AAPL")
        mock_stock_data.earnings.get_historical_earnings.assert_called_once_with("AAPL")
        assert not result.empty


class TestYFinanceDelegates:
    def test_get_analyst_price_targets(self, api, mock_stock_data):
        result = api.get_analyst_price_targets("AAPL")
        mock_stock_data.yfinance.get_analyst_price_targets.assert_called_once_with("AAPL")
        assert result is not None

    def test_get_recommendations(self, api, mock_stock_data):
        result = api.get_recommendations("AAPL")
        mock_stock_data.yfinance.get_recommendations_summary.assert_called_once_with("AAPL")
        assert isinstance(result, pd.DataFrame)

    def test_get_upgrades_downgrades(self, api, mock_stock_data):
        result = api.get_upgrades_downgrades("AAPL")
        mock_stock_data.yfinance.get_upgrades_downgrades.assert_called_once_with("AAPL")
        assert isinstance(result, pd.DataFrame)

    def test_get_float_data(self, api, mock_stock_data):
        result = api.get_float_data("AAPL")
        mock_stock_data.yfinance.get_float_data.assert_called_once_with("AAPL")
        assert "float_shares" in result

    def test_get_institutional_holders(self, api, mock_stock_data):
        result = api.get_institutional_holders("AAPL")
        mock_stock_data.yfinance.get_institutional_holders.assert_called_once_with("AAPL")
        assert isinstance(result, pd.DataFrame)

    def test_get_major_holders(self, api, mock_stock_data):
        result = api.get_major_holders("AAPL")
        mock_stock_data.yfinance.get_major_holders.assert_called_once_with("AAPL")
        assert isinstance(result, pd.DataFrame)

    def test_get_insider_transactions(self, api, mock_stock_data):
        result = api.get_insider_transactions("AAPL")
        mock_stock_data.yfinance.get_insider_transactions.assert_called_once_with("AAPL")
        assert isinstance(result, pd.DataFrame)

    def test_get_insider_purchases(self, api, mock_stock_data):
        result = api.get_insider_purchases("AAPL")
        mock_stock_data.yfinance.get_insider_purchases.assert_called_once_with("AAPL")
        assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# Earnings calendar
# ---------------------------------------------------------------------------

class TestGetNextEarnings:
    async def test_returns_info_dict(self, api, mock_stock_data):
        result = await api.get_next_earnings("AAPL")
        mock_stock_data.earnings.get_next_earnings_info.assert_called_once_with("AAPL")
        assert result["ticker"] == "AAPL"

    async def test_returns_none_when_absent(self, api, mock_stock_data):
        mock_stock_data.earnings.get_next_earnings_info = AsyncMock(return_value=None)
        result = await api.get_next_earnings("AAPL")
        assert result is None


class TestGetEarningsCalendar:
    async def test_returns_dataframe_for_date(self, api, mock_stock_data):
        date = datetime.date(2026, 5, 1)
        result = await api.get_earnings_calendar(date)
        mock_stock_data.earnings.get_earnings_on_date.assert_called_once_with(date)
        assert not result.empty


class TestGetEpsEstimate:
    def test_quarter_calls_quarterly_method(self, api, mock_stock_data):
        api.get_eps_estimate("AAPL", period="quarter")
        mock_stock_data.nasdaq.get_earnings_forecast_quarterly.assert_called_once_with("AAPL")
        mock_stock_data.nasdaq.get_earnings_forecast_yearly.assert_not_called()

    def test_year_calls_yearly_method(self, api, mock_stock_data):
        api.get_eps_estimate("AAPL", period="year")
        mock_stock_data.nasdaq.get_earnings_forecast_yearly.assert_called_once_with("AAPL")
        mock_stock_data.nasdaq.get_earnings_forecast_quarterly.assert_not_called()

    def test_default_is_quarter(self, api, mock_stock_data):
        api.get_eps_estimate("AAPL")
        mock_stock_data.nasdaq.get_earnings_forecast_quarterly.assert_called_once()


# ---------------------------------------------------------------------------
# Options & IV
# ---------------------------------------------------------------------------

class TestGetOptionsChain:
    async def test_returns_chain_dict(self, api, mock_stock_data):
        result = await api.get_options_chain("AAPL")
        mock_stock_data.schwab.get_options_chain.assert_called_once_with("AAPL")
        assert "callExpDateMap" in result

    async def test_propagates_error(self, api, mock_stock_data):
        mock_stock_data.schwab.get_options_chain = AsyncMock(side_effect=RuntimeError("rate limit"))
        with pytest.raises(RuntimeError):
            await api.get_options_chain("AAPL")


class TestGetOptionsChainsBatch:
    async def test_fans_out_concurrently(self, api, mock_stock_data):
        result = await api.get_options_chains_batch(["AAPL", "MSFT"])
        assert set(result.keys()) == {"AAPL", "MSFT"}
        assert mock_stock_data.schwab.get_options_chain.call_count == 2

    async def test_isolates_per_ticker_errors(self, api, mock_stock_data):
        async def _chain(ticker):
            if ticker == "MSFT":
                raise RuntimeError("no options")
            return {"callExpDateMap": {}}

        mock_stock_data.schwab.get_options_chain = AsyncMock(side_effect=_chain)
        result = await api.get_options_chains_batch(["AAPL", "MSFT"])
        assert "AAPL" in result
        assert "MSFT" not in result


class TestGetIvHistory:
    async def test_returns_dataframe(self, api, mock_stock_data):
        result = await api.get_iv_history("AAPL", days=30)
        mock_stock_data.iv_history.get_iv_history.assert_called_once_with("AAPL", 30)
        assert not result.empty


class TestGetLatestIv:
    async def test_returns_float(self, api, mock_stock_data):
        result = await api.get_latest_iv("AAPL")
        mock_stock_data.iv_history.get_latest_iv.assert_called_once_with("AAPL")
        assert result == pytest.approx(0.3)

    async def test_returns_none_when_absent(self, api, mock_stock_data):
        mock_stock_data.iv_history.get_latest_iv = AsyncMock(return_value=None)
        result = await api.get_latest_iv("ZZZZ")
        assert result is None


# ---------------------------------------------------------------------------
# Sentiment & flow
# ---------------------------------------------------------------------------

class TestGetPopularity:
    async def test_returns_dataframe(self, api, mock_stock_data):
        result = await api.get_popularity("GME")
        mock_stock_data.popularity.fetch_popularity.assert_called_once_with(
            ticker="GME", start_date=None, end_date=None
        )
        assert not result.empty

    async def test_passes_date_range(self, api, mock_stock_data):
        start = datetime.date(2026, 1, 1)
        end = datetime.date(2026, 1, 31)
        await api.get_popularity("GME", start_date=start, end_date=end)
        mock_stock_data.popularity.fetch_popularity.assert_called_once_with(
            ticker="GME", start_date=start, end_date=end
        )


class TestGetPopularTickers:
    def test_calls_ape_wisdom_with_defaults(self, api, mock_stock_data):
        result = api.get_popular_tickers()
        mock_stock_data.popularity_client.get_popular_stocks.assert_called_once_with(
            filter_name="all stock subreddits", num_stocks=100
        )
        assert isinstance(result, pd.DataFrame)

    def test_passes_custom_filter_and_limit(self, api, mock_stock_data):
        api.get_popular_tickers(filter_name="wallstreetbets", limit=50)
        mock_stock_data.popularity_client.get_popular_stocks.assert_called_once_with(
            filter_name="wallstreetbets", num_stocks=50
        )


class TestGetNews:
    def test_delegates_to_news_client(self, api, mock_stock_data):
        result = api.get_news("Apple earnings")
        mock_stock_data.news.get_news.assert_called_once_with("Apple earnings")
        assert "articles" in result

    def test_passes_kwargs(self, api, mock_stock_data):
        api.get_news("Apple", page_size=10)
        mock_stock_data.news.get_news.assert_called_once_with("Apple", page_size=10)


class TestGetRecentFilings:
    async def test_returns_dataframe(self, api, mock_stock_data):
        result = await api.get_recent_filings("AAPL", latest=5)
        mock_stock_data.sec.get_recent_filings.assert_called_once_with("AAPL", latest=5)
        assert not result.empty


class TestGetFilingLink:
    async def test_returns_url_string(self, api, mock_stock_data):
        filing = {"accessionNumber": "0000320193-26-000001", "primaryDocument": "aapl.htm"}
        result = await api.get_filing_link("AAPL", filing)
        mock_stock_data.sec.get_link_to_filing.assert_called_once_with("AAPL", filing)
        assert result is not None


class TestGetPoliticianTrades:
    def test_delegates_to_capitol_trades(self, api, mock_stock_data):
        result = api.get_politician_trades("nancy-pelosi")
        mock_stock_data.capitol_trades.trades.assert_called_once_with("nancy-pelosi")
        assert isinstance(result, pd.DataFrame)


class TestGetAllPoliticians:
    async def test_returns_list(self, api, mock_stock_data):
        result = await api.get_all_politicians()
        mock_stock_data.capitol_trades.all_politicians.assert_called_once()
        assert isinstance(result, list)
        assert result[0]["name"] == "Nancy Pelosi"


# ---------------------------------------------------------------------------
# Movers
# ---------------------------------------------------------------------------

class TestMovers:
    def test_get_premarket_gainers(self, api, mock_stock_data):
        result = api.get_premarket_gainers()
        mock_stock_data.trading_view.get_premarket_gainers.assert_called_once()
        assert isinstance(result, pd.DataFrame)

    def test_get_intraday_gainers(self, api, mock_stock_data):
        result = api.get_intraday_gainers()
        mock_stock_data.trading_view.get_intraday_gainers.assert_called_once()
        assert isinstance(result, pd.DataFrame)

    def test_get_postmarket_gainers(self, api, mock_stock_data):
        result = api.get_postmarket_gainers()
        mock_stock_data.trading_view.get_postmarket_gainers.assert_called_once()
        assert isinstance(result, pd.DataFrame)

    def test_get_unusual_volume_movers(self, api, mock_stock_data):
        result = api.get_unusual_volume_movers()
        mock_stock_data.trading_view.get_unusual_volume_movers.assert_called_once()
        assert isinstance(result, pd.DataFrame)

    def test_get_market_caps_default_limit(self, api, mock_stock_data):
        api.get_market_caps()
        mock_stock_data.trading_view.get_market_caps.assert_called_once_with(10000)

    def test_get_market_caps_custom_limit(self, api, mock_stock_data):
        api.get_market_caps(limit=500)
        mock_stock_data.trading_view.get_market_caps.assert_called_once_with(500)

    async def test_get_schwab_movers(self, api, mock_stock_data):
        result = await api.get_schwab_movers()
        mock_stock_data.schwab.get_movers.assert_called_once_with(None)
        assert isinstance(result, dict)

    async def test_get_schwab_movers_with_sort_order(self, api, mock_stock_data):
        await api.get_schwab_movers(sort_order="PERCENT_CHANGE_DOWN")
        mock_stock_data.schwab.get_movers.assert_called_once_with("PERCENT_CHANGE_DOWN")


# ---------------------------------------------------------------------------
# Watchlists
# ---------------------------------------------------------------------------

class TestGetWatchlists:
    async def test_returns_list_of_names(self, api, mock_stock_data):
        result = await api.get_watchlists()
        mock_stock_data.watchlists.get_watchlists.assert_called_once_with(watchlist_types=None)
        assert result == ["Tech", "Biotech"]

    async def test_passes_types_filter(self, api, mock_stock_data):
        await api.get_watchlists(types=["named", "personal"])
        mock_stock_data.watchlists.get_watchlists.assert_called_once_with(
            watchlist_types=["named", "personal"]
        )


class TestGetWatchlistTickers:
    async def test_returns_ticker_list(self, api, mock_stock_data):
        result = await api.get_watchlist_tickers("Tech")
        mock_stock_data.watchlists.get_watchlist_tickers.assert_called_once_with("Tech")
        assert "AAPL" in result

    async def test_empty_watchlist_returns_empty_list(self, api, mock_stock_data):
        mock_stock_data.watchlists.get_watchlist_tickers = AsyncMock(return_value=[])
        result = await api.get_watchlist_tickers("Empty")
        assert result == []
