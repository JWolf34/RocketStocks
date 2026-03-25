"""Tests for rocketstocks.bot.cogs.reports — post_earnings_spotlight, build_stock_report."""
import datetime
import pytest
import pandas as pd
from unittest.mock import AsyncMock, MagicMock, patch

import discord

from rocketstocks.bot.cogs.reports import Reports


def _make_bot():
    bot = MagicMock(name="Bot")
    bot.emitter = MagicMock()
    bot.iter_channels = AsyncMock(return_value=[])
    bot.get_channel_for_guild = AsyncMock(return_value=MagicMock())
    return bot


def _make_cog():
    bot = _make_bot()
    sd = MagicMock(name="StockData")
    sd.earnings = MagicMock()
    sd.earnings.get_earnings_on_date = AsyncMock(return_value=pd.DataFrame())
    sd.tickers = MagicMock()
    sd.alert_tickers = {}
    with (
        patch.object(Reports, "post_popularity_screener"),
        patch.object(Reports, "post_volume_screener"),
        patch.object(Reports, "post_volume_at_time_screener"),
        patch.object(Reports, "post_gainer_screener"),
        patch.object(Reports, "update_earnings_calendar"),
        patch.object(Reports, "post_earnings_spotlight"),
        patch.object(Reports, "post_weekly_earnings"),
        patch.object(Reports, "post_earnings_results"),
        patch("rocketstocks.bot.cogs.reports.DiscordState"),
    ):
        cog = Reports(bot=bot, stock_data=sd)
    cog.dstate = MagicMock()
    cog.mutils = MagicMock()
    return cog


class TestPostEarningsSpotlight:
    @pytest.mark.asyncio
    async def test_skips_when_market_closed(self):
        """Does nothing when the market is not open today."""
        cog = _make_cog()
        cog.mutils.market_open_today.return_value = False

        with patch("rocketstocks.bot.cogs.reports.send_report", new_callable=AsyncMock) as mock_send:
            await cog._post_earnings_spotlight_impl()

        mock_send.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_earnings_skips_gracefully(self):
        """Empty earnings DataFrame should result in an early return without error."""
        cog = _make_cog()
        cog.mutils.market_open_today.return_value = True
        cog.stock_data.earnings.get_earnings_on_date.return_value = pd.DataFrame()

        with patch("rocketstocks.bot.cogs.reports.send_report", new_callable=AsyncMock) as mock_send:
            # Must not raise ValueError (was: randint(0, -1))
            await cog._post_earnings_spotlight_impl()

        mock_send.assert_not_called()

    @pytest.mark.asyncio
    async def test_all_invalid_tickers_exits_cleanly(self):
        """If all tickers fail validate_ticker, the method exits cleanly without sending."""
        cog = _make_cog()
        cog.mutils.market_open_today.return_value = True
        cog.stock_data.earnings.get_earnings_on_date.return_value = pd.DataFrame({
            "ticker": ["AAPL", "MSFT", "GOOG"],
        })
        cog.stock_data.tickers.validate_ticker = AsyncMock(return_value=False)

        with patch("rocketstocks.bot.cogs.reports.send_report", new_callable=AsyncMock) as mock_send:
            # Must not loop forever — exits via next(..., None) sentinel
            await cog._post_earnings_spotlight_impl()

        mock_send.assert_not_called()

    @pytest.mark.asyncio
    async def test_valid_ticker_sends_report(self):
        """A valid ticker should result in a spotlight report being sent."""
        cog = _make_cog()
        cog.mutils.market_open_today.return_value = True
        cog.stock_data.earnings.get_earnings_on_date.return_value = pd.DataFrame({
            "ticker": ["AAPL", "MSFT"],
        })
        cog.stock_data.tickers.validate_ticker = AsyncMock(return_value=True)
        cog.bot.iter_channels = AsyncMock(return_value=[(1, AsyncMock())])

        mock_report = MagicMock()
        mock_report.ticker = "AAPL"

        with (
            patch.object(cog, "build_earnings_spotlight_report", new_callable=AsyncMock, return_value=mock_report),
            patch("rocketstocks.bot.cogs.reports.send_report", new_callable=AsyncMock) as mock_send,
            patch("rocketstocks.bot.cogs.reports.StockReportButtons"),
        ):
            await cog._post_earnings_spotlight_impl()

        mock_send.assert_called_once()

    @pytest.mark.asyncio
    async def test_first_valid_ticker_is_used(self):
        """Only the first valid ticker (after shuffle) is used for the spotlight."""
        cog = _make_cog()
        cog.mutils.market_open_today.return_value = True
        cog.stock_data.earnings.get_earnings_on_date.return_value = pd.DataFrame({
            "ticker": ["AAPL", "MSFT"],
        })

        valid_calls = []

        async def validate_side_effect(ticker):
            valid_calls.append(ticker)
            return ticker == "MSFT"  # Only MSFT is valid

        cog.stock_data.tickers.validate_ticker = AsyncMock(side_effect=validate_side_effect)
        cog.bot.iter_channels = AsyncMock(return_value=[(1, AsyncMock())])

        mock_report = MagicMock()
        mock_report.ticker = "MSFT"

        with (
            patch.object(cog, "build_earnings_spotlight_report", new_callable=AsyncMock, return_value=mock_report) as mock_build,
            patch("rocketstocks.bot.cogs.reports.send_report", new_callable=AsyncMock),
            patch("rocketstocks.bot.cogs.reports.StockReportButtons"),
        ):
            await cog._post_earnings_spotlight_impl()

        # build was called with the single valid ticker
        mock_build.assert_called_once_with(ticker="MSFT")


def _make_stock_data_for_report(cog):
    """Set up stock_data mocks needed by build_stock_report."""
    cog.stock_data.tickers.get_ticker_info = AsyncMock(return_value={})
    cog.stock_data.price_history.fetch_daily_price_history = AsyncMock(return_value=pd.DataFrame())
    cog.stock_data.popularity.fetch_popularity = AsyncMock(return_value=pd.DataFrame())
    cog.stock_data.sec.get_recent_filings = AsyncMock(return_value=pd.DataFrame())
    cog.stock_data.earnings.get_historical_earnings = AsyncMock(return_value=pd.DataFrame())
    cog.stock_data.earnings.get_next_earnings_info = AsyncMock(return_value={})
    cog.stock_data.schwab.get_quote = AsyncMock(return_value={
        'symbol': 'AAPL',
        'quote': {'openPrice': 180, 'highPrice': 190, 'lowPrice': 175,
                  'totalVolume': 50_000_000, 'netPercentChange': 1.5},
        'regular': {'regularMarketLastPrice': 185.0},
        'reference': {'exchangeName': 'NASDAQ', 'isShortable': True, 'isHardToBorrow': False},
    })
    cog.stock_data.schwab.get_fundamentals = AsyncMock(return_value={
        'instruments': [{'fundamental': {
            'marketCap': 3_000_000_000_000, 'eps': 6.5, 'epsTTM': 6.5,
            'peRatio': 28.5, 'beta': 1.2, 'dividendAmount': 0.96,
        }}]
    })


class TestBuildStockReport:
    @pytest.mark.asyncio
    async def test_no_guild_id_yields_empty_recent_alerts(self):
        cog = _make_cog()
        _make_stock_data_for_report(cog)
        cog.dstate.get_recent_alerts_for_ticker = AsyncMock(return_value=[
            (datetime.date.today(), 'EARNINGS_MOVER', '111'),
        ])
        report = await cog.build_stock_report(ticker='AAPL', guild_id=None)
        assert report.data.recent_alerts == []

    @pytest.mark.asyncio
    async def test_with_guild_id_and_alerts_builds_jump_urls(self):
        cog = _make_cog()
        _make_stock_data_for_report(cog)
        today = datetime.date.today()
        cog.dstate.get_recent_alerts_for_ticker = AsyncMock(return_value=[
            (today, 'EARNINGS_MOVER', '111'),
        ])
        cog.stock_data.channel_config.get_channel_id = AsyncMock(return_value=999)
        report = await cog.build_stock_report(ticker='AAPL', guild_id=12345)
        assert len(report.data.recent_alerts) == 1
        entry = report.data.recent_alerts[0]
        assert entry['alert_type'] == 'EARNINGS_MOVER'
        assert '12345/999/111' in entry['url']

    @pytest.mark.asyncio
    async def test_alerts_channel_not_configured_url_is_none(self):
        cog = _make_cog()
        _make_stock_data_for_report(cog)
        today = datetime.date.today()
        cog.dstate.get_recent_alerts_for_ticker = AsyncMock(return_value=[
            (today, 'WATCHLIST_MOVER', '222'),
        ])
        cog.stock_data.channel_config.get_channel_id = AsyncMock(return_value=None)
        report = await cog.build_stock_report(ticker='AAPL', guild_id=12345)
        assert len(report.data.recent_alerts) == 1
        assert report.data.recent_alerts[0]['url'] is None

    @pytest.mark.asyncio
    async def test_no_alerts_returns_empty_list(self):
        cog = _make_cog()
        _make_stock_data_for_report(cog)
        cog.dstate.get_recent_alerts_for_ticker = AsyncMock(return_value=[])
        report = await cog.build_stock_report(ticker='AAPL', guild_id=12345)
        assert report.data.recent_alerts == []


class TestRunTask:
    @pytest.mark.asyncio
    async def test_emits_success_on_completion(self):
        """_run_task emits SUCCESS notification when coroutine completes."""
        cog = _make_cog()

        async def noop():
            pass

        await cog._run_task("test_job", noop())

        cog.bot.emitter.emit.assert_called_once()
        event = cog.bot.emitter.emit.call_args[0][0]
        from rocketstocks.core.notifications.config import NotificationLevel
        assert event.level == NotificationLevel.SUCCESS
        assert event.job_name == "test_job"

    @pytest.mark.asyncio
    async def test_emits_failure_on_exception(self):
        """_run_task emits FAILURE notification without re-raising."""
        cog = _make_cog()

        async def failing():
            raise ValueError("oops")

        await cog._run_task("bad_job", failing())

        cog.bot.emitter.emit.assert_called_once()
        event = cog.bot.emitter.emit.call_args[0][0]
        from rocketstocks.core.notifications.config import NotificationLevel
        assert event.level == NotificationLevel.FAILURE
        assert "oops" in event.message

    @pytest.mark.asyncio
    async def test_no_reraise(self):
        """_run_task must not re-raise so discord.py reschedules normally."""
        cog = _make_cog()

        async def failing():
            raise RuntimeError("fatal")

        try:
            await cog._run_task("job", failing())
        except RuntimeError:
            pytest.fail("_run_task must not re-raise exceptions")


# ---------------------------------------------------------------------------
# TestNewsCommand
# ---------------------------------------------------------------------------

class TestNewsCommand:
    def _make_interaction(self):
        interaction = MagicMock(name="Interaction")
        interaction.response = MagicMock()
        interaction.response.defer = AsyncMock()
        interaction.followup = MagicMock()
        interaction.followup.send = AsyncMock()
        interaction.user = MagicMock()
        interaction.user.name = "TestUser"
        return interaction

    @pytest.mark.asyncio
    async def test_news_error_sends_ephemeral_followup(self):
        """When get_news raises, /news sends an ephemeral error followup."""
        cog = _make_cog()
        interaction = self._make_interaction()

        with patch("rocketstocks.bot.cogs.reports.asyncio.to_thread", new_callable=AsyncMock, side_effect=Exception("API error")):
            await cog.news.callback(cog, interaction, query="AAPL", sort_by="publishedAt")

        interaction.response.defer.assert_called_once()
        interaction.followup.send.assert_called_once()
        call_kwargs = interaction.followup.send.call_args.kwargs
        assert call_kwargs.get("ephemeral") is True

    @pytest.mark.asyncio
    async def test_news_success_sends_embed(self):
        """When get_news succeeds, /news sends an embed in the followup."""
        cog = _make_cog()
        interaction = self._make_interaction()
        fake_news = {'articles': []}
        mock_embed = MagicMock()

        with (
            patch("rocketstocks.bot.cogs.reports.asyncio.to_thread", new_callable=AsyncMock, return_value=fake_news),
            patch("rocketstocks.bot.cogs.reports.spec_to_embed", return_value=mock_embed),
            patch("rocketstocks.bot.cogs.reports.NewsReport"),
        ):
            await cog.news.callback(cog, interaction, query="AAPL", sort_by="publishedAt")

        interaction.response.defer.assert_called_once()
        interaction.followup.send.assert_called_once()
        call_kwargs = interaction.followup.send.call_args.kwargs
        assert call_kwargs.get("embed") is mock_embed


# ---------------------------------------------------------------------------
# Plan F — _post_weekly_earnings_impl discord.File guard
# ---------------------------------------------------------------------------

class TestPostWeeklyEarningsImpl:
    @pytest.mark.asyncio
    async def test_oserror_on_file_sends_screener_without_file(self):
        """If discord.File(content.filepath) raises OSError, screener is still sent (files=[])."""
        cog = _make_cog()
        cog.mutils.market_open_today.return_value = True
        cog.bot.iter_channels = AsyncMock(return_value=[(None, AsyncMock())])

        mock_content = MagicMock()
        mock_content.filepath = "/tmp/nonexistent.csv"

        with (
            patch.object(cog, "build_weekly_earnings_screener", new_callable=AsyncMock, return_value=mock_content),
            patch("rocketstocks.bot.cogs.reports.discord.File", side_effect=OSError("file not found")),
            patch("rocketstocks.bot.cogs.reports.send_screener", new_callable=AsyncMock) as mock_send,
        ):
            # Monday = weekday 0
            fixed_date = datetime.datetime(2026, 3, 16)  # a Monday
            with patch("rocketstocks.bot.cogs.reports.datetime") as mock_dt:
                mock_dt.datetime.now.return_value = fixed_date
                mock_dt.datetime.combine = datetime.datetime.combine
                mock_dt.datetime.strptime = datetime.datetime.strptime
                mock_dt.time = datetime.time
                mock_dt.date = datetime.date
                mock_dt.timedelta = datetime.timedelta
                await cog._post_weekly_earnings_impl()

        mock_send.assert_called_once()
        _, call_kwargs = mock_send.call_args
        assert call_kwargs.get("files") == []


# ---------------------------------------------------------------------------
# Plan F — _update_earnings_calendar_impl: missing time field + per-ticker guard
# ---------------------------------------------------------------------------

class TestUpdateEarningsCalendarImpl:
    @pytest.mark.asyncio
    async def test_missing_time_field_does_not_raise(self):
        """earnings_info with no 'time' key should default to 'unspecified' release time."""
        cog = _make_cog()
        cog.stock_data.watchlists.get_all_watchlist_tickers = AsyncMock(return_value=["AAPL"])

        earnings_info = {
            'date': datetime.date(2026, 4, 1),
            'time': [],   # empty list — no [0] element
            'fiscal_quarter_ending': 'Q1 2026',
            'eps_forecast': '1.50',
            'last_year_eps': '1.30',
            'last_year_rpt_dt': '2025-04-01',
        }
        cog.stock_data.earnings.get_next_earnings_info = AsyncMock(return_value=earnings_info)

        mock_guild = AsyncMock()
        mock_guild.name = "TestGuild"
        mock_guild.fetch_scheduled_events = AsyncMock(return_value=[])
        mock_guild.create_scheduled_event = AsyncMock()
        cog.bot.guilds = [mock_guild]

        await cog._update_earnings_calendar_impl()

        mock_guild.create_scheduled_event.assert_called_once()
        call_kwargs = mock_guild.create_scheduled_event.call_args.kwargs
        assert call_kwargs["name"] == "AAPL Earnings"

    @pytest.mark.asyncio
    async def test_per_ticker_error_does_not_abort_remaining_tickers(self):
        """An exception for one ticker should not prevent processing subsequent tickers."""
        cog = _make_cog()
        cog.stock_data.watchlists.get_all_watchlist_tickers = AsyncMock(return_value=["AAPL", "MSFT"])

        good_earnings = {
            'date': datetime.date(2026, 4, 1),
            'time': [],
            'fiscal_quarter_ending': 'Q1 2026',
            'eps_forecast': '2.00',
            'last_year_eps': '1.80',
            'last_year_rpt_dt': '2025-04-01',
        }
        cog.stock_data.earnings.get_next_earnings_info = AsyncMock(
            side_effect=[Exception("DB error"), good_earnings]
        )

        mock_guild = AsyncMock()
        mock_guild.name = "TestGuild"
        mock_guild.fetch_scheduled_events = AsyncMock(return_value=[])
        mock_guild.create_scheduled_event = AsyncMock()
        cog.bot.guilds = [mock_guild]

        await cog._update_earnings_calendar_impl()

        # MSFT should still be processed despite AAPL failing
        mock_guild.create_scheduled_event.assert_called_once()
        call_kwargs = mock_guild.create_scheduled_event.call_args.kwargs
        assert call_kwargs["name"] == "MSFT Earnings"


# ---------------------------------------------------------------------------
# _post_earnings_results_impl
# ---------------------------------------------------------------------------

class TestPostEarningsResultsImpl:
    def _make_cog_with_results_support(self):
        cog = _make_cog()
        cog.mutils.market_open_today.return_value = True

        # Wire up the earnings results chain
        cog.stock_data.earnings.get_earnings_on_date = AsyncMock(
            return_value=pd.DataFrame({'ticker': ['AAPL']})
        )
        cog.stock_data.watchlists = MagicMock()
        cog.stock_data.watchlists.get_all_watchlist_tickers = AsyncMock(return_value=['AAPL'])
        cog.stock_data.earnings_results = MagicMock()
        cog.stock_data.earnings_results.get_posted_tickers_today = AsyncMock(return_value=set())
        cog.stock_data.earnings_results.insert_result = AsyncMock()
        cog.stock_data.yfinance = MagicMock()
        return cog

    @pytest.mark.asyncio
    async def test_skips_when_market_closed(self):
        cog = _make_cog()
        cog.mutils.market_open_today.return_value = False

        with patch("rocketstocks.bot.cogs.reports.send_report", new_callable=AsyncMock) as mock_send:
            await cog._post_earnings_results_impl()

        mock_send.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_no_earnings_today(self):
        cog = self._make_cog_with_results_support()
        cog.stock_data.earnings.get_earnings_on_date.return_value = pd.DataFrame()

        with patch("rocketstocks.bot.cogs.reports.send_report", new_callable=AsyncMock) as mock_send:
            await cog._post_earnings_results_impl()

        mock_send.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_already_posted_tickers(self):
        cog = self._make_cog_with_results_support()
        cog.stock_data.earnings_results.get_posted_tickers_today.return_value = {'AAPL'}

        with patch("rocketstocks.bot.cogs.reports.send_report", new_callable=AsyncMock) as mock_send:
            await cog._post_earnings_results_impl()

        mock_send.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_non_watchlist_tickers(self):
        cog = self._make_cog_with_results_support()
        # AAPL is in earnings but NOT in the watchlist
        cog.stock_data.watchlists.get_all_watchlist_tickers.return_value = ['MSFT']

        with patch("rocketstocks.bot.cogs.reports.send_report", new_callable=AsyncMock) as mock_send:
            await cog._post_earnings_results_impl()

        mock_send.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_result_not_yet_available(self):
        cog = self._make_cog_with_results_support()
        cog.stock_data.yfinance.get_earnings_result.return_value = None

        with patch("rocketstocks.bot.cogs.reports.asyncio.to_thread", new=AsyncMock(return_value=None)), \
             patch("rocketstocks.bot.cogs.reports.send_report", new_callable=AsyncMock) as mock_send:
            await cog._post_earnings_results_impl()

        mock_send.assert_not_called()
        cog.stock_data.earnings_results.insert_result.assert_not_called()

    @pytest.mark.asyncio
    async def test_posts_and_inserts_when_result_available(self):
        import datetime as _dt
        cog = self._make_cog_with_results_support()
        result = {'eps_actual': 1.52, 'eps_estimate': 1.45, 'surprise_pct': 4.83}

        mock_report = MagicMock()
        cog.bot.iter_channels = AsyncMock(return_value=[(None, MagicMock())])

        # Pin UTC hour to 15 so the time-of-day guard passes regardless of when tests run
        fixed_now = _dt.datetime(2026, 3, 23, 15, 0, 0)
        with patch("rocketstocks.bot.cogs.reports.datetime") as mock_dt, \
             patch("rocketstocks.bot.cogs.reports.asyncio.to_thread", new=AsyncMock(return_value=result)), \
             patch("rocketstocks.bot.cogs.reports.send_report", new_callable=AsyncMock) as mock_send, \
             patch.object(cog, "build_earnings_result_report", new=AsyncMock(return_value=mock_report)):
            mock_dt.datetime.utcnow.return_value = fixed_now
            mock_dt.date.today.return_value = fixed_now.date()
            mock_dt.time = _dt.time
            await cog._post_earnings_results_impl()

        mock_send.assert_called_once()
        cog.stock_data.earnings_results.insert_result.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_handles_exception_per_ticker_without_crashing(self):
        cog = self._make_cog_with_results_support()

        with patch("rocketstocks.bot.cogs.reports.asyncio.to_thread", side_effect=RuntimeError("oops")), \
             patch("rocketstocks.bot.cogs.reports.send_report", new_callable=AsyncMock) as mock_send:
            # Should not raise
            await cog._post_earnings_results_impl()

        mock_send.assert_not_called()


class TestBuildFullStockReport:
    """Tests for build_full_stock_report() and the detail parameter."""

    def _make_cog(self):
        return _make_cog()

    @pytest.mark.asyncio
    async def test_returns_full_stock_report_instance(self):
        from rocketstocks.core.content.reports.stock_report import FullStockReport
        cog = self._make_cog()

        base_report = MagicMock()
        base_report.data.ticker_info = {}
        base_report.data.quote = {}
        base_report.data.fundamentals = {}
        base_report.data.daily_price_history = pd.DataFrame()
        base_report.data.popularity = pd.DataFrame()
        base_report.data.historical_earnings = pd.DataFrame()
        base_report.data.next_earnings_info = {}
        base_report.data.recent_sec_filings = pd.DataFrame()
        base_report.data.recent_alerts = []

        cog.stock_data.yfinance = MagicMock()
        cog.stock_data.yfinance.get_analyst_price_targets = MagicMock(return_value=None)
        cog.stock_data.yfinance.get_recommendations_summary = MagicMock(return_value=pd.DataFrame())
        cog.stock_data.yfinance.get_upgrades_downgrades = MagicMock(return_value=pd.DataFrame())
        cog.stock_data.nasdaq = MagicMock()
        cog.stock_data.nasdaq.get_earnings_forecast_quarterly = MagicMock(return_value=pd.DataFrame())
        cog.stock_data.nasdaq.get_earnings_forecast_yearly = MagicMock(return_value=pd.DataFrame())
        cog.stock_data.ticker_stats = MagicMock()
        cog.stock_data.ticker_stats.get_stats = AsyncMock(return_value=None)

        with patch.object(cog, 'build_stock_report', new=AsyncMock(return_value=base_report)), \
             patch('rocketstocks.bot.cogs.reports.asyncio.to_thread', new=AsyncMock(return_value=None)):
            result = await cog.build_full_stock_report(ticker='AAPL')

        assert isinstance(result, FullStockReport)

    @pytest.mark.asyncio
    async def test_graceful_on_yfinance_failure(self):
        from rocketstocks.core.content.reports.stock_report import FullStockReport
        cog = self._make_cog()

        base_report = MagicMock()
        base_report.data.ticker_info = {}
        base_report.data.quote = {}
        base_report.data.fundamentals = {}
        base_report.data.daily_price_history = pd.DataFrame()
        base_report.data.popularity = pd.DataFrame()
        base_report.data.historical_earnings = pd.DataFrame()
        base_report.data.next_earnings_info = {}
        base_report.data.recent_sec_filings = pd.DataFrame()
        base_report.data.recent_alerts = []

        cog.stock_data.yfinance = MagicMock()
        cog.stock_data.nasdaq = MagicMock()
        cog.stock_data.ticker_stats = MagicMock()
        cog.stock_data.ticker_stats.get_stats = AsyncMock(return_value=None)

        async def _raise(*_args, **_kwargs):
            raise RuntimeError("API error")

        with patch.object(cog, 'build_stock_report', new=AsyncMock(return_value=base_report)), \
             patch('rocketstocks.bot.cogs.reports.asyncio.to_thread', new=_raise):
            result = await cog.build_full_stock_report(ticker='AAPL')

        assert isinstance(result, FullStockReport)


class TestBuildComparisonReport:
    """Tests for build_comparison_report()."""

    def _make_cog(self):
        return _make_cog()

    @pytest.mark.asyncio
    async def test_returns_comparison_report_instance(self):
        from rocketstocks.core.content.reports.comparison_report import ComparisonReport
        cog = self._make_cog()

        cog.stock_data.schwab = MagicMock()
        cog.stock_data.schwab.get_quotes = AsyncMock(return_value={
            "AAPL": {"regular": {"regularMarketLastPrice": 188.9}, "quote": {"netPercentChange": 2.5, "totalVolume": 50000000}, "reference": {}},
            "MSFT": {"regular": {"regularMarketLastPrice": 410.0}, "quote": {"netPercentChange": 1.0, "totalVolume": 30000000}, "reference": {}},
        })
        cog.stock_data.schwab.get_fundamentals = AsyncMock(return_value={
            "instruments": [{"fundamental": {"marketCap": 2_900_000_000_000, "eps": 6.42, "peRatio": 29.0, "beta": 1.2}}]
        })
        cog.stock_data.tickers = MagicMock()
        cog.stock_data.tickers.get_ticker_info = AsyncMock(return_value={"name": "Test Corp"})
        cog.stock_data.price_history = MagicMock()
        cog.stock_data.price_history.fetch_daily_price_history = AsyncMock(return_value=pd.DataFrame())
        cog.stock_data.popularity = MagicMock()
        cog.stock_data.popularity.fetch_popularity = AsyncMock(return_value=pd.DataFrame())
        cog.stock_data.ticker_stats = MagicMock()
        cog.stock_data.ticker_stats.get_stats = AsyncMock(return_value=None)

        result = await cog.build_comparison_report(tickers=["AAPL", "MSFT"])
        assert isinstance(result, ComparisonReport)

    @pytest.mark.asyncio
    async def test_benchmark_appended_to_tickers(self):
        from rocketstocks.core.content.reports.comparison_report import ComparisonReport
        cog = self._make_cog()

        cog.stock_data.schwab = MagicMock()
        cog.stock_data.schwab.get_quotes = AsyncMock(return_value={})
        cog.stock_data.schwab.get_fundamentals = AsyncMock(return_value={})
        cog.stock_data.tickers = MagicMock()
        cog.stock_data.tickers.get_ticker_info = AsyncMock(return_value={})
        cog.stock_data.price_history = MagicMock()
        cog.stock_data.price_history.fetch_daily_price_history = AsyncMock(return_value=pd.DataFrame())
        cog.stock_data.popularity = MagicMock()
        cog.stock_data.popularity.fetch_popularity = AsyncMock(return_value=pd.DataFrame())
        cog.stock_data.ticker_stats = MagicMock()
        cog.stock_data.ticker_stats.get_stats = AsyncMock(return_value=None)

        result = await cog.build_comparison_report(tickers=["AAPL", "MSFT"], benchmark_ticker="SPY")
        assert isinstance(result, ComparisonReport)
        assert "SPY" in result.data.tickers
        assert result.data.benchmark_ticker == "SPY"

    @pytest.mark.asyncio
    async def test_schwab_error_does_not_raise(self):
        from rocketstocks.data.clients.schwab import SchwabTokenError
        from rocketstocks.core.content.reports.comparison_report import ComparisonReport
        cog = self._make_cog()

        cog.stock_data.schwab = MagicMock()
        cog.stock_data.schwab.get_quotes = AsyncMock(side_effect=SchwabTokenError("no token"))
        cog.stock_data.schwab.get_fundamentals = AsyncMock(side_effect=SchwabTokenError("no token"))
        cog.stock_data.tickers = MagicMock()
        cog.stock_data.tickers.get_ticker_info = AsyncMock(return_value={})
        cog.stock_data.price_history = MagicMock()
        cog.stock_data.price_history.fetch_daily_price_history = AsyncMock(return_value=pd.DataFrame())
        cog.stock_data.popularity = MagicMock()
        cog.stock_data.popularity.fetch_popularity = AsyncMock(return_value=pd.DataFrame())
        cog.stock_data.ticker_stats = MagicMock()
        cog.stock_data.ticker_stats.get_stats = AsyncMock(return_value=None)

        result = await cog.build_comparison_report(tickers=["AAPL", "MSFT"])
        assert isinstance(result, ComparisonReport)


class TestBuildTechnicalReport:
    """Tests for build_technical_report()."""

    def _make_cog(self):
        return _make_cog()

    def _wire_technical_report(self, cog, quote=None, schwab_error=None):
        """Wire up all dependencies needed by build_technical_report()."""
        cog.stock_data.schwab = MagicMock()
        if schwab_error:
            cog.stock_data.schwab.get_quote = AsyncMock(side_effect=schwab_error)
        else:
            cog.stock_data.schwab.get_quote = AsyncMock(return_value=quote or {})
        cog.stock_data.tickers = MagicMock()
        cog.stock_data.tickers.get_ticker_info = AsyncMock(return_value={'name': 'Apple Inc'})
        cog.stock_data.price_history = MagicMock()
        cog.stock_data.price_history.fetch_daily_price_history = AsyncMock(return_value=pd.DataFrame())
        cog.stock_data.ticker_stats = MagicMock()
        cog.stock_data.ticker_stats.get_stats = AsyncMock(return_value=None)
        cog.stock_data.yfinance = MagicMock()
        cog.stock_data.yfinance.get_float_data = MagicMock(return_value={})

    @pytest.mark.asyncio
    async def test_returns_technical_report_instance(self):
        from rocketstocks.core.content.reports.technical_report import TechnicalReport
        cog = self._make_cog()
        self._wire_technical_report(cog, quote={
            'regular': {'regularMarketLastPrice': 188.9},
            'quote': {'netPercentChange': 2.5, 'totalVolume': 50000000},
            'reference': {'exchangeName': 'NASDAQ', 'isShortable': True, 'isHardToBorrow': False},
            'symbol': 'AAPL',
        })

        with patch("rocketstocks.bot.cogs.reports.asyncio.to_thread", new=AsyncMock(return_value={})):
            result = await cog.build_technical_report(ticker='AAPL')
        assert isinstance(result, TechnicalReport)

    @pytest.mark.asyncio
    async def test_schwab_error_uses_empty_quote(self):
        from rocketstocks.data.clients.schwab import SchwabTokenError
        from rocketstocks.core.content.reports.technical_report import TechnicalReport
        cog = self._make_cog()
        self._wire_technical_report(cog, schwab_error=SchwabTokenError("no token"))

        with patch("rocketstocks.bot.cogs.reports.asyncio.to_thread", new=AsyncMock(return_value={})):
            result = await cog.build_technical_report(ticker='AAPL')
        assert isinstance(result, TechnicalReport)
        assert result.data.quote == {}

    @pytest.mark.asyncio
    async def test_float_data_attached_to_report(self):
        from rocketstocks.core.content.reports.technical_report import TechnicalReport
        cog = self._make_cog()
        float_data = {'float_shares': 1_000_000, 'short_pct_float': 0.05, 'short_ratio': 3.0}
        self._wire_technical_report(cog)
        cog.stock_data.yfinance.get_float_data = MagicMock(return_value=float_data)

        with patch("rocketstocks.bot.cogs.reports.asyncio.to_thread", new=AsyncMock(return_value=float_data)):
            result = await cog.build_technical_report(ticker='AAPL')
        assert isinstance(result, TechnicalReport)
        assert result.data.float_data == float_data


class TestBuildOptionsReport:
    """Tests for build_options_report()."""

    def _make_cog(self):
        return _make_cog()

    def _minimal_chain(self):
        strikes = [185.0, 190.0, 195.0]
        exp_key = '2024-06-21:30'
        def _c(s, ds):
            return {'strikePrice': s, 'totalVolume': 500, 'openInterest': 2000,
                    'volatility': 25.0, 'delta': ds * 0.5, 'gamma': 0.04,
                    'theta': -0.10, 'vega': 0.15, 'bid': 2.0, 'ask': 2.1, 'mark': 2.05}
        return {
            'status': 'SUCCESS', 'volatility': 25.5, 'putCallRatio': 0.85,
            'underlyingPrice': 190.0,
            'callExpDateMap': {exp_key: {str(s): [_c(s, 1.0)] for s in strikes}},
            'putExpDateMap':  {exp_key: {str(s): [_c(s, -1.0)] for s in strikes}},
        }

    @pytest.mark.asyncio
    async def test_returns_options_report_instance(self):
        from rocketstocks.core.content.reports.options_report import OptionsReport
        cog = self._make_cog()
        cog.stock_data.schwab = MagicMock()
        cog.stock_data.schwab.get_options_chain = AsyncMock(return_value=self._minimal_chain())
        cog.stock_data.schwab.get_quote = AsyncMock(return_value={
            'regular': {'regularMarketLastPrice': 190.0},
            'quote': {'netPercentChange': 1.0, 'totalVolume': 20000000},
            'reference': {}, 'symbol': 'AAPL',
        })
        cog.stock_data.tickers = MagicMock()
        cog.stock_data.tickers.get_ticker_info = AsyncMock(return_value={'name': 'Apple Inc'})
        cog.stock_data.price_history = MagicMock()
        cog.stock_data.price_history.fetch_daily_price_history = AsyncMock(return_value=pd.DataFrame())
        cog.stock_data.iv_history = MagicMock()
        cog.stock_data.iv_history.get_iv_history = AsyncMock(return_value=pd.DataFrame())

        result = await cog.build_options_report(ticker='AAPL')
        assert isinstance(result, OptionsReport)

    @pytest.mark.asyncio
    async def test_schwab_chain_error_returns_empty_chain(self):
        from rocketstocks.data.clients.schwab import SchwabTokenError
        from rocketstocks.core.content.reports.options_report import OptionsReport
        cog = self._make_cog()
        cog.stock_data.schwab = MagicMock()
        cog.stock_data.schwab.get_options_chain = AsyncMock(side_effect=SchwabTokenError("no token"))
        cog.stock_data.schwab.get_quote = AsyncMock(side_effect=SchwabTokenError("no token"))
        cog.stock_data.tickers = MagicMock()
        cog.stock_data.tickers.get_ticker_info = AsyncMock(return_value={})
        cog.stock_data.price_history = MagicMock()
        cog.stock_data.price_history.fetch_daily_price_history = AsyncMock(return_value=pd.DataFrame())
        cog.stock_data.iv_history = MagicMock()
        cog.stock_data.iv_history.get_iv_history = AsyncMock(return_value=pd.DataFrame())

        result = await cog.build_options_report(ticker='AAPL')
        assert isinstance(result, OptionsReport)
        assert result.data.options_chain == {}
