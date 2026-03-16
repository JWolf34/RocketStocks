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


class TestResolveSinceDt:
    """Unit tests for the _resolve_since_dt module-level helper."""

    def test_market_open_today(self):
        from rocketstocks.bot.cogs.reports import _resolve_since_dt
        dt, label = _resolve_since_dt('market_open_today')
        assert dt.time() == datetime.time(14, 30)
        assert "market open" in label

    def test_last_3_days(self):
        from rocketstocks.bot.cogs.reports import _resolve_since_dt
        dt, label = _resolve_since_dt('last_3_days')
        expected = datetime.date.today() - datetime.timedelta(days=3)
        assert dt.date() == expected
        assert dt.time() == datetime.time.min
        assert "3 days" in label

    def test_last_7_days(self):
        from rocketstocks.bot.cogs.reports import _resolve_since_dt
        dt, label = _resolve_since_dt('last_7_days')
        expected = datetime.date.today() - datetime.timedelta(days=7)
        assert dt.date() == expected
        assert "7 days" in label

    def test_last_close_returns_previous_trading_day(self):
        from rocketstocks.bot.cogs.reports import _resolve_since_dt
        dt, label = _resolve_since_dt('last_close')
        assert dt.time() == datetime.time(21, 0)
        assert dt.date() < datetime.date.today()
        assert "last close" in label

    def test_unknown_value_defaults_to_last_close(self):
        from rocketstocks.bot.cogs.reports import _resolve_since_dt
        dt, label = _resolve_since_dt('unknown_value')
        assert dt.time() == datetime.time(21, 0)


class TestSendSubscriptionSelect:
    def _make_cog_with_roles(self, guild_roles=None):
        cog = _make_cog()
        cog.bot.stock_data.alert_roles = MagicMock()
        cog.bot.stock_data.alert_roles.get_all_for_guild = AsyncMock(return_value=guild_roles or {})
        return cog

    def _make_interaction(self, guild_id=123, user_role_ids=None):
        interaction = MagicMock(spec=discord.Interaction)
        interaction.guild_id = guild_id
        roles = []
        for rid in (user_role_ids or []):
            role = MagicMock(spec=discord.Role)
            role.id = rid
            roles.append(role)
        interaction.user = MagicMock(spec=discord.Member)
        interaction.user.roles = roles
        interaction.response.send_message = AsyncMock()
        return interaction

    @pytest.mark.asyncio
    async def test_send_subscription_select_sends_ephemeral_message(self):
        cog = self._make_cog_with_roles()
        interaction = self._make_interaction()
        with patch("rocketstocks.bot.cogs.reports.AlertSubscriptionSelect"), \
             patch("rocketstocks.bot.cogs.reports.AlertSubscriptionView"):
            await cog._send_subscription_select(interaction)
        interaction.response.send_message.assert_called_once()
        assert interaction.response.send_message.call_args.kwargs.get("ephemeral") is True

    @pytest.mark.asyncio
    async def test_send_subscription_select_fetches_guild_roles(self):
        guild_roles = {"earnings_mover": 100}
        cog = self._make_cog_with_roles(guild_roles)
        interaction = self._make_interaction(guild_id=999)
        with patch("rocketstocks.bot.cogs.reports.AlertSubscriptionSelect"), \
             patch("rocketstocks.bot.cogs.reports.AlertSubscriptionView"):
            await cog._send_subscription_select(interaction)
        cog.bot.stock_data.alert_roles.get_all_for_guild.assert_called_once_with(999)


class TestAlertSubscribe:
    @pytest.mark.asyncio
    async def test_alert_subscribe_delegates_to_send_subscription_select(self):
        cog = _make_cog()
        interaction = MagicMock()
        interaction.response = AsyncMock()

        with patch.object(cog, "_send_subscription_select", new_callable=AsyncMock) as mock_send:
            await cog.alert_subscribe.callback(cog, interaction)

        mock_send.assert_called_once_with(interaction)


class TestAlertSummaryCommand:
    def _make_interaction(self):
        interaction = MagicMock()
        interaction.response = AsyncMock()
        interaction.followup = AsyncMock()
        interaction.user.name = "testuser"
        interaction.guild_id = 12345
        return interaction

    @pytest.mark.asyncio
    async def test_defaults_to_last_close_when_no_args(self):
        cog = _make_cog()
        interaction = self._make_interaction()
        mock_content = MagicMock()
        mock_message = MagicMock()
        mock_message.jump_url = "https://discord.com/channels/1/2/3"

        with (
            patch.object(cog, 'build_alert_summary', new_callable=AsyncMock, return_value=mock_content) as mock_build,
            patch("rocketstocks.bot.cogs.reports.send_report", new_callable=AsyncMock, return_value=mock_message),
            patch("rocketstocks.bot.cogs.reports._resolve_since_dt", return_value=(
                datetime.datetime(2026, 3, 7, 21, 0), "since last close (Mar 07)"
            )) as mock_resolve,
        ):
            await cog.alert_summary.callback(cog, interaction, since_when=None, visibility=None)

        mock_resolve.assert_called_with('last_close')

    @pytest.mark.asyncio
    async def test_uses_since_when_value(self):
        cog = _make_cog()
        interaction = self._make_interaction()
        mock_content = MagicMock()
        mock_message = MagicMock()
        mock_message.jump_url = "https://discord.com/channels/1/2/3"

        since_when = MagicMock()
        since_when.value = 'last_3_days'

        with (
            patch.object(cog, 'build_alert_summary', new_callable=AsyncMock, return_value=mock_content),
            patch("rocketstocks.bot.cogs.reports.send_report", new_callable=AsyncMock, return_value=mock_message),
            patch("rocketstocks.bot.cogs.reports._resolve_since_dt", return_value=(
                datetime.datetime(2026, 3, 5), "last 3 days"
            )) as mock_resolve,
        ):
            await cog.alert_summary.callback(cog, interaction, since_when=since_when, visibility=None)

        mock_resolve.assert_called_with('last_3_days')

    @pytest.mark.asyncio
    async def test_private_by_default(self):
        cog = _make_cog()
        interaction = self._make_interaction()
        mock_content = MagicMock()
        mock_message = MagicMock()
        mock_message.jump_url = "https://discord.com/channels/1/2/3"

        with (
            patch.object(cog, 'build_alert_summary', new_callable=AsyncMock, return_value=mock_content),
            patch("rocketstocks.bot.cogs.reports.send_report", new_callable=AsyncMock, return_value=mock_message) as mock_send,
            patch("rocketstocks.bot.cogs.reports._resolve_since_dt", return_value=(
                datetime.datetime(2026, 3, 7, 21, 0), "since last close (Mar 07)"
            )),
        ):
            await cog.alert_summary.callback(cog, interaction, since_when=None, visibility=None)

        call_kwargs = mock_send.call_args[1]
        assert call_kwargs.get('visibility') == 'private'

    @pytest.mark.asyncio
    async def test_public_visibility_passes_through(self):
        cog = _make_cog()
        interaction = self._make_interaction()
        mock_content = MagicMock()
        mock_message = MagicMock()
        mock_message.jump_url = "https://discord.com/channels/1/2/3"

        visibility = MagicMock()
        visibility.value = 'public'

        with (
            patch.object(cog, 'build_alert_summary', new_callable=AsyncMock, return_value=mock_content),
            patch("rocketstocks.bot.cogs.reports.send_report", new_callable=AsyncMock, return_value=mock_message) as mock_send,
            patch("rocketstocks.bot.cogs.reports._resolve_since_dt", return_value=(
                datetime.datetime(2026, 3, 7, 21, 0), "since last close (Mar 07)"
            )),
        ):
            await cog.alert_summary.callback(cog, interaction, since_when=None, visibility=visibility)

        call_kwargs = mock_send.call_args[1]
        assert call_kwargs.get('visibility') == 'public'

    @pytest.mark.asyncio
    async def test_build_alert_summary_calls_dstate(self):
        cog = _make_cog()
        since_dt = datetime.datetime(2026, 3, 7, 21, 0)
        cog.dstate.get_alerts_since = AsyncMock(return_value=[])
        result = await cog.build_alert_summary(since_dt, "test label")
        cog.dstate.get_alerts_since.assert_called_once_with(since_dt)
        from rocketstocks.core.content.reports.alert_summary import AlertSummary
        assert isinstance(result, AlertSummary)
