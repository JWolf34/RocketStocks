"""Tests for rocketstocks.bot.cogs.reports — post_earnings_spotlight."""
import datetime
import pytest
import pandas as pd
from unittest.mock import AsyncMock, MagicMock, patch

from rocketstocks.bot.cogs.reports import Reports


def _make_bot():
    bot = MagicMock(name="Bot")
    bot.emitter = MagicMock()
    bot.iter_channels.return_value = []
    return bot


def _make_cog():
    bot = _make_bot()
    sd = MagicMock(name="StockData")
    sd.earnings = MagicMock()
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
        cog.bot.iter_channels.return_value = [(1, AsyncMock())]

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
        cog.bot.iter_channels.return_value = [(1, AsyncMock())]

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
