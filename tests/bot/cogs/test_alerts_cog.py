"""Tests for rocketstocks.bot.cogs.alerts — send_earnings_movers."""
import datetime
import pytest
import pandas as pd
from unittest.mock import AsyncMock, MagicMock, patch

from rocketstocks.bot.cogs.alerts import Alerts


def _make_bot():
    bot = MagicMock(name="Bot")
    bot.emitter = MagicMock()
    bot.iter_channels.return_value = []
    return bot


def _make_stock_data(earnings_df: pd.DataFrame):
    sd = MagicMock(name="StockData")
    sd.earnings.get_earnings_on_date.return_value = earnings_df
    sd.alert_tickers = {}
    return sd


def _make_cog(earnings_df: pd.DataFrame):
    bot = _make_bot()
    sd = _make_stock_data(earnings_df)
    with (
        patch.object(Alerts, "post_alerts_date"),
        patch.object(Alerts, "send_popularity_movers"),
        patch.object(Alerts, "send_alerts"),
    ):
        cog = Alerts(bot=bot, stock_data=sd)
    cog.dstate = MagicMock()
    return cog


class TestSendEarningsMovers:
    @pytest.mark.asyncio
    async def test_empty_dataframe_returns_early(self):
        """Should not raise KeyError when no earnings exist for today."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        # Must not raise — previously raised KeyError: 'ticker'
        await cog.send_earnings_movers(quotes={"AAPL": {"quote": {"netPercentChange": 10.0}}}, channels=[])

    @pytest.mark.asyncio
    async def test_no_matching_tickers_sends_no_alerts(self):
        """Tickers in quotes not in earnings_today produce no alerts."""
        df = pd.DataFrame({"ticker": ["MSFT"], "date": [datetime.date.today()]})
        cog = _make_cog(earnings_df=df)
        channel = AsyncMock()
        await cog.send_earnings_movers(quotes={"AAPL": {"quote": {"netPercentChange": 10.0}}}, channels=[channel])
        channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_matching_ticker_below_threshold_sends_no_alert(self):
        """A reporting ticker with < ±5% change should not trigger an alert."""
        df = pd.DataFrame({"ticker": ["AAPL"], "date": [datetime.date.today()]})
        cog = _make_cog(earnings_df=df)
        channel = AsyncMock()
        await cog.send_earnings_movers(
            quotes={"AAPL": {"quote": {"netPercentChange": 3.0}}}, channels=[channel]
        )
        channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_matching_ticker_above_threshold_sends_alert(self):
        """A reporting ticker with > ±5% change should send an alert."""
        df = pd.DataFrame({
            "ticker": ["AAPL"],
            "date": [datetime.date.today()],
            "eps_estimate": [1.5],
            "eps_actual": [1.8],
            "revenue_estimate": [None],
            "revenue_actual": [None],
        })
        cog = _make_cog(earnings_df=df)
        channel = AsyncMock()

        with (
            patch.object(cog, "build_earnings_mover", new_callable=AsyncMock) as mock_build,
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            mock_build.return_value = MagicMock()
            await cog.send_earnings_movers(
                quotes={"AAPL": {"quote": {"netPercentChange": 8.5}}}, channels=[channel]
            )
            mock_build.assert_called_once()
            mock_send.assert_called_once()
