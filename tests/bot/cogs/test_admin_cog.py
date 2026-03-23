"""Tests for rocketstocks.bot.cogs.admin."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from rocketstocks.bot.cogs.admin import Admin, _build_dummy_alert, _build_dummy_screener, _build_dummy_report


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cog() -> Admin:
    bot = MagicMock(name="Bot")
    sd = MagicMock(name="StockData")
    sd.tickers = MagicMock()
    sd.tickers.get_all_tickers = AsyncMock()
    sd.price_history = MagicMock()
    sd.price_history.update_5m_price_history = AsyncMock()
    sd.price_history.update_daily_price_history = AsyncMock()
    return Admin(bot=bot, stock_data=sd)


def _make_interaction() -> MagicMock:
    interaction = MagicMock(name="Interaction")
    interaction.response = MagicMock()
    interaction.response.defer = AsyncMock()
    interaction.response.send_message = AsyncMock()
    interaction.followup = MagicMock()
    interaction.followup.send = AsyncMock()
    interaction.user = MagicMock()
    interaction.user.name = "AdminUser"
    return interaction


def _make_choice(value: str) -> MagicMock:
    choice = MagicMock()
    choice.value = value
    return choice


# ---------------------------------------------------------------------------
# /admin update-5m
# ---------------------------------------------------------------------------

class TestAdminUpdate5m:
    @pytest.mark.asyncio
    async def test_calls_update_5m_price_history(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.get_all_tickers.return_value = ["AAPL", "MSFT"]  # AsyncMock return_value

        await cog.admin_update_5m.callback(cog, interaction)

        cog.stock_data.price_history.update_5m_price_history.assert_called_once()
        interaction.followup.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_defers_interaction(self):
        cog = _make_cog()
        interaction = _make_interaction()

        await cog.admin_update_5m.callback(cog, interaction)

        interaction.response.defer.assert_called_once()


# ---------------------------------------------------------------------------
# /admin update-daily
# ---------------------------------------------------------------------------

class TestAdminUpdateDaily:
    @pytest.mark.asyncio
    async def test_calls_update_daily_price_history(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.get_all_tickers.return_value = ["AAPL"]

        await cog.admin_update_daily.callback(cog, interaction)

        cog.stock_data.price_history.update_daily_price_history.assert_called_once()
        interaction.followup.send.assert_called_once()


# ---------------------------------------------------------------------------
# /admin test-alert
# ---------------------------------------------------------------------------

class TestAdminTestAlert:
    @pytest.mark.asyncio
    async def test_sends_embed_for_valid_alert_type(self):
        cog = _make_cog()
        interaction = _make_interaction()
        choice = _make_choice("earnings_mover")

        with patch("rocketstocks.bot.cogs.admin.spec_to_embed", return_value=MagicMock()) as mock_embed:
            await cog.admin_test_alert.callback(cog, interaction, alert_type=choice)

        mock_embed.assert_called_once()
        interaction.followup.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_sends_error_for_invalid_alert_type(self):
        cog = _make_cog()
        interaction = _make_interaction()
        choice = _make_choice("invalid_type")

        await cog.admin_test_alert.callback(cog, interaction, alert_type=choice)

        sent = interaction.followup.send.call_args
        assert sent is not None

    @pytest.mark.parametrize("alert_type", [
        "earnings_mover", "watchlist_mover", "popularity_surge",
        "momentum_confirmation", "market_alert",
    ])
    def test_build_dummy_alert_all_types(self, alert_type):
        alert = _build_dummy_alert(alert_type)
        assert alert is not None
        spec = alert.build()
        assert spec is not None


# ---------------------------------------------------------------------------
# /admin test-screener
# ---------------------------------------------------------------------------

class TestAdminTestScreener:
    @pytest.mark.asyncio
    async def test_sends_embed_for_valid_screener_type(self):
        cog = _make_cog()
        interaction = _make_interaction()
        choice = _make_choice("gainers")

        with patch("rocketstocks.bot.cogs.admin.spec_to_embed", return_value=MagicMock()) as mock_embed:
            await cog.admin_test_screener.callback(cog, interaction, screener=choice)

        mock_embed.assert_called_once()
        interaction.followup.send.assert_called_once()

    @pytest.mark.parametrize("screener_type", ["gainers", "volume", "popularity", "earnings"])
    def test_build_dummy_screener_all_types(self, screener_type):
        screener = _build_dummy_screener(screener_type)
        assert screener is not None
        spec = screener.build()
        assert spec is not None


# ---------------------------------------------------------------------------
# /admin test-report
# ---------------------------------------------------------------------------

class TestAdminTestReport:
    @pytest.mark.asyncio
    async def test_sends_embed_for_valid_report_type(self):
        cog = _make_cog()
        interaction = _make_interaction()
        choice = _make_choice("stock")

        with patch("rocketstocks.bot.cogs.admin.spec_to_embed", return_value=MagicMock()) as mock_embed:
            await cog.admin_test_report.callback(cog, interaction, report=choice)

        mock_embed.assert_called_once()
        interaction.followup.send.assert_called_once()

    @pytest.mark.parametrize("report_type", ["stock", "earnings", "news", "popularity", "politician"])
    def test_build_dummy_report_all_types(self, report_type):
        report = _build_dummy_report(report_type)
        assert report is not None
        spec = report.build()
        assert spec is not None


# ---------------------------------------------------------------------------
# TestAdminPermissionGate
# ---------------------------------------------------------------------------

from discord import app_commands as _app_commands


class TestAdminPermissionGate:
    @pytest.mark.asyncio
    async def test_non_admin_gets_ephemeral_error(self):
        """cog_app_command_error sends ephemeral message for MissingPermissions."""
        cog = _make_cog()
        interaction = _make_interaction()
        error = _app_commands.errors.MissingPermissions(["administrator"])

        await cog.cog_app_command_error(interaction, error)

        interaction.response.send_message.assert_called_once()
        call_kwargs = interaction.response.send_message.call_args
        assert call_kwargs.kwargs.get("ephemeral") is True
        assert "Administrator" in call_kwargs.args[0]

    @pytest.mark.asyncio
    async def test_other_errors_are_reraised(self):
        """cog_app_command_error re-raises non-permission errors."""
        cog = _make_cog()
        interaction = _make_interaction()
        error = _app_commands.errors.CommandInvokeError(MagicMock(), Exception("unexpected"))

        with pytest.raises(_app_commands.errors.CommandInvokeError):
            await cog.cog_app_command_error(interaction, error)
