"""Tests for rocketstocks.bot.cogs.schwab_auth."""
import datetime
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from rocketstocks.core.auth.token_manager import TokenInfo, TokenStatus


def _make_bot():
    bot = MagicMock(name="Bot")
    bot.stock_data = MagicMock(name="StockData")
    bot.stock_data.schwab = MagicMock(name="Schwab")
    bot.stock_data.schwab.token_path = "data/schwab-token.json"
    bot.stock_data.schwab._token_invalid = False
    return bot


def _make_cog(bot=None):
    if bot is None:
        bot = _make_bot()
    with patch("rocketstocks.bot.cogs.schwab_auth.secrets"):
        from rocketstocks.bot.cogs.schwab_auth import SchwabAuth
        cog = SchwabAuth(bot=bot)
    return cog, bot


def _make_interaction():
    interaction = AsyncMock(name="Interaction")
    interaction.response = AsyncMock()
    interaction.followup = AsyncMock()
    return interaction


def _make_token_info(status, hours_remaining=None):
    if hours_remaining is not None:
        remaining = datetime.timedelta(hours=hours_remaining)
        expires_at = datetime.datetime.now() + remaining
    else:
        remaining = None
        expires_at = None
    return TokenInfo(status=status, expires_at=expires_at, time_remaining=remaining)


# ---------------------------------------------------------------------------
# /schwab-status
# ---------------------------------------------------------------------------

class TestSchwabStatusCommand:
    @pytest.mark.asyncio
    async def test_healthy_token_shows_green(self):
        cog, bot = _make_cog()
        bot.stock_data.schwab.get_token_info.return_value = _make_token_info(
            TokenStatus.HEALTHY, hours_remaining=96
        )
        interaction = _make_interaction()
        await cog.schwab_status.callback(cog, interaction)
        interaction.response.send_message.assert_awaited_once()
        kwargs = interaction.response.send_message.call_args.kwargs
        assert kwargs.get("ephemeral") is True
        embed = kwargs["embed"]
        assert "Healthy" in embed.description

    @pytest.mark.asyncio
    async def test_expiring_soon_shows_hours_remaining(self):
        cog, bot = _make_cog()
        bot.stock_data.schwab.get_token_info.return_value = _make_token_info(
            TokenStatus.EXPIRING_SOON, hours_remaining=18
        )
        interaction = _make_interaction()
        await cog.schwab_status.callback(cog, interaction)
        embed = interaction.response.send_message.call_args.kwargs["embed"]
        assert "Expiring" in embed.description
        assert "18.0" in embed.description or "schwab auth" in embed.description.lower()

    @pytest.mark.asyncio
    async def test_expired_token_shows_reauth_prompt(self):
        cog, bot = _make_cog()
        bot.stock_data.schwab.get_token_info.return_value = _make_token_info(TokenStatus.EXPIRED)
        interaction = _make_interaction()
        await cog.schwab_status.callback(cog, interaction)
        embed = interaction.response.send_message.call_args.kwargs["embed"]
        assert "schwab auth" in embed.description.lower()

    @pytest.mark.asyncio
    async def test_invalid_token_shows_reauth_prompt(self):
        cog, bot = _make_cog()
        bot.stock_data.schwab.get_token_info.return_value = _make_token_info(TokenStatus.INVALID)
        interaction = _make_interaction()
        await cog.schwab_status.callback(cog, interaction)
        embed = interaction.response.send_message.call_args.kwargs["embed"]
        assert "schwab auth" in embed.description.lower()

    @pytest.mark.asyncio
    async def test_missing_token_shows_reauth_prompt(self):
        cog, bot = _make_cog()
        bot.stock_data.schwab.get_token_info.return_value = _make_token_info(TokenStatus.MISSING)
        interaction = _make_interaction()
        await cog.schwab_status.callback(cog, interaction)
        embed = interaction.response.send_message.call_args.kwargs["embed"]
        assert "schwab auth" in embed.description.lower()


# ---------------------------------------------------------------------------
# /schwab-auth
# ---------------------------------------------------------------------------

class TestSchwabAuthCommand:
    def _make_auth_context(self):
        ctx = MagicMock()
        ctx.authorization_url = "https://schwab.com/oauth/authorize?..."
        ctx.state = "random_state_value"
        return ctx

    @pytest.mark.asyncio
    async def test_rejects_when_flow_already_in_progress(self):
        cog, bot = _make_cog()
        cog._active_auth = self._make_auth_context()  # already active
        interaction = _make_interaction()
        await cog.schwab_auth.callback(cog, interaction)
        interaction.response.send_message.assert_awaited_once()
        call_args = interaction.response.send_message.call_args
        msg = call_args.args[0] if call_args.args else call_args.kwargs.get("content", "")
        assert "already" in msg.lower() or "progress" in msg.lower()

    @pytest.mark.asyncio
    async def test_sends_auth_link_and_button(self):
        cog, bot = _make_cog()
        auth_context = self._make_auth_context()
        interaction = _make_interaction()

        with patch("rocketstocks.bot.cogs.schwab_auth.schwab_pkg") as mock_schwab:
            mock_schwab.auth.get_auth_context.return_value = auth_context
            await cog.schwab_auth.callback(cog, interaction)

        assert cog._active_auth is auth_context
        interaction.response.send_message.assert_awaited_once()
        kwargs = interaction.response.send_message.call_args.kwargs
        assert kwargs.get("ephemeral") is True
        embed = kwargs["embed"]
        assert auth_context.authorization_url in embed.description
        assert kwargs.get("view") is not None

    @pytest.mark.asyncio
    async def test_handles_auth_context_error(self):
        cog, bot = _make_cog()
        interaction = _make_interaction()

        with patch("rocketstocks.bot.cogs.schwab_auth.schwab_pkg") as mock_schwab:
            mock_schwab.auth.get_auth_context.side_effect = RuntimeError("API error")
            await cog.schwab_auth.callback(cog, interaction)

        # _active_auth should not be set on failure
        assert cog._active_auth is None
        interaction.response.send_message.assert_awaited_once()


# ---------------------------------------------------------------------------
# SchwabCallbackModal
# ---------------------------------------------------------------------------

class TestSchwabCallbackModal:
    @pytest.mark.asyncio
    async def test_successful_exchange_reloads_client(self):
        cog, bot = _make_cog()
        auth_context = MagicMock()
        new_client = MagicMock(name="NewSchwabClient")
        cog._active_auth = auth_context

        with patch("rocketstocks.bot.cogs.schwab_auth.secrets"), \
             patch("rocketstocks.bot.cogs.schwab_auth.asyncio") as mock_asyncio, \
             patch("rocketstocks.bot.cogs.schwab_auth.exchange_code_for_token"):
            mock_asyncio.to_thread = AsyncMock(return_value=new_client)

            from rocketstocks.bot.cogs.schwab_auth import SchwabCallbackModal
            modal = SchwabCallbackModal(cog, auth_context)
            modal.redirect_url = MagicMock()
            modal.redirect_url.value = "https://127.0.0.1:8182/?code=abc&state=xyz"

            interaction = _make_interaction()
            await modal.on_submit(interaction)

        # Client should be updated and flow cleared
        assert bot.stock_data.schwab.client is new_client
        assert bot.stock_data.schwab._token_invalid is False
        assert cog._active_auth is None
        interaction.followup.send.assert_awaited_once()
        embed = interaction.followup.send.call_args.kwargs["embed"]
        assert "success" in embed.title.lower()

    @pytest.mark.asyncio
    async def test_failed_exchange_shows_error(self):
        cog, bot = _make_cog()
        auth_context = MagicMock()
        cog._active_auth = auth_context

        with patch("rocketstocks.bot.cogs.schwab_auth.secrets"), \
             patch("rocketstocks.bot.cogs.schwab_auth.asyncio") as mock_asyncio, \
             patch("rocketstocks.bot.cogs.schwab_auth.exchange_code_for_token"):
            mock_asyncio.to_thread = AsyncMock(side_effect=ValueError("invalid code"))

            from rocketstocks.bot.cogs.schwab_auth import SchwabCallbackModal
            modal = SchwabCallbackModal(cog, auth_context)
            modal.redirect_url = MagicMock()
            modal.redirect_url.value = "https://127.0.0.1:8182/?code=bad"

            interaction = _make_interaction()
            await modal.on_submit(interaction)

        assert cog._active_auth is None
        interaction.followup.send.assert_awaited_once()
        embed = interaction.followup.send.call_args.kwargs["embed"]
        assert "fail" in embed.title.lower()


# ---------------------------------------------------------------------------
# SchwabAuth.get_token_info delegation
# ---------------------------------------------------------------------------

class TestSchwabGetTokenInfo:
    def test_returns_invalid_when_flag_set(self):
        from rocketstocks.data.clients.schwab import Schwab
        with patch("rocketstocks.data.clients.schwab.schwab") as mock_s, \
             patch("rocketstocks.data.clients.schwab.secrets"):
            mock_s.auth.client_from_token_file.side_effect = FileNotFoundError
            schwab_client = Schwab()
        schwab_client._token_invalid = True
        info = schwab_client.get_token_info()
        assert info.status == TokenStatus.INVALID

    def test_delegates_to_token_manager_when_valid(self, tmp_path):
        import json, datetime
        from rocketstocks.core.auth.token_manager import REFRESH_TOKEN_LIFETIME
        from rocketstocks.data.clients.schwab import Schwab
        token_file = tmp_path / "token.json"
        # creation 2 days ago → refresh token expires in 5 days (HEALTHY)
        creation = datetime.datetime.now() - datetime.timedelta(days=2)
        token_file.write_text(json.dumps({"creation_timestamp": creation.timestamp(), "token": {}}))

        with patch("rocketstocks.data.clients.schwab.schwab") as mock_s, \
             patch("rocketstocks.data.clients.schwab.secrets"):
            mock_s.auth.client_from_token_file.side_effect = FileNotFoundError
            schwab_client = Schwab(token_path=str(token_file))
        schwab_client._token_invalid = False
        info = schwab_client.get_token_info()
        assert info.status == TokenStatus.HEALTHY
