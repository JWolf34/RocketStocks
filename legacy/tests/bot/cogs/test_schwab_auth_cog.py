"""Tests for rocketstocks.bot.cogs.schwab_auth."""
import datetime
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from rocketstocks.core.auth.token_manager import TokenInfo, TokenStatus


def _make_bot():
    bot = MagicMock(name="Bot")
    bot.stock_data = MagicMock(name="StockData")
    bot.stock_data.schwab = MagicMock(name="Schwab")
    bot.stock_data.schwab._token_invalid = False
    bot.stock_data.schwab.reload_client = AsyncMock()
    bot.stock_data.schwab_token_store = AsyncMock(name="SchwabTokenRepository")
    bot.emitter = MagicMock(name="EventEmitter")
    return bot


def _make_cog(bot=None):
    if bot is None:
        bot = _make_bot()
    with patch("rocketstocks.bot.cogs.schwab_auth.settings"):
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
        bot.stock_data.schwab.get_token_info = AsyncMock(
            return_value=_make_token_info(TokenStatus.HEALTHY, hours_remaining=96)
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
        bot.stock_data.schwab.get_token_info = AsyncMock(
            return_value=_make_token_info(TokenStatus.EXPIRING_SOON, hours_remaining=18)
        )
        interaction = _make_interaction()
        await cog.schwab_status.callback(cog, interaction)
        embed = interaction.response.send_message.call_args.kwargs["embed"]
        assert "Expiring" in embed.description
        assert "18.0" in embed.description or "schwab auth" in embed.description.lower()

    @pytest.mark.asyncio
    async def test_expired_token_shows_reauth_prompt(self):
        cog, bot = _make_cog()
        bot.stock_data.schwab.get_token_info = AsyncMock(
            return_value=_make_token_info(TokenStatus.EXPIRED)
        )
        interaction = _make_interaction()
        await cog.schwab_status.callback(cog, interaction)
        embed = interaction.response.send_message.call_args.kwargs["embed"]
        assert "schwab auth" in embed.description.lower()

    @pytest.mark.asyncio
    async def test_invalid_token_shows_reauth_prompt(self):
        cog, bot = _make_cog()
        bot.stock_data.schwab.get_token_info = AsyncMock(
            return_value=_make_token_info(TokenStatus.INVALID)
        )
        interaction = _make_interaction()
        await cog.schwab_status.callback(cog, interaction)
        embed = interaction.response.send_message.call_args.kwargs["embed"]
        assert "schwab auth" in embed.description.lower()

    @pytest.mark.asyncio
    async def test_missing_token_shows_reauth_prompt(self):
        cog, bot = _make_cog()
        bot.stock_data.schwab.get_token_info = AsyncMock(
            return_value=_make_token_info(TokenStatus.MISSING)
        )
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
        token_dict = {"creation_timestamp": 1234567890, "token": {}}
        cog._active_auth = auth_context

        with patch("rocketstocks.bot.cogs.schwab_auth.settings"), \
             patch("rocketstocks.bot.cogs.schwab_auth.asyncio") as mock_asyncio, \
             patch("rocketstocks.bot.cogs.schwab_auth.exchange_code_for_token"):
            mock_asyncio.to_thread = AsyncMock(return_value=(new_client, token_dict))

            from rocketstocks.bot.cogs.schwab_auth import SchwabCallbackModal
            modal = SchwabCallbackModal(cog, auth_context)
            modal.redirect_url = MagicMock()
            modal.redirect_url.value = "https://127.0.0.1:8182/?code=abc&state=xyz"

            interaction = _make_interaction()
            await modal.on_submit(interaction)

        # Token should be saved to DB, then client reloaded (not directly assigned)
        bot.stock_data.schwab_token_store.save_token.assert_awaited_once_with(token_dict)
        bot.stock_data.schwab.reload_client.assert_awaited_once()
        assert cog._active_auth is None
        interaction.followup.send.assert_awaited_once()
        embed = interaction.followup.send.call_args.kwargs["embed"]
        assert "success" in embed.title.lower()

    @pytest.mark.asyncio
    async def test_failed_exchange_shows_error(self):
        cog, bot = _make_cog()
        auth_context = MagicMock()
        cog._active_auth = auth_context

        with patch("rocketstocks.bot.cogs.schwab_auth.settings"), \
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
# cog_load → _check_token_on_startup
# ---------------------------------------------------------------------------

class TestCogLoad:
    @pytest.mark.asyncio
    async def test_cog_load_calls_startup_check(self):
        cog, bot = _make_cog()
        bot.stock_data.schwab.get_token_info = AsyncMock(
            return_value=_make_token_info(TokenStatus.HEALTHY, hours_remaining=96)
        )
        await cog.cog_load()
        bot.stock_data.schwab.get_token_info.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_startup_emits_warning_for_expiring_soon(self):
        from rocketstocks.core.notifications.config import NotificationLevel
        cog, bot = _make_cog()
        bot.stock_data.schwab.get_token_info = AsyncMock(
            return_value=_make_token_info(TokenStatus.EXPIRING_SOON, hours_remaining=20)
        )
        await cog._check_token_on_startup()
        bot.emitter.emit.assert_called_once()
        event = bot.emitter.emit.call_args.args[0]
        assert event.level == NotificationLevel.WARNING

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [TokenStatus.EXPIRED, TokenStatus.INVALID, TokenStatus.MISSING])
    async def test_startup_emits_failure_for_critical_statuses(self, status):
        from rocketstocks.core.notifications.config import NotificationLevel
        cog, bot = _make_cog()
        bot.stock_data.schwab.get_token_info = AsyncMock(
            return_value=_make_token_info(status)
        )
        await cog._check_token_on_startup()
        bot.emitter.emit.assert_called_once()
        event = bot.emitter.emit.call_args.args[0]
        assert event.level == NotificationLevel.FAILURE

    @pytest.mark.asyncio
    async def test_startup_does_not_emit_for_healthy_token(self):
        cog, bot = _make_cog()
        bot.stock_data.schwab.get_token_info = AsyncMock(
            return_value=_make_token_info(TokenStatus.HEALTHY, hours_remaining=96)
        )
        await cog._check_token_on_startup()
        bot.emitter.emit.assert_not_called()


# ---------------------------------------------------------------------------
# SchwabAuth.get_token_info delegation
# ---------------------------------------------------------------------------

class TestSchwabGetTokenInfo:
    @pytest.mark.asyncio
    async def test_returns_invalid_when_flag_set(self):
        from rocketstocks.data.clients.schwab import Schwab
        token_store = AsyncMock()
        schwab_client = Schwab(token_store=token_store)
        schwab_client._token_invalid = True
        info = await schwab_client.get_token_info()
        assert info.status == TokenStatus.INVALID

    @pytest.mark.asyncio
    async def test_delegates_to_token_manager_when_valid(self):
        import datetime
        from rocketstocks.core.auth.token_manager import REFRESH_TOKEN_LIFETIME
        from rocketstocks.data.clients.schwab import Schwab
        # creation 2 days ago → refresh token expires in 5 days (HEALTHY)
        creation = datetime.datetime.now() - datetime.timedelta(days=2)
        token_dict = {"creation_timestamp": creation.timestamp(), "token": {}}

        token_store = AsyncMock()
        token_store.load_token.return_value = token_dict
        schwab_client = Schwab(token_store=token_store)
        schwab_client._token_invalid = False
        info = await schwab_client.get_token_info()
        assert info.status == TokenStatus.HEALTHY
