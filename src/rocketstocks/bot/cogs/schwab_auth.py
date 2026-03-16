"""Schwab OAuth re-authentication cog.

Provides two slash commands:
  /schwab-status  — show current token health (admin-only)
  /schwab-auth    — start the browser-based OAuth flow (admin-only)

The flow is entirely Discord-native — no callback server, no exposed ports:
  1. Bot generates Schwab authorization URL and sends it ephemerally.
  2. User logs in on Schwab's website; browser gets redirected to
     https://127.0.0.1:8182/?code=...&state=... (shows "can't connect").
  3. User copies the full redirect URL and clicks "Paste URL" in Discord.
  4. A Modal pops up; user pastes the URL.
  5. Bot exchanges the code for tokens, saves them, and reloads the client.
"""
import asyncio
import logging
import datetime

import schwab as schwab_pkg
import discord
from discord import app_commands
from discord.ext import commands

from rocketstocks.core.auth.token_manager import TokenStatus
from rocketstocks.core.config.settings import settings
from rocketstocks.data.auth.token_exchange import exchange_code_for_token

logger = logging.getLogger(__name__)

_CALLBACK_URL = "https://127.0.0.1:8182"
_AUTH_TIMEOUT_SECONDS = 300  # 5 minutes


# ---------------------------------------------------------------------------
# Status colours
# ---------------------------------------------------------------------------

_STATUS_COLORS = {
    TokenStatus.HEALTHY: discord.Color.green(),
    TokenStatus.EXPIRING_SOON: discord.Color.orange(),
    TokenStatus.EXPIRED: discord.Color.red(),
    TokenStatus.INVALID: discord.Color.red(),
    TokenStatus.MISSING: discord.Color.dark_gray(),
}

_STATUS_LABELS = {
    TokenStatus.HEALTHY: "Healthy",
    TokenStatus.EXPIRING_SOON: "Expiring Soon",
    TokenStatus.EXPIRED: "Expired",
    TokenStatus.INVALID: "Invalid (revoked by Schwab)",
    TokenStatus.MISSING: "Missing",
}


# ---------------------------------------------------------------------------
# Modal — URL paste
# ---------------------------------------------------------------------------

class SchwabCallbackModal(discord.ui.Modal, title="Paste Schwab Redirect URL"):
    """Modal that collects the full redirect URL after the user authenticates."""

    redirect_url: discord.ui.TextInput = discord.ui.TextInput(
        label="Redirect URL",
        placeholder="https://127.0.0.1:8182/?code=...&state=...",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=2000,
    )

    def __init__(self, cog: "SchwabAuth", auth_context):
        super().__init__()
        self._cog = cog
        self._auth_context = auth_context

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        received_url = self.redirect_url.value.strip()

        try:
            new_client, token_dict = await asyncio.to_thread(
                exchange_code_for_token,
                settings.schwab_api_key,
                settings.schwab_api_secret,
                self._auth_context,
                received_url,
                True,  # asyncio=True
            )
            await self._cog.bot.stock_data.schwab_token_store.save_token(token_dict)
            # Inject new client and clear invalid flag
            self._cog.bot.stock_data.schwab.client = new_client
            self._cog.bot.stock_data.schwab._token_invalid = False
            self._cog._active_auth = None
            logger.info("Schwab re-authentication successful via Discord OAuth flow")
            await interaction.followup.send(
                embed=discord.Embed(
                    title="Schwab Authentication Successful",
                    description="Token saved and Schwab client reloaded.",
                    color=discord.Color.green(),
                ),
                ephemeral=True,
            )
        except Exception as exc:
            self._cog._active_auth = None
            logger.error(f"Schwab OAuth exchange failed: {exc}")
            await interaction.followup.send(
                embed=discord.Embed(
                    title="Authentication Failed",
                    description=(
                        f"Could not exchange the redirect URL for a token.\n\n"
                        f"**Error:** {exc}\n\n"
                        "Make sure you pasted the full URL from the browser address bar "
                        "and that the link was clicked within ~60 seconds."
                    ),
                    color=discord.Color.red(),
                ),
                ephemeral=True,
            )

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        self._cog._active_auth = None
        logger.error(f"SchwabCallbackModal error: {error}")
        await interaction.response.send_message(
            "An unexpected error occurred. Please try `/schwab auth` again.",
            ephemeral=True,
        )


# ---------------------------------------------------------------------------
# Button — triggers the modal
# ---------------------------------------------------------------------------

class PasteURLView(discord.ui.View):
    """Ephemeral view with a single button that opens the URL-paste modal."""

    def __init__(self, cog: "SchwabAuth", auth_context):
        super().__init__(timeout=_AUTH_TIMEOUT_SECONDS)
        self._cog = cog
        self._auth_context = auth_context

    @discord.ui.button(label="Paste URL", style=discord.ButtonStyle.primary)
    async def paste_url(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = SchwabCallbackModal(self._cog, self._auth_context)
        await interaction.response.send_modal(modal)

    async def on_timeout(self):
        self._cog._active_auth = None
        logger.info("Schwab auth flow timed out (no URL pasted within 5 minutes)")


# ---------------------------------------------------------------------------
# Cog
# ---------------------------------------------------------------------------

class SchwabAuth(commands.Cog):
    """Schwab OAuth token management commands."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._active_auth = None  # stores the current AuthContext while flow is in progress

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"{__name__} loaded")

    schwab_group = app_commands.Group(
        name="schwab",
        description="Manage Schwab API connection and authentication",
        default_permissions=discord.Permissions(administrator=True),
    )

    # ------------------------------------------------------------------
    # /schwab status
    # ------------------------------------------------------------------

    @schwab_group.command(name="status", description="Show Schwab API token status")
    async def schwab_status(self, interaction: discord.Interaction):
        """Display colour-coded token health."""
        info = await self.bot.stock_data.schwab.get_token_info()
        label = _STATUS_LABELS.get(info.status, info.status.value)
        color = _STATUS_COLORS.get(info.status, discord.Color.greyple())

        lines = [f"**Status:** {label}"]

        if info.expires_at is not None:
            lines.append(f"**Expires at:** {info.expires_at.strftime('%Y-%m-%d %H:%M:%S')}")

        if info.time_remaining is not None and info.time_remaining > datetime.timedelta(0):
            total_hours = info.time_remaining.total_seconds() / 3600
            lines.append(f"**Time remaining:** {total_hours:.1f} hours")

        if info.status in (TokenStatus.EXPIRING_SOON, TokenStatus.EXPIRED, TokenStatus.INVALID, TokenStatus.MISSING):
            lines.append("\nRun `/schwab auth` to re-authenticate.")

        embed = discord.Embed(
            title="Schwab Token Status",
            description="\n".join(lines),
            color=color,
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    # ------------------------------------------------------------------
    # /schwab auth
    # ------------------------------------------------------------------

    @schwab_group.command(name="auth", description="Re-authenticate with Schwab via OAuth")
    async def schwab_auth(self, interaction: discord.Interaction):
        """Start the browser-based Schwab OAuth flow."""
        if self._active_auth is not None:
            await interaction.response.send_message(
                "An authentication flow is already in progress. "
                "Complete it or wait 5 minutes for it to expire.",
                ephemeral=True,
            )
            return

        try:
            auth_context = schwab_pkg.auth.get_auth_context(
                api_key=settings.schwab_api_key,
                callback_url=_CALLBACK_URL,
            )
        except Exception as exc:
            logger.error(f"Failed to generate Schwab auth context: {exc}")
            await interaction.response.send_message(
                f"Failed to generate Schwab authorization URL: {exc}",
                ephemeral=True,
            )
            return

        self._active_auth = auth_context

        description = (
            "**Step 1:** Click the link below to log in to Schwab in your browser.\n"
            f"[Schwab Login Link]({auth_context.authorization_url})\n\n"
            "**Step 2:** After logging in, your browser will show a "
            "\"can't connect\" error — this is expected.\n\n"
            "**Step 3:** Copy the full URL from your browser address bar "
            "(it starts with `https://127.0.0.1:8182/?code=...`) and click **Paste URL** below.\n\n"
            f"*This flow expires in 5 minutes.*"
        )
        embed = discord.Embed(
            title="Schwab Re-Authentication",
            description=description,
            color=discord.Color.blurple(),
        )
        view = PasteURLView(self, auth_context)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(SchwabAuth(bot))
