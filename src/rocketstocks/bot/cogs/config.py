"""Config cog — per-guild channel setup and status commands."""
import logging
import re
import discord
from discord import app_commands
from discord.ext import commands

from rocketstocks.data.channel_config import (
    REPORTS, ALERTS, SCREENERS, CHARTS, NOTIFICATIONS, ALL_CONFIG_TYPES,
)

logger = logging.getLogger(__name__)

_PRIORITY_CHANNEL_NAMES = ["bot-commands", "bot", "general", "welcome"]


def _parse_channel(value: str, guild: discord.Guild) -> discord.TextChannel | None:
    """Resolve a user-supplied string to a TextChannel in *guild*.

    Accepts:
    - ``<#CHANNEL_ID>`` mention format
    - Raw numeric channel ID
    - Channel name (with or without a leading ``#``)
    """
    value = value.strip()
    if not value:
        return None
    # <#CHANNEL_ID> mention format
    m = re.match(r"^<#(\d+)>$", value)
    if m:
        ch = guild.get_channel(int(m.group(1)))
        return ch if isinstance(ch, discord.TextChannel) else None
    # Raw numeric ID
    if value.isdigit():
        ch = guild.get_channel(int(value))
        return ch if isinstance(ch, discord.TextChannel) else None
    # Channel name (strip leading #)
    name = value.lstrip("#")
    return discord.utils.get(guild.text_channels, name=name)


class SetupModal(discord.ui.Modal, title="Configure RocketStocks Channels"):
    """Modal form for configuring the five channel types."""

    reports = discord.ui.TextInput(
        label="Reports Channel",
        placeholder="<#ID>, channel ID, or name",
        required=False,
        max_length=100,
    )
    alerts = discord.ui.TextInput(
        label="Alerts Channel",
        placeholder="<#ID>, channel ID, or name",
        required=False,
        max_length=100,
    )
    screeners = discord.ui.TextInput(
        label="Screeners Channel",
        placeholder="<#ID>, channel ID, or name",
        required=False,
        max_length=100,
    )
    charts = discord.ui.TextInput(
        label="Charts Channel",
        placeholder="<#ID>, channel ID, or name",
        required=False,
        max_length=100,
    )
    notifications = discord.ui.TextInput(
        label="Notifications Channel",
        placeholder="<#ID>, channel ID, or name",
        required=False,
        max_length=100,
    )

    def __init__(self, cog: "Config"):
        super().__init__()
        self._cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        raw = {
            REPORTS: self.reports.value,
            ALERTS: self.alerts.value,
            SCREENERS: self.screeners.value,
            CHARTS: self.charts.value,
            NOTIFICATIONS: self.notifications.value,
        }
        resolved = {k: _parse_channel(v, guild) for k, v in raw.items()}
        resolved = {k: ch for k, ch in resolved.items() if ch is not None}
        if not resolved:
            await interaction.followup.send(
                "No valid channels were provided. Use a channel mention, numeric ID, or channel name.",
                ephemeral=True,
            )
            return
        repo = self._cog.bot.stock_data.channel_config
        lines = []
        for config_type, channel in resolved.items():
            repo.upsert_channel(interaction.guild_id, config_type, channel.id)
            lines.append(f"**{config_type}**: {channel.mention}")
            logger.info(f"/setup modal: guild={interaction.guild_id} {config_type}={channel.id}")
        embed = discord.Embed(
            title="Channel Configuration Updated",
            description="\n".join(lines),
            color=discord.Color.green(),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        logger.error(f"SetupModal error: {error}")
        await interaction.followup.send(
            "An unexpected error occurred during setup. Please try `/setup` again.",
            ephemeral=True,
        )


class SetupView(discord.ui.View):
    """Persistent view attached to the setup prompt embed."""

    def __init__(self, cog: "Config"):
        super().__init__(timeout=None)
        self._cog = cog

    @discord.ui.button(label="Configure Channels", style=discord.ButtonStyle.primary)
    async def configure_channels(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(SetupModal(self._cog))


class Config(commands.Cog):
    """Manage per-guild channel configuration for RocketStocks."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"{__name__} loaded")
        # Prompt any guild that isn't fully configured yet
        repo = self.bot.stock_data.channel_config
        guild_ids = [g.id for g in self.bot.guilds]
        unconfigured = repo.get_unconfigured_guilds(guild_ids)
        for guild_id in unconfigured:
            guild = self.bot.get_guild(guild_id)
            if guild:
                target = await self._discover_fallback_channel(guild)
                if target:
                    await self._send_setup_prompt(target, guild)

    @commands.Cog.listener()
    async def on_guild_join(self, guild: discord.Guild):
        """Immediately prompt a newly joined guild to run /setup."""
        target = await self._discover_fallback_channel(guild)
        if target:
            await self._send_setup_prompt(target, guild)

    async def _discover_fallback_channel(self, guild: discord.Guild):
        """Find the best channel to post setup instructions to.

        Priority order:
        1. A text channel matching a known bot channel name
        2. First writable text channel
        3. DM to guild owner
        """
        writable = [
            ch for ch in guild.text_channels
            if ch.permissions_for(guild.me).send_messages
        ]
        for name in _PRIORITY_CHANNEL_NAMES:
            for ch in writable:
                if ch.name == name:
                    return ch
        if writable:
            return writable[0]
        # Last resort: DM the guild owner
        try:
            return await guild.owner.create_dm()
        except Exception:
            logger.warning(f"Could not DM owner of guild {guild.id}")
            return None

    async def _send_setup_prompt(self, target, guild: discord.Guild):
        """Send an orange setup-instructions embed with a Configure Channels button."""
        embed = discord.Embed(
            title="RocketStocks Setup Required",
            description=(
                f"Thanks for adding RocketStocks to **{guild.name}**!\n\n"
                "Click **Configure Channels** below to set up which channels receive "
                "reports, alerts, screeners, charts, and notifications.\n\n"
                "You can also run `/setup` at any time to update the configuration."
            ),
            color=discord.Color.orange(),
        )
        view = SetupView(self)
        try:
            await target.send(embed=embed, view=view)
        except Exception as exc:
            logger.warning(f"Could not send setup prompt to {target}: {exc}")

    # -------------------------------------------------------------------------
    # Slash commands
    # -------------------------------------------------------------------------

    @app_commands.command(name="setup", description="Configure RocketStocks channels for this server")
    @app_commands.default_permissions(administrator=True)
    async def setup(self, interaction: discord.Interaction):
        """Open the channel configuration modal."""
        await interaction.response.send_modal(SetupModal(self))

    @app_commands.command(name="setup-status", description="Show current channel configuration for this server")
    @app_commands.default_permissions(administrator=True)
    async def setup_status(self, interaction: discord.Interaction):
        """Display all 5 channel types and their current mentions or 'Not configured'."""
        repo = self.bot.stock_data.channel_config
        configured = repo.get_all_for_guild(interaction.guild_id)

        lines = []
        all_set = True
        for config_type in ALL_CONFIG_TYPES:
            channel_id = configured.get(config_type)
            if channel_id:
                channel = self.bot.get_channel(channel_id)
                mention = channel.mention if channel else f"<#{channel_id}>"
                lines.append(f"**{config_type}**: {mention}")
            else:
                lines.append(f"**{config_type}**: Not configured")
                all_set = False

        color = discord.Color.green() if all_set else discord.Color.orange()
        embed = discord.Embed(
            title="Channel Configuration Status",
            description="\n".join(lines),
            color=color,
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(Config(bot))
