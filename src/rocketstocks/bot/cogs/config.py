"""Config cog — per-guild channel setup and status commands."""
import logging
import re
import discord
from discord import app_commands
from discord.ext import commands

from rocketstocks.data.channel_config import (
    REPORTS, ALERTS, SCREENERS, CHARTS, NOTIFICATIONS, ALL_CONFIG_TYPES,
)
from rocketstocks.bot.views.subscription_views import ALERT_ROLE_DEFS, SubscriptionEntryView

logger = logging.getLogger(__name__)

# Colour assigned to each alert role (matches alert embed colours roughly)
_ROLE_COLOURS: dict[str, discord.Colour] = {
    "earnings_mover": discord.Colour.red(),
    "watchlist_mover": discord.Colour.blue(),
    "popularity_surge": discord.Colour.purple(),
    "momentum_confirmed": discord.Colour.gold(),
    "market_mover_sustained": discord.Colour.teal(),
    "market_mover_price_accelerating": discord.Colour.green(),
    "market_mover_volume_accelerating": discord.Colour.dark_green(),
    "market_mover_volume_extreme": discord.Colour.dark_blue(),
    "all_alerts": discord.Colour.orange(),
}


async def setup_alert_subscriptions(bot: commands.Bot, guild: discord.Guild) -> None:
    """Create Discord roles for each alert type and post the subscription panel.

    Called after /server setup completes for a guild. Idempotent — reuses existing roles.
    """
    alert_roles_repo = bot.stock_data.alert_roles

    for role_key, label in ALERT_ROLE_DEFS:
        # Find existing role by name or create it
        existing = discord.utils.get(guild.roles, name=label)
        if existing is None:
            colour = _ROLE_COLOURS.get(role_key, discord.Colour.default())
            try:
                existing = await guild.create_role(
                    name=label,
                    colour=colour,
                    mentionable=True,
                    reason="RocketStocks alert subscription role",
                )
                logger.info(f"Created role '{label}' (id={existing.id}) in guild={guild.id}")
            except discord.Forbidden:
                logger.warning(f"Missing permissions to create role '{label}' in guild={guild.id}")
                continue

        await alert_roles_repo.upsert(guild.id, role_key, existing.id)

    # Post the subscription panel to the configured ALERTS channel
    alerts_channel_id = await bot.stock_data.channel_config.get_channel_id(guild.id, ALERTS)
    if alerts_channel_id is None:
        logger.warning(f"No ALERTS channel configured for guild={guild.id} — skipping panel post")
        return

    channel = bot.get_channel(alerts_channel_id)
    if channel is None:
        logger.warning(f"ALERTS channel {alerts_channel_id} not in cache for guild={guild.id}")
        return

    embed = discord.Embed(
        title="Alert Subscriptions",
        description=(
            "Get notified when specific alerts fire.\n\n"
            "Click **Manage My Subscriptions** to choose which alerts you want to receive."
        ),
        colour=discord.Colour.blurple(),
    )
    try:
        await channel.send(embed=embed, view=SubscriptionEntryView())
        logger.info(f"Posted subscription panel to channel={alerts_channel_id} guild={guild.id}")
    except discord.Forbidden:
        logger.warning(f"Missing permissions to post to alerts channel in guild={guild.id}")


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
            await repo.upsert_channel(interaction.guild_id, config_type, channel.id)
            lines.append(f"**{config_type}**: {channel.mention}")
            logger.info(f"/server setup modal: guild={interaction.guild_id} {config_type}={channel.id}")
        embed = discord.Embed(
            title="Channel Configuration Updated",
            description="\n".join(lines),
            color=discord.Color.green(),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

        await setup_alert_subscriptions(self._cog.bot, guild)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        logger.error(f"SetupModal error: {error}")
        await interaction.followup.send(
            "An unexpected error occurred during setup. Please try `/server setup` again.",
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
        unconfigured = await repo.get_unconfigured_guilds(guild_ids)
        for guild_id in unconfigured:
            guild = self.bot.get_guild(guild_id)
            if guild:
                target = await self._discover_fallback_channel(guild)
                if target:
                    await self._send_setup_prompt(target, guild)

    @commands.Cog.listener()
    async def on_guild_join(self, guild: discord.Guild):
        """Immediately prompt a newly joined guild to run /server setup."""
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
                "You can also run `/server setup` at any time to update the configuration."
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

    server_group = app_commands.Group(
        name="server",
        description="Configure RocketStocks for this server",
        default_permissions=discord.Permissions(administrator=True),
    )

    @server_group.command(name="setup", description="Configure RocketStocks channels for this server")
    async def server_setup(self, interaction: discord.Interaction):
        """Open the channel configuration modal."""
        await interaction.response.send_modal(SetupModal(self))

    @server_group.command(name="status", description="Show current channel and subscription role configuration")
    async def server_status(self, interaction: discord.Interaction):
        """Display all 5 channel types and all 9 alert subscription roles."""
        repo = self.bot.stock_data.channel_config
        configured = await repo.get_all_for_guild(interaction.guild_id)

        # Channel configuration section
        channel_lines = []
        channels_all_set = True
        for config_type in ALL_CONFIG_TYPES:
            channel_id = configured.get(config_type)
            if channel_id:
                channel = self.bot.get_channel(channel_id)
                mention = channel.mention if channel else f"<#{channel_id}>"
                channel_lines.append(f"**{config_type}**: {mention}")
            else:
                channel_lines.append(f"**{config_type}**: Not configured")
                channels_all_set = False

        # Alert subscription roles section
        guild_roles = await self.bot.stock_data.alert_roles.get_all_for_guild(interaction.guild_id)
        role_lines = []
        roles_all_set = True
        for role_key, label in ALERT_ROLE_DEFS:
            role_id = guild_roles.get(role_key)
            if role_id:
                role = interaction.guild.get_role(role_id)
                mention = role.mention if role else f"<@&{role_id}>"
                role_lines.append(f"✅ {label}: {mention}")
            else:
                role_lines.append(f"❌ {label}: Not configured")
                roles_all_set = False

        all_set = channels_all_set and roles_all_set
        color = discord.Color.green() if all_set else discord.Color.orange()

        description = (
            "**Channel Configuration**\n"
            + "\n".join(channel_lines)
            + "\n\n**Alert Subscription Roles**\n"
            + "\n".join(role_lines)
        )

        embed = discord.Embed(
            title="Server Configuration",
            description=description,
            color=color,
        )
        if not all_set:
            embed.set_footer(text="Run /server setup to configure automatically.")
        await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(Config(bot))
