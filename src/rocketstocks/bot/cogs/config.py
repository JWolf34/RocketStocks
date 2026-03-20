"""Config cog — per-guild channel setup and status commands."""
import logging
import os
import discord
from discord import app_commands
from discord.ext import commands

from rocketstocks.core.config.settings import settings
from rocketstocks.core.utils.dates import configure_tz
from rocketstocks.core.notifications.config import NotificationFilter
from rocketstocks.data.channel_config import (
    REPORTS, ALERTS, SCREENERS, CHARTS, NOTIFICATIONS, ALL_CONFIG_TYPES,
)
from rocketstocks.bot.views.subscription_views import ALERT_ROLE_DEFS, SubscriptionEntryView

logger = logging.getLogger(__name__)

_CHANNEL_TYPES = [
    (REPORTS, "Reports Channel"),
    (ALERTS, "Alerts Channel"),
    (SCREENERS, "Screeners Channel"),
    (CHARTS, "Charts Channel"),
    (NOTIFICATIONS, "Notifications Channel"),
]

_TIMEZONES = [
    "America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles",
    "America/Phoenix", "America/Anchorage", "Pacific/Honolulu", "America/Toronto",
    "America/Vancouver", "Europe/London", "Europe/Paris", "Europe/Berlin",
    "Europe/Amsterdam", "Europe/Rome", "Europe/Stockholm", "Asia/Tokyo",
    "Asia/Shanghai", "Asia/Hong_Kong", "Asia/Kolkata", "Asia/Dubai",
    "Asia/Singapore", "Australia/Sydney", "Australia/Melbourne", "UTC",
]

_NOTIFICATION_FILTER_OPTIONS = [
    ("All Events", "all"),
    ("Failures Only", "failures_only"),
    ("Off", "off"),
]

_FILTER_MAP = {
    "all": NotificationFilter.ALL,
    "failures_only": NotificationFilter.FAILURES_ONLY,
    "off": NotificationFilter.OFF,
}

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


def _build_channel_embed(current: dict) -> discord.Embed:
    """Build an embed showing the current channel configuration."""
    lines = []
    for config_type, _ in _CHANNEL_TYPES:
        channel_id = current.get(config_type)
        if channel_id:
            lines.append(f"**{config_type}**: <#{channel_id}>")
        else:
            lines.append(f"**{config_type}**: Not set")
    return discord.Embed(
        title="Channel Configuration",
        description="\n".join(lines),
        color=discord.Color.blurple(),
    )


def _build_settings_embed(current_tz: str, current_filter: str) -> discord.Embed:
    """Build an embed showing the current bot settings."""
    lines = []
    if "TZ" in os.environ:
        lines.append(f"**Timezone**: {current_tz} *[ENV override]*")
    else:
        lines.append(f"**Timezone**: {current_tz}")
    if "NOTIFICATION_FILTER" in os.environ:
        lines.append(f"**Notification Filter**: {current_filter} *[ENV override]*")
    else:
        lines.append(f"**Notification Filter**: {current_filter}")
    return discord.Embed(
        title="Bot Settings",
        description="\n".join(lines),
        color=discord.Color.blurple(),
    )


class _ChannelTypeSelect(discord.ui.ChannelSelect):
    """A ChannelSelect that auto-saves the selection for one channel type."""

    def __init__(self, config_type: str, placeholder: str, default_values: list, row: int):
        super().__init__(
            placeholder=placeholder,
            channel_types=[discord.ChannelType.text],
            min_values=0,
            max_values=1,
            default_values=default_values,
            row=row,
        )
        self.config_type = config_type

    async def callback(self, interaction: discord.Interaction):
        view: ChannelSetupView = self.view
        if self.values:
            channel_id = int(self.values[0].id)
            try:
                await view._bot.stock_data.channel_config.upsert_channel(
                    interaction.guild_id, self.config_type, channel_id
                )
            except Exception:
                logger.error(
                    f"Failed to save channel config: guild={interaction.guild_id} "
                    f"{self.config_type}={channel_id}",
                    exc_info=True,
                )
                await interaction.response.send_message(
                    "Failed to save channel setting — please try again.", ephemeral=True
                )
                return
            view._current[self.config_type] = channel_id
            logger.info(f"Channel setup: guild={interaction.guild_id} {self.config_type}={channel_id}")
        embed = _build_channel_embed(view._current)
        await interaction.response.edit_message(embed=embed)


class ChannelSetupView(discord.ui.View):
    """Five ChannelSelect rows, one per channel type. Each auto-saves on selection."""

    def __init__(self, bot: commands.Bot, guild_id: int, current_config: dict):
        super().__init__(timeout=None)
        self._bot = bot
        self._guild_id = guild_id
        self._current = dict(current_config)

        for i, (config_type, placeholder) in enumerate(_CHANNEL_TYPES):
            channel_id = current_config.get(config_type)
            default_vals = []
            if channel_id:
                default_vals = [discord.SelectDefaultValue(
                    id=channel_id,
                    type=discord.SelectDefaultValueType.channel,
                )]
            self.add_item(_ChannelTypeSelect(
                config_type=config_type,
                placeholder=placeholder,
                default_values=default_vals,
                row=i,
            ))


class BotSettingsView(discord.ui.View):
    """Timezone and notification filter selects with a Save Settings button."""

    def __init__(self, bot: commands.Bot, guild_id: int, current_tz: str, current_filter: str):
        super().__init__(timeout=None)
        self._bot = bot
        self._guild_id = guild_id
        self._tz = current_tz
        self._filter = current_filter

        tz_disabled = "TZ" in os.environ
        tz_placeholder = "Timezone" + (" [ENV override — read only]" if tz_disabled else "")
        tz_options = [
            discord.SelectOption(label=tz, value=tz, default=(tz == current_tz))
            for tz in _TIMEZONES
        ]
        self._tz_select = discord.ui.Select(
            placeholder=tz_placeholder,
            options=tz_options,
            disabled=tz_disabled,
            row=0,
        )
        self._tz_select.callback = self._on_tz_select
        self.add_item(self._tz_select)

        filter_disabled = "NOTIFICATION_FILTER" in os.environ
        filter_placeholder = "Notification Filter" + (" [ENV override — read only]" if filter_disabled else "")
        filter_options = [
            discord.SelectOption(label=label, value=value, default=(value == current_filter))
            for label, value in _NOTIFICATION_FILTER_OPTIONS
        ]
        self._filter_select = discord.ui.Select(
            placeholder=filter_placeholder,
            options=filter_options,
            disabled=filter_disabled,
            row=1,
        )
        self._filter_select.callback = self._on_filter_select
        self.add_item(self._filter_select)

        save_button = discord.ui.Button(
            label="Save Settings",
            style=discord.ButtonStyle.primary,
            row=2,
        )
        save_button.callback = self._on_save
        self.add_item(save_button)

    async def _on_tz_select(self, interaction: discord.Interaction):
        if self._tz_select.values:
            self._tz = self._tz_select.values[0]
        await interaction.response.defer()

    async def _on_filter_select(self, interaction: discord.Interaction):
        if self._filter_select.values:
            self._filter = self._filter_select.values[0]
        await interaction.response.defer()

    async def _on_save(self, interaction: discord.Interaction):
        settings_repo = self._bot.stock_data.bot_settings

        if "TZ" not in os.environ:
            await settings_repo.set("tz", self._tz)
            configure_tz(self._tz)
            logger.info(f"Bot settings: tz={self._tz} saved for guild={interaction.guild_id}")

        if "NOTIFICATION_FILTER" not in os.environ:
            await settings_repo.set("notification_filter", self._filter)
            if self._filter in _FILTER_MAP:
                self._bot.notification_config.filter = _FILTER_MAP[self._filter]
            logger.info(f"Bot settings: notification_filter={self._filter} saved for guild={interaction.guild_id}")

        embed = _build_settings_embed(self._tz, self._filter)
        embed.color = discord.Color.green()
        embed.set_footer(text="Settings saved!")
        await interaction.response.edit_message(embed=embed)

        # Trigger subscriptions if all channels are now configured
        guild = interaction.guild
        if guild:
            config = await self._bot.stock_data.channel_config.get_all_for_guild(guild.id)
            if all(ct in config for ct in ALL_CONFIG_TYPES):
                await setup_alert_subscriptions(self._bot, guild)


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
        """Send channel setup and bot settings embeds with Select UI to the target channel."""
        guild_id = guild.id
        current_config = await self.bot.stock_data.channel_config.get_all_for_guild(guild_id)
        db_tz = await self.bot.stock_data.bot_settings.get("tz")
        current_tz = db_tz or settings.tz
        db_filter = await self.bot.stock_data.bot_settings.get("notification_filter")
        current_filter = db_filter or settings.notification_filter

        channels_embed = _build_channel_embed(current_config)
        channels_embed.description = (
            f"Thanks for adding RocketStocks to **{guild.name}**!\n\n"
            "Select channels below to configure where each type of content is posted.\n\n"
            + (channels_embed.description or "")
        )
        try:
            await target.send(
                embed=channels_embed,
                view=ChannelSetupView(self.bot, guild_id, current_config),
            )
            settings_embed = _build_settings_embed(current_tz, current_filter)
            await target.send(
                embed=settings_embed,
                view=BotSettingsView(self.bot, guild_id, current_tz, current_filter),
            )
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

    @server_group.command(name="setup", description="Set up which channels the bot posts to")
    async def server_setup(self, interaction: discord.Interaction):
        """Send channel setup and bot settings views as ephemeral messages."""
        guild_id = interaction.guild_id
        current_config = await self.bot.stock_data.channel_config.get_all_for_guild(guild_id)

        if "TZ" in os.environ:
            current_tz = os.environ["TZ"]
        else:
            db_tz = await self.bot.stock_data.bot_settings.get("tz")
            current_tz = db_tz or settings.tz

        if "NOTIFICATION_FILTER" in os.environ:
            current_filter = os.environ["NOTIFICATION_FILTER"].lower()
        else:
            db_filter = await self.bot.stock_data.bot_settings.get("notification_filter")
            current_filter = db_filter or settings.notification_filter

        channels_embed = _build_channel_embed(current_config)
        await interaction.response.send_message(
            embed=channels_embed,
            view=ChannelSetupView(self.bot, guild_id, current_config),
            ephemeral=True,
        )

        settings_embed = _build_settings_embed(current_tz, current_filter)
        await interaction.followup.send(
            embed=settings_embed,
            view=BotSettingsView(self.bot, guild_id, current_tz, current_filter),
            ephemeral=True,
        )

    @server_group.command(name="status", description="View current channel, role, and bot settings")
    async def server_status(self, interaction: discord.Interaction):
        """Display channel config, alert roles, and bot settings."""
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

        # Bot settings section
        settings_lines = []
        if "TZ" in os.environ:
            settings_lines.append(f"**Timezone**: {os.environ['TZ']} *[ENV override]*")
        else:
            db_tz = await self.bot.stock_data.bot_settings.get("tz")
            tz_val = db_tz or settings.tz
            settings_lines.append(f"**Timezone**: {tz_val}")

        if "NOTIFICATION_FILTER" in os.environ:
            settings_lines.append(f"**Notification Filter**: {os.environ['NOTIFICATION_FILTER']} *[ENV override]*")
        else:
            db_filter = await self.bot.stock_data.bot_settings.get("notification_filter")
            filter_val = db_filter or settings.notification_filter
            settings_lines.append(f"**Notification Filter**: {filter_val}")

        all_set = channels_all_set and roles_all_set
        color = discord.Color.green() if all_set else discord.Color.orange()

        description = (
            "**Channel Configuration**\n"
            + "\n".join(channel_lines)
            + "\n\n**Alert Subscription Roles**\n"
            + "\n".join(role_lines)
            + "\n\n**Bot Settings**\n"
            + "\n".join(settings_lines)
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
