"""Subscriptions cog — alert role setup and user-facing subscription management."""
import logging
import discord
from discord import app_commands
from discord.ext import commands

from rocketstocks.data.channel_config import ALERTS
from rocketstocks.bot.views.subscription_views import (
    ALERT_ROLE_DEFS,
    AlertSubscriptionSelect,
    AlertSubscriptionView,
    SubscriptionEntryView,
)

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

    Called after /setup completes for a guild. Idempotent — reuses existing roles.
    """
    alert_roles_repo = bot.stock_data.alert_roles
    label_map = dict(ALERT_ROLE_DEFS)

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


class Subscriptions(commands.Cog):
    """Manage alert role subscriptions per guild."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"{__name__} loaded")

    # -------------------------------------------------------------------------
    # Shared helper
    # -------------------------------------------------------------------------

    async def _send_subscription_select(self, interaction: discord.Interaction) -> None:
        """Send an ephemeral subscription dropdown to the interacting user."""
        guild_roles = await self.bot.stock_data.alert_roles.get_all_for_guild(interaction.guild_id)
        member_role_ids = {r.id for r in interaction.user.roles}
        select = AlertSubscriptionSelect(guild_roles, member_role_ids)
        view = AlertSubscriptionView(select)
        await interaction.response.send_message(
            "Select the alerts you want to be notified about:",
            view=view,
            ephemeral=True,
        )

    # -------------------------------------------------------------------------
    # Admin command group
    # -------------------------------------------------------------------------

    subscriptions_group = app_commands.Group(
        name="subscriptions",
        description="Manage alert subscription roles",
        default_permissions=discord.Permissions(administrator=True),
    )

    @subscriptions_group.command(name="status", description="Show alert role configuration status")
    async def subscriptions_status(self, interaction: discord.Interaction):
        """Show which of the 9 alert roles are configured vs missing."""
        guild_roles = await self.bot.stock_data.alert_roles.get_all_for_guild(interaction.guild_id)
        lines = []
        all_configured = True
        for role_key, label in ALERT_ROLE_DEFS:
            role_id = guild_roles.get(role_key)
            if role_id:
                role = interaction.guild.get_role(role_id)
                mention = role.mention if role else f"<@&{role_id}>"
                lines.append(f"✅ {label}: {mention}")
            else:
                lines.append(f"❌ {label}: Not configured")
                all_configured = False

        colour = discord.Colour.green() if all_configured else discord.Colour.orange()
        footer = None if all_configured else "Run /setup to configure roles automatically."
        embed = discord.Embed(
            title="Alert Subscription Roles",
            description="\n".join(lines),
            colour=colour,
        )
        if footer:
            embed.set_footer(text=footer)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    # -------------------------------------------------------------------------
    # User-facing command
    # -------------------------------------------------------------------------

    @app_commands.command(
        name="alert-subscriptions",
        description="Manage your alert notification subscriptions",
    )
    async def alert_subscriptions(self, interaction: discord.Interaction):
        """Open the subscription selector for the interacting user."""
        await self._send_subscription_select(interaction)


async def setup(bot: commands.Bot):
    cog = Subscriptions(bot)
    await bot.add_cog(cog)
