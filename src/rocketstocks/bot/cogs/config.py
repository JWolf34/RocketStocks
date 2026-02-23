"""Config cog — per-guild channel setup and status commands."""
import logging
import discord
from discord import app_commands
from discord.ext import commands

from rocketstocks.data.channel_config import (
    REPORTS, ALERTS, SCREENERS, CHARTS, NOTIFICATIONS, ALL_CONFIG_TYPES,
)

logger = logging.getLogger(__name__)

_PRIORITY_CHANNEL_NAMES = ["bot-commands", "bot", "general", "welcome"]


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
        """Send an orange setup-instructions embed to target (channel or DM)."""
        embed = discord.Embed(
            title="RocketStocks Setup Required",
            description=(
                f"Thanks for adding RocketStocks to **{guild.name}**!\n\n"
                "Use `/setup` to configure the channels for reports, alerts, screeners, "
                "charts, and notifications.\n\n"
                "**Example:**\n"
                "`/setup reports:#reports alerts:#alerts screeners:#screeners "
                "charts:#charts notifications:#bot-log`"
            ),
            color=discord.Color.orange(),
        )
        try:
            await target.send(embed=embed)
        except Exception as exc:
            logger.warning(f"Could not send setup prompt to {target}: {exc}")

    # -------------------------------------------------------------------------
    # Slash commands
    # -------------------------------------------------------------------------

    @app_commands.command(name="setup", description="Configure RocketStocks channels for this server")
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(
        reports="Channel for stock reports",
        alerts="Channel for stock alerts",
        screeners="Channel for screeners",
        charts="Channel for charts",
        notifications="Channel for bot notifications",
    )
    async def setup(
        self,
        interaction: discord.Interaction,
        reports: discord.TextChannel = None,
        alerts: discord.TextChannel = None,
        screeners: discord.TextChannel = None,
        charts: discord.TextChannel = None,
        notifications: discord.TextChannel = None,
    ):
        """Upsert channel configuration for the calling guild."""
        params = {
            REPORTS: reports,
            ALERTS: alerts,
            SCREENERS: screeners,
            CHARTS: charts,
            NOTIFICATIONS: notifications,
        }
        provided = {k: v for k, v in params.items() if v is not None}
        if not provided:
            await interaction.response.send_message(
                "Please provide at least one channel. Example:\n"
                "`/setup reports:#reports alerts:#alerts`",
                ephemeral=True,
            )
            return

        repo = self.bot.stock_data.channel_config
        configured_lines = []
        for config_type, channel in provided.items():
            repo.upsert_channel(interaction.guild_id, config_type, channel.id)
            configured_lines.append(f"**{config_type}**: {channel.mention}")
            logger.info(f"/setup: guild={interaction.guild_id} {config_type}={channel.id}")

        embed = discord.Embed(
            title="Channel Configuration Updated",
            description="\n".join(configured_lines),
            color=discord.Color.green(),
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

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
