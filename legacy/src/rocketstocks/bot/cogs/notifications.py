import collections
import logging
import datetime
import discord
from discord import app_commands
from discord.ext import commands, tasks

from rocketstocks.data.channel_config import NOTIFICATIONS
from rocketstocks.core.notifications.config import NotificationConfig, NotificationFilter, NotificationLevel
from rocketstocks.core.notifications.emitter import EventEmitter
from rocketstocks.core.notifications.event import NotificationEvent
from rocketstocks.bot.senders.notification_sender import format_notification

logger = logging.getLogger(__name__)

_RING_BUFFER_SIZE = 50


class Notifications(commands.Cog):
    """Sentinel notification system — drains the event queue and posts embeds to the notifications channel."""

    def __init__(self, bot: commands.Bot, emitter: EventEmitter, config: NotificationConfig):
        self.bot = bot
        self.emitter = emitter
        self.config = config
        self._last_disconnect: datetime.datetime | None = None
        self._recent_events: collections.deque[NotificationEvent] = collections.deque(maxlen=_RING_BUFFER_SIZE)
        self.drain_notifications.start()
        self.check_latency.start()

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"{__name__} loaded")

    @commands.Cog.listener()
    async def on_disconnect(self):
        self._last_disconnect = datetime.datetime.now()
        logger.warning("Bot disconnected from Discord")

    @commands.Cog.listener()
    async def on_resumed(self):
        if self._last_disconnect is not None:
            downtime = (datetime.datetime.now() - self._last_disconnect).total_seconds()
            self.emitter.emit(NotificationEvent(
                level=NotificationLevel.WARNING,
                source=__name__,
                job_name="heartbeat",
                message=f"Bot reconnected after {downtime:.1f}s downtime",
                elapsed_seconds=downtime,
            ))
            self._last_disconnect = None

    @tasks.loop(seconds=10)
    async def drain_notifications(self):
        """Drain the event queue and post filtered embeds to all configured notification channels."""
        events = self.emitter.drain()
        if not events:
            return

        for event in events:
            self._recent_events.append(event)

        for _, channel in await self.bot.iter_channels(NOTIFICATIONS):
            for event in events:
                if self.config.should_notify(event):
                    embed = format_notification(event)
                    try:
                        await channel.send(embed=embed)
                    except discord.HTTPException as exc:
                        logger.error(f"Failed to send notification embed: {exc}")

    @tasks.loop(seconds=30)
    async def check_latency(self):
        """Emit a WARNING if bot websocket latency exceeds the configured threshold."""
        latency = self.bot.latency
        if latency > self.config.latency_threshold_seconds:
            self.emitter.emit(NotificationEvent(
                level=NotificationLevel.WARNING,
                source=__name__,
                job_name="heartbeat",
                message=f"High websocket latency: {latency * 1000:.0f}ms (threshold: {self.config.latency_threshold_seconds * 1000:.0f}ms)",
            ))

    @drain_notifications.before_loop
    @check_latency.before_loop
    async def wait_until_ready(self):
        await self.bot.wait_until_ready()

    #####################
    # Slash commands    #
    #####################

    notifications_group = app_commands.Group(
        name="notifications",
        description="Manage bot event notifications",
        default_permissions=discord.Permissions(administrator=True),
    )

    @notifications_group.command(name="filter", description="Set which events to receive (all, failures only, off)")
    @app_commands.describe(level="Filter level: all, failures_only, or off")
    @app_commands.choices(level=[
        app_commands.Choice(name="all", value="all"),
        app_commands.Choice(name="failures_only", value="failures_only"),
        app_commands.Choice(name="off", value="off"),
    ])
    async def notifications_filter(self, interaction: discord.Interaction, level: app_commands.Choice[str]):
        """Update the notification filter at runtime."""
        filter_map = {
            "all": NotificationFilter.ALL,
            "failures_only": NotificationFilter.FAILURES_ONLY,
            "off": NotificationFilter.OFF,
        }
        self.config.filter = filter_map[level.value]
        logger.info(f"Notification filter set to '{level.value}' by {interaction.user.name}")
        await interaction.response.send_message(
            f"Notification filter updated to **{level.value}**.", ephemeral=True
        )

    @notifications_group.command(name="status", description="View notification settings and the last few events")
    async def notifications_status(self, interaction: discord.Interaction):
        """Display current filter and the last 5 notification events."""
        embed = discord.Embed(
            title="Sentinel Notification Status",
            color=discord.Color.blurple(),
            timestamp=datetime.datetime.now(),
        )
        embed.add_field(name="Filter", value=self.config.filter.value, inline=True)
        embed.add_field(name="Heartbeat", value="enabled" if self.config.heartbeat_enabled else "disabled", inline=True)
        embed.add_field(name="Latency Threshold", value=f"{self.config.latency_threshold_seconds * 1000:.0f}ms", inline=True)

        recent = list(self._recent_events)[-5:]
        if recent:
            lines = []
            for ev in reversed(recent):
                ts = ev.timestamp.strftime("%H:%M:%S")
                lines.append(f"`{ts}` **{ev.level.value.upper()}** — {ev.job_name}: {ev.message[:60]}")
            embed.add_field(name="Last 5 Events", value="\n".join(lines), inline=False)
        else:
            embed.add_field(name="Last 5 Events", value="No events recorded yet.", inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    config = NotificationConfig.from_env()
    await bot.add_cog(Notifications(bot=bot, emitter=bot.emitter, config=config))
