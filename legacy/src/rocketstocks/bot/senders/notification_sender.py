import logging
import discord
from rocketstocks.core.notifications.config import NotificationLevel
from rocketstocks.core.notifications.event import NotificationEvent

logger = logging.getLogger(__name__)

_LEVEL_COLORS = {
    NotificationLevel.SUCCESS: discord.Color.green(),
    NotificationLevel.FAILURE: discord.Color.red(),
    NotificationLevel.WARNING: discord.Color.orange(),
}

_LEVEL_LABELS = {
    NotificationLevel.SUCCESS: "SUCCESS",
    NotificationLevel.FAILURE: "FAILURE",
    NotificationLevel.WARNING: "WARNING",
}

_MAX_TRACEBACK_CHARS = 1000


def format_notification(event: NotificationEvent) -> discord.Embed:
    """Build a color-coded Discord embed from a NotificationEvent."""
    color = _LEVEL_COLORS.get(event.level, discord.Color.default())
    label = _LEVEL_LABELS.get(event.level, event.level.value.upper())

    title = f"{label}: {event.job_name}"
    embed = discord.Embed(title=title, color=color, timestamp=event.timestamp)

    embed.add_field(name="Source", value=event.source, inline=True)

    if event.elapsed_seconds is not None:
        embed.add_field(name="Duration", value=f"{event.elapsed_seconds:.2f}s", inline=True)

    if event.message:
        embed.add_field(name="Message", value=event.message[:1024], inline=False)

    if event.traceback:
        truncated = event.traceback[-_MAX_TRACEBACK_CHARS:]
        embed.add_field(name="Traceback", value=f"```\n{truncated}\n```", inline=False)

    return embed
