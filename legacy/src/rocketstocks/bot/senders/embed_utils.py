"""Shared utility for converting core-layer EmbedSpec objects to discord.Embed."""
import datetime
import discord

from rocketstocks.core.content.models import EmbedSpec


def spec_to_embed(spec: EmbedSpec) -> discord.Embed:
    """Convert a core-layer EmbedSpec to a discord.Embed."""
    embed = discord.Embed(
        title=spec.title,
        description=spec.description,
        color=spec.color,
        url=spec.url or None,
        timestamp=datetime.datetime.now(datetime.timezone.utc) if spec.timestamp else None,
    )
    for f in spec.fields:
        embed.add_field(name=f.name, value=f.value, inline=f.inline)
    if spec.footer:
        embed.set_footer(text=spec.footer)
    if spec.thumbnail_url:
        embed.set_thumbnail(url=spec.thumbnail_url)
    return embed
