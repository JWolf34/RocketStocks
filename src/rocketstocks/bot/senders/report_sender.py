import datetime
import logging
import discord
from rocketstocks.data.discord_state import DiscordState
from rocketstocks.core.utils.dates import date_utils
from rocketstocks.bot.senders.embed_utils import spec_to_embed

logger = logging.getLogger(__name__)


def _try_build_embed(content) -> discord.Embed | None:
    """Try to build a discord.Embed from content.build_embed_spec().

    Returns None if the content class has not implemented build_embed_spec().
    """
    try:
        spec = content.build_embed_spec()
        return spec_to_embed(spec)
    except NotImplementedError:
        return None


async def send_report(content, channel: discord.TextChannel,
                      interaction: discord.Interaction = None,
                      visibility: str = "public",
                      files=None, view=None) -> discord.Message:
    """Send a Report content object to the target channel.

    Attempts to send as a rich Discord embed via content.build_embed_spec().
    Falls back to plain text via content.build_report() if not implemented.

    Args:
        content: A Report subclass instance with a build_report() method.
        channel: The Discord channel to post to when visibility is 'public'.
        interaction: Discord interaction (required when visibility is 'private').
        visibility: 'public' to post in channel, 'private' to DM the interaction user.
        files: Optional list of discord.File attachments.
        view: Optional discord.ui.View with buttons.

    Returns:
        The Discord Message object that was sent.
    """
    embed = _try_build_embed(content)
    logger.info("Sending report...")

    if embed is not None:
        logger.debug(f"Sending report as embed (title: {embed.title!r})")
        if visibility == 'private' and interaction:
            message = await interaction.user.send(embed=embed, files=files, view=view)
        else:
            message = await channel.send(embed=embed, files=files, view=view)
    else:
        message_text = content.build_report() + "\n\n"
        logger.debug(f"Sending report as plaintext (length: {len(message_text)})")
        if visibility == 'private' and interaction:
            message = await interaction.user.send(message_text, files=files, view=view)
        else:
            message = await channel.send(message_text, files=files, view=view)

    return message


async def send_screener(content, channel: discord.TextChannel,
                        dstate: DiscordState,
                        view=None, files=None) -> discord.Message:
    """Post or edit-in-place a Screener content object.

    Attempts to send as a rich Discord embed via content.build_embed_spec().
    Falls back to plain text via content.build_report() if not implemented.

    NOTE (multi-guild limitation): The `reports` table uses `type VARCHAR(64) PRIMARY KEY`
    with no `guild_id`, so edit-in-place tracking works correctly for only one guild.
    In a multi-guild deployment, each guild will receive a new screener message on each
    run instead of editing the previous one. Fixing this requires adding `guild_id` to
    the `reports` table (deferred).

    Checks for an existing message posted today for this screener type. If found,
    edits it instead of posting a new one. Stores the new message ID in the DB.

    Args:
        content: A Screener subclass instance with a build_report() method and
                 screener_type attribute.
        channel: The Discord channel to post to.
        dstate: DiscordState instance for DB-backed message ID tracking.
        view: Optional discord.ui.View with buttons.
        files: Optional list of discord.File attachments.

    Returns:
        The Discord Message object that was sent or edited (None when editing).
    """
    embed = _try_build_embed(content)
    use_embed = embed is not None

    # Normalize screener_type for DB key
    screener_type = content.screener_type.upper().replace("-", "_")
    logger.info(f"Sending '{screener_type}' screener...")

    today = datetime.datetime.now(tz=date_utils.timezone()).date()
    message_id = dstate.get_screener_message_id(screener_type=screener_type)

    if message_id:
        logger.debug(f"Existing screener '{screener_type}' found with ID {message_id}")
        curr_message = await channel.fetch_message(message_id)
        message_create_date = curr_message.created_at.astimezone(date_utils.timezone()).date()

        if message_create_date < today:
            # Old message — create new one for today
            if use_embed:
                message = await channel.send(embed=embed, view=view, files=files)
            else:
                message_text = content.build_report() + "\n\n"
                message = await channel.send(message_text, view=view, files=files)
            logger.info(f"Posted new '{screener_type}' screener for today")
            dstate.update_screener_message_id(message_id=message.id, screener_type=screener_type)
            return message
        else:
            # Same-day — edit in-place
            if use_embed:
                await curr_message.edit(embed=embed, content=None)
            else:
                message_text = content.build_report() + "\n\n"
                await curr_message.edit(content=message_text)
            logger.info(f"Updated '{screener_type}' screener")
            return None
    else:
        logger.debug(f"No existing message for '{screener_type}' screener")
        if use_embed:
            message = await channel.send(embed=embed, view=view)
        else:
            message_text = content.build_report() + "\n\n"
            message = await channel.send(message_text, view=view)
        logger.info(f"Posted new '{screener_type}' screener for today")
        dstate.insert_screener_message_id(message_id=message.id, screener_type=screener_type)
        return message
