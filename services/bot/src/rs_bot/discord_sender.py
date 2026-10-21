"""The Discord side of delivery: send, thread and history, behind the sweep's `AlertSender`.

Every send carries the row id as its nonce. discord.py 2.7.1 sends a given nonce with
`enforce_nonce` set, so Discord returns the message already created by the same author
with that nonce (inside a window of a few minutes) instead of making a second one. That
covers a resend soon after a crash only. A later check reads the channel's or thread's
history for the bot's own message whose footer names the row (`render.marked_row_id`):
the nonce is not relied on there, since history reads do not reliably carry it.
"""

import logging
from datetime import datetime, timedelta

import discord

from rs_bot.render import marked_row_id

logger = logging.getLogger(__name__)

# Discord's error code for starting a thread on a message that already has one.
_THREAD_ALREADY_EXISTS = 160004
# History is read from the first send's time minus this, so a skew between the database's
# clock and Discord's cannot hide the message.
HISTORY_MARGIN = timedelta(minutes=5)


class ChannelSender:
    """AlertSender backed by the resolved alert channel and the gateway client."""

    def __init__(self, client: discord.Client, channel: discord.TextChannel, bot_user_id: int) -> None:
        self._client = client
        self._channel = channel
        self._bot_user_id = bot_user_id

    async def send_alert(self, embed: discord.Embed, *, nonce: int) -> int:
        message = await self._channel.send(embed=embed, nonce=nonce)
        return message.id

    async def send_reply(self, thread_id: int, embed: discord.Embed, *, nonce: int) -> int:
        thread = await self._thread(thread_id)
        # A reply never pings anyone: roles are pinged by an Alert, once.
        message = await thread.send(
            embed=embed, nonce=nonce, allowed_mentions=discord.AllowedMentions.none()
        )
        return message.id

    async def open_thread(self, message_id: int, name: str) -> int:
        try:
            thread = await self._channel.get_partial_message(message_id).create_thread(name=name)
        except discord.HTTPException as exc:
            if exc.code != _THREAD_ALREADY_EXISTS:
                raise
            # A crash between creating the thread and storing its id: a thread started from
            # a message has that message's id.
            logger.info("message %d already has a thread", message_id)
            return (await self._thread(message_id)).id
        return thread.id

    async def find_alert(self, row_id: int, since: datetime) -> int | None:
        return await self._find(self._channel, row_id, since)

    async def find_reply(self, thread_id: int, row_id: int, since: datetime) -> int | None:
        return await self._find(await self._thread(thread_id), row_id, since)

    async def _thread(self, thread_id: int) -> discord.Thread:
        thread = await self._client.fetch_channel(thread_id)
        if not isinstance(thread, discord.Thread):
            raise TypeError(f"channel {thread_id} is not a thread ({type(thread).__name__})")
        return thread

    async def _find(
        self, where: discord.abc.Messageable, row_id: int, since: datetime
    ) -> int | None:
        """The id of the bot's own message for `row_id`, or None. A failed read raises."""
        async for message in where.history(limit=None, after=since - HISTORY_MARGIN):
            if message.author.id != self._bot_user_id:
                continue
            if any(marked_row_id(embed) == row_id for embed in message.embeds):
                return message.id
        return None
