"""ChannelSender against stand-ins for the discord.py objects it calls: what it sends (the
row id as the nonce, no mentions for a reply), which history message it takes for a row
(the bot's own, whose footer names the row), and how it starts a thread."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import discord
import pytest

from rs_bot.discord_sender import HISTORY_MARGIN, ChannelSender
from rs_bot.render import footer_marker

BOT_ID = 42
SINCE = datetime(2026, 7, 6, 14, 30, tzinfo=UTC)


def _embed(row_id: int) -> discord.Embed:
    return discord.Embed(title="x").set_footer(text=footer_marker(row_id))


def _message(message_id: int, row_id: int | None, *, author: int = BOT_ID) -> SimpleNamespace:
    embeds = [] if row_id is None else [_embed(row_id)]
    return SimpleNamespace(id=message_id, author=SimpleNamespace(id=author), embeds=embeds)


class _Where:
    """What a channel and a thread share: sends are recorded, history is planted."""

    def __init__(self, history: list[SimpleNamespace] | None = None) -> None:
        self.sent: list[dict] = []
        self.planted = history or []
        self.history_calls: list[dict] = []
        self.history_error: Exception | None = None

    async def send(self, **kwargs):
        self.sent.append(kwargs)
        return SimpleNamespace(id=900 + len(self.sent))

    async def history(self, **kwargs):
        self.history_calls.append(kwargs)
        if self.history_error is not None:
            raise self.history_error
        for message in self.planted:
            yield message


class _Thread(_Where, discord.Thread):
    def __init__(self, thread_id: int, history: list[SimpleNamespace] | None = None) -> None:
        _Where.__init__(self, history)
        self.id = thread_id


class _PartialMessage:
    def __init__(self, channel: "_Channel", message_id: int) -> None:
        self._channel = channel
        self._message_id = message_id

    async def create_thread(self, *, name: str) -> SimpleNamespace:
        if self._channel.thread_error is not None:
            raise self._channel.thread_error
        self._channel.thread_names.append(name)
        return SimpleNamespace(id=self._message_id)


class _Channel(_Where):
    def __init__(self, history: list[SimpleNamespace] | None = None) -> None:
        super().__init__(history)
        self.thread_error: Exception | None = None
        self.thread_names: list[str] = []

    def get_partial_message(self, message_id: int) -> _PartialMessage:
        return _PartialMessage(self, message_id)


class _Client:
    def __init__(self, *threads: _Thread) -> None:
        self._threads = {t.id: t for t in threads}

    async def fetch_channel(self, channel_id: int):
        return self._threads[channel_id]


def _sender(channel: _Channel, *threads: _Thread) -> ChannelSender:
    return ChannelSender(_Client(*threads), channel, BOT_ID)  # type: ignore[arg-type]


def _http_error(code: int) -> discord.HTTPException:
    return discord.HTTPException(SimpleNamespace(status=400, reason="test"), {"code": code, "message": "test"})


async def test_an_alert_is_sent_to_the_channel_with_the_row_id_as_its_nonce():
    channel = _Channel()
    embed = _embed(7)

    message_id = await _sender(channel).send_alert(embed, nonce=7)

    assert message_id == 901
    assert channel.sent == [{"embed": embed, "nonce": 7}]


async def test_a_reply_is_sent_to_the_thread_with_the_nonce_and_no_mentions_allowed():
    thread = _Thread(555)
    embed = _embed(8)

    await _sender(_Channel(), thread).send_reply(555, embed, nonce=8)

    (sent,) = thread.sent
    assert (sent["embed"], sent["nonce"]) == (embed, 8)
    assert sent["allowed_mentions"].to_dict() == discord.AllowedMentions.none().to_dict()


async def test_history_is_read_from_a_margin_before_the_first_send():
    channel = _Channel()

    await _sender(channel).find_alert(7, SINCE)

    assert channel.history_calls == [{"limit": None, "after": SINCE - HISTORY_MARGIN}]
    assert HISTORY_MARGIN > timedelta(0)


async def test_the_row_is_found_by_the_bots_own_message_naming_it():
    channel = _Channel(
        [
            _message(1, 7, author=BOT_ID + 1),  # someone quoting the id
            _message(2, None),  # the bot's, but without the marker
            _message(3, 70),  # the bot's, another row (the id is matched whole)
            _message(4, 7),
        ]
    )

    assert await _sender(channel).find_alert(7, SINCE) == 4
    assert await _sender(channel).find_alert(9, SINCE) is None


async def test_a_reply_is_looked_for_in_its_thread_not_the_channel():
    thread = _Thread(555, [_message(6, 8)])
    channel = _Channel([_message(5, 8)])

    assert await _sender(channel, thread).find_reply(555, 8, SINCE) == 6
    assert channel.history_calls == []


async def test_a_history_that_cannot_be_read_raises_rather_than_saying_not_found():
    channel = _Channel()
    channel.history_error = ConnectionError("down")

    with pytest.raises(ConnectionError):
        await _sender(channel).find_alert(7, SINCE)


async def test_a_thread_is_started_from_the_alerts_message():
    channel = _Channel()

    thread_id = await _sender(channel).open_thread(321, "GME alert #7")

    assert thread_id == 321
    assert channel.thread_names == ["GME alert #7"]


async def test_a_message_that_already_has_a_thread_gives_that_thread():
    channel = _Channel()
    channel.thread_error = _http_error(160004)

    assert await _sender(channel, _Thread(321)).open_thread(321, "GME alert #7") == 321


async def test_any_other_refusal_to_start_a_thread_is_raised():
    channel = _Channel()
    channel.thread_error = _http_error(50013)  # missing permissions

    with pytest.raises(discord.HTTPException):
        await _sender(channel).open_thread(321, "GME alert #7")
