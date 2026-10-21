"""Daemon seams that don't need Discord: the sender adapter and the DSN
conversion for the raw-psycopg LISTEN connection. The full lifecycle (channel
resolution, SIGTERM, kill/restart) is covered by the live acceptance runbook."""

import discord

from rs_bot.__main__ import DiscordChannelSender, psycopg_conninfo


class _StubMessage:
    id = 987654321


class _StubChannel:
    def __init__(self):
        self.sent_embeds = []

    async def send(self, *, embed):
        self.sent_embeds.append(embed)
        return _StubMessage()


async def test_channel_sender_posts_embed_and_returns_message_id():
    channel = _StubChannel()
    sender = DiscordChannelSender(channel)
    embed = discord.Embed(title="SPY · volume_threshold")
    assert await sender.send(embed) == 987654321
    assert channel.sent_embeds == [embed]


def test_psycopg_conninfo_strips_the_sqlalchemy_driver():
    assert (
        psycopg_conninfo("postgresql+psycopg://u:p@localhost:5498/rs")
        == "postgresql://u:p@localhost:5498/rs"
    )
