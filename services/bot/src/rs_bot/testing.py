"""The AlertSender seam in memory: one channel, its threads, and Discord's nonce rule.

Messages are kept the way Discord keeps them, so a test can plant what a crashed process
left behind. A second send with a nonce already used is answered with the first message
(`enforce_nonce`), and history finds a message by the id in its embed footer, as the real
sender does, unless the test hides it.

Test-support only (the rs_persistence.testing pattern): runtime code must never import it.
"""

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import SimpleNamespace

import discord

from rs_bot.pings import GuildUnavailable, RoleInfo
from rs_bot.render import marked_row_id
from rs_domain import Alert, FactUnavailable, PopularitySurgePayload, SurgeFacts

HIT = datetime(2026, 7, 6, 14, 30, tzinfo=UTC)  # five minutes before the clock's default now
CHANNEL = 0  # the `where` of a message posted in the alert channel itself


@dataclass
class Message:
    id: int
    where: int  # CHANNEL, or the id of the thread it was posted in
    embed: discord.Embed
    nonce: int | None
    from_bot: bool = True
    mentions_allowed: bool = True
    role_ids: tuple[int, ...] = ()  # the roles it pinged


class FakeDiscord:
    def __init__(self) -> None:
        self.messages: list[Message] = []
        self.threads: dict[int, str] = {}  # thread id (the message id it started from) -> name
        # Planted behaviour: send failures consumed one per send (None succeeds), a send that
        # posts and then fails, failures for the next thread opens and history reads.
        self.send_failures: list[Exception | None] = []
        self.fail_after_posting: list[Exception] = []
        self.open_thread_failures: list[Exception] = []
        self.history_failure: Exception | None = None
        self.fail_when: Callable[[discord.Embed], bool] | None = None
        self.hide_from_history = False
        self.before_send: Callable[[], Awaitable[None]] | None = None
        self.history_reads: list[tuple[int, datetime]] = []
        self._next_id = 1000

    @property
    def sent(self) -> list[discord.Embed]:
        return [m.embed for m in self.messages if m.from_bot]

    def plant(
        self, embed: discord.Embed, nonce: int | None, *, where: int = CHANNEL, at: datetime | None = None
    ) -> Message:
        """A message already in Discord, as a previous run of the bot left it; `at` gives it a
        real snowflake id, so the time it was created can be read back from the id."""
        message = Message(self._mint() if at is None else discord.utils.time_snowflake(at), where, embed, nonce)
        self.messages.append(message)
        return message

    def in_thread(self, thread_id: int) -> list[Message]:
        return [m for m in self.messages if m.where == thread_id]

    async def send_alert(
        self, embed: discord.Embed, *, nonce: int, role_ids: Sequence[int] = ()
    ) -> int:
        return await self._send(CHANNEL, embed, nonce, mentions_allowed=True, role_ids=tuple(role_ids))

    async def send_reply(self, thread_id: int, embed: discord.Embed, *, nonce: int) -> int:
        return await self._send(thread_id, embed, nonce, mentions_allowed=False)

    async def open_thread(self, message_id: int, name: str) -> int:
        if self.open_thread_failures:
            raise self.open_thread_failures.pop(0)
        self.threads[message_id] = name
        return message_id

    async def find_alert(self, row_id: int, since: datetime) -> int | None:
        return self._find(CHANNEL, row_id, since)

    async def find_reply(self, thread_id: int, row_id: int, since: datetime) -> int | None:
        return self._find(thread_id, row_id, since)

    async def _send(
        self,
        where: int,
        embed: discord.Embed,
        nonce: int,
        *,
        mentions_allowed: bool,
        role_ids: tuple[int, ...] = (),
    ) -> int:
        if self.before_send is not None:
            await self.before_send()
        failure = self.send_failures.pop(0) if self.send_failures else None
        if failure is not None:
            raise failure
        if self.fail_when is not None and self.fail_when(embed):
            raise RuntimeError("planted send failure")
        for message in self.messages:
            if message.from_bot and message.nonce == nonce and message.where == where:
                return message.id  # enforce_nonce: this author already made it
        message = Message(
            self._mint(), where, embed, nonce, mentions_allowed=mentions_allowed, role_ids=role_ids
        )
        self.messages.append(message)
        if self.fail_after_posting:
            raise self.fail_after_posting.pop(0)
        return message.id

    def _find(self, where: int, row_id: int, since: datetime) -> int | None:
        self.history_reads.append((where, since))
        if self.history_failure is not None:
            raise self.history_failure
        if self.hide_from_history:
            return None
        for message in self.messages:
            if message.where == where and message.from_bot and marked_row_id(message.embed) == row_id:
                return message.id
        return None

    def _mint(self) -> int:
        self._next_id += 1
        return self._next_id


class FakeRoles:
    """The server's roles, as `GuildRoles` reads them: add one, delete one, make one unmanageable."""

    def __init__(self) -> None:
        self._roles: dict[int, RoleInfo] = {}
        self.guild_known = True

    def add(
        self, role_id: int, name: str = "role", *, assignable: bool = True, mentionable: bool = True
    ) -> RoleInfo:
        self._roles[role_id] = RoleInfo(role_id, name, assignable, mentionable)
        return self._roles[role_id]

    def delete(self, role_id: int) -> None:
        del self._roles[role_id]

    def role(self, role_id: int) -> RoleInfo | None:
        if not self.guild_known:
            raise GuildUnavailable
        return self._roles.get(role_id)


@dataclass
class FakeMember:
    """One member's roles, changed by the bot as Discord would: a role the bot may not touch is
    refused with a 403, and `fail_with` plants any other refusal."""

    id: int = 7
    role_ids: set[int] = field(default_factory=set)
    forbidden: set[int] = field(default_factory=set)
    fail_with: discord.HTTPException | None = None

    @property
    def roles(self) -> list[discord.Role]:
        return [SimpleNamespace(id=role_id) for role_id in self.role_ids]  # type: ignore[misc]

    async def add_roles(self, *roles: discord.abc.Snowflake, reason: str | None = None) -> None:
        for role in roles:
            self._check(role.id)
            self.role_ids.add(role.id)

    async def remove_roles(self, *roles: discord.abc.Snowflake, reason: str | None = None) -> None:
        for role in roles:
            self._check(role.id)
            self.role_ids.discard(role.id)

    def _check(self, role_id: int) -> None:
        if self.fail_with is not None:
            raise self.fail_with
        if role_id in self.forbidden:
            response = SimpleNamespace(status=403, reason="Missing Permissions")
            raise discord.Forbidden(response, "Missing Permissions")


class FixedClock:
    def __init__(self, now: datetime = datetime(2026, 7, 6, 14, 35, tzinfo=UTC)) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now


def alert(symbol: str) -> Alert:
    return Alert(
        symbol=symbol,
        trigger="volume_threshold",
        triggered_at=HIT,
        payload={"volume": 750_000, "threshold": 500_000},
    )


def surge_alert(facts: SurgeFacts | None = None) -> Alert:
    """A popularity-surge Alert on GME; with no `facts`, every supporting fact is unavailable."""
    unavailable = FactUnavailable(reason="test")
    payload = PopularitySurgePayload(
        rule_version="1",
        polled_at=HIT,
        fired=("rank_jump",),
        rank=5,
        mentions=100,
        rank_24h_ago=80,
        mentions_24h_ago=None,
        rank_gain=75,
        mention_ratio=None,
        mention_multiplier=5.0,
        mention_min_prior=15,
        facts=facts
        or SurgeFacts(price_move=unavailable, next_earnings=unavailable),
    )
    return Alert(
        symbol="GME",
        trigger="popularity_surge",
        triggered_at=HIT,
        payload=payload.model_dump(mode="json"),
        rule_version="1",
    )
