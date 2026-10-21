"""Role pings by Trigger: which server role each Trigger pings, and the members who opt in.

An admin maps a Trigger to an existing role with `/setup roles`; the bot never creates one.
A member opts in with `/pings`, an ephemeral menu: adding or removing the role is the bot's
action (Manage Roles), so the member needs no permission. An Alert pings the role mapped to
its Trigger (`SweepWorker`); a reply never does.

The logic here is discord-free over two small seams, `GuildRoles` (what the server's roles
look like) and `PingMember` (one member's roles), so tests drive it without a gateway.
Every reply is ephemeral.
"""

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

import discord
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_bot.render import POPULARITY_SURGE
from rs_persistence import TriggerRoleRepository

logger = logging.getLogger(__name__)

# The Triggers a role can be mapped to, with the name members see.
TRIGGER_LABELS = {POPULARITY_SURGE: "Popularity surge"}

NO_MAPPINGS = (
    "No Alert pings are set up yet. An admin can map a Trigger to a role with /setup roles add."
)


@dataclass(frozen=True)
class RoleInfo:
    id: int
    name: str
    # The bot can add and remove it: it has Manage Roles and the role sits below its top role.
    assignable: bool
    # Members of the server can @mention it, so an Alert that names it notifies them.
    mentionable: bool


class GuildRoles(Protocol):
    def role(self, role_id: int) -> RoleInfo | None:
        """The server's role with this id, or None when it does not exist (any more)."""
        ...


class PingMember(Protocol):
    """The part of a guild member `/pings` uses (a `discord.Member`)."""

    @property
    def roles(self) -> Sequence[discord.Role]: ...

    async def add_roles(self, *roles: discord.abc.Snowflake, reason: str | None = None) -> None: ...

    async def remove_roles(self, *roles: discord.abc.Snowflake, reason: str | None = None) -> None: ...


def role_info(role: discord.Role) -> RoleInfo:
    return RoleInfo(role.id, role.name, role.is_assignable(), role.mentionable)


class DiscordGuildRoles:
    """`GuildRoles` over a gateway guild. `get_guild` is called on each lookup, so a guild the
    cache has not loaded yet reads as having no roles rather than being held stale."""

    def __init__(self, get_guild: Callable[[], discord.Guild | None]) -> None:
        self._get_guild = get_guild

    def role(self, role_id: int) -> RoleInfo | None:
        guild = self._get_guild()
        role = None if guild is None else guild.get_role(role_id)
        return None if role is None else role_info(role)


@dataclass(frozen=True)
class PingChoice:
    trigger: str
    label: str
    role: RoleInfo
    on: bool  # the member has the role
    problem: str | None = None  # why a change they asked for did not happen


@dataclass(frozen=True)
class PingMenu:
    choices: tuple[PingChoice, ...]
    # Triggers whose mapped role no longer exists.
    unavailable: tuple[str, ...] = ()

    @property
    def text(self) -> str:
        if not self.choices and not self.unavailable:
            return NO_MAPPINGS
        lines = ["Pick the Alerts you want to be pinged for. A selected Alert pings you."]
        for choice in self.choices:
            if choice.problem is not None:
                lines.append(f"Could not change {choice.label}: {choice.problem}")
        for label in self.unavailable:
            lines.append(f"{label} is mapped to a role that no longer exists. Ask an admin to map it again.")
        return "\n".join(lines)


def _label(trigger: str) -> str:
    return TRIGGER_LABELS.get(trigger, trigger)


class PingRoles:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def _mappings(self) -> dict[str, int]:
        async with self._session_factory() as session:
            return await TriggerRoleRepository(session).all()

    # -- /setup roles ----------------------------------------------------------------------

    async def add(self, trigger: str, role: RoleInfo) -> str:
        """Map `trigger` to `role`, unless the bot could not manage it for members."""
        if trigger not in TRIGGER_LABELS:
            return f"{trigger} is not a Trigger. Choose one from the list."
        if not role.assignable:
            return (
                f"I cannot manage <@&{role.id}>, so I will not map it: members could not opt in. "
                "I need the Manage Roles permission, and my role must sit above it in "
                "Server Settings, Roles. The @everyone role and roles an integration manages "
                "cannot be mapped."
            )
        async with self._session_factory() as session:
            await TriggerRoleRepository(session).set(trigger, role.id)
            await session.commit()
        text = f"{_label(trigger)} Alerts now ping <@&{role.id}>."
        if not role.mentionable:
            text += (
                " That role is not mentionable, so an Alert will not notify its members. "
                "Turn on 'Allow anyone to @mention this role' for it."
            )
        return text

    async def remove(self, trigger: str) -> str:
        async with self._session_factory() as session:
            removed = await TriggerRoleRepository(session).remove(trigger)
            await session.commit()
        if not removed:
            return f"{_label(trigger)} has no role mapped."
        return (
            f"{_label(trigger)} Alerts no longer ping a role. "
            "Members keep the role; nothing is removed from them."
        )

    async def show(self, roles: GuildRoles) -> str:
        mapped = await self._mappings()
        lines = []
        for trigger, label in TRIGGER_LABELS.items():
            role_id = mapped.get(trigger)
            if role_id is None:
                lines.append(f"{label}: not mapped")
                continue
            role = roles.role(role_id)
            if role is None:
                lines.append(f"{label}: a role that no longer exists (id {role_id}). Map it again.")
            elif not role.assignable:
                lines.append(f"{label}: <@&{role_id}> - I cannot manage it, so /pings cannot change it")
            else:
                lines.append(f"{label}: <@&{role_id}>")
        return "\n".join(lines)

    # -- /pings ----------------------------------------------------------------------------

    async def menu(self, roles: GuildRoles, member: PingMember) -> PingMenu:
        return await self._build(roles, member, None)

    async def apply(self, roles: GuildRoles, member: PingMember, wanted: set[str]) -> PingMenu:
        """Make the member's roles match `wanted` (the Triggers they want pinged for). A role the
        bot cannot add or remove is left as it is and named in the menu's problems."""
        return await self._build(roles, member, wanted)

    async def _build(
        self, roles: GuildRoles, member: PingMember, wanted: set[str] | None
    ) -> PingMenu:
        held = {role.id for role in member.roles}
        choices = []
        unavailable = []
        for trigger, role_id in (await self._mappings()).items():
            role = roles.role(role_id)
            label = _label(trigger)
            if role is None:
                unavailable.append(label)
                continue
            on = role.id in held
            problem = None
            if wanted is not None and (trigger in wanted) != on:
                problem = await _change(member, role, add=not on)
                if problem is None:
                    on = not on
            choices.append(PingChoice(trigger, label, role, on, problem))
        return PingMenu(tuple(choices), tuple(unavailable))


async def _change(member: PingMember, role: RoleInfo, *, add: bool) -> str | None:
    """Add or remove `role`; None when done, else why it could not be."""
    if not role.assignable:
        return f"I cannot manage <@&{role.id}>. Ask an admin to move my role above it."
    reason = "Alert pings chosen with /pings"
    try:
        if add:
            await member.add_roles(discord.Object(role.id), reason=reason)
        else:
            await member.remove_roles(discord.Object(role.id), reason=reason)
    except discord.HTTPException as exc:
        logger.warning("could not %s role %d: %s", "add" if add else "remove", role.id, exc.status)
        return f"Discord refused (error {exc.status}). Ask an admin to check my role."
    return None
