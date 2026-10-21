"""`/pings` and `/setup roles`: mapping a Trigger to a role, and members opting in.

The mapping is real (the test database); the server's roles and the member are the stand-ins
in rs_bot.testing. The slash commands are driven through their callbacks with a stand-in
interaction, as test_schwab_auth does.
"""

from types import SimpleNamespace

import discord
import pytest
from discord import app_commands

from rs_bot.commands import SETUP_PERMISSIONS, register_commands
from rs_bot.pings import NO_MAPPINGS, PingRoles
from rs_bot.render import POPULARITY_SURGE
from rs_bot.schwab_auth import SchwabAuthFlow
from rs_bot.testing import FakeMember, FakeRoles
from rs_persistence import TriggerRoleRepository

SURGE_ROLE = 501
OTHER_ROLE = 502


@pytest.fixture
def pings(session_factory) -> PingRoles:
    return PingRoles(session_factory)


@pytest.fixture
def roles() -> FakeRoles:
    return FakeRoles()


async def _mapped(session_factory) -> dict[str, int]:
    async with session_factory() as s:
        return await TriggerRoleRepository(s).all()


async def _map(session_factory, trigger: str = POPULARITY_SURGE, role_id: int = SURGE_ROLE) -> None:
    async with session_factory() as s:
        await TriggerRoleRepository(s).set(trigger, role_id)
        await s.commit()


# -- /setup roles add, remove, show ------------------------------------------------------


async def test_add_maps_the_trigger_to_the_role_and_it_survives(pings, roles, session_factory):
    role = roles.add(SURGE_ROLE, "Surge watchers")

    reply = await pings.add(POPULARITY_SURGE, role)

    assert f"<@&{SURGE_ROLE}>" in reply
    assert await _mapped(session_factory) == {POPULARITY_SURGE: SURGE_ROLE}


async def test_add_again_replaces_the_role(pings, roles, session_factory):
    await pings.add(POPULARITY_SURGE, roles.add(SURGE_ROLE))
    await pings.add(POPULARITY_SURGE, roles.add(OTHER_ROLE))

    assert await _mapped(session_factory) == {POPULARITY_SURGE: OTHER_ROLE}


async def test_add_refuses_a_role_the_bot_cannot_manage_and_stores_nothing(pings, roles, session_factory):
    above_the_bot = roles.add(SURGE_ROLE, "Mods", assignable=False)

    reply = await pings.add(POPULARITY_SURGE, above_the_bot)

    assert "I cannot manage" in reply
    assert "Manage Roles" in reply
    assert await _mapped(session_factory) == {}


async def test_add_warns_when_the_role_cannot_be_mentioned(pings, roles, session_factory):
    reply = await pings.add(POPULARITY_SURGE, roles.add(SURGE_ROLE, mentionable=False))

    assert "not mentionable" in reply
    assert await _mapped(session_factory) == {POPULARITY_SURGE: SURGE_ROLE}


async def test_add_refuses_a_name_that_is_not_a_trigger(pings, roles, session_factory):
    reply = await pings.add("volume_threshold", roles.add(SURGE_ROLE))

    assert "not a Trigger" in reply
    assert await _mapped(session_factory) == {}


async def test_remove_drops_the_mapping_and_says_when_there_was_none(pings, roles, session_factory):
    await _map(session_factory)

    assert "no longer ping" in await pings.remove(POPULARITY_SURGE)
    assert await _mapped(session_factory) == {}
    assert "no role mapped" in await pings.remove(POPULARITY_SURGE)


async def test_show_lists_the_role_or_says_the_trigger_is_not_mapped(pings, roles, session_factory):
    assert await pings.show(roles) == "Popularity surge: not mapped"

    roles.add(SURGE_ROLE)
    await _map(session_factory)

    assert await pings.show(roles) == f"Popularity surge: <@&{SURGE_ROLE}>"


async def test_show_flags_a_deleted_role_and_one_the_bot_can_no_longer_manage(pings, roles, session_factory):
    roles.add(SURGE_ROLE)
    await _map(session_factory)

    roles.add(SURGE_ROLE, assignable=False)  # moved above the bot since it was mapped
    assert "I cannot manage it" in await pings.show(roles)

    roles.delete(SURGE_ROLE)
    shown = await pings.show(roles)
    assert "no longer exists" in shown
    assert str(SURGE_ROLE) in shown


# -- /pings: toggling ----------------------------------------------------------------------


async def test_the_menu_shows_each_mapped_trigger_and_whether_the_member_has_its_role(
    pings, roles, session_factory
):
    roles.add(SURGE_ROLE, "Surge watchers")
    await _map(session_factory)

    off = await pings.menu(roles, FakeMember())
    on = await pings.menu(roles, FakeMember(role_ids={SURGE_ROLE}))

    assert [(c.trigger, c.on) for c in off.choices] == [(POPULARITY_SURGE, False)]
    assert [(c.trigger, c.on) for c in on.choices] == [(POPULARITY_SURGE, True)]
    assert off.choices[0].role.name == "Surge watchers"


async def test_the_menu_says_so_when_no_trigger_has_a_role(pings, roles):
    menu = await pings.menu(roles, FakeMember())

    assert menu.choices == ()
    assert menu.text == NO_MAPPINGS


async def test_choosing_a_trigger_gives_the_member_its_role_and_clearing_it_takes_it_away(
    pings, roles, session_factory
):
    roles.add(SURGE_ROLE)
    await _map(session_factory)
    member = FakeMember(role_ids={999})  # an unrelated role of theirs

    on = await pings.apply(roles, member, {POPULARITY_SURGE})
    assert member.role_ids == {999, SURGE_ROLE}
    assert [c.on for c in on.choices] == [True]
    assert [c.problem for c in on.choices] == [None]

    off = await pings.apply(roles, member, set())
    assert member.role_ids == {999}
    assert [c.on for c in off.choices] == [False]


async def test_applying_what_the_member_already_has_changes_nothing(pings, roles, session_factory):
    roles.add(SURGE_ROLE)
    await _map(session_factory)
    member = FakeMember(role_ids={SURGE_ROLE}, forbidden={SURGE_ROLE})  # any change would be refused

    menu = await pings.apply(roles, member, {POPULARITY_SURGE})

    assert member.role_ids == {SURGE_ROLE}
    assert menu.choices[0].problem is None


async def test_a_role_the_bot_cannot_manage_is_left_alone_and_named(pings, roles, session_factory):
    roles.add(SURGE_ROLE, assignable=False)
    roles.add(OTHER_ROLE)
    await _map(session_factory)
    await _map(session_factory, "other", OTHER_ROLE)
    member = FakeMember()

    menu = await pings.apply(roles, member, {POPULARITY_SURGE, "other"})

    assert member.role_ids == {OTHER_ROLE}  # the one it can manage still changed
    by_trigger = {c.trigger: c for c in menu.choices}
    assert by_trigger[POPULARITY_SURGE].on is False
    assert f"<@&{SURGE_ROLE}>" in (by_trigger[POPULARITY_SURGE].problem or "")
    assert by_trigger["other"].problem is None
    assert "Could not change Popularity surge" in menu.text


async def test_a_refusal_from_discord_is_reported_and_the_member_is_unchanged(pings, roles, session_factory):
    roles.add(SURGE_ROLE)
    await _map(session_factory)
    member = FakeMember(forbidden={SURGE_ROLE})

    menu = await pings.apply(roles, member, {POPULARITY_SURGE})

    assert member.role_ids == set()
    assert menu.choices[0].on is False
    assert "403" in (menu.choices[0].problem or "")


async def test_a_mapped_role_that_was_deleted_is_not_offered_and_the_menu_says_so(
    pings, roles, session_factory
):
    roles.add(SURGE_ROLE)
    await _map(session_factory)
    roles.delete(SURGE_ROLE)
    member = FakeMember()

    menu = await pings.apply(roles, member, {POPULARITY_SURGE})

    assert menu.choices == ()
    assert menu.unavailable == ("Popularity surge",)
    assert "no longer exists" in menu.text
    assert member.role_ids == set()


# -- the slash commands --------------------------------------------------------------------


class Replies:
    """Collects what a handler says; the first message and later edits alike."""

    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.deferred = False

    async def send_message(self, content, **kwargs):
        self.sent.append({"content": content, **kwargs})

    async def defer(self, **kwargs):
        self.deferred = True

    async def edit_original_response(self, **kwargs):
        self.sent.append(kwargs)


def _discord_role(role_id: int, name: str = "role", *, assignable: bool = True, mentionable: bool = True):
    return SimpleNamespace(id=role_id, name=name, mentionable=mentionable, is_assignable=lambda: assignable)


def _interaction(member: FakeMember, *server_roles) -> tuple[SimpleNamespace, Replies]:
    replies = Replies()
    by_id = {role.id: role for role in server_roles}
    return (
        SimpleNamespace(
            user=member,
            guild=SimpleNamespace(get_role=by_id.get),
            response=replies,
            edit_original_response=replies.edit_original_response,
        ),
        replies,
    )


@pytest.fixture
def tree(session_factory) -> app_commands.CommandTree:
    tree = app_commands.CommandTree(discord.Client(intents=discord.Intents.default()))
    register_commands(
        tree, flow=SchwabAuthFlow(session_factory), pings=PingRoles(session_factory), admin_user_id=None
    )
    return tree


def test_setup_is_hidden_from_members_and_pings_is_not(tree):
    setup = tree.get_command("setup")
    assert setup.default_permissions == SETUP_PERMISSIONS
    assert SETUP_PERMISSIONS.administrator
    assert tree.get_command("pings").default_permissions is None
    assert setup.guild_only and tree.get_command("pings").guild_only
    assert {c.name for c in setup.commands} == {"schwab", "roles"}
    assert {c.name for c in setup.get_command("roles").commands} == {"add", "remove", "show"}


async def test_setup_roles_add_remove_and_show_reply_privately(tree, session_factory):
    roles = tree.get_command("setup").get_command("roles")
    interaction, replies = _interaction(FakeMember(), _discord_role(SURGE_ROLE, "Surge watchers"))

    await roles.get_command("add").callback(
        interaction, trigger=POPULARITY_SURGE, role=_discord_role(SURGE_ROLE)
    )
    await roles.get_command("show").callback(interaction)
    await roles.get_command("remove").callback(interaction, trigger=POPULARITY_SURGE)

    assert [r["ephemeral"] for r in replies.sent] == [True, True, True]
    assert f"<@&{SURGE_ROLE}>" in replies.sent[0]["content"]
    assert replies.sent[1]["content"] == f"Popularity surge: <@&{SURGE_ROLE}>"
    assert await _mapped(session_factory) == {}


async def test_setup_roles_add_refuses_an_unmanageable_role(tree, session_factory):
    add = tree.get_command("setup").get_command("roles").get_command("add")
    interaction, replies = _interaction(FakeMember())

    await add.callback(interaction, trigger=POPULARITY_SURGE, role=_discord_role(SURGE_ROLE, assignable=False))

    assert "I cannot manage" in replies.sent[0]["content"]
    assert await _mapped(session_factory) == {}


async def test_the_trigger_choices_are_the_known_triggers(tree):
    add = tree.get_command("setup").get_command("roles").get_command("add")
    (trigger,) = [p for p in add.parameters if p.name == "trigger"]
    assert [(c.name, c.value) for c in trigger.choices] == [(("Popularity surge"), POPULARITY_SURGE)]


async def test_pings_opens_a_private_menu_with_the_members_current_choices_selected(tree, session_factory):
    await _map(session_factory)
    member = FakeMember(role_ids={SURGE_ROLE})
    interaction, replies = _interaction(member, _discord_role(SURGE_ROLE, "Surge watchers"))

    await tree.get_command("pings").callback(interaction)

    (sent,) = replies.sent
    assert sent["ephemeral"] is True
    (select,) = sent["view"].children
    assert [(o.value, o.default, o.description) for o in select.options] == [
        (POPULARITY_SURGE, True, "Surge watchers")
    ]
    assert select.min_values == 0  # clearing every choice is allowed


async def test_picking_in_the_menu_toggles_the_role_and_redraws_the_menu(tree, session_factory):
    await _map(session_factory)
    member = FakeMember()
    interaction, replies = _interaction(member, _discord_role(SURGE_ROLE, "Surge watchers"))
    await tree.get_command("pings").callback(interaction)
    (select,) = replies.sent[0]["view"].children

    select._values = [POPULARITY_SURGE]
    await select.callback(interaction)
    assert member.role_ids == {SURGE_ROLE}
    (redrawn,) = replies.sent[1]["view"].children
    assert [o.default for o in redrawn.options] == [True]

    redrawn._values = []
    await redrawn.callback(interaction)
    assert member.role_ids == set()
    assert [o.default for o in replies.sent[2]["view"].children[0].options] == [False]


async def test_pings_says_who_to_ask_when_nothing_is_mapped(tree):
    interaction, replies = _interaction(FakeMember())

    await tree.get_command("pings").callback(interaction)

    assert replies.sent == [{"content": NO_MAPPINGS, "ephemeral": True}]
