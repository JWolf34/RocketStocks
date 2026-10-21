"""The slash commands the bot registers: `/pings` for every member and the admin `/setup`.

Handlers are thin: each hands its arguments to `PingRoles` or `SchwabAuthFlow` and sends the
text back. `/setup` is hidden from members who are not server administrators (Discord's
default member permissions; the Schwab sign-in then checks the owner or configured admin on
its own). Administrator is the one permission both John and the operator hold and no member
has by default; a server admin can still widen or narrow it in Server Settings, Integrations.
"""

from typing import cast

import discord
from discord import app_commands

from rs_bot.pings import (
    TRIGGER_LABELS,
    DiscordGuildRoles,
    PingMember,
    PingMenu,
    PingRoles,
    role_info,
)
from rs_bot.schwab_auth import SchwabAuthFlow, add_schwab_command

SETUP_PERMISSIONS = discord.Permissions(administrator=True)
MENU_TIMEOUT_SECONDS = 300.0

_TRIGGER_CHOICES = [app_commands.Choice(name=label, value=key) for key, label in TRIGGER_LABELS.items()]


def _guild_roles(interaction: discord.Interaction) -> DiscordGuildRoles:
    return DiscordGuildRoles(lambda: interaction.guild)


def _member(interaction: discord.Interaction) -> PingMember:
    # Both commands are guild_only, so the user is a member of the guild.
    return cast(PingMember, interaction.user)


class _PingsSelect(discord.ui.Select):
    def __init__(self, pings: PingRoles, menu: PingMenu) -> None:
        options = [
            discord.SelectOption(
                label=choice.label, value=choice.trigger, description=choice.role.name, default=choice.on
            )
            for choice in menu.choices
        ]
        super().__init__(
            placeholder="Choose your Alert pings", min_values=0, max_values=len(options), options=options
        )
        self._pings = pings

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        menu = await self._pings.apply(
            _guild_roles(interaction), _member(interaction), set(self.values)
        )
        await interaction.edit_original_response(content=menu.text, view=_view(self._pings, menu))


def _view(pings: PingRoles, menu: PingMenu) -> discord.ui.View | None:
    if not menu.choices:
        return None
    view = discord.ui.View(timeout=MENU_TIMEOUT_SECONDS)
    view.add_item(_PingsSelect(pings, menu))
    return view


def add_pings_command(tree: app_commands.CommandTree, pings: PingRoles) -> None:
    @tree.command(name="pings", description="Choose which Alerts ping you")
    @app_commands.guild_only()
    async def pings_command(interaction: discord.Interaction) -> None:
        menu = await pings.menu(_guild_roles(interaction), _member(interaction))
        view = _view(pings, menu)
        if view is None:
            await interaction.response.send_message(menu.text, ephemeral=True)
        else:
            await interaction.response.send_message(menu.text, view=view, ephemeral=True)


def add_roles_commands(setup: app_commands.Group, pings: PingRoles) -> None:
    roles = app_commands.Group(name="roles", description="Which role each Alert pings", parent=setup)

    @roles.command(name="add", description="Make a Trigger's Alerts ping an existing role")
    @app_commands.describe(trigger="The Trigger", role="An existing role below mine")
    @app_commands.choices(trigger=_TRIGGER_CHOICES)
    async def add(interaction: discord.Interaction, trigger: str, role: discord.Role) -> None:
        await interaction.response.send_message(
            await pings.add(trigger, role_info(role)), ephemeral=True
        )

    @roles.command(name="remove", description="Stop a Trigger's Alerts pinging a role")
    @app_commands.describe(trigger="The Trigger")
    @app_commands.choices(trigger=_TRIGGER_CHOICES)
    async def remove(interaction: discord.Interaction, trigger: str) -> None:
        await interaction.response.send_message(await pings.remove(trigger), ephemeral=True)

    @roles.command(name="show", description="Show which role each Trigger pings")
    async def show(interaction: discord.Interaction) -> None:
        await interaction.response.send_message(
            await pings.show(_guild_roles(interaction)), ephemeral=True
        )


def register_commands(
    tree: app_commands.CommandTree,
    *,
    flow: SchwabAuthFlow,
    pings: PingRoles,
    admin_user_id: int | None,
) -> None:
    setup = app_commands.Group(
        name="setup",
        description="Bot setup",
        guild_only=True,
        default_permissions=SETUP_PERMISSIONS,
    )
    add_schwab_command(setup, flow, admin_user_id)
    add_roles_commands(setup, pings)
    tree.add_command(setup)
    add_pings_command(tree, pings)
