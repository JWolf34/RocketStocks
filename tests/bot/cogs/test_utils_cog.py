"""Tests for rocketstocks.bot.cogs.utils."""
import pytest
from unittest.mock import AsyncMock, MagicMock

import discord


def _make_bot():
    bot = MagicMock(name="Bot")
    bot.tree = MagicMock()
    bot.tree.get_commands.return_value = []
    bot.tree.sync = AsyncMock()
    return bot


def _make_cog(bot=None):
    from rocketstocks.bot.cogs.utils import Utils
    if bot is None:
        bot = _make_bot()
    return Utils(bot=bot), bot


def _make_interaction(is_admin=False):
    interaction = AsyncMock(name="Interaction")
    interaction.response = AsyncMock()
    interaction.followup = AsyncMock()
    perms = MagicMock(spec=discord.Permissions)
    perms.administrator = is_admin
    interaction.user = MagicMock()
    interaction.user.guild_permissions = perms
    interaction.user.name = "TestUser"
    return interaction


def _make_mock_group(name, description, subcommand_specs):
    """Mock object resembling an app_commands.Group (has a .commands list)."""
    group = MagicMock()
    group.name = name
    group.description = description
    subcmds = []
    for subcmd_name, subcmd_desc in subcommand_specs:
        subcmd = MagicMock()
        subcmd.name = subcmd_name
        subcmd.description = subcmd_desc
        subcmds.append(subcmd)
    group.commands = subcmds
    return group


def _make_mock_command(name, description):
    """Mock object resembling a top-level app_commands.Command (no .commands attribute)."""
    cmd = MagicMock(spec=["name", "description"])
    cmd.name = name
    cmd.description = description
    return cmd


class TestHelpCommand:
    @pytest.mark.asyncio
    async def test_help_sends_overview_ephemeral(self):
        from rocketstocks.bot.cogs.utils import HelpView
        cog, _ = _make_cog()
        interaction = _make_interaction(is_admin=False)

        await cog.help.callback(cog, interaction)

        interaction.response.send_message.assert_awaited_once()
        kwargs = interaction.response.send_message.call_args.kwargs
        assert kwargs.get("ephemeral") is True
        assert isinstance(kwargs.get("embed"), discord.Embed)
        assert isinstance(kwargs.get("view"), HelpView)

    @pytest.mark.asyncio
    async def test_help_overview_embed_title(self):
        cog, _ = _make_cog()
        interaction = _make_interaction()

        await cog.help.callback(cog, interaction)

        embed = interaction.response.send_message.call_args.kwargs["embed"]
        assert embed.title == "RocketStocks"

    @pytest.mark.asyncio
    async def test_help_overview_embed_has_footer(self):
        cog, _ = _make_cog()
        interaction = _make_interaction()

        await cog.help.callback(cog, interaction)

        embed = interaction.response.send_message.call_args.kwargs["embed"]
        assert embed.footer.text is not None

    @pytest.mark.asyncio
    async def test_help_admin_sees_admin_categories(self):
        from rocketstocks.bot.cogs.utils import HelpCategorySelect
        cog, _ = _make_cog()
        interaction = _make_interaction(is_admin=True)

        await cog.help.callback(cog, interaction)

        view = interaction.response.send_message.call_args.kwargs["view"]
        select = next(c for c in view.children if isinstance(c, HelpCategorySelect))
        option_values = [o.value for o in select.options]
        assert "notifications" in option_values
        assert "admin" in option_values

    @pytest.mark.asyncio
    async def test_help_regular_user_no_admin_categories(self):
        from rocketstocks.bot.cogs.utils import HelpCategorySelect
        cog, _ = _make_cog()
        interaction = _make_interaction(is_admin=False)

        await cog.help.callback(cog, interaction)

        view = interaction.response.send_message.call_args.kwargs["view"]
        select = next(c for c in view.children if isinstance(c, HelpCategorySelect))
        option_values = [o.value for o in select.options]
        assert "notifications" not in option_values
        assert "admin" not in option_values

    @pytest.mark.asyncio
    async def test_help_regular_user_sees_public_categories(self):
        from rocketstocks.bot.cogs.utils import HelpCategorySelect
        cog, _ = _make_cog()
        interaction = _make_interaction(is_admin=False)

        await cog.help.callback(cog, interaction)

        view = interaction.response.send_message.call_args.kwargs["view"]
        select = next(c for c in view.children if isinstance(c, HelpCategorySelect))
        option_values = [o.value for o in select.options]
        assert "reports" in option_values
        assert "data" in option_values
        assert "watchlists" in option_values


class TestBuildOverviewEmbed:
    def test_returns_embed_with_correct_title(self):
        from rocketstocks.bot.cogs.utils import build_overview_embed
        embed = build_overview_embed()
        assert embed.title == "RocketStocks"

    def test_has_fields_for_public_categories(self):
        from rocketstocks.bot.cogs.utils import build_overview_embed
        embed = build_overview_embed()
        field_names = [f.name for f in embed.fields]
        assert any("Reports" in name for name in field_names)
        assert any("Stock Data" in name for name in field_names)
        assert any("Watchlists" in name for name in field_names)

    def test_does_not_include_admin_category_fields(self):
        from rocketstocks.bot.cogs.utils import build_overview_embed
        embed = build_overview_embed()
        field_names = [f.name for f in embed.fields]
        assert not any("Admin" in name for name in field_names)
        assert not any("Notifications" in name for name in field_names)


class TestBuildCategoryEmbed:
    def test_group_commands_produce_subcommand_fields(self):
        from rocketstocks.bot.cogs.utils import build_category_embed
        bot = _make_bot()
        data_group = _make_mock_group("data", "Data commands", [
            ("price", "Get price data"),
            ("quote", "Get real-time quote"),
        ])
        bot.tree.get_commands.return_value = [data_group]

        embed = build_category_embed(bot, "data")

        field_names = [f.name for f in embed.fields]
        assert "/data price" in field_names
        assert "/data quote" in field_names

    def test_top_level_command_produces_single_field(self):
        from rocketstocks.bot.cogs.utils import build_category_embed
        bot = _make_bot()
        news_cmd = _make_mock_command("news", "Fetch news articles")
        bot.tree.get_commands.return_value = [news_cmd]

        embed = build_category_embed(bot, "reports")

        field_names = [f.name for f in embed.fields]
        assert "/news" in field_names

    def test_missing_commands_skipped_gracefully(self):
        from rocketstocks.bot.cogs.utils import build_category_embed
        bot = _make_bot()
        bot.tree.get_commands.return_value = []

        embed = build_category_embed(bot, "data")

        assert len(embed.fields) == 0

    def test_embed_title_matches_category_label(self):
        from rocketstocks.bot.cogs.utils import build_category_embed, CATEGORIES
        bot = _make_bot()
        bot.tree.get_commands.return_value = []

        embed = build_category_embed(bot, "watchlists")

        assert embed.title == CATEGORIES["watchlists"]["label"]

    def test_mixed_groups_and_commands_in_same_category(self):
        from rocketstocks.bot.cogs.utils import build_category_embed
        bot = _make_bot()
        report_group = _make_mock_group("report", "Report commands", [
            ("ticker", "Stock report"),
        ])
        news_cmd = _make_mock_command("news", "Fetch news")
        bot.tree.get_commands.return_value = [report_group, news_cmd]

        embed = build_category_embed(bot, "reports")

        field_names = [f.name for f in embed.fields]
        assert "/report ticker" in field_names
        assert "/news" in field_names


class TestHelpCategorySelectCallback:
    @pytest.mark.asyncio
    async def test_callback_edits_message_with_embed(self):
        from rocketstocks.bot.cogs.utils import HelpCategorySelect
        bot = _make_bot()
        select = HelpCategorySelect(bot, is_admin=False)
        select._values = ["reports"]

        interaction = AsyncMock()
        interaction.response = AsyncMock()

        await select.callback(interaction)

        interaction.response.edit_message.assert_awaited_once()
        kwargs = interaction.response.edit_message.call_args.kwargs
        assert isinstance(kwargs.get("embed"), discord.Embed)

    @pytest.mark.asyncio
    async def test_callback_embed_title_matches_selected_category(self):
        from rocketstocks.bot.cogs.utils import HelpCategorySelect, CATEGORIES
        bot = _make_bot()
        select = HelpCategorySelect(bot, is_admin=False)
        select._values = ["watchlists"]

        interaction = AsyncMock()
        interaction.response = AsyncMock()

        await select.callback(interaction)

        embed = interaction.response.edit_message.call_args.kwargs["embed"]
        assert embed.title == CATEGORIES["watchlists"]["label"]
