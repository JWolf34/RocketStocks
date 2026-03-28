import discord
from discord import app_commands
from discord.ext import commands
import logging

logger = logging.getLogger(__name__)

# Maps category keys to user-friendly metadata and the command names that belong to them.
# admin_only categories are hidden from non-administrator users.
CATEGORIES = {
    "reports": {
        "label": "📊 Reports",
        "description": "Generate detailed stock reports, research trending stocks, browse politician trades, and fetch news.",
        "commands": ["report", "news", "alert"],
    },
    "data": {
        "label": "📈 Stock Data",
        "description": "Fetch prices, quotes, earnings, financials, options chains, SEC filings, and more for any ticker.",
        "commands": ["data"],
    },
    "watchlists": {
        "label": "📋 Watchlists",
        "description": "Create and manage personal watchlists to track your favorite stocks.",
        "commands": ["watchlist"],
    },
    "paper_trading": {
        "label": "💸 Paper Trading",
        "description": "Trade stocks with virtual money. Buy, sell, manage your portfolio, track performance, and compete on the leaderboard.",
        "commands": ["trade"],
    },
    "notifications": {
        "label": "🔕 Notifications",
        "description": "Control how and when the bot sends you system event notifications.",
        "commands": ["notifications"],
        "admin_only": True,
    },
    "admin": {
        "label": "🔧 Server Admin",
        "description": "Configure channels, manage the Schwab API connection, and run admin diagnostics.",
        "commands": ["server", "admin", "schwab", "sync"],
        "admin_only": True,
    },
}


def build_overview_embed() -> discord.Embed:
    """Return the introductory embed shown when /help is first called."""
    embed = discord.Embed(
        title="RocketStocks",
        description=(
            "Real-time stock market alerts, screeners, and on-demand reports "
            "powered by technical analysis and sentiment data."
        ),
        color=discord.Color.blurple(),
    )
    embed.add_field(
        name="📊 Reports",
        value="Deep-dive stock reports, watchlist summaries, popularity trends, politician trades, and news.",
        inline=False,
    )
    embed.add_field(
        name="📈 Stock Data",
        value="Prices, real-time quotes, earnings history, financials, options chains, and SEC filings.",
        inline=False,
    )
    embed.add_field(
        name="📋 Watchlists",
        value="Create and manage personal watchlists to track your favorite stocks.",
        inline=False,
    )
    embed.add_field(
        name="💸 Paper Trading",
        value="Buy and sell stocks with virtual money, track your portfolio, and compete on the leaderboard.",
        inline=False,
    )
    embed.set_footer(text="Select a category below to explore available commands.")
    return embed


def build_category_embed(bot: commands.Bot, category_key: str) -> discord.Embed:
    """Return an embed listing every command in the given category.

    Command groups are expanded so each subcommand gets its own field.
    Top-level (ungrouped) commands each get a single field.
    """
    category = CATEGORIES[category_key]
    embed = discord.Embed(
        title=category["label"],
        description=category["description"],
        color=discord.Color.blurple(),
    )
    tree_commands = {cmd.name: cmd for cmd in bot.tree.get_commands()}
    for cmd_name in category["commands"]:
        cmd = tree_commands.get(cmd_name)
        if cmd is None:
            continue
        subcmds = getattr(cmd, "commands", None)
        if subcmds is not None:
            for subcmd in subcmds:
                embed.add_field(
                    name=f"/{cmd.name} {subcmd.name}",
                    value=subcmd.description or "No description.",
                    inline=False,
                )
        else:
            embed.add_field(
                name=f"/{cmd.name}",
                value=getattr(cmd, "description", None) or "No description.",
                inline=False,
            )
    return embed


class HelpCategorySelect(discord.ui.Select):
    def __init__(self, bot: commands.Bot, is_admin: bool):
        self.bot = bot
        options = [
            discord.SelectOption(label=meta["label"], value=key)
            for key, meta in CATEGORIES.items()
            if not (meta.get("admin_only") and not is_admin)
        ]
        super().__init__(placeholder="Select a category...", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        embed = build_category_embed(self.bot, self.values[0])
        await interaction.response.edit_message(embed=embed)


class HelpView(discord.ui.View):
    def __init__(self, bot: commands.Bot, is_admin: bool):
        super().__init__(timeout=180)
        self.add_item(HelpCategorySelect(bot, is_admin))


class Utils(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    @app_commands.command(name='sync', description='Sync slash commands with Discord (run after bot updates)')
    @app_commands.checks.has_permissions(administrator=True)
    async def sync(self, interaction: discord.Interaction):
        """Sync bot commands to Discord's servers. Use this after adding or removing an app command"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/sync command called by user {interaction.user.name}")
        await self.bot.tree.sync()
        await interaction.followup.send("Bot commands synced!", ephemeral=True)
        logger.info("Bot commands synced!")

    @app_commands.command(name="help", description="Explore what RocketStocks can do")
    async def help(self, interaction: discord.Interaction):
        """Post an interactive help message with a category dropdown."""
        logger.info(f"/help command called by user {interaction.user.name}")
        perms = getattr(interaction.user, "guild_permissions", None)
        is_admin = perms.administrator if perms is not None else False
        embed = build_overview_embed()
        view = HelpView(self.bot, is_admin)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


async def setup(bot):
    await bot.add_cog(Utils(bot))
