import discord
from discord import app_commands
from discord.ext import commands
from rocketstocks.data.stockdata import StockData
from rocketstocks.core.utils.formatting import ticker_string
import logging

logger = logging.getLogger(__name__)


class ConfirmDeleteView(discord.ui.View):
    """Simple Yes/No confirmation view for destructive actions."""

    def __init__(self, watchlist: str):
        super().__init__(timeout=30)
        self.watchlist = watchlist
        self.confirmed: bool | None = None

    @discord.ui.button(label="Yes, delete it", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.confirmed = True
        self.stop()
        await interaction.response.defer()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.confirmed = False
        self.stop()
        await interaction.response.defer()


class Watchlists(commands.Cog):
    """Cog for managing watchlists in the database"""
    def __init__(self, bot: commands.Bot, stock_data: StockData):
        self.bot = bot
        self.stock_data = stock_data
        self.watchlists = self.stock_data.watchlists

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    async def watchlist_options(self, interaction: discord.Interaction, current: str):
        """Autocomplete helper - return all watchlist names that match input 'current' """
        watchlists = await self.watchlists.get_watchlists(no_systemGenerated=False)
        return [
            app_commands.Choice(name=watchlist, value=watchlist)
            for watchlist in watchlists if current.lower() in watchlist.lower()
        ][:25]

    async def ticker_autocomplete(self, interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
        """Autocomplete last token in space-separated tickers string from DB."""
        tokens = current.upper().split()
        if not current.endswith(" ") and tokens:
            prefix_tokens = tokens[:-1]
            partial = tokens[-1]
        else:
            prefix_tokens = tokens
            partial = ""
        all_tickers = await self.stock_data.tickers.get_all_tickers()
        prefix_str = (" ".join(prefix_tokens) + " ") if prefix_tokens else ""
        return [
            app_commands.Choice(name=f"{prefix_str}{ticker}", value=f"{prefix_str}{ticker}")
            for ticker in all_tickers if ticker.startswith(partial)
        ][:25]

    watchlist_group = app_commands.Group(name="watchlist", description="View and manage your watchlists")

    @watchlist_group.command(name="add", description="Add tickers to the selected watchlist")
    @app_commands.describe(tickers="Tickers to add to watchlist (separated by spaces)")
    @app_commands.describe(watchlist="Which watchlist you want to make changes to")
    @app_commands.autocomplete(watchlist=watchlist_options)
    @app_commands.autocomplete(tickers=ticker_autocomplete)
    async def watchlist_add(self, interaction: discord.Interaction, tickers: str, watchlist: str):
        """Add valid input tickers to input watchlist"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/watchlist add function called by user {interaction.user.name}")

        new_tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers.upper())

        watchlist_id = watchlist if watchlist != 'personal' else str(interaction.user.id)

        if not await self.watchlists.validate_watchlist(watchlist_id=watchlist_id):
            await self.watchlists.create_watchlist(watchlist_id=watchlist_id, tickers=[], systemGenerated=False)

        symbols = await self.watchlists.get_watchlist_tickers(watchlist_id)
        duplicate_tickers = [x for x in new_tickers if x in symbols]
        added_tickers = [x for x in new_tickers if x not in symbols]
        merged = list(set(symbols + new_tickers))
        await self.watchlists.update_watchlist(watchlist_id=watchlist_id, tickers=merged)
        logger.info(f"Added tickers {added_tickers} to watchlist '{watchlist}'")
        logger.info(f"Watchlist '{watchlist}' has tickers {merged}")

        if added_tickers:
            message = f"Added {ticker_string(added_tickers)} to *{watchlist}* watchlist! ({len(merged)} tickers total)"
        else:
            message = f"No new tickers added to *{watchlist}* watchlist."
        if invalid_tickers:
            message += f" Invalid tickers: {ticker_string(invalid_tickers)}."
        if duplicate_tickers:
            message += f" Already on watchlist: {ticker_string(duplicate_tickers)}."

        await interaction.followup.send(message, ephemeral=True if watchlist_id.isdigit() else False)

    @watchlist_group.command(name="remove", description="Remove tickers from the selected watchlist")
    @app_commands.describe(tickers="Tickers to remove from watchlist (separated by spaces)")
    @app_commands.describe(watchlist="Which watchlist you want to make changes to")
    @app_commands.autocomplete(watchlist=watchlist_options)
    @app_commands.autocomplete(tickers=ticker_autocomplete)
    async def watchlist_remove(self, interaction: discord.Interaction, tickers: str, watchlist: str):
        """Remove valid input tickers from input watchlist"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/watchlist remove function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers.upper())

        watchlist_id = watchlist if watchlist != 'personal' else str(interaction.user.id)

        if not await self.watchlists.validate_watchlist(watchlist_id=watchlist_id):
            message = f"Watchlist *{watchlist}* does not exist. Use `/watchlist add`, `/watchlist remove` or `/watchlist set` to update this watchlist."
            await interaction.followup.send(message, ephemeral=True)
            return

        symbols = await self.watchlists.get_watchlist_tickers(watchlist_id)
        removed_tickers = [x for x in tickers if x in symbols]
        excess_tickers = [x for x in tickers if x not in symbols]
        remaining = [x for x in symbols if x not in tickers]
        await self.watchlists.update_watchlist(watchlist_id=watchlist_id, tickers=remaining)
        logger.info(f"Removed tickers {removed_tickers} from watchlist '{watchlist}'")
        logger.info(f"Watchlist '{watchlist}' has tickers {remaining}")

        if removed_tickers:
            message = f"Removed {ticker_string(removed_tickers)} from *{watchlist}* watchlist. ({len(remaining)} tickers total)"
        else:
            message = f"No tickers removed from *{watchlist}* watchlist."
        if excess_tickers:
            message += f" Not on watchlist: {ticker_string(excess_tickers)}."
        if invalid_tickers:
            message += f" Invalid tickers: {ticker_string(invalid_tickers)}."

        await interaction.followup.send(message, ephemeral=True if watchlist_id.isdigit() else False)

    @watchlist_group.command(name="view", description="List the tickers on the selected watchlist")
    @app_commands.describe(watchlist="Which watchlist you want to view")
    @app_commands.autocomplete(watchlist=watchlist_options)
    async def watchlist_view(self, interaction: discord.Interaction, watchlist: str):
        """Post contents of input watchlist to Discord"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/watchlist view function called by user {interaction.user.name}")

        watchlist_id = watchlist if watchlist != 'personal' else str(interaction.user.id)

        if await self.watchlists.validate_watchlist(watchlist_id=watchlist_id):
            tickers = await self.watchlists.get_watchlist_tickers(watchlist_id)
            if tickers:
                await interaction.followup.send(f"*{watchlist}* ({len(tickers)} tickers): {ticker_string(tickers)}", ephemeral=True if watchlist_id.isdigit() else False)
                logger.info(f"Watchlist '{watchlist}' has tickers {tickers}")
            else:
                await interaction.followup.send(f"No tickers on watchlist *{watchlist}*", ephemeral=True if watchlist_id.isdigit() else False)
                logger.info(f"Watchlist '{watchlist}' has no tickers")
        else:
            await interaction.followup.send(f"Watchlist *{watchlist}* does not exist. Use `/watchlist create` to make a new watchlist.", ephemeral=True)
            logger.info(f"Invalid watchlist input: '{watchlist}'")

    @watchlist_group.command(name="set", description="Overwrite a watchlist with the specified tickers")
    @app_commands.describe(tickers="Tickers to add to watchlist (separated by spaces)")
    @app_commands.describe(watchlist="Which watchlist you want to make changes to")
    @app_commands.autocomplete(watchlist=watchlist_options)
    @app_commands.autocomplete(tickers=ticker_autocomplete)
    async def watchlist_set(self, interaction: discord.Interaction, tickers: str, watchlist: str):
        """Set input watchlist to valid input tickers"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/watchlist set function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers.upper())

        watchlist_id = watchlist if watchlist != 'personal' else str(interaction.user.id)

        if not await self.watchlists.validate_watchlist(watchlist_id=watchlist_id):
            await self.watchlists.create_watchlist(watchlist_id=watchlist_id, tickers=[], systemGenerated=False)

        await self.watchlists.update_watchlist(watchlist_id=watchlist_id, tickers=tickers)
        logger.info(f"Watchlist '{watchlist}' set to {tickers}")

        if tickers:
            message = f"Set watchlist *{watchlist}* to {ticker_string(tickers)}. ({len(tickers)} tickers total)"
        else:
            message = "No changes made to watchlist."
        if invalid_tickers:
            message += f" Invalid tickers: {ticker_string(invalid_tickers)}."

        await interaction.followup.send(message, ephemeral=True if watchlist_id.isdigit() else False)

    @watchlist_group.command(name="create", description="Create a new watchlist with the specified tickers")
    @app_commands.describe(watchlist="Name of the watchlist to create")
    @app_commands.describe(tickers="Tickers to add to watchlist (separated by spaces)")
    @app_commands.autocomplete(tickers=ticker_autocomplete)
    async def watchlist_create(self, interaction: discord.Interaction, watchlist: str, tickers: str):
        """Create watchlist with valid input tickers"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/watchlist create function called by user {interaction.user.name}")

        watchlist_id = watchlist if watchlist != 'personal' else str(interaction.user.id)

        if not await self.watchlists.validate_watchlist(watchlist_id=watchlist_id):
            tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers.upper())
            await self.watchlists.create_watchlist(watchlist_id=watchlist_id, tickers=tickers, systemGenerated=False)
            logger.info(f"Watchlist '{watchlist}' set to {tickers}")

            if tickers:
                message = f"Created watchlist *{watchlist}* with {ticker_string(tickers)}. ({len(tickers)} tickers total)"
            else:
                message = f"Created empty watchlist *{watchlist}*."
            if invalid_tickers:
                message += f" Invalid tickers: {ticker_string(invalid_tickers)}."

            await interaction.followup.send(message, ephemeral=True if watchlist_id.isdigit() else False)
        else:
            message = f"Watchlist *{watchlist}* already exists. Use `/watchlist add`, `/watchlist remove` or `/watchlist set` to update this watchlist."
            await interaction.followup.send(message, ephemeral=True)

    @watchlist_group.command(name="delete", description="Delete a watchlist")
    @app_commands.describe(watchlist="Watchlist to delete")
    @app_commands.autocomplete(watchlist=watchlist_options)
    async def watchlist_delete(self, interaction: discord.Interaction, watchlist: str):
        """Delete input watchlist from database"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/watchlist delete function called by user {interaction.user.name}")

        if watchlist == "personal":
            await interaction.followup.send("Cannot delete a personal watchlist. Use /watchlist set to clear its contents if you wish", ephemeral=True)
            logger.info("Selected watchlist is 'personal' - cannot delete a personal watchlist")
            return

        watchlist_id = watchlist

        if not await self.watchlists.validate_watchlist(watchlist_id=watchlist_id):
            await interaction.followup.send(f"Watchlist *{watchlist}* does not exist.", ephemeral=True)
            logger.info(f"Delete attempted on non-existent watchlist '{watchlist}'")
            return

        view = ConfirmDeleteView(watchlist)
        await interaction.followup.send(
            f"Are you sure you want to delete watchlist *{watchlist}*? This cannot be undone.",
            view=view,
            ephemeral=True,
        )
        await view.wait()

        if view.confirmed:
            await self.watchlists.delete_watchlist(watchlist_id=watchlist_id)
            await interaction.followup.send(f"Deleted watchlist *{watchlist}*.", ephemeral=True)
            logger.info(f"Watchlist '{watchlist}' deleted")
        else:
            await interaction.followup.send("Deletion cancelled.", ephemeral=True)
            logger.info(f"Deletion of watchlist '{watchlist}' cancelled by user")

    @watchlist_group.command(name="list", description="List all available watchlists with their ticker counts")
    async def watchlist_list(self, interaction: discord.Interaction):
        """Show all watchlists and how many tickers each contains"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/watchlist list function called by user {interaction.user.name}")

        watchlists = await self.watchlists.get_watchlists(no_personal=True, no_systemGenerated=True)
        if not watchlists or watchlists == ["personal"]:
            await interaction.followup.send("No watchlists found. Use `/watchlist create` to create one.", ephemeral=True)
            return

        lines = []
        for wl_id in watchlists:
            if wl_id == "personal":
                continue
            tickers = await self.watchlists.get_watchlist_tickers(wl_id)
            count = len(tickers) if tickers else 0
            lines.append(f"**{wl_id}** — {count} ticker{'s' if count != 1 else ''}")

        if not lines:
            await interaction.followup.send("No public watchlists found.", ephemeral=True)
            return

        message = "**Available watchlists:**\n" + "\n".join(lines)
        await interaction.followup.send(message, ephemeral=False)

    @watchlist_group.command(name="rename", description="Rename an existing watchlist")
    @app_commands.describe(watchlist="Watchlist to rename")
    @app_commands.describe(new_name="New name for the watchlist")
    @app_commands.autocomplete(watchlist=watchlist_options)
    async def watchlist_rename(self, interaction: discord.Interaction, watchlist: str, new_name: str):
        """Rename a watchlist to a new name"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/watchlist rename function called by user {interaction.user.name}")

        if watchlist == "personal":
            await interaction.followup.send("Cannot rename a personal watchlist.", ephemeral=True)
            return

        success = await self.watchlists.rename_watchlist(old_id=watchlist, new_id=new_name)
        if success:
            await interaction.followup.send(f"Renamed watchlist *{watchlist}* to *{new_name}*.", ephemeral=False)
            logger.info(f"Watchlist '{watchlist}' renamed to '{new_name}'")
        else:
            if not await self.watchlists.validate_watchlist(watchlist):
                await interaction.followup.send(f"Watchlist *{watchlist}* does not exist.", ephemeral=True)
            else:
                await interaction.followup.send(f"Watchlist *{new_name}* already exists. Choose a different name.", ephemeral=True)


async def setup(bot):
    await bot.add_cog(Watchlists(bot, bot.stock_data))
