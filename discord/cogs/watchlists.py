import discord
from discord import app_commands
from discord.ext import commands
from stock_data import StockData
import logging

# Logging configuration
logger = logging.getLogger(__name__)

class Watchlists(commands.Cog):
    """Cog for managing watchlists in the database"""
    def __init__(self, bot:commands.Bot, stock_data:StockData):
        self.bot = bot
        self.stock_data = stock_data
        self.watchlists = self.stock_data.watchlists

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    def ticker_string(self, tickers:list):
        return f"`{", ".join(tickers)}`"

    async def watchlist_options(self, interaction: discord.Interaction, current: str):
        """Autocomplete helper - return all watchlist names that match input 'current' """
        watchlists = self.watchlists.get_watchlists(no_systemGenerated=False)
        return [
            app_commands.Choice(name = watchlist, value= watchlist)
            for watchlist in watchlists if current.lower() in watchlist.lower()
        ]

    @app_commands.command(name = "add-tickers", description= "Add tickers to the selected watchlist",)
    @app_commands.describe(tickers = "Ticker to add to watchlist (separated by spaces)")
    @app_commands.describe(watchlist = "Which watchlist you want to make changes to")
    @app_commands.autocomplete(watchlist=watchlist_options,)
    async def addtickers(self, interaction: discord.Interaction, tickers: str, watchlist: str):
        """Add valid input tickers to input watchlist"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/add-tickers function called by user {interaction.user.name}")
        
        # Parse list from ticker input and identify invalid tickers
        tickers, invalid_tickers = await self.stock_data.parse_valid_tickers(tickers.upper())

        # If personal watchlist, watchlist ID is user ID
        watchlist_id = watchlist if watchlist != 'personal' else str(interaction.user.id)

        # Confirm watchlist exists, otherwise create it
        if not self.watchlists.validate_watchlist(watchlist_id=watchlist_id):
            self.watchlists.create_watchlist(watchlist_id=watchlist_id, tickers=[], systemGenerated=False)

        # Update watchlist with new tickers
        symbols = self.watchlists.get_watchlist_tickers(watchlist_id)
        duplicate_tickers = [x for x in tickers if x in symbols]
        tickers = list(set(symbols + tickers))
        self.watchlists.update_watchlist(watchlist_id=watchlist_id, tickers=tickers)
        logger.info(f"Added tickers {tickers} to watchlist '{watchlist}'")
        logger.info(f"Watchlist '{watchlist}' has tickers {tickers}")
        
        # Generate message
        message = ''
        if tickers:
            message = f"Added {self.ticker_string(tickers)} to *{watchlist}* watchlist!"
        else:
            message = f"No tickers added to *{watchlist}* watchlist."
        if invalid_tickers:
                message += f" Invalid tickers: {self.ticker_string(invalid_tickers)}."
        if duplicate_tickers:
            message += f" Duplicate tickers: {self.ticker_string(duplicate_tickers)}"
        
        await interaction.followup.send(message, ephemeral=True if watchlist_id.isdigit() else False)
        
    @app_commands.command(name = "remove-tickers", description= "Remove tickers from the selected watchlist",)
    @app_commands.describe(tickers = "Tickers to remove from watchlist (separated by spaces)")
    @app_commands.describe(watchlist = "Which watchlist you want to make changes to")
    @app_commands.autocomplete(watchlist=watchlist_options,)
    async def removetickers(self, interaction: discord.Interaction, tickers: str, watchlist: str):
        """Remove valid input tickers to input watchlist"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/remove-tickers function called by user {interaction.user.name}")
        
        # Parse list from ticker input and identify invalid tickers
        tickers, invalid_tickers = await self.stock_data.parse_valid_tickers(tickers.upper())

        # If personal watchlist, watchlist ID is user ID
        watchlist_id = watchlist if watchlist != 'personal' else str(interaction.user.id)

        # Confirm watchlist exists, otherwise create it
        if not self.watchlists.validate_watchlist(watchlist_id=watchlist_id):
            message = f"Watchlist *{watchlist}* does not exist. Use `/add-tickers`, `/remove-tickers` or `/set-watchlist` to update this watchlist."
            await interaction.followup.send(message, ephemeral=True)
            return


        # Update watchlist with new tickers
        symbols = self.watchlists.get_watchlist_tickers(watchlist_id)
        removed_tickers = [x for x in tickers if x in symbols]
        excess_tickers = [x for x in tickers if x not in symbols]
        tickers = [x for x in symbols if x not in tickers]
        self.watchlists.update_watchlist(watchlist_id=watchlist_id, tickers=tickers)
        logger.info(f"Removed tickers {removed_tickers} from watchlist '{watchlist}'")
        logger.info(f"Watchlist '{watchlist}' has tickers {tickers}")
        
        # Generate message
        message = ''
        if removed_tickers:
            message = f"Removed {self.ticker_string(removed_tickers)} from *{watchlist}* watchlist."
        else:
            message = f"No tickers removed from *{watchlist}* watchlist."
        if excess_tickers:
            message += f"Tickers not on watchlist: {self.ticker_string(excess_tickers)}."
        if invalid_tickers:
                message += f" Invalid tickers: {self.ticker_string(invalid_tickers)}."
        
        await interaction.followup.send(message, ephemeral=True if watchlist_id.isdigit() else False)
            

    @app_commands.command(name = "watchlist", description= "List the tickers on the selected watchlist",)
    @app_commands.describe(watchlist = "Which watchlist you want to make changes to")
    @app_commands.autocomplete(watchlist=watchlist_options,)
    async def watchlist(self, interaction: discord.Interaction, watchlist: str):
        """Post contents of input watchlist to Discord"""
        logger.info(f"/watchlist function called by user {interaction.user.name}")

        # If personal watchlist, watchlist ID is user ID
        watchlist_id = watchlist if watchlist != 'personal' else str(interaction.user.id)

        # Validate watchlist
        if self.watchlists.validate_watchlist(watchlist_id=watchlist_id):
            tickers = self.watchlists.get_watchlist_tickers(watchlist_id)
            if tickers:
                await interaction.response.send_message(f"*{watchlist}*: {self.ticker_string(tickers)}", ephemeral=True if watchlist_id.isdigit() else False)
                logger.info(f"Watchlist '{watchlist}' has tickers {tickers}")
            else:
                await interaction.response.send_message(f"No tickers on watchlist *{watchlist}*", ephemeral=True if watchlist_id.isdigit() else False)
                logger.info(f"Watchlist '{watchlist}' has no tickers")
        else:
            await interaction.response.send_message(f"Watchlist *{watchlist}* does not exist. Use `/create-watchlist` to make a new watchlist.", ephemeral=True)
            logger.info(f"Invalid watchlist input: '{watchlist}'")

    @app_commands.command(name = "set-watchlist", description= "Overwrite a watchlist with the specified tickers",)
    @app_commands.describe(tickers = "Tickers to add to watchlist (separated by spaces)")
    @app_commands.describe(watchlist = "Which watchlist you want to make changes to")
    @app_commands.autocomplete(watchlist=watchlist_options,)
    async def set_watchlist(self, interaction: discord.Interaction, tickers: str, watchlist: str):
        """Set input watchlist to valid input tickers"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/set-tickers function called by user {interaction.user.name}")
        
        # Parse list from ticker input and identify invalid tickers
        tickers, invalid_tickers = await self.stock_data.parse_valid_tickers(tickers.upper())

        # If personal watchlist, watchlist ID is user ID
        watchlist_id = watchlist if watchlist != 'personal' else str(interaction.user.id)

        # Confirm watchlist exists, otherwise create it
        if not self.watchlists.validate_watchlist(watchlist_id=watchlist_id):
            self.watchlists.create_watchlist(watchlist_id=watchlist_id, tickers=[], systemGenerated=False)

        # Update watchlist with new tickers
        self.watchlists.update_watchlist(watchlist_id=watchlist_id, tickers=tickers)
        logger.info(f"Watchlist '{watchlist}' set to {tickers}")
        
        # Generate message
        message = ''
        if tickers:
            message = f"Set watchlist *{watchlist}* to {self.ticker_string(tickers)}."
        else:
            message = "No changes made to watchlist."
        if invalid_tickers:
                message += f" Invalid tickers: {self.ticker_string(invalid_tickers)}."
        
        await interaction.followup.send(message, ephemeral=True if watchlist_id.isdigit() else False)
        
    @app_commands.command(name = "create-watchlist", description= "List the tickers on the selected watchlist",)
    @app_commands.describe(watchlist = "Name of the watchlist to create")
    @app_commands.describe(tickers = "Tickers to add to watchlist (separated by spaces)")
    async def create_watchlist(self, interaction: discord.Interaction, watchlist: str, tickers: str):
        """Create watchlist with valid input tickets"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/create-watchlist function called by user {interaction.user.name}")
        
        # If personal watchlist, watchlist ID is user ID
        watchlist_id = watchlist if watchlist != 'personal' else str(interaction.user.id)

        # Confirm watchlist does not exist
        if not self.watchlists.validate_watchlist(watchlist_id=watchlist_id):
            # Parse list from ticker input and identify invalid tickers
            tickers, invalid_tickers = await self.stock_data.parse_valid_tickers(tickers.upper())
            self.watchlists.create_watchlist(watchlist_id=watchlist, tickers=tickers, systemGenerated=False)
            logger.info(f"Watchlist '{watchlist}' set to {tickers}")

            # Generate message
            message = ''
            if tickers:
                message = f"Created watchlist *{watchlist}* with tickers {self.ticker_string(tickers)}."
            else:
                message = "No watchlist created."
            if invalid_tickers:
                    message += f" Invalid tickers: {self.ticker_string(invalid_tickers)}."
            
            await interaction.followup.send(message, ephemeral=True if watchlist_id.isdigit() else False)
        # Watchlist already exists
        else:
            message = f"Watchlist *{watchlist}* already exists. Use `/add-tickers`, `/remove-tickers` or `/set-watchlist` to update this watchlist."
            await interaction.followup.send(message, ephemeral=True)

    @app_commands.command(name = "delete-watchlist", description= "Delete a watchlist",)
    @app_commands.describe(watchlist = "Watchlist to delete")
    @app_commands.autocomplete(watchlist=watchlist_options,)
    async def delete_watchlist(self, interaction: discord.Interaction, watchlist: str):
        """Delete input watchlist from dataase"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/delete-watchlist function called by user {interaction.user.name}")

        if watchlist == "personal":
            await interaction.followup.send("Cannot delete a personal watchlist. Use /set-watchlist to clear its contents if you wish", ephemeral=True)
            logger.info("Selected watchlist is 'personal' - cannot delete a personal watchlist")
        else:
            self.watchlists.delete_watchlist(watchlist_id=watchlist)
            await interaction.followup.send(f"Deleted watchlist *{watchlist}*", ephemeral=False)
            logger.info(f"Watchlist '{watchlist}' deleted")


#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Watchlists(bot, bot.stock_data))