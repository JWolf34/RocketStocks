import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
import stockdata as sd
import logging

# Logging configuration
logger = logging.getLogger(__name__)

class Watchlists(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    async def watchlist_options(self, interaction: discord.Interaction, current: str):
        watchlists = sd.Watchlists().get_watchlists(no_systemGenerated=False)
        return [
            app_commands.Choice(name = watchlist, value= watchlist)
            for watchlist in watchlists if current.lower() in watchlist.lower()
        ]

    @app_commands.command(name = "add-tickers", description= "Add tickers to the selected watchlist",)
    @app_commands.describe(tickers = "Ticker to add to watchlist (separated by spaces)")
    @app_commands.describe(watchlist = "Which watchlist you want to make changes to")
    @app_commands.autocomplete(watchlist=watchlist_options,)
    async def addtickers(self, interaction: discord.Interaction, tickers: str, watchlist: str):
        await interaction.response.defer(ephemeral=True)
        logger.info("/add-tickers function called by user {}".format(self, interaction.user.name))
        
        # Parse list from ticker input and identify invalid tickers
        tickers, invalid_tickers = sd.StockData.get_valid_tickers(tickers)

        message_flavor = watchlist
        is_personal = False

        # Set message flavor based on value of watchlist argument
        watchlist_id = watchlist

        if watchlist == 'personal':
            watchlist_id = str(interaction.user.id)
            is_personal = True
            message_flavor = "your"

        # Confirm watchlists exists, otherwise create it
        if watchlist_id not in sd.Watchlists().get_watchlists(no_personal=False):
            sd.Watchlists().create_watchlist(watchlist_id=watchlist_id, tickers=[])

        logger.info(f"Tickers {tickers} to be added to watchlist {watchlist}")

        symbols = sd.Watchlists().get_tickers_from_watchlist(watchlist_id)
        duplicate_tickers = [x for x in tickers if x in symbols]
        tickers = [x for x in tickers if x not in symbols]

        # Update watchlist with new tickers
        sd.Watchlists().update_watchlist(watchlist_id=watchlist_id, tickers=symbols + tickers)
        
        message = ''
        if len(tickers) > 0:
            message = "Added {} to {} watchlist!".format(", ".join(tickers), message_flavor)
        else:
            message = "No tickers added to {} watchlist.".format(message_flavor)
        if len(invalid_tickers) > 0:
                message += " Invalid tickers: {}.".format(", ".join(invalid_tickers))
        if len(duplicate_tickers) > 0:
            message += " Duplicate tickers: {}".format(", ".join(duplicate_tickers))
        
        await interaction.followup.send(message, ephemeral=is_personal)
        
    @app_commands.command(name = "remove-tickers", description= "Remove tickers from the selected watchlist",)
    @app_commands.describe(tickers = "Tickers to remove from watchlist (separated by spaces)")
    @app_commands.describe(watchlist = "Which watchlist you want to make changes to")
    @app_commands.autocomplete(watchlist=watchlist_options,)
    async def removetickers(self, interaction: discord.Interaction, tickers: str, watchlist: str):
        await interaction.response.defer(ephemeral=True)
        logger.info("/remove-tickers function called by user {}".format(self, interaction.user.name))
        
    # Parse list from ticker input and identify invalid tickers
        tickers, invalid_tickers = sd.StockData.get_valid_tickers(tickers)

        message_flavor = watchlist
        is_personal = False

        # Set message flavor based on value of watchlist argument
        watchlist_id = watchlist

        if watchlist == 'personal':
            watchlist_id = str(interaction.user.id)
            is_personal = True
            message_flavor = "your"
        
        logger.info(f"Tickers {tickers} to be removed from watchlist {watchlist}")

        symbols = sd.Watchlists().get_tickers_from_watchlist(watchlist_id)
        
        # If watchlist is empty, return
        if symbols is None:
            await interaction.followup.send("There are no tickers in {} watchlist. Use /add-tickers or /create-watchlist to begin building a watchlist.".format(message_flavor), ephemeral=is_personal)
            logger.info(f"No tickers exist on watchlist {watchlist}. None removed")
        else:
            # Identify input tickers not in the watchlist
            NA_tickers = [ticker for ticker in tickers if ticker not in symbols]
            tickers = [ticker for ticker in tickers if ticker not in NA_tickers]
            
            
            # Update watchlist without input tickers                   
            sd.Watchlists().update_watchlist(watchlist_id=watchlist_id, tickers=[ticker for ticker in symbols if ticker not in tickers])                   
            
            message = ''
            if len(tickers) > 0:
                message = "Removed {} from {} watchlist!".format(", ".join(tickers), message_flavor)
            else:
                message = "No tickers removed {} watchlist.".format(message_flavor)
            if len(invalid_tickers) > 0:
                    message += " Invalid tickers: {}.".format(", ".join(invalid_tickers))
            if len(NA_tickers) > 0:
                message += " Tickers not in watchlist: {}".format(", ".join(NA_tickers))
        
            await interaction.followup.send(message, ephemeral=is_personal)
            logger.debug(f"Invalid input tickers: {invalid_tickers}")
            logger.debug(f"Duplicate input tickers: {duplicate_tickers}")
            

    @app_commands.command(name = "watchlist", description= "List the tickers on the selected watchlist",)
    @app_commands.describe(watchlist = "Which watchlist you want to make changes to")
    @app_commands.autocomplete(watchlist=watchlist_options,)
    async def watchlist(self, interaction: discord.Interaction, watchlist: str):
        logger.info("/watchlist function called by user {}".format(self, interaction.user.name))
    
        is_personal = False
        watchlist_id = watchlist
        if watchlist == 'personal':
            watchlist_id = str(interaction.user.id)
            is_personal = True
        
        tickers = sd.Watchlists().get_tickers_from_watchlist(watchlist_id)
        if tickers is not None:
            await interaction.response.send_message(f"Watchlist '{watchlist_id}': {', '.join(tickers)}", ephemeral=is_personal)
        else:
            await interaction.response.send_message(f"No tickers on watchlist '{watchlist}'", ephemeral=is_personal)

    @app_commands.command(name = "set-watchlist", description= "Overwrite a watchlist with the specified tickers",)
    @app_commands.describe(tickers = "Tickers to add to watchlist (separated by spaces)")
    @app_commands.describe(watchlist = "Which watchlist you want to make changes to")
    @app_commands.autocomplete(watchlist=watchlist_options,)
    async def set_watchlist(self, interaction: discord.Interaction, tickers: str, watchlist: str):
        await interaction.response.defer(ephemeral=True)
        logger.info("/set-watchlist function called by user {}".format(self, interaction.user.name))

        # Parse list from ticker input and identify invalid tickers
        tickers, invalid_tickers = sd.StockData.get_valid_tickers(tickers)

        symbols = []
        message_flavor = watchlist
        is_personal = False
        watchlist_id = watchlist

        # Get watchlist path and watchlist contents based on value of watchlist input
        if watchlist == 'personal':
                watchlist_id = str(interaction.user.id)
                message_flavor = "your"
                is_personal = True
            
        # Confirm watchlists exists, otherwise create it
        if watchlist_id not in sd.Watchlists().get_watchlists(no_personal=False):
            sd.Watchlists().create_watchlist(watchlist_id=watchlist_id, tickers=[], systemGenerated=False)
    
        # Update watchlist with new tickers         
        sd.Watchlists().update_watchlist(watchlist_id=watchlist_id, tickers=tickers)
        if len(tickers) > 0 and len(invalid_tickers) > 0:
            await interaction.followup.send("Set {} watchlist to {} but could not add the following tickers: {}".format(message_flavor, ", ".join(tickers), ", ".join(invalid_tickers)), ephemeral=is_personal)
        elif len(tickers) > 0:
            await interaction.followup.send("Set {} watchlist to {}.".format(message_flavor, ", ".join(tickers)), ephemeral=is_personal)
        else:
            await interaction.followup.send("No tickers added to {} watchlist. Invalid tickers: {}".format(message_flavor, ", ".join(invalid_tickers)), ephemeral=is_personal)
        
    @app_commands.command(name = "create-watchlist", description= "List the tickers on the selected watchlist",)
    @app_commands.describe(watchlist = "Name of the watchlist to create")
    @app_commands.describe(tickers = "Tickers to add to watchlist (separated by spaces)")
    async def create_watchlist(self, interaction: discord.Interaction, watchlist: str, tickers: str):
        await interaction.response.defer(ephemeral=True)
        logger.info("/create-watchlist function called by user {}".format(self, interaction.user.name))

        # Parse list from ticker input and identify invalid tickers
        tickers, invalid_tickers = sd.StockData.get_valid_tickers(tickers)

        sd.Watchlists().create_watchlist(watchlist_id=watchlist, tickers=tickers, systemGenerated=False)
        await interaction.followup.send("Created watchlist '{}' with tickers: ".format(watchlist) + ', '.join(tickers), ephemeral=False)

    @app_commands.command(name = "delete-watchlist", description= "Delete a watchlist",)
    @app_commands.describe(watchlist = "Watchlist to delete")
    @app_commands.autocomplete(watchlist=watchlist_options,)
    async def delete_watchlist(self, interaction: discord.Interaction, watchlist: str):
        await interaction.response.defer(ephemeral=True)
        logger.info("/delete-watchlist function called by user {}".format(self, interaction.user.name))

        if watchlist == "personal":
            await interaction.followup.send("Cannot delete a personal watchlist. Use /set-watchlist to clear its contents if you wish", ephemeral=True)
        else:
            sd.Watchlists().delete_watchlist(watchlist_id=watchlist)
            await interaction.followup.send("Deleted watchlist '{}'".format(watchlist), ephemeral=False)


#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Watchlists(bot))