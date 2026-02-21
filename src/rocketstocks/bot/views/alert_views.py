import discord
import logging

logger = logging.getLogger(__name__)


class AlertButtons(discord.ui.View):
    """Standard URL buttons shown on most alert messages."""

    def __init__(self, ticker: str):
        super().__init__(timeout=None)
        self.ticker = ticker
        self.add_item(discord.ui.Button(
            label="Google it",
            style=discord.ButtonStyle.url,
            url=f"https://www.google.com/search?q={ticker}",
        ))
        self.add_item(discord.ui.Button(
            label="StockInvest",
            style=discord.ButtonStyle.url,
            url=f"https://stockinvest.us/stock/{ticker}",
        ))
        self.add_item(discord.ui.Button(
            label="FinViz",
            style=discord.ButtonStyle.url,
            url=f"https://finviz.com/quote.ashx?t={ticker}",
        ))
        self.add_item(discord.ui.Button(
            label="Yahoo! Finance",
            style=discord.ButtonStyle.url,
            url=f"https://finance.yahoo.com/quote/{ticker}",
        ))


class PoliticianTradeButtons(discord.ui.View):
    """URL button linking to a politician's Capitol Trades profile."""

    def __init__(self, politician: dict):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(
            label="Capitol Trades",
            style=discord.ButtonStyle.url,
            url=f"https://www.capitoltrades.com/politicians/{politician['politician_id']}",
        ))
