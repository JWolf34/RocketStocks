import discord
import logging

from rocketstocks.core.utils.formatting import finviz_url

logger = logging.getLogger(__name__)


class StockReportButtons(discord.ui.View):
    """Buttons for Stock Report and Earnings Spotlight Report:
    - Google shortcut
    - StockInvest shortcut
    - FinViz shortcut
    - Yahoo Finance shortcut
    - Generate chart button
    - Get News button
    """
    def __init__(self, ticker: str):
        super().__init__(timeout=None)
        self.ticker = ticker
        self.add_item(discord.ui.Button(label="Google it", style=discord.ButtonStyle.url,
                                        url=f"https://www.google.com/search?q={ticker}"))
        self.add_item(discord.ui.Button(label="StockInvest", style=discord.ButtonStyle.url,
                                        url=f"https://stockinvest.us/stock/{ticker}"))
        self.add_item(discord.ui.Button(label="FinViz", style=discord.ButtonStyle.url,
                                        url=finviz_url(ticker)))
        self.add_item(discord.ui.Button(label="Yahoo! Finance", style=discord.ButtonStyle.url,
                                        url=f"https://finance.yahoo.com/quote/{ticker}"))

    @discord.ui.button(label="Generate chart", style=discord.ButtonStyle.primary)
    async def generate_chart(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Generate chart!")

    @discord.ui.button(label="Get news", style=discord.ButtonStyle.primary)
    async def get_news(self, interaction: discord.Interaction, button: discord.ui.Button):
        from rocketstocks.data.clients.news import News
        from rocketstocks.core.content.reports.news_report import NewsReport
        from rocketstocks.core.content.models import NewsReportData
        from rocketstocks.bot.senders.embed_utils import spec_to_embed
        news = News().get_news(query=self.ticker)
        news_report = NewsReport(data=NewsReportData(query=self.ticker, news=news))
        embed = spec_to_embed(news_report.build())
        await interaction.response.send_message(embed=embed, ephemeral=True)


class GainerScreenerButtons(discord.ui.View):
    """Buttons for Gainer Screener:
    - TradingView shortcut based on market period
    """
    def __init__(self, market_period: str):
        super().__init__(timeout=None)
        if market_period == 'premarket':
            url = "https://www.tradingview.com/markets/stocks-usa/market-movers-pre-market-gainers/"
        elif market_period == 'intraday':
            url = "https://www.tradingview.com/markets/stocks-usa/market-movers-gainers/"
        elif market_period == 'aftermarket':
            url = "https://www.tradingview.com/markets/stocks-usa/market-movers-after-hours-gainers/"
        else:
            url = "https://www.tradingview.com/"
        self.add_item(discord.ui.Button(label="TradingView", style=discord.ButtonStyle.url, url=url))


class VolumeScreenerButtons(discord.ui.View):
    """Buttons for Volume Screener:
    - TradingView shortcut
    """
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(
            label="TradingView",
            style=discord.ButtonStyle.url,
            url="https://www.tradingview.com/markets/stocks-usa/market-movers-unusual-volume/",
        ))


class PopularityScreenerButtons(discord.ui.View):
    """Buttons for Popularity Screener:
    - ApeWisdom shortcut
    """
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(label="ApeWisdom", style=discord.ButtonStyle.url, url="https://apewisdom.io/"))


class PopularityReportButtons(discord.ui.View):
    """Buttons for Popularity Report:
    - ApeWisdom shortcut
    """
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(label="ApeWisdom", style=discord.ButtonStyle.url, url="https://apewisdom.io/"))


class PoliticianReportButtons(discord.ui.View):
    """Buttons for Politician Report:
    - Capitol Trades shortcut
    """
    def __init__(self, pid: str):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(
            label="Capitol Trades",
            style=discord.ButtonStyle.url,
            url=f"https://www.capitoltrades.com/politicians/{pid}",
        ))


class TechnicalReportButtons(discord.ui.View):
    """Buttons for Technical Report:
    - TradingView chart shortcut
    - FinViz shortcut
    """
    def __init__(self, ticker: str):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(
            label="TradingView",
            style=discord.ButtonStyle.url,
            url=f"https://www.tradingview.com/chart/?symbol={ticker}",
        ))
        self.add_item(discord.ui.Button(
            label="FinViz",
            style=discord.ButtonStyle.url,
            url=finviz_url(ticker),
        ))


class ComparisonReportButtons(discord.ui.View):
    """Buttons for Comparison Report:
    - FinViz compare shortcut
    """
    def __init__(self, tickers: list):
        super().__init__(timeout=None)
        tickers_str = ','.join(tickers)
        self.add_item(discord.ui.Button(
            label="FinViz Compare",
            style=discord.ButtonStyle.url,
            url=f"https://finviz.com/compare.ashx?t={tickers_str}&ta=0&p=d",
        ))
