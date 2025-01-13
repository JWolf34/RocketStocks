import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
from reports import Report
from reports import StockReport
import stockdata as sd
import analysis as an
import pandas as pd
import config
from config import market_utils
from config import date_utils
import datetime
import logging
import asyncio

# Logging configuration
logger = logging.getLogger(__name__)

class Alerts(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.alerts_channel=self.bot.get_channel(config.discord_utils.alerts_channel_id)
        self.reports_channel= self.bot.get_channel(config.discord_utils.reports_channel_id)
        self.alert_tickers = {}
        self.post_alerts_date.start()
        self.send_popularity_movers.start()
        self.send_alerts.start()
        

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    async def update_alert_tickers(self, key:str, tickers:list):
        self.alert_tickers[key] = tickers

    @tasks.loop(time=datetime.time(hour=6, minute=0, second=0)) # time in UTC
    async def post_alerts_date(self):
        if (market_utils.market_open_today()):
            await self.alerts_channel.send(f"# :rotating_light: Alerts for {date_utils.format_date_mdy(datetime.datetime.today())} :rotating_light:")

    @tasks.loop(minutes = 2)
    async def send_alerts(self):
        if (market_utils.market_open_today() and (market_utils.in_extended_hours() or market_utils.in_intraday())):
            all_alert_tickers = list(set([ticker for tickers in self.alert_tickers.values() for ticker in tickers]))
            #all_alert_tickers = ['PHIO', 'ATPC', 'SLRX', 'KAPA']
            quotes = {}
            chunk_size = 10
            for i in range(0, len(all_alert_tickers), chunk_size):
                tickers = all_alert_tickers[i:i+chunk_size]
                quotes = quotes | sd.Schwab().get_quotes(tickers=tickers)
            quotes.pop('errors', None)
            all_alert_tickers = [ticker for ticker in quotes]

            # Send alerts
            logger.info("Processing alerts")
            await self.send_unusual_volume_movers(tickers=all_alert_tickers, quotes=quotes)
            await self.send_volume_spike_movers(tickers=all_alert_tickers, quotes=quotes)
            await self.send_earnings_movers(tickers=all_alert_tickers, quotes=quotes)
            #await self.send_sec_filing_movers(tickers= all_alert_tickers, quotes=quotes)
            await self.send_watchlist_movers(tickers=all_alert_tickers, quotes=quotes)
            logger.info("Alerts posted")

    # Start posting report at next 0 or 5 minute interval
    # + 30 seconds to allow for reports to generate and add tickers to the alert list
    @send_alerts.before_loop
    async def send_alerts_before_loop(self):
        DELTA = 30
        await asyncio.sleep(config.date_utils.seconds_until_5m_interval() + DELTA)

    async def send_earnings_movers(self, tickers:list, quotes:dict):
        today = datetime.datetime.today()
        for ticker in tickers:
            earnings_date = sd.StockData.Earnings.get_next_earnings_date(ticker)
            if earnings_date != "N/A":
                pct_change = quotes[ticker]['quote']['netPercentChange']   
                if earnings_date == today.date() and pct_change > 10.0:
                    alert_data = {}
                    alert_data['pct_change'] = pct_change
                    alert_data['earnings_date'] = earnings_date
                    alert = EarningsMoverAlert(ticker=ticker, channel=self.alerts_channel, alert_data=alert_dta)
                    await alert.send_alert()

    async def send_sec_filing_movers(self, gainers):
        today = datetime.datetime.today()
        for index, row in gainers.iterrows():
            ticker = row['Ticker']
            filings = sd.SEC().get_filings_from_today(ticker)
            if filings.size > 0:
                alert = SECFilingMoverAlert(ticker=ticker, channel=self.alerts_channel, gainer_row=row)
                await alert.send_alert()
            await asyncio.sleep(1)

    async def send_watchlist_movers(self, tickers:list, quotes:dict):
        for ticker in tickers:
            watchlists = sd.Watchlists().get_watchlists()
            for watchlist in watchlists:
                if watchlist == 'personal':
                    pass
                else:
                    watchlist_tickers = sd.Watchlists().get_tickers_from_watchlist(watchlist)
                    pct_change = quotes[ticker]['quote']['netPercentChange']   
                    if ticker in watchlist_tickers and pct_change > 10.0:
                        alert_data = {}
                        alert_data['pct_change'] = pct_change
                        alert_data['watchlist'] = watchlist
                        alert = WatchlistMoverAlert(ticker=ticker, channel=self.alerts_channel, alert_data=alert_data)
                        await alert.send_alert()
        
    async def send_unusual_volume_movers(self, tickers:list, quotes:dict):
        for ticker in tickers:
            data = sd.StockData.fetch_daily_price_history(ticker)
            curr_volume = quotes[ticker]['quote']['totalVolume']
            rvol = an.indicators.volume.rvol(data=data, curr_volume=curr_volume)
            pct_change = quotes[ticker]['quote']['netPercentChange']   
            market_cap = sd.StockData.get_market_cap(ticker=ticker) 
            if rvol > 25.0 and pct_change > 10.0: # and market_cap > 50000000: # see that Relative Volume exceeds 25x and change > 10% and market cap is > 50M
                alert_data = {}
                alert_data = {}
                alert_data['pct_change'] = pct_change
                alert_data['rvol'] = rvol
                alert_data['volume'] = curr_volume
                alert_data['avg_vol_10'] = data['volume'].tail(10).mean()
                alert_data['avg_vol_30'] = data['volume'].tail(30).mean()
                alert_data['avg_vol_90'] = data['volume'].tail(90).mean()
                alert = VolumeMoverAlert(ticker=ticker, channel=self.alerts_channel, alert_data=alert_data)
                await alert.send_alert()
                await asyncio.sleep(1)

    async def send_volume_spike_movers(self, tickers:list, quotes:dict):
        for ticker in tickers:
            data = sd.StockData.fetch_5m_price_history(ticker)
            if data.empty:
                sd.StockData.update_5m_price_history_by_ticker(ticker=ticker)
                data = sd.StockData.fetch_5m_price_history(ticker=ticker)
            now = datetime.datetime.now()
            rvol_at_time = an.indicators.volume.rvol_at_time(data=data, dt=now)
            avg_vol_at_time, time = an.indicators.volume.avg_vol_at_time(data=data)
            pct_change = quotes[ticker]['quote']['netPercentChange']   
            market_cap = sd.StockData.get_market_cap(ticker=ticker) 
            if rvol_at_time > 50.0 and pct_change > 10.0: # and market_cap > 50000000: # see that Relative Volume at Time exceeds 60x and change > 10% and market cap is > 50M
                alert_data = {}
                alert_data['pct_change'] = pct_change
                alert_data['rvol_at_time'] = rvol_at_time
                alert_data['avg_vol_at_time'] = avg_vol_at_time
                alert_data['volume'] = quotes[ticker]['quote']['totalVolume']
                alert_data['time'] = datetime.time.strftime(time, "%-I:%M %p %z")
                alert = VolumeSpikeAlert(ticker=ticker, channel=self.alerts_channel, alert_data=alert_data)
                await alert.send_alert()
                await asyncio.sleep(1)
    
    @tasks.loop(minutes=1)
    async def send_popularity_movers(self):
        blacklist_tickers = ['DTE', 'AM', 'PM', 'DM']
        top_stocks = sd.ApeWisdom().get_top_stocks()[:50]
        top_stocks = top_stocks[~top_stocks['ticker'].isin(blacklist_tickers)]
        await self.update_alert_tickers(key='popular-stocks', tickers=top_stocks['ticker'].to_list())
        
        for index, row in top_stocks.iterrows():
            ticker = row['ticker']
            if ticker not in blacklist_tickers:
                todays_rank = row['rank']
                popularity = sd.StockData.get_historical_popularity(ticker)
                popularity = popularity[popularity['date'] > (datetime.date.today() - datetime.timedelta(days=5))]
                low_rank = 0
                low_rank_date = None
                high_rank = 0
                high_rank_date = None
                for index, popular_row in popularity.iterrows(): # replace with iterrows logic
                    if low_rank < popular_row['rank'] or low_rank == 0:
                        low_rank = popular_row['rank']
                        low_rank_date = datetime.date.today() - datetime.timedelta(days=index)
                    if high_rank > popular_row['rank'] or high_rank == 0:
                        high_rank = popular_row['rank']
                        high_rank_date = datetime.date.today() - datetime.timedelta(days=index)

                today_is_max = False
                if low_rank < todays_rank:
                    low_rank = todays_rank
                    low_rank_date = datetime.date.today()
                if high_rank > todays_rank:
                    today_is_max = True
                    high_rank = todays_rank
                    high_rank_date = datetime.date.today() - datetime.timedelta(days=index)
                    
            
                if (((float(low_rank) - float(high_rank)) / float(low_rank)) * 100.0 > 75.0) and low_rank > 10 and sd.StockData.validate_ticker(ticker) and today_is_max:
                    alert_data = {}
                    alert_data['todays_rank'] = row['rank']
                    alert_data['high_rank'] = high_rank
                    alert_data['high_rank_date'] = high_rank_date
                    alert_data['low_rank'] = low_rank
                    alert_data['low_rank_date'] = low_rank_date
                    alert = PopularityAlert(ticker = ticker, 
                                            channel=self.alerts_channel,
                                            alert_data=alert_data) 
                    await alert.send_alert()
            else:
                pass

    # Start posting report at next 0 or 5 minute interval
    @send_popularity_movers.before_loop
    async def sleep_until_5m_interval(self):
        await asyncio.sleep(config.date_utils.seconds_until_5m_interval())

##################
# Alerts Classes #
##################
    
class Alert(Report):
    def __init__(self, ticker, channel, alert_data):
        self.ticker = ticker
        self.channel = channel
        self.alert_data = alert_data
        self.message = self.build_alert()
        self.buttons = self.Buttons(self.ticker, channel)
    
    def build_alert_header(self):
        header = f"# :rotatinglight: {self.ticker} ALERT :rotatinglight:\n\n"
        return header 

    def build_alert(self):
        alert = ''
        alert += self.build_alert_header()
        return alert

    async def send_alert(self):
        today = datetime.datetime.today()
        market_period = market_utils.get_market_period()
        message_id = config.discord_utils.get_alert_message_id(date=today.date(), ticker=self.ticker, alert_type=self.alert_type)
        if message_id is not None:
            logger.debug(f"Alert {self.alert_type} already reported for ticker {self.ticker} today")
            pass
        else:
            message = await self.channel.send(self.message, view=self.buttons)
            config.discord_utils.insert_alert_message_id(date=today.date(), ticker=self.ticker, alert_type=self.alert_type, message_id=message.id)
            return message


    class Buttons(discord.ui.View):
            def __init__(self, ticker : str, channel):
                super().__init__(timeout=None)
                self.ticker = ticker
                self.channel = channel
                self.add_item(discord.ui.Button(label="Google it", style=discord.ButtonStyle.url, url = "https://www.google.com/search?q={}".format(self.ticker)))
                self.add_item(discord.ui.Button(label="StockInvest", style=discord.ButtonStyle.url, url = "https://stockinvest.us/stock/{}".format(self.ticker)))
                self.add_item(discord.ui.Button(label="FinViz", style=discord.ButtonStyle.url, url = "https://finviz.com/quote.ashx?t={}".format(self.ticker)))
                self.add_item(discord.ui.Button(label="Yahoo! Finance", style=discord.ButtonStyle.url, url = "https://finance.yahoo.com/quote/{}".format(self.ticker)))

                
            @discord.ui.button(label="Generate report", style=discord.ButtonStyle.primary)
            async def generate_chart(self, interaction:discord.Interaction, button:discord.ui.Button,):
                report = StockReport(ticker=self.ticker, channel=self.channel)
                await report.send_report(interaction, visibility="public")

            @discord.ui.button(label="Get news", style=discord.ButtonStyle.primary)
            async def get_news(self, interaction:discord.Interaction, button:discord.ui.Button):
                news_report = NewsReport(self.ticker)
                await news_report.send_report(interaction)
                await interaction.response.send_message(f"Fetched news for {self.ticker}!", ephemeral=True)

class EarningsMoverAlert(Alert):
    def __init__(self, ticker, channel, alert_data):
        self.alert_type = "EARNINGS_MOVER"
        super().__init__(ticker, channel, alert_data)

    def build_alert_header(self):
        header = f"## :rotating_light: Earnings Mover: {self.ticker}\n\n\n"
        return header

    def build_todays_change(self):
        symbol = ":green_circle:" if self.alert_data['pct_change'] > 0 else ":small_red_triangle_down:"
        return f"**{self.ticker}** is {symbol} **{"{:.2f}".format(self.alert_data['pct_change'])}%**  {market_utils.get_market_period()} and has earnings today\n\n"

    def build_alert(self):
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        alert += self.build_earnings_date()
        alert += self.build_recent_earnings()
        return alert

class SECFilingMoverAlert(Alert):
    def __init__(self, ticker, channel, alert_data):
        self.alert_type = "SEC_FILING_MOVER"
        super().__init__(ticker, channel, alert_data)

    def build_alert_header(self):
        header = f"## :rotating_light: SEC Filing Mover: {self.ticker}\n\n\n"
        return header

    def build_todays_change(self):
        symbol = ":green_circle:" if self.alert_data['pct_change']> 0 else ":small_red_triangle_down:"
        return f"**{self.ticker}** is {symbol} **{"{:.2f}".format(self.alert_data['pct_change'])}%** {market_utils.get_market_period()} and filed with the SEC today\n"

    def build_alert(self):
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        alert += self.build_todays_sec_filings()
        return alert

class WatchlistMoverAlert(Alert):
    def __init__(self, ticker, channel, alert_data):
        self.alert_type = "WATCHLIST_MOVER"
        super().__init__(ticker, channel, alert_data)

    def build_alert_header(self):
        header = f"## :rotating_light: Watchlist Mover: {self.ticker}\n\n\n"
        return header

    def build_todays_change(self):
        symbol = ":green_circle:" if self.alert_data['pct_change'] > 0 else ":small_red_triangle_down:"
        return f"**{self.ticker}** is {symbol} **{"{:.2f}".format(self.alert_data['pct_change'])}%**  and is on your **{self.alert_data['watchlist']}** watchlist\n"

    def build_alert(self):
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        return alert

class VolumeMoverAlert(Alert):
    def __init__(self, ticker, channel, alert_data):
        self.alert_type = "VOLUME_MOVER"
        super().__init__(ticker, channel, alert_data)

    def build_alert_header(self):
        header = f"## :rotating_light: Intraday Volume Mover: {self.ticker}\n\n\n"
        return header

    def build_todays_change(self):
        symbol = ":green_circle:" if self.alert_data['pct_change'] > 0 else ":small_red_triangle_down:"
        return f"**{self.ticker}** is {symbol} **{"{:.2f}".format(self.alert_data['pct_change'])}%** with volume up **{"{:.2f} times".format(self.alert_data['rvol'])}** the 10-day average\n\n"

    def build_volume_stats(self):
        return f"""## Volume Stats
        - **Today's Volume:** {self.format_large_num(self.alert_data['volume'])}
        - **Relative Volume (10 Day):** {"{:.2f}x".format(self.alert_data['rvol'])}
        - **Average Volume  (10 Day):** {self.format_large_num(self.alert_data['avg_vol_10'])}
        - **Average Volume  (30 Day):** {self.format_large_num(self.alert_data['avg_vol_30'])}
        - **Average Volume  (90 Day):** {self.format_large_num(self.alert_data['avg_vol_90'])}
        \n\n"""

    def build_alert(self):
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        alert += self.build_volume_stats()
        return alert

class VolumeSpikeAlert(Alert):
    def __init__(self, ticker, channel, alert_data):
        self.alert_type = "VOLUME_SPIKE"
        super().__init__(ticker, channel, alert_data)

    def build_alert_header(self):
        header = f"## :rotating_light: Volume Spike: {self.ticker}\n\n\n"
        return header

    def build_todays_change(self):
        symbol = ":green_circle:" if self.alert_data['pct_change'] > 0 else ":small_red_triangle_down:"
        return f"**{self.ticker}** is {symbol} **{"{:.2f}".format(self.alert_data['pct_change'])}%** with volume up **{"{:.2f} times".format(self.alert_data['rvol_at_time'])}** the normal at this time\n\n"

    def build_volume_stats(self):
        return f"""## Volume Stats
        - **Today's Volume:** {self.format_large_num(self.alert_data['volume'])}
        - **Relative Volume at Time ( {self.alert_data['time']}):** {"{:.2f}x".format(self.alert_data['rvol_at_time'])}
        - **Current Volume at Time  ( {self.alert_data['time']}):** {self.format_large_num(self.alert_data['rvol_at_time'] * self.alert_data['avg_vol_at_time'])}
        - **Average Volume at Time  ( {self.alert_data['time']}):** {self.format_large_num(self.alert_data['avg_vol_at_time'])}
        \n\n"""

    def build_alert(self):
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        alert += self.build_volume_stats()
        return alert

class PopularityAlert(Alert):
    def __init__(self, ticker, channel, alert_data):
        self.alert_type = "POPULARITY"
        super().__init__(ticker, channel, alert_data)
            
    def build_alert_header(self):
        header = f"## :rotating_light: Popularity Mover: {self.ticker}\n\n\n"
        return header

    def build_todays_change(self):
        return f"**{self.ticker}** has moved {self.alert_data['low_rank'] - self.alert_data['high_rank']} spots between {date_utils.format_date_mdy(self.alert_data['high_rank_date'])} **({self.alert_data['high_rank']})** and {date_utils.format_date_mdy(self.alert_data['low_rank_date'])} **({self.alert_data['low_rank']})** \n"

    def build_alert(self):
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        alert += self.build_popularity()
        return alert


#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Alerts(bot))