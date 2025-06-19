import discord
from discord.ext import commands
from discord.ext import tasks
from reports import Report
from stock_data import StockData
import analysis as an
import numpy as np
import pandas as pd
from utils import market_utils, date_utils, discord_utils
import datetime
import logging
import asyncio

# Logging configuration
logger = logging.getLogger(__name__)

class Alerts(commands.Cog):
    """Push alerts to discord when criteria for stock movements is met"""
    def __init__(self, bot:commands.Bot, stock_data:StockData):
        self.bot = bot
        self.stock_data = stock_data
        self.mutils = market_utils()
        self.reports = self.bot.get_cog('Reports')

        # Init channels to post alerts to 
        self.alerts_channel=self.bot.get_channel(discord_utils.alerts_channel_id)
        self.reports_channel= self.bot.get_channel(discord_utils.reports_channel_id)

        # Dict of tickers to send alerts on
        self.alert_tickers = {}

        # Start alerts
        self.post_alerts_date.start()
        #self.send_popularity_movers.start() TODO
        #self.send_politician_trade_alerts.start() TODO
        self.send_alerts.start()
        

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")


    async def update_alert_tickers(self, key:str, tickers:list):
        """Updates tickers to trigger alerts for"""
        logger.info(f"Updating alert tickers - key: {key}, tickers: {tickers}")
        self.alert_tickers[key] = tickers

    @tasks.loop(time=datetime.time(hour=6, minute=0, second=0)) # time in UTC
    async def post_alerts_date(self):
        """Post message separating alerts by date in the alerts channel"""
        if (self.mutils.market_open_today()):
            date_string = date_utils.format_date_mdy(datetime.datetime.today())
            await self.alerts_channel.send(f"# :rotating_light: Alerts for {date_string} :rotating_light:")

    @tasks.loop(minutes = 5)
    async def send_alerts(self):
        '''Process alerts every 5 minutes if the market is open and in intraday or after hours'''
        market_period = self.mutils.get_market_period()
        if (self.mutils.market_open_today() and market_period != 'EOD'):
            logger.info("Processing alerts")

            # Fetch alert tickers and get quotes to analyze movement
            self.all_alert_tickers = all_alert_tickers = list(set([ticker for tickers in self.stock_data.alert_tickers.values() for ticker in tickers]))
            #all_alert_tickers = ['RYDE', 'APVO', 'GURE', 'ZEO']

            # Fetch quotes for tickers from Schwab in chunks of 'chunk_size'
            quotes = {}
            chunk_size = 25
            for i in range(0, len(all_alert_tickers), chunk_size):
                tickers = all_alert_tickers[i:i+chunk_size]
                quotes = quotes | await self.stock_data.schwab.get_quotes(tickers=tickers)
            logger.info(f"Encountered {len(quotes.pop('errors', []))} errors fetching quotes for alert tickers")

            # Send alerts
            await self.send_unusual_volume_movers(quotes=quotes)
            await self.send_volume_spike_movers(quotes=quotes)
            await self.send_earnings_movers(quotes=quotes)
            #await self.send_sec_filing_movers(tickers= all_alert_tickers, quotes=quotes)
            await self.send_watchlist_movers(quotes=quotes)
            logger.info("Alerts posted")

    # Start posting report at next 0 or 5 minute interval
    # + 30 seconds to allow for reports to generate and add tickers to the alert list
    @send_alerts.before_loop
    async def send_alerts_before_loop(self):
        """Before loop for 'send_alerts'"""
        DELTA = 30
        await asyncio.sleep(date_utils.seconds_until_minute_interval(5) + DELTA)

    async def send_earnings_movers(self, quotes:dict):
        """Send earnings alerts to Discord if criteria is met
        
        Criteria:
            - stock has earnings today
            - % change > +-5%
        """
        logger.info("Processing earnings movers")
        today = datetime.date.today()
        earnings_today = self.stock_data.earnings.get_earnings_on_date(date=today)

        # Filters quotes for tickers who report earnings today
        quotes = {ticker:quote for ticker, quote in quotes.items() if ticker in earnings_today['ticker'].to_list()}

        # Check to see if tickers reporting earninsg today have a % change greater than threshold
        for ticker, quote in quotes.items():
            pct_change = quote['quote']['netPercentChange']   
            if abs(pct_change) > 5.0:
                logger.debug(f"Identified ticker '{ticker}' reporting earnings today with percent change {"{:.2f}%".format(pct_change)}")
                next_earnings_info = earnings_today[earnings_today['ticker'] == ticker].to_dict(orient='records')[0]
                alert = await self.build_earnings_mover(ticker=ticker,
                                                        quote=quote,
                                                        next_earnings_info=next_earnings_info)
                await alert.send_alert()

    async def send_sec_filing_movers(self, gainers):
        logger.info("Processing SEC filing movers")
        today = datetime.datetime.today()
        for index, row in gainers.iterrows():
            ticker = row['Ticker']
            filings = self.bot.sec.get_filings_from_today(ticker)
            pct_change = quotes[ticker]['quote']['netPercentChange']   
            if filings.size > 0 and abs(pct_change) > 10.0:
                logger.debug(f"Identified ticker '{ticker}' with SEC filings today and percent change {"{:.2f}%".format(pct_change)}")
                alert = SECFilingMoverAlert(ticker=ticker, channel=self.alerts_channel, gainer_row=row)
                await alert.send_alert()
            await asyncio.sleep(1)

    async def send_watchlist_movers(self, quotes:dict):
        """Send watchlist alerts to Discord if criteria is met
        
        Criteria:
            - % change > +-10%
        """
        logger.info("Processing watchlist movers")

        # Filter quotes for tickers on watchlists
        all_watchlist_tickers = self.stock_data.watchlists.get_all_watchlist_tickers()
        quotes = {ticker:quote for ticker, quote in quotes.items() if ticker in all_watchlist_tickers}

        # Check to see if each ticker has % change greater than threshold
        for ticker, quote in quotes.items():
            pct_change = quote['quote']['netPercentChange']  
            if abs(pct_change) > 10.0:

                logger.debug(f"Identified ticker '{ticker}' on watchlist with percent change {"{:.2f}%".format(pct_change)}")
                alert = await self.build_watchlist_mover(ticker=ticker)
                await alert.send_alert()
        
    async def send_unusual_volume_movers(self, quotes:dict):
        """Send unusual volume alerts to Discord if criteria is met
        
        Criteria:
            - RVOL > 25 (volume is 25x the average volume over the last x periods)
            - % change > +-10%
        """
        logger.info("Processing unusual volume movers")
                
        
        for ticker, quote in quotes.items():
            # Validate daliy price history
            daily_price_history = self.stock_data.fetch_daily_price_history(ticker=ticker)
            if not daily_price_history.empty:
                # Calculate RVOL for each ticker over x periods
                periods = 10
                curr_volume = quote['quote']['totalVolume']
                rvol = an.indicators.volume.rvol(data=daily_price_history, periods=periods, curr_volume=curr_volume)
                pct_change = quote['quote']['netPercentChange'] 

                # If criteria met, create Volume Mover Alert
                if rvol > 25.0 and abs(pct_change) > 10.0 and rvol is not np.nan: # and market_cap > 50000000: 
                    logger.debug(f"Identified ticker '{ticker}' with RVOL {"{:.2f}x".format(rvol)} and percent change {"{:.2f}%".format(pct_change)}")
                    alert = await self.build_volume_mover(ticker=ticker,
                                                        rvol=rvol,
                                                        quote=quote,
                                                        daily_price_history=daily_price_history)
                    await alert.send_alert()

    async def send_volume_spike_movers(self, quotes:dict):
        """Send volume spike alerts to Discord if criteria is met
        
        Criteria:
            - RVOL_AT_TIME > 50 (volume at this time is 50x the average volume over the last x periods)
            - % change > +-10%
        """
        logger.info("Processing volume spike movers")

        for ticker, quote in quotes.items():
            # Validate stock has 5m data
            now = datetime.datetime.now()
            fivem_price_history = self.stock_data.fetch_5m_price_history(ticker=ticker)
            
            if not fivem_price_history.empty:
                # Calculate RVOL_AT_TIME for each ticker over x periods
                periods = 10
                today_data = await self.stock_data.schwab.get_5m_price_history(ticker=ticker, start_datetime=now)
                rvol_at_time = an.indicators.volume.rvol_at_time(data=fivem_price_history, today_data=today_data, periods=periods, dt=now)
                avg_vol_at_time, time = an.indicators.volume.avg_vol_at_time(data=fivem_price_history, periods=periods)
                pct_change = quote['quote']['netPercentChange']   

                # If criteria met, create Volume Spike Alert
                if rvol_at_time > 50.0 and abs(pct_change) > 10.0 and rvol_at_time is not np.nan and avg_vol_at_time is not np.nan:
                    logger.debug(f"Identified ticker '{ticker}' with RVOL at time ({time}) {"{:.2f}x".format(rvol_at_time)} and percent change {"{:.2f}%".format(pct_change)}")

                    alert = await self.build_volume_spike_alert(ticker=ticker,
                                                        quote=quote,
                                                        rvol_at_time=rvol_at_time,
                                                        avg_vol_at_time=avg_vol_at_time,
                                                        time=time)
                    await alert.send_alert()
                    await asyncio.sleep(1)
    
    @tasks.loop(minutes=30)
    async def send_popularity_movers(self):
        logger.info("Processing popularity movers")
        
        for ticker in self.all_alert_tickers:
            # Validate stock has popularity
            popularity = self.stock_data.fetch_popularity(ticker=ticker)
            if not popularity.empty:
                # Get current rank and ensure the stock has one today
                now = date_utils.round_down_nearest_minute(30)
                popularity_today = popularity[(self.popularity['datetime'] == now)]
                current_rank = popularity_today['rank'].iloc[0] if not popularity_today.empty else 'N/A'

                if not current_rank == 'N/A':

                    # Get highest popularity rank across select intervals
                    interval_map = {"High 1D":1,
                                    "High 2D":2,
                                    "High 3D":3,
                                    "High 4D":4,
                                    "High 5D":5,
                                    }


                    for label, interval in interval_map.items():
                        # Find max rank within defined interval and compare against current rank
                        interval_date = now - datetime.timedelta(days=interval)
                        interval_popularity = popularity[popularity['datetime']==interval_date]
                        if not interval_popularity.empty:
                            interval_max_rank = interval_popularity['rank'].min()
                        else:
                            interval_max_rank = 'N/A'

                        # If difference between max rank and current rank is > 75%, post popularity alert
                        pct_diff = abs((float(current_rank) - float(interval_max_rank)) / float(interval_max_rank))*100.0
                        if pct_diff > 75.00:
                            #Popularity ALert
                            pass


                        
                # Not a popular stock today
                else:
                    pass   
         
            # No popularity data for this ticker                                  
            else:
                pass

    @tasks.loop(hours=1)
    async def send_politician_trade_alerts(self):
        politician = sd.CapitolTrades.politician(name='Nancy Pelosi')
        trades = sd.CapitolTrades.trades(pid=politician['politician_id'])
        today = utils.date_utils.format_date_mdy(datetime.date.today())
        todays_trades = trades[trades['Published Date'].apply(lambda x: x == today)]
        if not todays_trades.empty:
            alert_data = {}
            alert_data['trades'] = todays_trades
            alert_data['politician'] =  politician
            alert = self.PoliticianTradeAlert(channel=self.alerts_channel,
                                         alert_data=alert_data)
            await alert.send_alert()


    # Start posting report at next 0 or 5 minute interval
    @send_popularity_movers.before_loop
    @send_politician_trade_alerts.before_loop
    async def sleep_until_5m_interval(self):
        await asyncio.sleep(date_utils.seconds_until_minute_interval(5))

    
    async def build_earnings_mover(self, ticker:str, **kwargs):
        """Builder for EarningsMoverAlert"""

        # Collect data to build alert
        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        next_earnings_info = kwargs.pop('next_earnings_info', self.stock_data.earnings.get_next_earnings_info(ticker=ticker))
        historical_earnings = kwargs.pop('historical_earnings', self.stock_data.earnings.get_historical_earnings(ticker=ticker))

        # Generate alert
        alert = self.EarningsMoverAlert(channel=self.alerts_channel,
                                   ticker=ticker,
                                   quote=quote,
                                   next_earnings_info=next_earnings_info,
                                   historical_earnings=historical_earnings)
        
        return alert

    async def build_watchlist_mover(self, ticker:str, **kwargs):
        """Builder for WatchlistMoverAlert"""

        def get_ticker_watchlist(ticker:str):
            """Fetch watchlist that input ticker appears on"""
            watchlists = self.stock_data.watchlists.get_watchlists()
            watchlist = None

            for watchlist_id in watchlists:
                watchlist_tickers = self.stock_data.watchlists.get_watchlist_tickers(watchlist_id=watchlist_id)
                if ticker in watchlist_tickers:
                    return watchlist_id
  

        # Collect data to build alert
        quote = kwargs.pop('quote', self.stock_data.schwab.get_quote(ticker=ticker))
        watchlist = kwargs.pop('watchlist', get_ticker_watchlist(ticker=ticker))

        # Generate alert
        alert = self.WatchlistMoverAlert(channel=self.alerts_channel,
                                    ticker=ticker,
                                    quote=quote,
                                    watchlist=watchlist)
        
        return alert


    
    async def build_volume_mover(self, ticker:str, **kwargs):
        """Builder for VolumeMoverAlert"""

        # Collect data to build alert
        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        daily_price_history = kwargs.pop('daily_price_history', self.stock_data.fetch_daily_price_history(ticker=ticker))
        rvol = kwargs.pop('rvol', an.indicators.volume.rvol(data=daily_price_history,
                                                            curr_volume=quote['quote']['totalVolume']))

        # Generate alert
        alert = self.VolumeMoverAlert(channel=self.alerts_channel,
                                 ticker=ticker,
                                 rvol=rvol,
                                 quote=quote,
                                 daily_price_history=daily_price_history)
        
        return alert
    
    async def build_volume_spike_alert(self, ticker, **kwargs):
        """Builder for VolumeSpikeAlert"""

        # Collect data to build alert
        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        rvol_at_time = an.indicators.volume.rvol_at_time(data = self.stock_data.fetch_5m_price_history(ticker=ticker), 
                                                         today_data = await self.stock_data.schwab.get_5m_price_history(ticker=ticker,
                                                                                                                       start_datetime=datetime.datetime.now()),
                                                         dt = datetime.datetime.now())
        avg_vol_at_time, time = (kwargs.pop('avg_vol_at_time', None), kwargs.pop('time', None))

        # Validate certain variables
        if not avg_vol_at_time and not time:
            now = datetime.datetime.now()
            fivem_price_history = self.stock_data.schwab.get_5m_price_history(ticker=ticker, start_datetime=now)
            avg_vol_at_time, time = an.indicators.volume.avg_vol_at_time(data=fivem_price_history)

        if not isinstance(time, str):
            #time = time.astimezone(tz=date_utils.timezone())
            time = time.strftime("%I:%M %p")
        
        # Generate alert
        alert = self.VolumeSpikeAlert(channel=self.alerts_channel,
                                 ticker=ticker, 
                                 rvol_at_time=rvol_at_time,
                                 avg_vol_at_time=avg_vol_at_time,
                                 quote=quote,
                                 time=time)
        
        return alert

    

    
    ##################
    # Alerts Classes #
    ##################
        
    class Alert(Report):
        def __init__(self, channel:discord.channel, alert_type:str, override_buttons = False, **kwargs):
            super().__init__(channel=channel,
                            **kwargs)
            
            # Set alert type
            self.alert_type = alert_type

            # Discord Utils
            self.dutils = discord_utils()

            # Parse data from keyword args for building alerts
            self.market_period = kwargs.pop('market_period', None)
            self.pct_change = self.quote['quote']['netPercentChange'] if self.quote else None
            self.rvol = kwargs.pop('rvol', None)
            self.rvol_at_time = kwargs.pop('rvol_at_time', None)
            self.avg_vol_at_time = kwargs.pop('avg_vol_at_time', None)
            self.time = kwargs.pop('time', None)
            self.watchlist = kwargs.pop('watchlist', None)

            if not override_buttons:
                self.buttons = self.Buttons(self.ticker, channel)

            # Init alert data
            self.alert_data = {}
            self.build_alert_data()
            
        def build_alert_data(self):
            """Populate alert data with necessary information to compare against futures alerts to overriding and posting new alerts"""
            self.alert_data['pct_change'] = self.pct_change
        
        def build_alert_header(self):
            logger.debug("Building alert header...")
            header = f"# :rotatinglight: {self.ticker} ALERT :rotatinglight:\n\n"
            return header 

        def build_alert(self):
            alert = ''
            alert += self.build_alert_header()
            return alert
        
        def build_todays_change(self):
            logger.debug("Building today's change...")
            symbol = "🟢" if self.pct_change > 0 else "🔻"
            return f"`{self.ticker}` is {symbol} **{'{:.2f}'.format(self.pct_change)}%**"
        
        def build_volume_stats(self):
            """Return message content with statistics on volume of the alert's ticker
            
            Requires:
                - daily_price_history
                
            Optional:
                - rvol
                - rvol_at_time
            """

            logger.debug("Building volume stats...")

            message = '## Volume Stats\n'
            volume_stats = {}

            # Today's volume from quote
            if self.quote:
                volume_stats['Volume Today'] = self.format_large_num(self.quote['quote']['totalVolume'])

            # RVOL
            if self.rvol:
                volume_stats['Relative Volume (10 Day)'] = "{:.2f}x".format(self.rvol)

            # RVOL at time
            if self.rvol_at_time and self.avg_vol_at_time:
                volume_stats[f'Relative Volume at Time ({self.time})'] = "{:.2f}x".format(self.rvol_at_time)
                volume_stats[f'Current Volume at Time ({self.time})']  = self.format_large_num(self.rvol_at_time * self.avg_vol_at_time)
                volume_stats[f'Average Volume at Time ({self.time})']  = self.format_large_num(self.avg_vol_at_time)

            # Average volume from daily price history
            if not self.daily_price_history.empty:
                volume_stats['Average Volume (10 Day)'] = self.format_large_num(self.daily_price_history['volume'].tail(10).mean())
                volume_stats['Average Volume (30 Day)'] = self.format_large_num(self.daily_price_history['volume'].tail(30).mean())
                volume_stats['Average Volume (90 Day)'] = self.format_large_num(self.daily_price_history['volume'].tail(90).mean())

            # Check that volume stats has been populated with content
            if volume_stats:
                message += self.build_stats_table(header={}, body=volume_stats, adjust='right')
            else:
                message += f"No volume stats available"

            return message

        def build_todays_sec_filings(self):
            """Return message content containing SEC filings for the stock released today
            
            Requires:
                - recent_sec_filings
                - ticker
            """
            logger.debug("Building today's SEC filings...")
            message = "## Today's SEC Filings\n\n"

            # Filter recent filings to only get filings from today
            today_string = datetime.datetime.today().strftime("%Y-%m-%d")
            todays_filings = self.recent_sec_filings[self.recent_sec_filings['filingDate'] == today_string]
            for index, filing in todays_filings.iterrows():
                message += f"[Form {filing['form']} - {filing['filingDate']}]({filing['link']})\n"
            return message

        def override_and_edit(self, prev_alert_data):
            pct_diff = ((self.alert_data['pct_change'] - prev_alert_data['pct_change']) / abs(prev_alert_data['pct_change'])) * 100.0
            if pct_diff > 100.0:
                return True 
            else:
                return False

        async def send_alert(self):
            """Send alert to alert's channel, adding files and buttons as needed"""

            message = self.build_alert()

            # Check if alert has already been posted
            today = datetime.datetime.now(tz=date_utils.timezone()).date()
            message_id = self.dutils.get_alert_message_id(date=today, ticker=self.ticker, alert_type=self.alert_type)

            # Alert has been posted
            if message_id:
                logger.debug(f"Alert {self.alert_type} already reported for ticker '{self.ticker}' today")
                prev_alert_data = self.dutils.get_alert_message_data(date=today, ticker=self.ticker, alert_type=self.alert_type)

                # Alert has been posted, but threshold met to post an update
                if self.override_and_edit(prev_alert_data = prev_alert_data):
                    logger.debug(f"Significant movements on ticker {self.ticker} since alert last posted - updating...")

                    # Fetch previous message to be linked to in new alert
                    prev_message =  await self.channel.fetch_message(message_id)
                    prev_message_time = prev_message.created_at.astimezone(date_utils.timezone())
                    self.message += f"\n[Updated from last alert at {prev_message_time.strftime("%I:%M %p")} {prev_message_time.tzname()}]({prev_message.jump_url})"

                    # Send new alert
                    message = await self.channel.send(message, view=self.buttons)

                    # Update alert data
                    self.dutils.update_alert_message_data(date=today.date(), ticker=self.ticker, alert_type=self.alert_type, messageid=message.id, alert_data=self.alert_data)
                
                # Alert has been posted and does not meet criteria for an update
                else:
                    logger.debug(f"Movements for ticker {self.ticker} not significant enough to update alert")
                    pass
            # No alert has been posted, post a new one
            else:
                message = await self.channel.send(message, view=self.buttons)
                self.dutils.insert_alert_message_id(date=today, ticker=self.ticker, alert_type=self.alert_type, message_id=message.id, alert_data = self.alert_data)
                return message


        class Buttons(discord.ui.View):
                def __init__(self, ticker:str, reports:commands.Cog):
                    super().__init__(timeout=None)
                    self.ticker = ticker
                    self.reports = reports
                    self.add_item(discord.ui.Button(label="Google it", style=discord.ButtonStyle.url, url = "https://www.google.com/search?q={}".format(self.ticker)))
                    self.add_item(discord.ui.Button(label="StockInvest", style=discord.ButtonStyle.url, url = "https://stockinvest.us/stock/{}".format(self.ticker)))
                    self.add_item(discord.ui.Button(label="FinViz", style=discord.ButtonStyle.url, url = "https://finviz.com/quote.ashx?t={}".format(self.ticker)))
                    self.add_item(discord.ui.Button(label="Yahoo! Finance", style=discord.ButtonStyle.url, url = "https://finance.yahoo.com/quote/{}".format(self.ticker)))

                '''
                @discord.ui.button(label="Generate report", style=discord.ButtonStyle.primary)
                async def generate_chart(self, interaction:discord.Interaction, button:discord.ui.Button,):
                    report = self.reports.build_stock
                    await report.send_report(interaction, visibility="public")

                @discord.ui.button(label="Get news", style=discord.ButtonStyle.primary)
                async def get_news(self, interaction:discord.Interaction, button:discord.ui.Button):
                    news_report = Reports.build_news_report(self.ticker)
                    await news_report.send_report(interaction)
                    await interaction.response.send_message(f"Fetched news for {self.ticker}!", ephemeral=True)
                '''
    class EarningsMoverAlert(Alert):
        def __init__(self, channel:discord.channel, ticker:str, quote:dict, next_earnings_info:dict,
                    historical_earnings:pd.DataFrame):
            super().__init__(channel=channel,
                            alert_type="EARNINGS_MOVER",
                            ticker=ticker,
                            quote=quote,
                            next_earnings_info=next_earnings_info,
                            historical_earnings=historical_earnings)

        def build_alert_header(self):
            logger.debug("Building alert header...")
            header = f"## :rotating_light: Earnings Mover: {self.ticker}\n\n\n"
            return header

        def build_todays_change(self):
            logger.debug("Building today's change...")
            message = super().build_todays_change()
            message += f" and reports earnings today\n"
            return message

        def build_alert(self):
            logger.debug("Building Earnings Mover Alert...")
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
            logger.debug("Building alert header...")
            header = f"## :rotating_light: SEC Filing Mover: {self.ticker}\n\n\n"
            return header

        def build_todays_change(self):
            logger.debug("Building today's change...")
            symbol = ":green_circle:" if self.alert_data['pct_change']> 0 else ":small_red_triangle_down:"
            return f"**{self.ticker}** is {symbol} **{"{:.2f}".format(self.alert_data['pct_change'])}%** {self.mutils.get_market_period()} and filed with the SEC today\n"

        def build_alert(self):
            logger.debug("Building SEC Filing Mover Alert...")
            alert = ""
            alert += self.build_alert_header()
            alert += self.build_todays_change()
            alert += self.build_todays_sec_filings()
            return alert

    class WatchlistMoverAlert(Alert):
        def __init__(self, channel:discord.channel, ticker:str, quote:dict, watchlist:str):
            super().__init__(channel=channel,
                            alert_type="WATCHLIST_MOVER",
                            ticker=ticker,
                            quote=quote,
                            watchlist=watchlist)

        def build_alert_header(self):
            logger.debug("Building alert header...")
            header = f"## :rotating_light: Watchlist Mover: {self.ticker}\n"
            return header

        def build_todays_change(self):
            logger.debug("Building today's change...") 
            message = super().build_todays_change()
            message += f" and is on your *{self.watchlist}* watchlist\n"
            return message

        def build_alert(self):
            logger.debug("Building Watchlist Mover Alert...")
            alert = ""
            alert += self.build_alert_header()
            alert += self.build_todays_change()
            return alert

    class VolumeMoverAlert(Alert):
        """Alert subclass that posts an alert for stocks with high relative volume"""
        def __init__(self, channel:discord.channel, ticker:str, rvol:float, quote:dict, daily_price_history:pd.DataFrame):
            super().__init__(channel=channel,
                            alert_type="VOLUME_MOVER",
                            ticker=ticker,
                            rvol=rvol,
                            quote=quote,
                            daily_price_history=daily_price_history)
            
        def build_alert_data(self):
            """Extends parent class to add RVOL to alert data"""
            super().build_alert_data()
            self.alert_data['rvol'] = self.rvol

        def build_alert_header(self):
            """Overrides parent class to build custom header"""
            logger.debug("Building alert header...")
            header = f"## :rotating_light: Volume Mover: {self.ticker}\n\n\n"
            return header
        
        def build_alert(self):
            """Override parent class to build custom alert"""
            logger.debug("Building Volume Mover Alert...")
            alert = ""
            alert += self.build_alert_header()
            alert += self.build_todays_change()
            alert += self.build_volume_stats()
            return alert
        
        def build_todays_change(self):
            """Extends the parent function to include RVOL data"""
            logger.debug("Building today's change...")
            message = super().build_todays_change()
            message += f" with volume up **{'{:.2f} times'.format(self.rvol)}** the 10-day average\n"
            return message

        def override_and_edit(self, prev_alert_data):
            """Extends parent function to check RVOL change"""
            if super().override_and_edit(prev_alert_data=prev_alert_data):
                return True 
            elif self.alert_data['rvol'] > (2.0 * prev_alert_data['rvol']):
                return True
            else:
                return False
                
    class VolumeSpikeAlert(Alert):
        """Alert subclass that posts an alert for stocks with high relative volume at a time of day"""
        def __init__(self, channel:discord.channel, ticker:str, rvol_at_time:float, avg_vol_at_time:float,
                    quote:dict, time:str):
            super().__init__(channel=channel,
                            alert_type="VOLUME_SPIKE",
                            ticker=ticker,
                            rvol_at_time=rvol_at_time,
                            avg_vol_at_time=avg_vol_at_time,
                            quote=quote,
                            time=time)
            
        def build_alert_data(self):
            """Extends parent class to add RVOL to alert data"""
            super().build_alert_data()
            self.alert_data['rvol_at_time'] = self.rvol_at_time

        def build_alert_header(self):
            """Overrides parent class to build custom header"""
            logger.debug("Building alert header...")
            header = f"## :rotating_light: Volume Spike: {self.ticker}\n\n\n"
            return header

        def build_todays_change(self):
            """Extends the parent function to include RVOL_AT_TIME data"""
            logger.debug("Building today's change...")
            message = super().build_todays_change()
            message += f" with volume up **{'{:.2f} times'.format(self.rvol_at_time)}** the normal at this time\n"
            return message

        def build_alert(self):
            logger.debug("Building Volume Spike Alert...")
            alert = ""
            alert += self.build_alert_header()
            alert += self.build_todays_change()
            alert += self.build_volume_stats()
            return alert

        # Override
        def override_and_edit(self, old_alert_data):
            """Extends parent function to check RVOL_AT_TIME change"""
            if super().override_and_edit(old_alert_data=old_alert_data):
                return True
            if self.rvol_at_time > (1.5 * old_alert_data['rvol_at_time']):
                return True
            else:
                return False


    class PopularityAlert(Alert):
        def __init__(self, channel:discord.channel, ticker:str, quote:dict, popularity:str):
            super().__init__(channel=channel,
                            alert_type="POPUALRITY_MOVER",
                            ticker=ticker,
                            quote=quote,
                            popularity=popularity)
        
        def get_popularity_stats(self):
            # Calculate max rank and changes over last 5D
            pass
                
        def build_alert_header(self):
            header = f"## :rotating_light: Popularity Mover: {self.ticker}\n\n\n"
            return header

        def build_todays_change(self):
            logger.debug("Building today's change...")
            return f"**{self.ticker}** has moved {self.alert_data['low_rank'] - self.alert_data['high_rank']} spots between {self.alert_data['high_rank_date']} **({self.alert_data['high_rank']})** and {self.alert_data['low_rank_date']} **({self.alert_data['low_rank']})** \n"

        def build_alert(self):
            logger.debug("Building Popularity Alert...")
            alert = ""
            alert += self.build_alert_header()
            alert += self.build_todays_change()
            alert += self.build_popularity()
            return alert

        # Override
        def override_and_edit(self, old_alert_data):
            if self.alert_data['high_rank'] < (0.5 * float(old_alert_data['high_rank'])):
                return True
            else:
                return False

    class PoliticianTradeAlert(Alert):
        def __init__(self, channel, alert_data):
            self.politician = alert_data['politician']
            self.alert_type = f"POLITICIAN_TRADE_{self.politician['name'].upper().replace(" ","_")}"
            super().__init__(ticker='N/A', channel=channel, alert_data=alert_data, override_buttons = True)
            self.buttons = self.Buttons(politician=self.politician)

            # clean up data for saving to database
            self.alert_data['trades'] = self.alert_data['trades'].to_json()
        
        def build_alert_header(self):
            header = f"## :rotating_light: Politician Trade Alert: {self.politician['name']}\n\n\n"
            return header

        def build_todays_change(self):
            logger.debug("Building today's change...")
            return f"**{self.politician['name']}** has published **{len(self.alert_data['trades'])}** trades today, {utils.date_utils.format_date_mdy(datetime.date.today())} \n"


        def build_alert(self):
            logger.debug("Building Politician Trade Alert...")
            alert = ""
            alert += self.build_alert_header()
            alert += self.build_todays_change()
            alert += self.build_table(df=self.alert_data['trades'])
            return alert

        # Override
        def override_and_edit(self, old_alert_data):
            if len(self.alert_data['trades']) < len(old_alert_data['trades']):
                return True
            else:
                return False

            # Override
        class Buttons(discord.ui.View):
            def __init__(self, politician):
                super().__init__(timeout=None)
                self.add_item(discord.ui.Button(label="Capitol Trades", style=discord.ButtonStyle.url, url = f"https://www.capitoltrades.com/politicians/{politician['politician_id']}"))



#########        
# Setup #
#########

async def setup(bot:commands.Bot):
    await bot.add_cog(Alerts(bot, bot.stock_data))