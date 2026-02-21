import datetime
import logging
import asyncio
import numpy as np
import discord
from discord.ext import commands, tasks

from rocketstocks.data.stock_data import StockData
from rocketstocks.data.discord_state import DiscordState
from rocketstocks.core.utils.market import market_utils
from rocketstocks.core.utils.dates import date_utils
from rocketstocks.core.config.settings import alerts_channel_id, reports_channel_id
import rocketstocks.core.analysis.indicators as an

from rocketstocks.core.alerts.earnings_alert import EarningsMoverAlert
from rocketstocks.core.alerts.sec_filing_alert import SECFilingMoverAlert
from rocketstocks.core.alerts.watchlist_alert import WatchlistMoverAlert
from rocketstocks.core.alerts.volume_alert import VolumeMoverAlert
from rocketstocks.core.alerts.volume_spike_alert import VolumeSpikeAlert
from rocketstocks.core.alerts.popularity_alert import PopularityAlert
from rocketstocks.core.alerts.politician_alert import PoliticianTradeAlert

from rocketstocks.bot.views.alert_views import AlertButtons, PoliticianTradeButtons
from rocketstocks.bot.senders.alert_sender import send_alert

logger = logging.getLogger(__name__)


class Alerts(commands.Cog):
    """Push alerts to Discord when criteria for stock movements are met."""

    def __init__(self, bot: commands.Bot, stock_data: StockData):
        self.bot = bot
        self.stock_data = stock_data
        self.mutils = market_utils()
        self.dstate = DiscordState()

        self.alerts_channel = self.bot.get_channel(alerts_channel_id)
        self.reports_channel = self.bot.get_channel(reports_channel_id)

        self.alert_tickers = {}

        self.post_alerts_date.start()
        self.send_popularity_movers.start()
        # self.send_politician_trade_alerts.start()  # TODO
        self.send_alerts.start()

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    async def update_alert_tickers(self, key: str, tickers: list):
        """Update tickers to trigger alerts for."""
        logger.info(f"Updating alert tickers - key: {key}, tickers: {tickers}")
        self.alert_tickers[key] = tickers

    @tasks.loop(time=datetime.time(hour=6, minute=0, second=0))  # UTC
    async def post_alerts_date(self):
        """Post a date-separator message in the alerts channel."""
        if self.mutils.market_open_today():
            date_string = date_utils.format_date_mdy(datetime.datetime.today())
            await self.alerts_channel.send(f"# :rotating_light: Alerts for {date_string} :rotating_light:")

    @tasks.loop(minutes=5)
    async def send_alerts(self):
        """Process alerts every 5 minutes if the market is open."""
        market_period = self.mutils.get_market_period()
        if self.mutils.market_open_today() and market_period != 'EOD':
            logger.info("Processing alerts")

            self.all_alert_tickers = all_alert_tickers = list(
                set([ticker for tickers in self.stock_data.alert_tickers.values() for ticker in tickers])
            )

            quotes = {}
            chunk_size = 25
            for i in range(0, len(all_alert_tickers), chunk_size):
                tickers = all_alert_tickers[i:i + chunk_size]
                quotes = quotes | await self.stock_data.schwab.get_quotes(tickers=tickers)
            logger.info(f"Encountered {len(quotes.pop('errors', []))} errors fetching quotes for alert tickers")

            await self.send_unusual_volume_movers(quotes=quotes)
            await self.send_volume_spike_movers(quotes=quotes)
            await self.send_earnings_movers(quotes=quotes)
            # await self.send_sec_filing_movers(tickers=all_alert_tickers, quotes=quotes)
            await self.send_watchlist_movers(quotes=quotes)
            logger.info("Alerts posted")

    @send_alerts.before_loop
    async def send_alerts_before_loop(self):
        """Wait until the next 0- or 5-minute boundary before starting the alerts loop."""
        DELTA = 30
        await asyncio.sleep(date_utils.seconds_until_minute_interval(5) + DELTA)

    # -------------------------------------------------------------------------
    # Alert trigger methods
    # -------------------------------------------------------------------------

    async def send_earnings_movers(self, quotes: dict):
        """Send earnings alerts when a reporting stock moves > ±5%."""
        logger.info("Processing earnings movers")
        today = datetime.date.today()
        earnings_today = self.stock_data.earnings.get_earnings_on_date(date=today)

        quotes = {ticker: quote for ticker, quote in quotes.items()
                  if ticker in earnings_today['ticker'].to_list()}

        for ticker, quote in quotes.items():
            pct_change = quote['quote']['netPercentChange']
            if abs(pct_change) > 5.0:
                logger.debug(
                    f"Identified ticker '{ticker}' reporting earnings today with percent change "
                    f"{'{:.2f}%'.format(pct_change)}"
                )
                alert = await self.build_earnings_mover(ticker=ticker, quote=quote,
                                                        next_earnings_info=earnings_today[
                                                            earnings_today['ticker'] == ticker
                                                        ].to_dict(orient='records')[0])
                view = AlertButtons(ticker=ticker)
                await send_alert(alert, self.alerts_channel, self.dstate, view=view)

    async def send_sec_filing_movers(self, gainers, quotes: dict):
        logger.info("Processing SEC filing movers")
        for index, row in gainers.iterrows():
            ticker = row['Ticker']
            filings = self.bot.sec.get_filings_from_today(ticker)
            pct_change = quotes[ticker]['quote']['netPercentChange']
            if filings.size > 0 and abs(pct_change) > 10.0:
                logger.debug(
                    f"Identified ticker '{ticker}' with SEC filings today and percent change "
                    f"{'{:.2f}%'.format(pct_change)}"
                )
                alert = SECFilingMoverAlert(ticker=ticker, quote=quotes[ticker])
                view = AlertButtons(ticker=ticker)
                await send_alert(alert, self.alerts_channel, self.dstate, view=view)
            await asyncio.sleep(1)

    async def send_watchlist_movers(self, quotes: dict):
        """Send watchlist alerts when a watched stock moves > ±10%."""
        logger.info("Processing watchlist movers")

        all_watchlist_tickers = self.stock_data.watchlists.get_all_watchlist_tickers()
        quotes = {ticker: quote for ticker, quote in quotes.items()
                  if ticker in all_watchlist_tickers}

        for ticker, quote in quotes.items():
            pct_change = quote['quote']['netPercentChange']
            if abs(pct_change) > 10.0:
                logger.debug(
                    f"Identified ticker '{ticker}' on watchlist with percent change "
                    f"{'{:.2f}%'.format(pct_change)}"
                )
                alert = await self.build_watchlist_mover(ticker=ticker)
                view = AlertButtons(ticker=ticker)
                await send_alert(alert, self.alerts_channel, self.dstate, view=view)

    async def send_unusual_volume_movers(self, quotes: dict):
        """Send unusual-volume alerts when RVOL > 25 and % change > ±10%."""
        logger.info("Processing unusual volume movers")

        for ticker, quote in quotes.items():
            daily_price_history = self.stock_data.fetch_daily_price_history(ticker=ticker)
            if not daily_price_history.empty:
                periods = 10
                curr_volume = quote['quote']['totalVolume']
                rvol = an.indicators.volume.rvol(data=daily_price_history, periods=periods,
                                                  curr_volume=curr_volume)
                pct_change = quote['quote']['netPercentChange']

                if rvol > 25.0 and abs(pct_change) > 10.0 and rvol is not np.nan:
                    logger.debug(
                        f"Identified ticker '{ticker}' with RVOL {'{:.2f}x'.format(rvol)} "
                        f"and percent change {'{:.2f}%'.format(pct_change)}"
                    )
                    alert = await self.build_volume_mover(ticker=ticker, rvol=rvol, quote=quote,
                                                          daily_price_history=daily_price_history)
                    view = AlertButtons(ticker=ticker)
                    await send_alert(alert, self.alerts_channel, self.dstate, view=view)

    async def send_volume_spike_movers(self, quotes: dict):
        """Send volume-spike alerts when RVOL_AT_TIME > 50 and % change > ±10%."""
        logger.info("Processing volume spike movers")

        for ticker, quote in quotes.items():
            now = datetime.datetime.now()
            fivem_price_history = self.stock_data.fetch_5m_price_history(ticker=ticker)

            if not fivem_price_history.empty:
                periods = 10
                today_data = await self.stock_data.schwab.get_5m_price_history(
                    ticker=ticker, start_datetime=now
                )
                rvol_at_time = an.indicators.volume.rvol_at_time(
                    data=fivem_price_history, today_data=today_data, periods=periods, dt=now
                )
                avg_vol_at_time, time = an.indicators.volume.avg_vol_at_time(
                    data=fivem_price_history, periods=periods
                )
                pct_change = quote['quote']['netPercentChange']

                if (rvol_at_time > 50.0 and abs(pct_change) > 10.0
                        and rvol_at_time is not np.nan and avg_vol_at_time is not np.nan):
                    logger.debug(
                        f"Identified ticker '{ticker}' with RVOL at time ({time}) "
                        f"{'{:.2f}x'.format(rvol_at_time)} and percent change "
                        f"{'{:.2f}%'.format(pct_change)}"
                    )
                    alert = await self.build_volume_spike_alert(
                        ticker=ticker, quote=quote,
                        rvol_at_time=rvol_at_time, avg_vol_at_time=avg_vol_at_time, time=time
                    )
                    view = AlertButtons(ticker=ticker)
                    await send_alert(alert, self.alerts_channel, self.dstate, view=view)
                    await asyncio.sleep(1)

    @tasks.loop(minutes=30)
    async def send_popularity_movers(self):
        logger.info("Processing popularity movers")

        pop_stocks = self.stock_data.popularity.get_popular_stocks(num_stocks=100)['ticker'].to_list()

        for ticker in pop_stocks:
            popularity = self.stock_data.fetch_popularity(ticker=ticker)
            if not popularity.empty:
                now = date_utils.round_down_nearest_minute(30)
                popularity_today = popularity[(popularity['datetime'] == now)]
                current_rank = popularity_today['rank'].iloc[0] if not popularity_today.empty else 'N/A'

                if current_rank != 'N/A':
                    interval_map = {
                        "High 1D": 1,
                        "High 2D": 2,
                        "High 3D": 3,
                        "High 4D": 4,
                        "High 5D": 5,
                    }

                    for label, interval in interval_map.items():
                        interval_date = now - datetime.timedelta(days=interval)
                        interval_popularity = popularity[popularity['datetime'] == interval_date]
                        if not interval_popularity.empty:
                            interval_max_rank = interval_popularity['rank'].max()
                        else:
                            interval_max_rank = 'N/A'

                        pct_diff = (
                            abs((float(current_rank) - float(interval_max_rank)) / float(interval_max_rank)) * 100.0
                            if (current_rank != 'N/A' and interval_max_rank != 'N/A') else 0.0
                        )
                        if (current_rank != 'N/A' and interval_max_rank != 'N/A'
                                and pct_diff > 75.0 and current_rank < interval_max_rank
                                and interval_max_rank >= 10):
                            alert = await self.build_popularity_mover(
                                ticker=ticker, popularity=popularity
                            )
                            view = AlertButtons(ticker=ticker)
                            await send_alert(alert, self.alerts_channel, self.dstate, view=view)

    @tasks.loop(hours=1)
    async def send_politician_trade_alerts(self):
        politician = self.stock_data.capitol_trades.politician(name='Nancy Pelosi')
        trades = self.stock_data.capitol_trades.trades(pid=politician['politician_id'])
        today = date_utils.format_date_mdy(datetime.date.today())
        todays_trades = trades[trades['Published Date'].apply(lambda x: x == today)]
        if not todays_trades.empty:
            alert = PoliticianTradeAlert(
                politician=politician,
                trades=todays_trades,
            )
            view = PoliticianTradeButtons(politician=politician)
            await send_alert(alert, self.alerts_channel, self.dstate, view=view)

    @send_politician_trade_alerts.before_loop
    async def sleep_until_5m_interval(self):
        await asyncio.sleep(date_utils.seconds_until_minute_interval(5))

    # -------------------------------------------------------------------------
    # Builder methods
    # -------------------------------------------------------------------------

    async def build_earnings_mover(self, ticker: str, **kwargs) -> EarningsMoverAlert:
        """Build an EarningsMoverAlert for the given ticker."""
        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        next_earnings_info = kwargs.pop(
            'next_earnings_info', self.stock_data.earnings.get_next_earnings_info(ticker=ticker)
        )
        historical_earnings = kwargs.pop(
            'historical_earnings', self.stock_data.earnings.get_historical_earnings(ticker=ticker)
        )
        return EarningsMoverAlert(
            ticker=ticker,
            quote=quote,
            next_earnings_info=next_earnings_info,
            historical_earnings=historical_earnings,
        )

    async def build_watchlist_mover(self, ticker: str, **kwargs) -> WatchlistMoverAlert:
        """Build a WatchlistMoverAlert for the given ticker."""
        def get_ticker_watchlist(ticker: str):
            watchlists = self.stock_data.watchlists.get_watchlists()
            for watchlist_id in watchlists:
                watchlist_tickers = self.stock_data.watchlists.get_watchlist_tickers(
                    watchlist_id=watchlist_id
                )
                if ticker in watchlist_tickers:
                    return watchlist_id

        quote = kwargs.pop('quote', self.stock_data.schwab.get_quote(ticker=ticker))
        watchlist = kwargs.pop('watchlist', get_ticker_watchlist(ticker=ticker))
        return WatchlistMoverAlert(ticker=ticker, quote=quote, watchlist=watchlist)

    async def build_volume_mover(self, ticker: str, **kwargs) -> VolumeMoverAlert:
        """Build a VolumeMoverAlert for the given ticker."""
        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        daily_price_history = kwargs.pop(
            'daily_price_history', self.stock_data.fetch_daily_price_history(ticker=ticker)
        )
        rvol = kwargs.pop('rvol', an.indicators.volume.rvol(
            data=daily_price_history, curr_volume=quote['quote']['totalVolume']
        ))
        return VolumeMoverAlert(
            ticker=ticker, rvol=rvol, quote=quote, daily_price_history=daily_price_history
        )

    async def build_volume_spike_alert(self, ticker: str, **kwargs) -> VolumeSpikeAlert:
        """Build a VolumeSpikeAlert for the given ticker."""
        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        avg_vol_at_time = kwargs.pop('avg_vol_at_time', None)
        time = kwargs.pop('time', None)
        rvol_at_time = kwargs.pop('rvol_at_time', an.indicators.volume.rvol_at_time(
            data=self.stock_data.fetch_5m_price_history(ticker=ticker),
            today_data=await self.stock_data.schwab.get_5m_price_history(
                ticker=ticker, start_datetime=datetime.datetime.now()
            ),
            dt=datetime.datetime.now(),
        ))

        if not avg_vol_at_time and not time:
            fivem_price_history = self.stock_data.fetch_5m_price_history(ticker=ticker)
            avg_vol_at_time, time = an.indicators.volume.avg_vol_at_time(data=fivem_price_history)

        if not isinstance(time, str):
            time = time.strftime("%I:%M %p")

        return VolumeSpikeAlert(
            ticker=ticker, rvol_at_time=rvol_at_time,
            avg_vol_at_time=avg_vol_at_time, quote=quote, time=time,
        )

    async def build_popularity_mover(self, ticker: str, **kwargs) -> PopularityAlert:
        """Build a PopularityAlert for the given ticker."""
        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        popularity = kwargs.pop('popularity', self.stock_data.fetch_popularity(ticker=ticker))
        return PopularityAlert(ticker=ticker, quote=quote, popularity=popularity)


#########
# Setup #
#########

async def setup(bot: commands.Bot):
    await bot.add_cog(Alerts(bot, bot.stock_data))
