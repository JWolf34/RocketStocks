import datetime
import logging
import asyncio
import math
import time
import traceback as tb
import numpy as np
import pandas as pd
from discord.ext import commands, tasks

from src.rocketstocks.data.stockdata import StockData
from rocketstocks.data.channel_config import ALERTS
from rocketstocks.data.discord_state import DiscordState
from rocketstocks.core.utils.market import market_utils
from rocketstocks.core.utils.dates import date_utils
from rocketstocks.core.notifications.config import NotificationLevel
from rocketstocks.core.notifications.event import NotificationEvent
import rocketstocks.core.analysis.indicators as an

from rocketstocks.core.analysis.alert_strategy import evaluate_price_alert
from rocketstocks.core.analysis.indicators import indicators

from rocketstocks.core.content.models import (
    EarningsMoverData,
    VolumeMoverData,
    VolumeSpikeData,
    WatchlistMoverData,
    SECFilingData,
    PopularityAlertData,
    PoliticianTradeAlertData,
)
from rocketstocks.core.content.alerts.earnings_alert import EarningsMoverAlert
from rocketstocks.core.content.alerts.sec_filing_alert import SECFilingMoverAlert
from rocketstocks.core.content.alerts.watchlist_alert import WatchlistMoverAlert
from rocketstocks.core.content.alerts.volume_alert import VolumeMoverAlert
from rocketstocks.core.content.alerts.volume_spike_alert import VolumeSpikeAlert
from rocketstocks.core.content.alerts.popularity_alert import PopularityAlert
from rocketstocks.core.content.alerts.politician_alert import PoliticianTradeAlert

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

        self.post_alerts_date.start()
        self.send_popularity_movers.start()
        # self.send_politician_trade_alerts.start()  # TODO
        self.send_alerts.start()

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    # -------------------------------------------------------------------------
    # Task runner helper
    # -------------------------------------------------------------------------

    async def _run_task(self, name: str, coro) -> None:
        """Run *coro*, emit SUCCESS/FAILURE notification. Never re-raises."""
        _start = time.monotonic()
        try:
            await coro
            self.bot.emitter.emit(NotificationEvent(
                level=NotificationLevel.SUCCESS,
                source=__name__,
                job_name=name,
                message="Task completed successfully",
                elapsed_seconds=time.monotonic() - _start,
            ))
        except Exception as exc:
            self.bot.emitter.emit(NotificationEvent(
                level=NotificationLevel.FAILURE,
                source=__name__,
                job_name=name,
                message=str(exc),
                traceback=tb.format_exc(),
                elapsed_seconds=time.monotonic() - _start,
            ))

    # -------------------------------------------------------------------------
    # Tasks
    # -------------------------------------------------------------------------

    @tasks.loop(time=datetime.time(hour=6, minute=0, second=0))  # UTC
    async def post_alerts_date(self):
        """Post a date-separator message in all configured alerts channels."""
        await self._run_task("post_alerts_date", self._post_alerts_date_impl())

    async def _post_alerts_date_impl(self):
        if self.mutils.market_open_today():
            date_string = date_utils.format_date_mdy(datetime.datetime.today())
            for _, channel in self.bot.iter_channels(ALERTS):
                await channel.send(f"# :rotating_light: Alerts for {date_string} :rotating_light:")

    @tasks.loop(minutes=5)
    async def send_alerts(self):
        """Process alerts every 5 minutes if the market is open."""
        await self._run_task("send_alerts", self._send_alerts_impl())

    async def _send_alerts_impl(self):
        market_period = self.mutils.get_market_period()
        if self.mutils.market_open_today() and market_period != 'EOD':
            logger.info("Processing alerts")

            alert_channels = [ch for _, ch in self.bot.iter_channels(ALERTS)]
            if not alert_channels:
                logger.warning("No alerts channels configured — skipping alert processing")
                return

            self.all_alert_tickers = all_alert_tickers = list(
                set([ticker for tickers in self.stock_data.alert_tickers.values() for ticker in tickers])
            )

            quotes = {}
            chunk_size = 25
            for i in range(0, len(all_alert_tickers), chunk_size):
                tickers = all_alert_tickers[i:i + chunk_size]
                quotes = quotes | await self.stock_data.schwab.get_quotes(tickers=tickers)
            logger.info(f"Encountered {len(quotes.pop('errors', []))} errors fetching quotes for alert tickers")

            # Fetch all classifications once for the alert loop
            classifications = await asyncio.to_thread(
                self.stock_data.ticker_stats.get_all_classifications
            )

            labels = [
                "send_unusual_volume_movers",
                "send_volume_spike_movers",
                "send_earnings_movers",
                "send_watchlist_movers",
            ]
            results = await asyncio.gather(
                self.send_unusual_volume_movers(quotes=quotes, channels=alert_channels,
                                               classifications=classifications),
                self.send_volume_spike_movers(quotes=quotes, channels=alert_channels,
                                              classifications=classifications),
                self.send_earnings_movers(quotes=quotes, channels=alert_channels,
                                         classifications=classifications),
                self.send_watchlist_movers(quotes=quotes, channels=alert_channels,
                                          classifications=classifications),
                return_exceptions=True,
            )
            for label, result in zip(labels, results):
                if isinstance(result, Exception):
                    logger.error(f"[send_alerts] {label} failed: {result}", exc_info=result)

            logger.info("Alerts posted")

    @send_alerts.before_loop
    async def send_alerts_before_loop(self):
        """Wait until the next 0- or 5-minute boundary before starting the alerts loop."""
        DELTA = 30
        await asyncio.sleep(date_utils.seconds_until_minute_interval(5) + DELTA)

    @tasks.loop(minutes=30)
    async def send_popularity_movers(self):
        await self._run_task("send_popularity_movers", self._send_popularity_movers_impl())

    @send_popularity_movers.before_loop
    async def send_popularity_movers_before_loop(self):
        """Wait until the next 30-minute boundary before starting the popularity loop."""
        await asyncio.sleep(date_utils.seconds_until_minute_interval(30))

    async def _send_popularity_movers_impl(self):
        if not self.mutils.market_open_today():
            return
        logger.info("Processing popularity movers")

        pop_stocks = self.stock_data.popularity.get_popular_stocks(num_stocks=100)['ticker'].to_list()

        for ticker in pop_stocks:
            try:
                popularity = self.stock_data.popularity.fetch_popularity(ticker=ticker)
                if not popularity.empty:
                    now = date_utils.round_down_nearest_minute(30)
                    popularity_today = popularity[(popularity['datetime'] == now)]
                    current_rank = popularity_today['rank'].iloc[0] if not popularity_today.empty else None

                    if current_rank is not None:
                        # Compute rank velocity z-score
                        rv_zscore = indicators.popularity.rank_velocity_zscore(
                            popularity_df=popularity,
                            lookback=30,
                            velocity_window=5,
                        )
                        rv = indicators.popularity.rank_velocity(
                            popularity_df=popularity,
                            periods=5,
                        )

                        # Alert when rank velocity z-score indicates unusual acceleration
                        if not math.isnan(rv_zscore) and abs(rv_zscore) >= 2.0:
                            alert = await self.build_popularity_mover(
                                ticker=ticker,
                                popularity=popularity,
                                rank_velocity=rv,
                                rank_velocity_zscore=rv_zscore,
                            )
                            view = AlertButtons(ticker=ticker)
                            for _, channel in self.bot.iter_channels(ALERTS):
                                await send_alert(alert, channel, self.dstate, view=view)
            except Exception:
                logger.error(f"[send_popularity_movers] Failed to process ticker '{ticker}'", exc_info=True)
            await asyncio.sleep(0)

    @tasks.loop(hours=1)
    async def send_politician_trade_alerts(self):
        await self._run_task("send_politician_trade_alerts", self._send_politician_trade_alerts_impl())

    async def _send_politician_trade_alerts_impl(self):
        politician = self.stock_data.capitol_trades.politician(name='Nancy Pelosi')
        trades = self.stock_data.capitol_trades.trades(pid=politician['politician_id'])
        today = date_utils.format_date_mdy(datetime.date.today())
        todays_trades = trades[trades['Published Date'].apply(lambda x: x == today)]
        if not todays_trades.empty:
            alert = PoliticianTradeAlert(data=PoliticianTradeAlertData(
                politician=politician,
                trades=todays_trades,
            ))
            view = PoliticianTradeButtons(politician=politician)
            for _, channel in self.bot.iter_channels(ALERTS):
                await send_alert(alert, channel, self.dstate, view=view)

    @send_politician_trade_alerts.before_loop
    async def sleep_until_5m_interval(self):
        await asyncio.sleep(date_utils.seconds_until_minute_interval(5))

    # -------------------------------------------------------------------------
    # Alert trigger methods
    # -------------------------------------------------------------------------

    async def send_earnings_movers(
        self,
        quotes: dict,
        channels: list,
        classifications: dict | None = None,
    ):
        """Send earnings alerts when a reporting stock moves significantly (z-score based)."""
        logger.info("Processing earnings movers")
        today = datetime.date.today()
        earnings_today = self.stock_data.earnings.get_earnings_on_date(date=today)

        if earnings_today.empty:
            return

        quotes = {ticker: quote for ticker, quote in quotes.items()
                  if ticker in earnings_today['ticker'].to_list()}

        for ticker, quote in quotes.items():
            try:
                pct_change = quote['quote']['netPercentChange']
                classification = (classifications or {}).get(ticker, 'standard')
                daily_price_history = await asyncio.to_thread(
                    self.stock_data.price_history.fetch_daily_price_history, ticker=ticker
                )
                current_volume = quote['quote'].get('totalVolume')

                trigger_result = evaluate_price_alert(
                    classification=classification,
                    pct_change=pct_change,
                    daily_prices=daily_price_history,
                    current_volume=current_volume,
                )

                if trigger_result.should_alert:
                    logger.debug(
                        f"Identified ticker '{ticker}' reporting earnings today with percent change "
                        f"{pct_change:.2f}% (z-score: {trigger_result.zscore:.2f})"
                    )
                    alert = await self.build_earnings_mover(
                        ticker=ticker,
                        quote=quote,
                        next_earnings_info=earnings_today[
                            earnings_today['ticker'] == ticker
                        ].to_dict(orient='records')[0],
                        daily_price_history=daily_price_history,
                        trigger_result=trigger_result,
                    )
                    view = AlertButtons(ticker=ticker)
                    for channel in channels:
                        await send_alert(alert, channel, self.dstate, view=view)
            except Exception:
                logger.error(f"[send_earnings_movers] Failed to process ticker '{ticker}'", exc_info=True)

    async def send_sec_filing_movers(self, gainers, quotes: dict, channels: list):
        logger.info("Processing SEC filing movers")
        for index, row in gainers.iterrows():
            ticker = row['Ticker']
            filings = self.bot.sec.get_filings_from_today(ticker)
            pct_change = quotes[ticker]['quote']['netPercentChange']
            if filings.size > 0 and abs(pct_change) > 10.0:
                logger.debug(
                    f"Identified ticker '{ticker}' with SEC filings today and percent change "
                    f"{pct_change:.2f}%"
                )
                recent_sec_filings = self.stock_data.sec.get_recent_filings(ticker=ticker)
                alert = SECFilingMoverAlert(data=SECFilingData(
                    ticker=ticker,
                    ticker_info=self.stock_data.tickers.get_ticker_info(ticker=ticker),
                    quote=quotes[ticker],
                    recent_sec_filings=recent_sec_filings,
                ))
                view = AlertButtons(ticker=ticker)
                for channel in channels:
                    await send_alert(alert, channel, self.dstate, view=view)
            await asyncio.sleep(1)

    async def send_watchlist_movers(
        self,
        quotes: dict,
        channels: list,
        classifications: dict | None = None,
    ):
        """Send watchlist alerts when a watched stock moves significantly (z-score based)."""
        logger.info("Processing watchlist movers")

        all_watchlist_tickers = self.stock_data.watchlists.get_all_watchlist_tickers()
        quotes = {ticker: quote for ticker, quote in quotes.items()
                  if ticker in all_watchlist_tickers}

        for ticker, quote in quotes.items():
            try:
                pct_change = quote['quote']['netPercentChange']
                classification = (classifications or {}).get(ticker, 'standard')
                daily_price_history = await asyncio.to_thread(
                    self.stock_data.price_history.fetch_daily_price_history, ticker=ticker
                )
                current_volume = quote['quote'].get('totalVolume')

                trigger_result = evaluate_price_alert(
                    classification=classification,
                    pct_change=pct_change,
                    daily_prices=daily_price_history,
                    current_volume=current_volume,
                )

                if trigger_result.should_alert:
                    logger.debug(
                        f"Identified ticker '{ticker}' on watchlist with percent change "
                        f"{pct_change:.2f}% (z-score: {trigger_result.zscore:.2f})"
                    )
                    alert = await self.build_watchlist_mover(
                        ticker=ticker,
                        quote=quote,
                        daily_price_history=daily_price_history,
                        trigger_result=trigger_result,
                    )
                    view = AlertButtons(ticker=ticker)
                    for channel in channels:
                        await send_alert(alert, channel, self.dstate, view=view)
            except Exception:
                logger.error(f"[send_watchlist_movers] Failed to process ticker '{ticker}'", exc_info=True)

    async def send_unusual_volume_movers(
        self,
        quotes: dict,
        channels: list,
        classifications: dict | None = None,
    ):
        """Send unusual-volume alerts when z-score thresholds are met."""
        logger.info("Processing unusual volume movers")

        for ticker, quote in quotes.items():
            try:
                daily_price_history = self.stock_data.price_history.fetch_daily_price_history(ticker=ticker)
                if not daily_price_history.empty:
                    periods = 10
                    curr_volume = quote['quote']['totalVolume']
                    rvol = an.indicators.volume.rvol(data=daily_price_history, periods=periods,
                                                     curr_volume=curr_volume)
                    pct_change = quote['quote']['netPercentChange']
                    classification = (classifications or {}).get(ticker, 'standard')

                    trigger_result = evaluate_price_alert(
                        classification=classification,
                        pct_change=pct_change,
                        daily_prices=daily_price_history,
                        current_volume=curr_volume,
                    )

                    if trigger_result.should_alert and rvol is not np.nan:
                        logger.debug(
                            f"Identified ticker '{ticker}' with RVOL {rvol:.2f}x "
                            f"and percent change {pct_change:.2f}% "
                            f"(z-score: {trigger_result.zscore:.2f})"
                        )
                        alert = await self.build_volume_mover(
                            ticker=ticker,
                            rvol=rvol,
                            quote=quote,
                            daily_price_history=daily_price_history,
                            trigger_result=trigger_result,
                        )
                        view = AlertButtons(ticker=ticker)
                        for channel in channels:
                            await send_alert(alert, channel, self.dstate, view=view)
            except Exception:
                logger.error(f"[send_unusual_volume_movers] Failed to process ticker '{ticker}'", exc_info=True)

    async def send_volume_spike_movers(
        self,
        quotes: dict,
        channels: list,
        classifications: dict | None = None,
    ):
        """Send volume-spike alerts when z-score thresholds are met."""
        logger.info("Processing volume spike movers")

        for ticker, quote in quotes.items():
            try:
                now = datetime.datetime.now()
                fivem_price_history = self.stock_data.price_history.fetch_5m_price_history(ticker=ticker)

                if not fivem_price_history.empty:
                    periods = 10
                    today_data = await self.stock_data.schwab.get_5m_price_history(
                        ticker=ticker, start_datetime=now
                    )
                    rvol_at_time = an.indicators.volume.rvol_at_time(
                        data=fivem_price_history, today_data=today_data, periods=periods, dt=now
                    )
                    avg_vol_at_time, time_val = an.indicators.volume.avg_vol_at_time(
                        data=fivem_price_history, periods=periods
                    )
                    pct_change = quote['quote']['netPercentChange']
                    classification = (classifications or {}).get(ticker, 'standard')

                    # Fetch daily price history for statistical evaluation
                    daily_price_history = self.stock_data.price_history.fetch_daily_price_history(ticker=ticker)
                    curr_volume = quote['quote'].get('totalVolume')

                    trigger_result = evaluate_price_alert(
                        classification=classification,
                        pct_change=pct_change,
                        daily_prices=daily_price_history,
                        current_volume=curr_volume,
                    )

                    if (trigger_result.should_alert
                            and rvol_at_time is not np.nan
                            and avg_vol_at_time is not np.nan):
                        logger.debug(
                            f"Identified ticker '{ticker}' with RVOL at time ({time_val}) "
                            f"{rvol_at_time:.2f}x and percent change "
                            f"{pct_change:.2f}% (z-score: {trigger_result.zscore:.2f})"
                        )
                        alert = await self.build_volume_spike_alert(
                            ticker=ticker,
                            quote=quote,
                            rvol_at_time=rvol_at_time,
                            avg_vol_at_time=avg_vol_at_time,
                            time=time_val,
                            daily_price_history=daily_price_history,
                            trigger_result=trigger_result,
                        )
                        view = AlertButtons(ticker=ticker)
                        for channel in channels:
                            await send_alert(alert, channel, self.dstate, view=view)
                        await asyncio.sleep(1)
            except Exception:
                logger.error(f"[send_volume_spike_movers] Failed to process ticker '{ticker}'", exc_info=True)

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
        daily_price_history = kwargs.pop('daily_price_history', None)
        trigger_result = kwargs.pop('trigger_result', None)
        return EarningsMoverAlert(data=EarningsMoverData(
            ticker=ticker,
            ticker_info=self.stock_data.tickers.get_ticker_info(ticker=ticker),
            quote=quote,
            next_earnings_info=next_earnings_info,
            historical_earnings=historical_earnings,
            daily_price_history=daily_price_history if daily_price_history is not None else pd.DataFrame(),
            trigger_result=trigger_result,
        ))

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

        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        watchlist = kwargs.pop('watchlist', get_ticker_watchlist(ticker=ticker))
        daily_price_history = kwargs.pop('daily_price_history', None)
        trigger_result = kwargs.pop('trigger_result', None)
        return WatchlistMoverAlert(data=WatchlistMoverData(
            ticker=ticker,
            ticker_info=self.stock_data.tickers.get_ticker_info(ticker=ticker),
            quote=quote,
            watchlist=watchlist,
            daily_price_history=daily_price_history if daily_price_history is not None else pd.DataFrame(),
            trigger_result=trigger_result,
        ))

    async def build_volume_mover(self, ticker: str, **kwargs) -> VolumeMoverAlert:
        """Build a VolumeMoverAlert for the given ticker."""
        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        daily_price_history = kwargs.pop(
            'daily_price_history', self.stock_data.price_history.fetch_daily_price_history(ticker=ticker)
        )
        rvol = kwargs.pop('rvol', an.indicators.volume.rvol(
            data=daily_price_history, curr_volume=quote['quote']['totalVolume']
        ))
        trigger_result = kwargs.pop('trigger_result', None)
        return VolumeMoverAlert(data=VolumeMoverData(
            ticker=ticker,
            ticker_info=self.stock_data.tickers.get_ticker_info(ticker=ticker),
            quote=quote,
            rvol=rvol,
            daily_price_history=daily_price_history,
            trigger_result=trigger_result,
        ))

    async def build_volume_spike_alert(self, ticker: str, **kwargs) -> VolumeSpikeAlert:
        """Build a VolumeSpikeAlert for the given ticker."""
        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        avg_vol_at_time = kwargs.pop('avg_vol_at_time', None)
        time_val = kwargs.pop('time', None)
        daily_price_history = kwargs.pop('daily_price_history', None)
        trigger_result = kwargs.pop('trigger_result', None)
        rvol_at_time = kwargs.pop('rvol_at_time', an.indicators.volume.rvol_at_time(
            data=self.stock_data.price_history.fetch_5m_price_history(ticker=ticker),
            today_data=await self.stock_data.schwab.get_5m_price_history(
                ticker=ticker, start_datetime=datetime.datetime.now()
            ),
            dt=datetime.datetime.now(),
        ))

        if not avg_vol_at_time and not time_val:
            fivem_price_history = self.stock_data.price_history.fetch_5m_price_history(ticker=ticker)
            avg_vol_at_time, time_val = an.indicators.volume.avg_vol_at_time(data=fivem_price_history)

        if not isinstance(time_val, str):
            time_val = time_val.strftime("%I:%M %p")

        return VolumeSpikeAlert(data=VolumeSpikeData(
            ticker=ticker,
            ticker_info=self.stock_data.tickers.get_ticker_info(ticker=ticker),
            quote=quote,
            rvol_at_time=rvol_at_time,
            avg_vol_at_time=avg_vol_at_time,
            time=time_val,
            daily_price_history=daily_price_history if daily_price_history is not None else pd.DataFrame(),
            trigger_result=trigger_result,
        ))

    async def build_popularity_mover(self, ticker: str, **kwargs) -> PopularityAlert:
        """Build a PopularityAlert for the given ticker."""
        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        popularity = kwargs.pop('popularity', self.stock_data.popularity.fetch_popularity(ticker=ticker))
        rank_velocity = kwargs.pop('rank_velocity', 0.0)
        rank_velocity_zscore = kwargs.pop('rank_velocity_zscore', 0.0)
        return PopularityAlert(data=PopularityAlertData(
            ticker=ticker,
            ticker_info=self.stock_data.tickers.get_ticker_info(ticker=ticker),
            quote=quote,
            popularity=popularity,
            rank_velocity=rank_velocity,
            rank_velocity_zscore=rank_velocity_zscore,
        ))


#########
# Setup #
#########

async def setup(bot: commands.Bot):
    await bot.add_cog(Alerts(bot, bot.stock_data))
