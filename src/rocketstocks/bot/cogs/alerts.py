import datetime
import logging
import asyncio
import time
import traceback as tb

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
from rocketstocks.core.analysis.popularity_signals import evaluate_popularity_surge
from rocketstocks.core.analysis.composite_score import compute_composite_score
from rocketstocks.core.analysis.signal_confirmation import should_confirm_signal

from rocketstocks.core.content.models import (
    EarningsMoverData,
    WatchlistMoverData,
    PopularitySurgeData,
    MomentumConfirmationData,
    MarketAlertData,
    MarketMoverData,
)
from rocketstocks.core.content.alerts.earnings_alert import EarningsMoverAlert
from rocketstocks.core.content.alerts.watchlist_alert import WatchlistMoverAlert
from rocketstocks.core.content.alerts.popularity_surge_alert import PopularitySurgeAlert
from rocketstocks.core.content.alerts.momentum_confirmation_alert import MomentumConfirmationAlert
from rocketstocks.core.content.alerts.market_alert import MarketAlert
from rocketstocks.core.content.alerts.market_mover_alert import MarketMoverAlert

from rocketstocks.bot.views.alert_views import (
    AlertButtons,
    PopularitySurgeAlertButtons,
    POPULARITY_SURGE_DOC_URL,
    MOMENTUM_CONFIRMATION_DOC_URL,
    MARKET_ALERT_DOC_URL,
    MARKET_MOVER_DOC_URL,
    WATCHLIST_MOVER_DOC_URL,
    EARNINGS_MOVER_DOC_URL,
)
from rocketstocks.bot.senders.alert_sender import send_alert

logger = logging.getLogger(__name__)


class Alerts(commands.Cog):
    """Push alerts to Discord when criteria for stock movements are met."""

    def __init__(self, bot: commands.Bot, stock_data: StockData):
        self.bot = bot
        self.stock_data = stock_data
        self.mutils = market_utils()
        self.dstate = DiscordState(db=bot.stock_data.db)

        self.post_alerts_date.start()
        self.detect_popularity_surges.start()
        self.process_alerts.start()

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    # -------------------------------------------------------------------------
    # Role mention helper
    # -------------------------------------------------------------------------

    async def _build_role_mention(self, alert, channel) -> str | None:
        """Build a role mention string for the given alert and channel."""
        role_key = getattr(alert, 'role_key', None)
        if not role_key or not hasattr(channel, 'guild') or channel.guild is None:
            return None
        ids = await self.bot.stock_data.alert_roles.get_role_ids(
            channel.guild.id, [role_key, 'all_alerts']
        )
        return " ".join(f"<@&{rid}>" for rid in ids) or None

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
            for _, channel in await self.bot.iter_channels(ALERTS):
                try:
                    await channel.send(f"# :rotating_light: Alerts for {date_string} :rotating_light:")
                except Exception:
                    logger.error(f"Failed to send date header to channel {channel.id}", exc_info=True)

    @tasks.loop(minutes=30)
    async def detect_popularity_surges(self):
        """Detect popularity surges every 30 minutes."""
        await self._run_task("detect_popularity_surges", self._detect_popularity_surges_impl())

    @detect_popularity_surges.before_loop
    async def detect_popularity_surges_before_loop(self):
        """Wait until the next 30-minute boundary before starting the surge loop."""
        await asyncio.sleep(date_utils.seconds_until_minute_interval(30))

    @tasks.loop(minutes=5)
    async def process_alerts(self):
        """Process price/volume alerts every 5 minutes if the market is open."""
        await self._run_task("process_alerts", self._process_alerts_impl())

    @process_alerts.before_loop
    async def process_alerts_before_loop(self):
        """Wait until the next 0- or 5-minute boundary before starting the alerts loop."""
        DELTA = 30
        await asyncio.sleep(date_utils.seconds_until_minute_interval(5) + DELTA)

    # -------------------------------------------------------------------------
    # Tier 1: Popularity surge detection
    # -------------------------------------------------------------------------

    async def _detect_popularity_surges_impl(self):
        if not self.mutils.market_open_today():
            return
        logger.info("Processing popularity surge detection")

        alert_channels = [ch for _, ch in await self.bot.iter_channels(ALERTS)]
        if not alert_channels:
            logger.warning("No alerts channels configured — skipping surge detection")
            return

        pop_df = await asyncio.to_thread(
            self.stock_data.popularity.get_popular_stocks,
            num_stocks=1000,
        )
        if pop_df.empty:
            logger.warning("No popularity data returned from ApeWisdom")
            return

        for _, row in pop_df.iterrows():
            ticker = row.get('ticker')
            if not ticker:
                await asyncio.sleep(0)
                continue

            try:
                already_flagged = await self.stock_data.surge_store.is_already_flagged(ticker)
                if already_flagged:
                    await asyncio.sleep(0)
                    continue

                current_rank = row.get('rank')
                rank_24h_ago = row.get('rank_24h_ago')
                mentions = row.get('mentions')
                mentions_24h_ago = row.get('mentions_24h_ago')

                popularity_history = await self.stock_data.popularity.fetch_popularity(ticker=ticker)

                surge_result = evaluate_popularity_surge(
                    ticker=ticker,
                    current_rank=int(current_rank) if current_rank is not None else None,
                    rank_24h_ago=int(rank_24h_ago) if rank_24h_ago is not None else None,
                    mentions=int(mentions) if mentions is not None else None,
                    mentions_24h_ago=int(mentions_24h_ago) if mentions_24h_ago is not None else None,
                    popularity_history=popularity_history,
                )

                if surge_result.is_surging:
                    logger.info(
                        f"[detect_popularity_surges] Surge detected for '{ticker}': "
                        f"{[st.value for st in surge_result.surge_types]}"
                    )
                    quote = await self.stock_data.schwab.get_quote(ticker=ticker)
                    ticker_info = await self.stock_data.tickers.get_ticker_info(ticker=ticker)
                    market = market_utils()
                    price_at_flag = market.get_current_price(quote)

                    alert = PopularitySurgeAlert(data=PopularitySurgeData(
                        ticker=ticker,
                        ticker_info=ticker_info,
                        quote=quote,
                        surge_result=surge_result,
                        popularity_history=popularity_history,
                    ))
                    view = PopularitySurgeAlertButtons(ticker=ticker, doc_url=POPULARITY_SURGE_DOC_URL)

                    flagged_at = datetime.datetime.utcnow()
                    surge_types_str = ",".join(st.value for st in surge_result.surge_types)

                    first_message = None
                    for channel in alert_channels:
                        role_mention = await self._build_role_mention(alert, channel)
                        sent = await send_alert(alert, channel, self.dstate, view=view, role_mention=role_mention)
                        if sent is not None and first_message is None:
                            first_message = sent

                    message_id = first_message.id if first_message else None
                    await self.stock_data.surge_store.insert_surge(
                        ticker=ticker,
                        flagged_at=flagged_at,
                        surge_types=surge_types_str,
                        current_rank=surge_result.current_rank,
                        mention_ratio=surge_result.mention_ratio,
                        rank_change=surge_result.rank_change,
                        price_at_flag=price_at_flag,
                        alert_message_id=message_id,
                    )

            except Exception:
                logger.error(f"[detect_popularity_surges] Failed for '{ticker}'", exc_info=True)

            await asyncio.sleep(0)

    # -------------------------------------------------------------------------
    # Tier 2: Main alert processing loop
    # -------------------------------------------------------------------------

    async def _process_alerts_impl(self):
        market_period = self.mutils.get_market_period()
        if not self.mutils.market_open_today() or market_period == 'EOD':
            return

        logger.info("Processing alerts")

        alert_channels = [ch for _, ch in await self.bot.iter_channels(ALERTS)]
        if not alert_channels:
            logger.warning("No alerts channels configured — skipping alert processing")
            return

        # Gather screener tickers
        screener_tickers = list(
            set(t for tickers in self.stock_data.alert_tickers.values() for t in tickers)
        )

        # Gather active surge tickers
        active_surges = await self.stock_data.surge_store.get_active_surges()
        surge_tickers = [s['ticker'] for s in active_surges]

        # Gather active market signal tickers for confirmation pipeline
        active_signals = await self.stock_data.market_signal_store.get_active_signals()
        signal_tickers = [s['ticker'] for s in active_signals]

        all_tickers = list(set(screener_tickers + surge_tickers + signal_tickers))

        # Bulk Schwab quote fetch
        quotes = {}
        chunk_size = 25
        for i in range(0, len(all_tickers), chunk_size):
            chunk = all_tickers[i:i + chunk_size]
            quotes = quotes | await self.stock_data.schwab.get_quotes(tickers=chunk)
        quotes.pop('errors', None)

        # Fetch all classifications once
        classifications = await self.stock_data.ticker_stats.get_all_classifications()

        # Compute ticker partitions
        today = datetime.date.today()
        earnings_today = await self.stock_data.earnings.get_earnings_on_date(date=today)
        earnings_tickers = set(earnings_today['ticker'].tolist()) if not earnings_today.empty else set()
        watchlist_tickers = set(await self.stock_data.watchlists.get_all_watchlist_tickers())
        surge_ticker_set = set(surge_tickers)

        labels = [
            "_confirmation_pipeline",
            "_market_signal_pipeline",
            "_market_confirmation_pipeline",
            "_watchlist_pipeline",
            "_earnings_pipeline",
        ]
        results = await asyncio.gather(
            self._confirmation_pipeline(active_surges, quotes, classifications, alert_channels),
            self._market_signal_pipeline(
                quotes, classifications,
                exclude=surge_ticker_set | watchlist_tickers | earnings_tickers,
            ),
            self._market_confirmation_pipeline(active_signals, quotes, alert_channels),
            self._watchlist_pipeline(quotes, classifications, alert_channels, watchlist_tickers),
            self._earnings_pipeline(quotes, classifications, alert_channels, earnings_tickers),
            return_exceptions=True,
        )
        for label, result in zip(labels, results):
            if isinstance(result, Exception):
                logger.error(f"[process_alerts] {label} failed: {result}", exc_info=result)

        # Expire old surges and signals
        await self.stock_data.surge_store.expire_old_surges()
        await self.stock_data.market_signal_store.expire_old_signals()
        logger.info("Alerts posted")

    # -------------------------------------------------------------------------
    # Pipeline methods
    # -------------------------------------------------------------------------

    async def _confirmation_pipeline(
        self,
        active_surges: list[dict],
        quotes: dict,
        classifications: dict,
        channels: list,
    ):
        """Confirm popularity surges when price/volume follows."""
        logger.info("Processing confirmation pipeline")
        for surge in active_surges:
            ticker = surge['ticker']
            if ticker not in quotes:
                continue
            try:
                quote = quotes[ticker]
                pct_change = quote['quote']['netPercentChange']
                classification = classifications.get(ticker, 'standard')
                daily_price_history = await self.stock_data.price_history.fetch_daily_price_history(ticker=ticker)
                current_volume = quote['quote'].get('totalVolume')

                trigger_result = evaluate_price_alert(
                    classification=classification,
                    pct_change=pct_change,
                    daily_prices=daily_price_history,
                    current_volume=current_volume,
                )

                if trigger_result.should_alert:
                    price_at_flag = surge.get('price_at_flag')
                    market = market_utils()
                    current_price = market.get_current_price(quote)
                    price_change_since_flag = None
                    if price_at_flag and price_at_flag != 0:
                        price_change_since_flag = ((current_price - price_at_flag) / price_at_flag) * 100

                    surge_types = [
                        st.strip()
                        for st in (surge.get('surge_types') or '').split(',')
                        if st.strip()
                    ]

                    alert = await self.build_momentum_confirmation(
                        ticker=ticker,
                        quote=quote,
                        surge_flagged_at=surge.get('flagged_at'),
                        surge_types=surge_types,
                        price_at_flag=price_at_flag,
                        price_change_since_flag=price_change_since_flag,
                        surge_alert_message_id=surge.get('alert_message_id'),
                        daily_price_history=daily_price_history,
                        trigger_result=trigger_result,
                    )
                    view = PopularitySurgeAlertButtons(ticker=ticker, doc_url=MOMENTUM_CONFIRMATION_DOC_URL)
                    for channel in channels:
                        role_mention = await self._build_role_mention(alert, channel)
                        await send_alert(alert, channel, self.dstate, view=view, role_mention=role_mention)

                    await self.stock_data.surge_store.mark_confirmed(ticker, surge['flagged_at'])
            except Exception:
                logger.error(f"[_confirmation_pipeline] Failed for '{ticker}'", exc_info=True)

    async def _market_signal_pipeline(
        self,
        quotes: dict,
        classifications: dict,
        exclude: set,
    ):
        """Silently record market signals — no alerts sent here."""
        logger.info("Processing market signal pipeline")
        for ticker, quote in quotes.items():
            if ticker in exclude:
                continue
            try:
                pct_change = quote['quote']['netPercentChange']
                classification = classifications.get(ticker, 'standard')
                daily_price_history = await self.stock_data.price_history.fetch_daily_price_history(ticker=ticker)
                current_volume = quote['quote'].get('totalVolume')

                trigger_result = evaluate_price_alert(
                    classification=classification,
                    pct_change=pct_change,
                    daily_prices=daily_price_history,
                    current_volume=current_volume,
                )

                composite_result = compute_composite_score(trigger_result)

                if composite_result.should_alert:
                    vol_z = trigger_result.volume_zscore
                    price_z = trigger_result.zscore

                    rvol = None
                    if not daily_price_history.empty and current_volume is not None:
                        try:
                            rvol = an.indicators.volume.rvol(
                                data=daily_price_history,
                                periods=10,
                                curr_volume=current_volume,
                            )
                        except Exception:
                            pass

                    already_signaled = await self.stock_data.market_signal_store.is_already_signaled(ticker)

                    if already_signaled:
                        # Get the latest signal to update with new observation
                        latest_signal = await self.stock_data.market_signal_store.get_latest_signal(ticker)
                        if latest_signal is not None:
                            await self.stock_data.market_signal_store.update_observation(
                                ticker,
                                latest_signal['detected_at'],
                                pct_change,
                                composite_result.composite_score,
                                vol_z,
                                price_z,
                            )
                            logger.debug(
                                f"[_market_signal_pipeline] Updated observation for '{ticker}' "
                                f"composite={composite_result.composite_score:.2f}"
                            )
                    else:
                        detected_at = datetime.datetime.utcnow()
                        initial_obs = [{
                            'ts': detected_at.isoformat(),
                            'pct_change': pct_change,
                            'vol_z': vol_z,
                            'price_z': price_z,
                            'composite': composite_result.composite_score,
                        }]
                        await self.stock_data.market_signal_store.insert_signal(
                            ticker=ticker,
                            detected_at=detected_at,
                            composite_score=composite_result.composite_score,
                            price_z=price_z,
                            vol_z=vol_z,
                            pct_change=pct_change,
                            dominant_signal=composite_result.dominant_signal,
                            rvol=rvol,
                            signal_data=initial_obs,
                        )
                        logger.debug(
                            f"[_market_signal_pipeline] Recorded new signal for '{ticker}' "
                            f"composite={composite_result.composite_score:.2f}"
                        )
            except Exception:
                logger.error(f"[_market_signal_pipeline] Failed for '{ticker}'", exc_info=True)

    async def _market_confirmation_pipeline(
        self,
        active_signals: list[dict],
        quotes: dict,
        channels: list,
    ):
        """Post Market Mover alerts for signals that have sustained or accelerated."""
        logger.info("Processing market confirmation pipeline")
        for signal in active_signals:
            ticker = signal['ticker']
            if ticker not in quotes:
                continue
            try:
                quote = quotes[ticker]
                pct_change = quote['quote']['netPercentChange']
                current_volume = quote['quote'].get('totalVolume')

                daily_price_history = await self.stock_data.price_history.fetch_daily_price_history(ticker=ticker)

                trigger_result = evaluate_price_alert(
                    classification='standard',
                    pct_change=pct_change,
                    daily_prices=daily_price_history,
                    current_volume=current_volume,
                )
                vol_z = trigger_result.volume_zscore

                observations = await self.stock_data.market_signal_store.get_signal_history(ticker)

                confirmed, reason = should_confirm_signal(
                    signal=signal,
                    observations=observations,
                    current_pct_change=pct_change,
                    current_vol_z=vol_z,
                )

                if confirmed:
                    classification = signal.get('dominant_signal', 'standard')
                    composite_result = compute_composite_score(trigger_result)

                    rvol = signal.get('rvol')
                    if not daily_price_history.empty and current_volume is not None:
                        try:
                            rvol = an.indicators.volume.rvol(
                                data=daily_price_history,
                                periods=10,
                                curr_volume=current_volume,
                            )
                        except Exception:
                            pass

                    logger.info(
                        f"[_market_confirmation_pipeline] Confirming '{ticker}' "
                        f"reason={reason}, observations={len(observations)}"
                    )

                    alert = await self.build_market_mover(
                        ticker=ticker,
                        quote=quote,
                        composite_result=composite_result,
                        signal_detected_at=signal['detected_at'],
                        confirmation_reason=reason,
                        signal_observations=len(observations),
                        daily_price_history=daily_price_history,
                        rvol=rvol,
                    )
                    view = AlertButtons(ticker=ticker, doc_url=MARKET_MOVER_DOC_URL)
                    for channel in channels:
                        role_mention = await self._build_role_mention(alert, channel)
                        await send_alert(alert, channel, self.dstate, view=view, role_mention=role_mention)

                    await self.stock_data.market_signal_store.mark_confirmed(ticker, signal['detected_at'])
            except Exception:
                logger.error(f"[_market_confirmation_pipeline] Failed for '{ticker}'", exc_info=True)

    async def _watchlist_pipeline(
        self,
        quotes: dict,
        classifications: dict,
        channels: list,
        watchlist_tickers: set,
    ):
        """Send watchlist alerts when a watched stock moves significantly."""
        logger.info("Processing watchlist pipeline")
        watchlist_quotes = {t: q for t, q in quotes.items() if t in watchlist_tickers}

        for ticker, quote in watchlist_quotes.items():
            try:
                pct_change = quote['quote']['netPercentChange']
                classification = classifications.get(ticker, 'standard')
                daily_price_history = await self.stock_data.price_history.fetch_daily_price_history(ticker=ticker)
                current_volume = quote['quote'].get('totalVolume')

                trigger_result = evaluate_price_alert(
                    classification=classification,
                    pct_change=pct_change,
                    daily_prices=daily_price_history,
                    current_volume=current_volume,
                )

                if trigger_result.should_alert:
                    logger.debug(
                        f"[_watchlist_pipeline] Watchlist alert for '{ticker}': "
                        f"{pct_change:.2f}% (z-score: {trigger_result.zscore:.2f})"
                    )
                    alert = await self.build_watchlist_mover(
                        ticker=ticker,
                        quote=quote,
                        daily_price_history=daily_price_history,
                        trigger_result=trigger_result,
                    )
                    view = AlertButtons(ticker=ticker, doc_url=WATCHLIST_MOVER_DOC_URL)
                    for channel in channels:
                        role_mention = await self._build_role_mention(alert, channel)
                        await send_alert(alert, channel, self.dstate, view=view, role_mention=role_mention)
            except Exception:
                logger.error(f"[_watchlist_pipeline] Failed for '{ticker}'", exc_info=True)

    async def _earnings_pipeline(
        self,
        quotes: dict,
        classifications: dict,
        channels: list,
        earnings_tickers: set,
    ):
        """Send earnings alerts when a reporting stock moves significantly.

        Optimization: Skip re-evaluation for tickers that already have alerts posted today.
        Only check initial trigger for new tickers; let momentum acceleration logic in
        send_alert() handle subsequent updates via override_and_edit.
        """
        logger.info("Processing earnings pipeline")
        today = datetime.date.today()
        earnings_today = await self.stock_data.earnings.get_earnings_on_date(date=today)

        if earnings_today.empty:
            return

        earnings_quotes = {t: q for t, q in quotes.items() if t in earnings_tickers}
        
        # Fetch tickers that already have alerts posted today to skip re-evaluation
        posted_tickers = await self.dstate.get_alerts_by_type_today('EARNINGS_MOVER')
        posted_set = set(posted_tickers)

        for ticker, quote in earnings_quotes.items():
            try:
                pct_change = quote['quote']['netPercentChange']
                classification = classifications.get(ticker, 'standard')
                
                # Skip expensive re-evaluation for tickers with existing alerts
                # Let override_and_edit momentum logic decide on updates
                if ticker in posted_set:
                    logger.debug(
                        f"[_earnings_pipeline] Alert for '{ticker}' already posted; "
                        f"skipping re-evaluation (momentum logic will handle updates)"
                    )
                    # Fetch current data for potential update (without re-evaluating trigger)
                    next_earnings_info = earnings_today[
                        earnings_today['ticker'] == ticker
                    ].to_dict(orient='records')[0]
                    alert = await self.build_earnings_mover(
                        ticker=ticker,
                        quote=quote,
                        next_earnings_info=next_earnings_info,
                        daily_price_history=None,  # Skip fetch; not needed for override_and_edit
                        trigger_result=None,  # Don't re-evaluate; let override_and_edit decide
                    )
                    view = AlertButtons(ticker=ticker, doc_url=EARNINGS_MOVER_DOC_URL)
                    for channel in channels:
                        role_mention = await self._build_role_mention(alert, channel)
                        await send_alert(alert, channel, self.dstate, view=view, role_mention=role_mention)
                    continue

                # First-time check: evaluate if movement warrants initial alert
                daily_price_history = await self.stock_data.price_history.fetch_daily_price_history(ticker=ticker)
                current_volume = quote['quote'].get('totalVolume')

                trigger_result = evaluate_price_alert(
                    classification=classification,
                    pct_change=pct_change,
                    daily_prices=daily_price_history,
                    current_volume=current_volume,
                )

                if trigger_result.should_alert:
                    logger.debug(
                        f"[_earnings_pipeline] Earnings alert for '{ticker}': "
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
                    view = AlertButtons(ticker=ticker, doc_url=EARNINGS_MOVER_DOC_URL)
                    for channel in channels:
                        role_mention = await self._build_role_mention(alert, channel)
                        await send_alert(alert, channel, self.dstate, view=view, role_mention=role_mention)
            except Exception:
                logger.error(f"[_earnings_pipeline] Failed for '{ticker}'", exc_info=True)

    # -------------------------------------------------------------------------
    # Builder methods
    # -------------------------------------------------------------------------

    async def build_earnings_mover(self, ticker: str, **kwargs) -> EarningsMoverAlert:
        """Build an EarningsMoverAlert for the given ticker."""
        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        next_earnings_info = kwargs.pop(
            'next_earnings_info', await self.stock_data.earnings.get_next_earnings_info(ticker=ticker)
        )
        historical_earnings = kwargs.pop(
            'historical_earnings', await self.stock_data.earnings.get_historical_earnings(ticker=ticker)
        )
        daily_price_history = kwargs.pop('daily_price_history', None)
        trigger_result = kwargs.pop('trigger_result', None)
        return EarningsMoverAlert(data=EarningsMoverData(
            ticker=ticker,
            ticker_info=await self.stock_data.tickers.get_ticker_info(ticker=ticker),
            quote=quote,
            next_earnings_info=next_earnings_info,
            historical_earnings=historical_earnings,
            daily_price_history=daily_price_history if daily_price_history is not None else pd.DataFrame(),
            trigger_result=trigger_result,
        ))

    async def build_watchlist_mover(self, ticker: str, **kwargs) -> WatchlistMoverAlert:
        """Build a WatchlistMoverAlert for the given ticker."""
        async def get_ticker_watchlist(ticker: str):
            watchlists = await self.stock_data.watchlists.get_watchlists()
            for watchlist_id in watchlists:
                watchlist_tickers = await self.stock_data.watchlists.get_watchlist_tickers(
                    watchlist_id=watchlist_id
                )
                if ticker in watchlist_tickers:
                    return watchlist_id

        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        watchlist = kwargs.pop('watchlist', await get_ticker_watchlist(ticker=ticker))
        daily_price_history = kwargs.pop('daily_price_history', None)
        trigger_result = kwargs.pop('trigger_result', None)
        return WatchlistMoverAlert(data=WatchlistMoverData(
            ticker=ticker,
            ticker_info=await self.stock_data.tickers.get_ticker_info(ticker=ticker),
            quote=quote,
            watchlist=watchlist,
            daily_price_history=daily_price_history if daily_price_history is not None else pd.DataFrame(),
            trigger_result=trigger_result,
        ))

    async def build_market_alert(self, ticker: str, **kwargs) -> MarketAlert:
        """Build a MarketAlert for the given ticker."""
        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        composite_result = kwargs.pop('composite_result')
        daily_price_history = kwargs.pop('daily_price_history', pd.DataFrame())
        rvol = kwargs.pop('rvol', None)
        return MarketAlert(data=MarketAlertData(
            ticker=ticker,
            ticker_info=await self.stock_data.tickers.get_ticker_info(ticker=ticker),
            quote=quote,
            composite_result=composite_result,
            daily_price_history=daily_price_history,
            rvol=rvol,
        ))

    async def build_market_mover(self, ticker: str, **kwargs) -> MarketMoverAlert:
        """Build a MarketMoverAlert for the given ticker."""
        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        composite_result = kwargs.pop('composite_result')
        signal_detected_at = kwargs.pop('signal_detected_at', None)
        confirmation_reason = kwargs.pop('confirmation_reason', '')
        signal_observations = kwargs.pop('signal_observations', 0)
        daily_price_history = kwargs.pop('daily_price_history', pd.DataFrame())
        rvol = kwargs.pop('rvol', None)
        price_velocity = kwargs.pop('price_velocity', None)
        price_acceleration = kwargs.pop('price_acceleration', None)
        volume_velocity = kwargs.pop('volume_velocity', None)
        volume_acceleration = kwargs.pop('volume_acceleration', None)
        return MarketMoverAlert(data=MarketMoverData(
            ticker=ticker,
            ticker_info=await self.stock_data.tickers.get_ticker_info(ticker=ticker),
            quote=quote,
            composite_result=composite_result,
            signal_detected_at=signal_detected_at,
            confirmation_reason=confirmation_reason,
            signal_observations=signal_observations,
            daily_price_history=daily_price_history,
            rvol=rvol,
            price_velocity=price_velocity,
            price_acceleration=price_acceleration,
            volume_velocity=volume_velocity,
            volume_acceleration=volume_acceleration,
        ))

    async def build_momentum_confirmation(self, ticker: str, **kwargs) -> MomentumConfirmationAlert:
        """Build a MomentumConfirmationAlert for the given ticker."""
        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        surge_flagged_at = kwargs.pop('surge_flagged_at', None)
        surge_types = kwargs.pop('surge_types', [])
        price_at_flag = kwargs.pop('price_at_flag', None)
        price_change_since_flag = kwargs.pop('price_change_since_flag', None)
        surge_alert_message_id = kwargs.pop('surge_alert_message_id', None)
        daily_price_history = kwargs.pop('daily_price_history', pd.DataFrame())
        trigger_result = kwargs.pop('trigger_result', None)
        return MomentumConfirmationAlert(data=MomentumConfirmationData(
            ticker=ticker,
            ticker_info=await self.stock_data.tickers.get_ticker_info(ticker=ticker),
            quote=quote,
            surge_flagged_at=surge_flagged_at,
            surge_types=surge_types,
            price_at_flag=price_at_flag,
            price_change_since_flag=price_change_since_flag,
            surge_alert_message_id=surge_alert_message_id,
            daily_price_history=daily_price_history,
            trigger_result=trigger_result,
        ))


#########
# Setup #
#########

async def setup(bot: commands.Bot):
    await bot.add_cog(Alerts(bot, bot.stock_data))
