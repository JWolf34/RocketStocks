import datetime
import logging
import asyncio
import time
import traceback as tb

import discord
import pandas as pd
import pandas_market_calendars as mcal
from discord import app_commands
from discord.ext import commands, tasks

from rocketstocks.data.stockdata import StockData
from rocketstocks.data.channel_config import ALERTS
from rocketstocks.data.discord_state import DiscordState
from rocketstocks.data.clients.schwab import SchwabRateLimitError
from rocketstocks.core.utils.market import MarketUtils
from rocketstocks.core.utils.dates import format_date_mdy, seconds_until_minute_interval
from rocketstocks.core.notifications.config import NotificationLevel
from rocketstocks.core.notifications.event import NotificationEvent
import rocketstocks.core.analysis.indicators as an

from rocketstocks.core.analysis.alert_strategy import evaluate_price_alert, evaluate_confirmation
from rocketstocks.core.analysis.popularity_signals import evaluate_popularity_surge
from rocketstocks.core.analysis.volume_divergence import evaluate_volume_accumulation
from rocketstocks.core.analysis.options_flow import evaluate_options_flow
from rocketstocks.core.analysis.alert_performance import (
    compute_surge_confidence,
    compute_signal_confidence,
    compute_price_outcome,
)

from rocketstocks.core.content.models import (
    AlertHistoryData,
    AlertStatsData,
    AlertSummaryData,
    EarningsMoverData,
    WatchlistMoverData,
    PopularitySurgeData,
    MomentumConfirmationData,
    VolumeAccumulationAlertData,
    BreakoutAlertData,
)
from rocketstocks.core.content.alerts.earnings_alert import EarningsMoverAlert
from rocketstocks.core.content.alerts.watchlist_alert import WatchlistMoverAlert
from rocketstocks.core.content.alerts.popularity_surge_alert import PopularitySurgeAlert
from rocketstocks.core.content.alerts.momentum_confirmation_alert import MomentumConfirmationAlert
from rocketstocks.core.content.alerts.volume_accumulation_alert import VolumeAccumulationAlert
from rocketstocks.core.content.alerts.breakout_alert import BreakoutAlert
from rocketstocks.core.content.reports.alert_stats_report import AlertStats
from rocketstocks.core.content.reports.alert_history_report import AlertHistory
from rocketstocks.core.content.reports.alert_summary import AlertSummary

from rocketstocks.bot.views.alert_views import (
    AlertButtons,
    PopularitySurgeAlertButtons,
    POPULARITY_SURGE_DOC_URL,
    MOMENTUM_CONFIRMATION_DOC_URL,
    VOLUME_ACCUMULATION_DOC_URL,
    BREAKOUT_DOC_URL,
    WATCHLIST_MOVER_DOC_URL,
    EARNINGS_MOVER_DOC_URL,
)
from rocketstocks.bot.senders.alert_sender import send_alert
from rocketstocks.bot.senders.report_sender import send_report
from rocketstocks.bot.views.subscription_views import AlertSubscriptionSelect, AlertSubscriptionView

logger = logging.getLogger(__name__)


def _resolve_since_dt(value: str) -> tuple[datetime.datetime, str]:
    """Map choice value → (since_datetime, human_label)."""
    calendar = mcal.get_calendar('NYSE')
    today = datetime.date.today()

    if value == 'market_open_today':
        market_open = datetime.datetime.combine(today, datetime.time(14, 30))
        return market_open, 'since market open today'

    if value == 'last_3_days':
        return datetime.datetime.combine(today - datetime.timedelta(days=3), datetime.time.min), 'last 3 days'

    if value == 'last_7_days':
        return datetime.datetime.combine(today - datetime.timedelta(days=7), datetime.time.min), 'last 7 days'

    valid = calendar.valid_days(start_date=today - datetime.timedelta(days=10), end_date=today)
    prev_day = [d.date() for d in valid if d.date() < today][-1]
    prev_close = datetime.datetime.combine(prev_day, datetime.time(21, 0))
    return prev_close, f'since last close ({prev_day.strftime("%b %d")})'


class Alerts(commands.Cog):
    """Push alerts to Discord when criteria for stock movements are met."""

    def __init__(self, bot: commands.Bot, stock_data: StockData):
        self.bot = bot
        self.stock_data = stock_data
        self.mutils = MarketUtils()
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
            date_string = format_date_mdy(datetime.datetime.today())
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
        await asyncio.sleep(seconds_until_minute_interval(30))

    @tasks.loop(minutes=5)
    async def process_alerts(self):
        """Process price/volume alerts every 5 minutes if the market is open."""
        await self._run_task("process_alerts", self._process_alerts_impl())

    @process_alerts.before_loop
    async def process_alerts_before_loop(self):
        """Wait until the next 0- or 5-minute boundary before starting the alerts loop."""
        DELTA = 30
        await asyncio.sleep(seconds_until_minute_interval(5) + DELTA)

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

        # Batch: fetch all already-flagged tickers in one query instead of per-ticker SELECTs
        flagged_tickers = await self.stock_data.surge_store.get_flagged_tickers()

        # First pass: evaluate surges for non-flagged tickers
        surging: dict[str, tuple] = {}  # ticker → (surge_result, popularity_history)
        for _, row in pop_df.iterrows():
            ticker = row.get('ticker')
            if not ticker or ticker in flagged_tickers:
                await asyncio.sleep(0)
                continue

            try:
                current_rank = row.get('rank')
                rank_24h_ago = row.get('rank_24h_ago')
                mentions = row.get('mentions')
                mentions_24h_ago = row.get('mentions_24h_ago')

                popularity_history = await self.stock_data.popularity.fetch_popularity(
                    ticker=ticker, limit=35
                )

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
                    surging[ticker] = (surge_result, popularity_history)

            except Exception:
                logger.error(f"[detect_popularity_surges] Failed for '{ticker}'", exc_info=True)

            await asyncio.sleep(0)

        if not surging:
            return

        # Batch fetch quotes for all surging tickers — continue with partial results if a chunk fails
        surging_list = list(surging.keys())
        batch_quotes: dict = {}
        chunk_size = 25
        for i in range(0, len(surging_list), chunk_size):
            chunk = surging_list[i:i + chunk_size]
            try:
                batch = await self.stock_data.schwab.get_quotes(tickers=chunk)
                batch_quotes.update(batch)
            except SchwabRateLimitError as exc:
                self.bot.emitter.emit(NotificationEvent(
                    level=NotificationLevel.FAILURE,
                    source=__name__,
                    job_name="detect_popularity_surges",
                    message=str(exc),
                ))
                break
            except Exception:
                logger.error(
                    f"[_detect_popularity_surges] Quote fetch failed for chunk starting at index {i}",
                    exc_info=True,
                )
        batch_quotes.pop('errors', None)

        # Second pass: build and send alerts for surging tickers
        for ticker, (surge_result, popularity_history) in surging.items():
            try:
                quote = batch_quotes.get(ticker, {})
                ticker_info = await self.stock_data.tickers.get_ticker_info(ticker=ticker)
                market = MarketUtils()
                price_at_flag = market.get_current_price(quote) if quote else None

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
                    mention_acceleration=surge_result.mention_acceleration,
                )

            except Exception:
                logger.error(f"[detect_popularity_surges] Failed to send alert for '{ticker}'", exc_info=True)

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

        # Gather active volume accumulation signals for the breakout pipeline
        active_signals = await self.stock_data.market_signal_store.get_active_signals(
            signal_source='volume_accumulation'
        )
        signal_tickers = [s['ticker'] for s in active_signals]

        all_tickers = list(set(screener_tickers + surge_tickers + signal_tickers))

        # Bulk Schwab quote fetch — continue with partial results if a chunk fails
        quotes = {}
        chunk_size = 25
        for i in range(0, len(all_tickers), chunk_size):
            chunk = all_tickers[i:i + chunk_size]
            try:
                batch = await self.stock_data.schwab.get_quotes(tickers=chunk)
                quotes.update(batch)
            except SchwabRateLimitError as exc:
                self.bot.emitter.emit(NotificationEvent(
                    level=NotificationLevel.FAILURE,
                    source=__name__,
                    job_name="process_alerts",
                    message=str(exc),
                ))
                break
            except Exception:
                logger.error(
                    f"[_process_alerts_impl] Quote fetch failed for chunk starting at index {i}",
                    exc_info=True,
                )
        quotes.pop('errors', None)

        # Fetch all classifications once
        classifications = await self.stock_data.ticker_stats.get_all_classifications()

        # Compute ticker partitions
        today = datetime.date.today()
        earnings_today = await self.stock_data.earnings.get_earnings_on_date(date=today)
        earnings_tickers = set(earnings_today['ticker'].tolist()) if not earnings_today.empty else set()
        watchlist_tickers = set(await self.stock_data.watchlists.get_all_watchlist_tickers(
            watchlist_types=['named', 'personal']
        ))

        # Build ticker → watchlist mapping in a single query (no N+1)
        ticker_to_watchlist: dict = await self.stock_data.watchlists.get_ticker_to_watchlist_map(
            watchlist_types=['named', 'personal']
        )

        # Pre-fetch price history for all relevant tickers in a single batch query
        start_date = today - datetime.timedelta(days=90)
        all_price_tickers = list(set(
            surge_tickers + signal_tickers
            + list(watchlist_tickers) + list(earnings_tickers) + list(quotes.keys())
        ))
        price_cache = await self.stock_data.price_history.fetch_daily_price_history_batch(
            tickers=all_price_tickers, start_date=start_date,
        )

        labels = [
            "_confirmation_pipeline",
            "_volume_accumulation_pipeline",
            "_breakout_pipeline",
            "_watchlist_pipeline",
            "_earnings_pipeline",
        ]
        results = await asyncio.gather(
            self._confirmation_pipeline(active_surges, quotes, classifications, alert_channels, price_cache),
            self._volume_accumulation_pipeline(quotes, classifications, alert_channels, price_cache),
            self._breakout_pipeline(active_signals, quotes, alert_channels, price_cache),
            self._watchlist_pipeline(
                quotes, classifications, alert_channels, watchlist_tickers, price_cache, ticker_to_watchlist,
            ),
            self._earnings_pipeline(quotes, classifications, alert_channels, earnings_today, price_cache),
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
        price_cache: dict,
    ):
        """Confirm popularity surges when price moves significantly since the surge was flagged."""
        logger.info("Processing confirmation pipeline")
        utcnow = datetime.datetime.utcnow()
        # Fetch surge confidence for the embed (last 30 days)
        surge_confidence_pct = await self._fetch_surge_confidence_pct()

        for surge in active_surges:
            ticker = surge['ticker']
            if ticker not in quotes:
                continue
            try:
                # 15-minute minimum delay — ensures confirmation measures subsequent
                # price action, not the move that triggered the surge itself.
                flagged_at = surge.get('flagged_at')
                if flagged_at is not None:
                    elapsed = (utcnow - flagged_at).total_seconds()
                    if elapsed < 900:  # 15 minutes
                        logger.debug(
                            f"[_confirmation_pipeline] '{ticker}' skipped — "
                            f"only {elapsed:.0f}s since surge flagged (min 900s)"
                        )
                        continue

                quote = quotes[ticker]
                market = MarketUtils()
                current_price = market.get_current_price(quote)
                price_at_flag = surge.get('price_at_flag')

                # Compute price change since flag
                price_change_since_flag = None
                if price_at_flag and price_at_flag != 0 and current_price:
                    price_change_since_flag = (
                        (current_price - price_at_flag) / price_at_flag * 100
                    )

                # Use evaluate_confirmation() with ticker's own return distribution
                stats = await self.stock_data.ticker_stats.get_stats(ticker)
                mean_return = (stats or {}).get('mean_return_20d', 0.0) or 0.0
                std_return = (stats or {}).get('std_return_20d', 1.0) or 1.0

                trigger_result = evaluate_confirmation(
                    price_at_flag=price_at_flag or 0.0,
                    current_price=current_price,
                    mean_return=mean_return,
                    std_return=std_return,
                )

                if trigger_result.should_confirm:
                    surge_types = [
                        st.strip()
                        for st in (surge.get('surge_types') or '').split(',')
                        if st.strip()
                    ]

                    alert = await self.build_momentum_confirmation(
                        ticker=ticker,
                        quote=quote,
                        surge_flagged_at=flagged_at,
                        surge_types=surge_types,
                        price_at_flag=price_at_flag,
                        price_change_since_flag=price_change_since_flag,
                        surge_alert_message_id=surge.get('alert_message_id'),
                        daily_price_history=price_cache.get(ticker, pd.DataFrame()),
                        trigger_result=trigger_result,
                        confidence_pct=surge_confidence_pct,
                    )
                    view = PopularitySurgeAlertButtons(ticker=ticker, doc_url=MOMENTUM_CONFIRMATION_DOC_URL)
                    for channel in channels:
                        role_mention = await self._build_role_mention(alert, channel)
                        await send_alert(alert, channel, self.dstate, view=view, role_mention=role_mention)

                    await self.stock_data.surge_store.mark_confirmed(ticker, surge['flagged_at'])
            except Exception:
                logger.error(f"[_confirmation_pipeline] Failed for '{ticker}'", exc_info=True)

    async def _fetch_surge_confidence_pct(self) -> float | None:
        """Fetch the 30-day surge confirmation rate for display in embeds."""
        try:
            cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=30)
            rows = await self.stock_data.db.execute(
                "SELECT confirmed, expired FROM popularity_surges WHERE flagged_at >= %s",
                [cutoff],
            )
            if not rows:
                return None
            df = pd.DataFrame(rows, columns=['confirmed', 'expired'])
            stats = compute_surge_confidence(df)
            return stats.get('rate')
        except Exception:
            logger.debug("Could not fetch surge confidence", exc_info=True)
            return None

    async def _volume_accumulation_pipeline(
        self,
        quotes: dict,
        classifications: dict,
        channels: list,
        price_cache: dict,
    ):
        """Detect volume-price divergence and send VolumeAccumulationAlert (leading indicator).

        Scans all quoted tickers — including surge, watchlist, and earnings tickers.
        Confluence between a popularity surge and volume accumulation is valuable signal.
        """
        logger.info("Processing volume accumulation pipeline")

        # Batch: fetch all pending volume accumulation signals for today
        signaled_today = await self.stock_data.market_signal_store.get_signaled_tickers_today()

        for ticker, quote in quotes.items():
            try:
                pct_change = quote['quote'].get('netPercentChange', 0.0)
                current_volume = quote['quote'].get('totalVolume')
                daily_price_history = price_cache.get(ticker, pd.DataFrame())

                if daily_price_history.empty or current_volume is None:
                    continue

                # Compute price z-score
                price_z = an.indicators.price.intraday_zscore(
                    daily_prices_df=daily_price_history,
                    current_pct_change=pct_change,
                    period=20,
                )

                # Compute volume z-score
                from rocketstocks.core.analysis.signals import signals
                vol_z = signals.volume_zscore(
                    volume_series=daily_price_history['volume'],
                    curr_volume=float(current_volume),
                    period=20,
                )

                # Compute RVOL
                rvol = float('nan')
                try:
                    rvol = an.indicators.volume.rvol(
                        data=daily_price_history,
                        periods=10,
                        curr_volume=float(current_volume),
                    )
                except Exception:
                    pass

                import math
                if math.isnan(vol_z) or math.isnan(price_z) or math.isnan(rvol):
                    continue

                result = evaluate_volume_accumulation(
                    vol_zscore=vol_z,
                    price_zscore=price_z,
                    rvol=rvol,
                )

                if not result.is_accumulating:
                    continue

                # Already signaled today — skip
                if ticker in signaled_today:
                    logger.debug(
                        f"[_volume_accumulation_pipeline] '{ticker}' already signaled today — skipping"
                    )
                    continue

                # Fetch options chain and evaluate flow
                options_flow = None
                try:
                    current_price = MarketUtils().get_current_price(quote)
                    options_chain = await self.stock_data.schwab.get_options_chain(ticker=ticker)
                    if options_chain:
                        options_flow = evaluate_options_flow(
                            options_chain=options_chain,
                            underlying_price=current_price,
                        )
                        if options_flow.has_unusual_activity:
                            result.signal_strength = 'volume_plus_options'
                            result.options_flow = options_flow
                except Exception:
                    logger.debug(
                        f"[_volume_accumulation_pipeline] Options fetch failed for '{ticker}'",
                        exc_info=True,
                    )

                logger.info(
                    f"[_volume_accumulation_pipeline] Volume accumulation on '{ticker}': "
                    f"vol_z={vol_z:.2f}, price_z={price_z:.2f}, rvol={rvol:.2f}, "
                    f"strength={result.signal_strength}"
                )

                ticker_info = await self.stock_data.tickers.get_ticker_info(ticker=ticker)
                price_at_flag = MarketUtils().get_current_price(quote)

                alert = VolumeAccumulationAlert(data=VolumeAccumulationAlertData(
                    ticker=ticker,
                    ticker_info=ticker_info,
                    quote=quote,
                    vol_zscore=vol_z,
                    price_zscore=price_z,
                    rvol=rvol,
                    divergence_score=result.divergence_score,
                    signal_strength=result.signal_strength,
                    options_flow=options_flow,
                ))
                view = AlertButtons(ticker=ticker, doc_url=VOLUME_ACCUMULATION_DOC_URL)

                detected_at = datetime.datetime.utcnow()
                first_message = None
                for channel in channels:
                    role_mention = await self._build_role_mention(alert, channel)
                    sent = await send_alert(alert, channel, self.dstate, view=view, role_mention=role_mention)
                    if sent is not None and first_message is None:
                        first_message = sent

                message_id = first_message.id if first_message else None

                await self.stock_data.market_signal_store.insert_signal(
                    ticker=ticker,
                    detected_at=detected_at,
                    composite_score=result.divergence_score,
                    price_z=price_z,
                    vol_z=vol_z,
                    pct_change=pct_change,
                    dominant_signal='volume',
                    rvol=rvol,
                    signal_source='volume_accumulation',
                    price_at_flag=price_at_flag,
                    signal_data=[{
                        'ts': detected_at.isoformat(),
                        'pct_change': pct_change,
                        'vol_z': vol_z,
                        'price_z': price_z,
                        'divergence_score': result.divergence_score,
                    }],
                )

                if message_id is not None:
                    await self.stock_data.market_signal_store.update_alert_message_id(
                        ticker, detected_at, message_id
                    )

            except Exception:
                logger.error(f"[_volume_accumulation_pipeline] Failed for '{ticker}'", exc_info=True)

    async def _breakout_pipeline(
        self,
        active_signals: list[dict],
        quotes: dict,
        channels: list,
        price_cache: dict,
    ):
        """Send BreakoutAlert when price confirms a Volume Accumulation signal."""
        logger.info("Processing breakout pipeline")
        utcnow = datetime.datetime.utcnow()

        signal_confidence_pct = await self._fetch_signal_confidence_pct()

        for signal in active_signals:
            ticker = signal['ticker']
            if ticker not in quotes:
                continue
            try:
                detected_at = signal.get('detected_at')
                if detected_at is not None:
                    elapsed = (utcnow - detected_at).total_seconds()
                    if elapsed < 600:  # 10-minute minimum delay
                        logger.debug(
                            f"[_breakout_pipeline] '{ticker}' skipped — "
                            f"only {elapsed:.0f}s since signal (min 600s)"
                        )
                        continue

                quote = quotes[ticker]
                current_price = MarketUtils().get_current_price(quote)
                price_at_flag = signal.get('price_at_flag')

                stats = await self.stock_data.ticker_stats.get_stats(ticker)
                mean_return = (stats or {}).get('mean_return_20d', 0.0) or 0.0
                std_return = (stats or {}).get('std_return_20d', 1.0) or 1.0

                trigger_result = evaluate_confirmation(
                    price_at_flag=price_at_flag or 0.0,
                    current_price=current_price,
                    mean_return=mean_return,
                    std_return=std_return,
                )

                if not trigger_result.should_confirm:
                    continue

                pct_change = quote['quote'].get('netPercentChange', 0.0)
                current_volume = quote['quote'].get('totalVolume')
                daily_price_history = price_cache.get(ticker, pd.DataFrame())

                rvol = signal.get('rvol')
                if not daily_price_history.empty and current_volume is not None:
                    try:
                        rvol = an.indicators.volume.rvol(
                            data=daily_price_history,
                            periods=10,
                            curr_volume=float(current_volume),
                        )
                    except Exception:
                        pass

                current_vol_z = None
                if not daily_price_history.empty and current_volume is not None:
                    try:
                        from rocketstocks.core.analysis.signals import signals
                        current_vol_z = signals.volume_zscore(
                            volume_series=daily_price_history['volume'],
                            curr_volume=float(current_volume),
                            period=20,
                        )
                    except Exception:
                        pass

                price_change_since_flag = trigger_result.pct_since_flag
                price_z = an.indicators.price.intraday_zscore(
                    daily_prices_df=daily_price_history,
                    current_pct_change=pct_change,
                    period=20,
                )

                # Reconstruct options_flow from signal_data if present
                options_flow_data = None
                signal_data = signal.get('signal_data') or []
                if signal_data and isinstance(signal_data, list) and signal_data[0].get('options_flow'):
                    options_flow_data = signal_data[0]['options_flow']

                logger.info(
                    f"[_breakout_pipeline] Breakout confirmed for '{ticker}' "
                    f"pct_since_flag={price_change_since_flag:.2f}%, "
                    f"zscore={trigger_result.zscore_since_flag:.2f}"
                )

                alert = await self.build_breakout(
                    ticker=ticker,
                    quote=quote,
                    signal_detected_at=detected_at,
                    signal_alert_message_id=signal.get('alert_message_id'),
                    price_at_flag=price_at_flag,
                    price_change_since_flag=price_change_since_flag,
                    vol_z_at_signal=signal.get('vol_z'),
                    current_vol_z=current_vol_z,
                    price_zscore=price_z,
                    divergence_score=signal.get('composite_score'),
                    rvol=rvol,
                    signal_strength=signal.get('dominant_signal', 'volume_only'),
                    trigger_result=trigger_result,
                    confidence_pct=signal_confidence_pct,
                    daily_price_history=daily_price_history,
                )
                view = AlertButtons(ticker=ticker, doc_url=BREAKOUT_DOC_URL)
                for channel in channels:
                    role_mention = await self._build_role_mention(alert, channel)
                    await send_alert(alert, channel, self.dstate, view=view, role_mention=role_mention)

                await self.stock_data.market_signal_store.mark_confirmed(ticker, detected_at)

            except Exception:
                logger.error(f"[_breakout_pipeline] Failed for '{ticker}'", exc_info=True)

    async def _fetch_signal_confidence_pct(self) -> float | None:
        """Fetch the 30-day volume accumulation signal confirmation rate for display in embeds."""
        try:
            cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=30)
            rows = await self.stock_data.db.execute(
                "SELECT status FROM market_signals "
                "WHERE signal_source = 'volume_accumulation' AND detected_at >= %s",
                [cutoff],
            )
            if not rows:
                return None
            df = pd.DataFrame(rows, columns=['status'])
            stats = compute_signal_confidence(df)
            return stats.get('rate')
        except Exception:
            logger.debug("Could not fetch signal confidence", exc_info=True)
            return None

    async def _watchlist_pipeline(
        self,
        quotes: dict,
        classifications: dict,
        channels: list,
        watchlist_tickers: set,
        price_cache: dict,
        ticker_to_watchlist: dict,
    ):
        """Send watchlist alerts when a watched stock moves significantly."""
        logger.info("Processing watchlist pipeline")
        watchlist_quotes = {t: q for t, q in quotes.items() if t in watchlist_tickers}

        for ticker, quote in watchlist_quotes.items():
            try:
                pct_change = quote['quote']['netPercentChange']
                classification = classifications.get(ticker, 'standard')
                daily_price_history = price_cache.get(ticker, pd.DataFrame())
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
                        watchlist=ticker_to_watchlist.get(ticker),
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
        earnings_today: pd.DataFrame,
        price_cache: dict,
    ):
        """Send earnings alerts when a reporting stock moves significantly.

        Optimization: Skip re-evaluation for tickers that already have alerts posted today.
        Only check initial trigger for new tickers; let momentum acceleration logic in
        send_alert() handle subsequent updates via override_and_edit.
        """
        logger.info("Processing earnings pipeline")

        if earnings_today.empty:
            return

        earnings_tickers = set(earnings_today['ticker'].tolist())
        earnings_quotes = {t: q for t, q in quotes.items() if t in earnings_tickers}

        # Fetch tickers that already have alerts posted today to skip re-evaluation
        posted_tickers = await self.dstate.get_alerts_by_type_today('EARNINGS_MOVER')
        posted_set = set(posted_tickers)

        today = datetime.date.today()

        for ticker, quote in earnings_quotes.items():
            try:
                pct_change = quote['quote']['netPercentChange']
                classification = classifications.get(ticker, 'standard')

                # Look up any stored earnings result for enrichment
                earnings_result = await self.stock_data.earnings_results.get_result(
                    date=today, ticker=ticker
                )
                result_kwargs = {}
                if earnings_result:
                    result_kwargs = {
                        'eps_actual': earnings_result['eps_actual'],
                        'eps_estimate': earnings_result['eps_estimate'],
                        'surprise_pct': earnings_result['surprise_pct'],
                    }

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
                        **result_kwargs,
                    )
                    view = AlertButtons(ticker=ticker, doc_url=EARNINGS_MOVER_DOC_URL)
                    for channel in channels:
                        role_mention = await self._build_role_mention(alert, channel)
                        await send_alert(alert, channel, self.dstate, view=view, role_mention=role_mention)
                    continue

                # First-time check: evaluate if movement warrants initial alert
                daily_price_history = price_cache.get(ticker, pd.DataFrame())
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
                        **result_kwargs,
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
        eps_actual = kwargs.pop('eps_actual', None)
        eps_estimate = kwargs.pop('eps_estimate', None)
        surprise_pct = kwargs.pop('surprise_pct', None)
        return EarningsMoverAlert(data=EarningsMoverData(
            ticker=ticker,
            ticker_info=await self.stock_data.tickers.get_ticker_info(ticker=ticker),
            quote=quote,
            next_earnings_info=next_earnings_info,
            historical_earnings=historical_earnings,
            daily_price_history=daily_price_history if daily_price_history is not None else pd.DataFrame(),
            trigger_result=trigger_result,
            eps_actual=eps_actual,
            eps_estimate=eps_estimate,
            surprise_pct=surprise_pct,
        ))

    async def build_watchlist_mover(self, ticker: str, **kwargs) -> WatchlistMoverAlert:
        """Build a WatchlistMoverAlert for the given ticker."""
        async def get_ticker_watchlist(ticker: str):
            mapping = await self.stock_data.watchlists.get_ticker_to_watchlist_map(
                watchlist_types=['named', 'personal']
            )
            return mapping.get(ticker)

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

    async def build_volume_accumulation(self, ticker: str, **kwargs) -> VolumeAccumulationAlert:
        """Build a VolumeAccumulationAlert for the given ticker."""
        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        vol_zscore = kwargs.pop('vol_zscore')
        price_zscore = kwargs.pop('price_zscore')
        rvol = kwargs.pop('rvol')
        divergence_score = kwargs.pop('divergence_score')
        signal_strength = kwargs.pop('signal_strength', 'volume_only')
        options_flow = kwargs.pop('options_flow', None)
        return VolumeAccumulationAlert(data=VolumeAccumulationAlertData(
            ticker=ticker,
            ticker_info=await self.stock_data.tickers.get_ticker_info(ticker=ticker),
            quote=quote,
            vol_zscore=vol_zscore,
            price_zscore=price_zscore,
            rvol=rvol,
            divergence_score=divergence_score,
            signal_strength=signal_strength,
            options_flow=options_flow,
        ))

    async def build_breakout(self, ticker: str, **kwargs) -> BreakoutAlert:
        """Build a BreakoutAlert for the given ticker."""
        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        signal_detected_at = kwargs.pop('signal_detected_at', None)
        signal_alert_message_id = kwargs.pop('signal_alert_message_id', None)
        price_at_flag = kwargs.pop('price_at_flag', None)
        price_change_since_flag = kwargs.pop('price_change_since_flag', None)
        vol_z_at_signal = kwargs.pop('vol_z_at_signal', None)
        current_vol_z = kwargs.pop('current_vol_z', None)
        price_zscore = kwargs.pop('price_zscore', None)
        divergence_score = kwargs.pop('divergence_score', None)
        rvol = kwargs.pop('rvol', None)
        signal_strength = kwargs.pop('signal_strength', 'volume_only')
        options_flow = kwargs.pop('options_flow', None)
        trigger_result = kwargs.pop('trigger_result', None)
        confidence_pct = kwargs.pop('confidence_pct', None)
        daily_price_history = kwargs.pop('daily_price_history', pd.DataFrame())
        return BreakoutAlert(data=BreakoutAlertData(
            ticker=ticker,
            ticker_info=await self.stock_data.tickers.get_ticker_info(ticker=ticker),
            quote=quote,
            signal_detected_at=signal_detected_at,
            signal_alert_message_id=signal_alert_message_id,
            price_at_flag=price_at_flag,
            price_change_since_flag=price_change_since_flag,
            vol_z_at_signal=vol_z_at_signal,
            current_vol_z=current_vol_z,
            price_zscore=price_zscore,
            divergence_score=divergence_score,
            rvol=rvol,
            signal_strength=signal_strength,
            options_flow=options_flow,
            trigger_result=trigger_result,
            confidence_pct=confidence_pct,
            daily_price_history=daily_price_history,
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
        confidence_pct = kwargs.pop('confidence_pct', None)
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
            confidence_pct=confidence_pct,
        ))


    # -------------------------------------------------------------------------
    # /alert commands
    # -------------------------------------------------------------------------

    alert_group = app_commands.Group(name="alert", description="Alert performance and history")

    @alert_group.command(name="stats", description="Show alert predictive accuracy for a period")
    @app_commands.describe(
        period="Time period: today, 7d (default), or 30d",
        alert_type="Optional filter by alert type",
    )
    async def alert_stats(
        self,
        interaction: discord.Interaction,
        period: str = "7d",
        alert_type: str | None = None,
    ) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            # Resolve period to a start datetime
            now = datetime.datetime.utcnow()
            period_lower = period.lower()
            if period_lower == "today":
                since_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
                period_label = "Today"
            elif period_lower == "30d":
                since_dt = now - datetime.timedelta(days=30)
                period_label = "Last 30 Days"
            else:
                since_dt = now - datetime.timedelta(days=7)
                period_label = "Last 7 Days"

            cutoff = since_dt

            # Fetch surge and signal data
            surge_rows = await self.stock_data.db.execute(
                "SELECT confirmed, expired FROM popularity_surges WHERE flagged_at >= %s",
                [cutoff],
            ) or []
            surge_df = pd.DataFrame(surge_rows, columns=['confirmed', 'expired'])

            signal_rows = await self.stock_data.db.execute(
                "SELECT status FROM market_signals WHERE detected_at >= %s",
                [cutoff],
            ) or []
            signal_df = pd.DataFrame(signal_rows, columns=['status'])

            # Fetch recent alerts for price outcome computation
            alerts_raw = await self.dstate.get_alerts_since(since_dt)
            if alert_type:
                alerts_raw = [a for a in alerts_raw if a.get('alert_type') == alert_type.upper()]

            # Count by type
            alert_counts: dict = {}
            for a in alerts_raw:
                atype = a.get('alert_type', 'UNKNOWN')
                alert_counts[atype] = alert_counts.get(atype, 0) + 1

            # Compute price outcomes (uses existing price history cache)
            price_tickers = list({a['ticker'] for a in alerts_raw})
            start_date = cutoff.date() - datetime.timedelta(days=5)
            price_history = await self.stock_data.price_history.fetch_daily_price_history_batch(
                tickers=price_tickers, start_date=start_date,
            ) if price_tickers else {}

            price_outcomes = compute_price_outcome(
                alerts=alerts_raw, price_history=price_history
            )

            data = AlertStatsData(
                period_label=period_label,
                surge_confidence=compute_surge_confidence(surge_df),
                signal_confidence=compute_signal_confidence(signal_df),
                price_outcomes=price_outcomes,
                alert_counts=alert_counts,
            )
            embed_spec = AlertStats(data).build()

            from rocketstocks.bot.senders.embed_utils import spec_to_embed
            await interaction.followup.send(embed=spec_to_embed(embed_spec), ephemeral=True)
        except Exception:
            logger.error("[alert stats] Failed", exc_info=True)
            await interaction.followup.send("Failed to fetch alert stats.", ephemeral=True)

    @alert_group.command(name="history", description="Show recent alerts for a ticker with price outcomes")
    @app_commands.describe(
        ticker="Stock ticker symbol",
        count="Number of recent alerts to show (default 10)",
    )
    async def alert_history(
        self,
        interaction: discord.Interaction,
        ticker: str,
        count: int = 10,
    ) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            ticker = ticker.upper().strip()
            count = max(1, min(count, 25))  # clamp 1-25

            # Fetch alerts for this ticker (last 90 days)
            since_dt = datetime.datetime.utcnow() - datetime.timedelta(days=90)
            all_alerts = await self.dstate.get_alerts_since(since_dt)
            ticker_alerts = [a for a in all_alerts if a.get('ticker') == ticker]
            ticker_alerts.sort(key=lambda a: a.get('date', datetime.date.min), reverse=True)

            total = len(ticker_alerts)
            shown_alerts = ticker_alerts[:count]

            # Join with price history for outcomes
            if shown_alerts:
                start_date = since_dt.date() - datetime.timedelta(days=5)
                price_history = await self.stock_data.price_history.fetch_daily_price_history_batch(
                    tickers=[ticker], start_date=start_date,
                )
                outcomes_by_date = compute_price_outcome(
                    alerts=shown_alerts, price_history=price_history
                ).get('per_alert', [])
                outcome_map = {(o['ticker'], o['alert_date']): o for o in outcomes_by_date}

                enriched = []
                for a in shown_alerts:
                    key = (a.get('ticker'), a.get('date'))
                    outcome = outcome_map.get(key, {})
                    enriched.append({**a, **outcome})
            else:
                enriched = []

            data = AlertHistoryData(ticker=ticker, alerts=enriched, count=total)
            embed_spec = AlertHistory(data).build()

            from rocketstocks.bot.senders.embed_utils import spec_to_embed
            await interaction.followup.send(embed=spec_to_embed(embed_spec), ephemeral=True)
        except Exception:
            logger.error(f"[alert history] Failed for '{ticker}'", exc_info=True)
            await interaction.followup.send("Failed to fetch alert history.", ephemeral=True)

    async def build_alert_summary(self, since_dt: datetime.datetime, label: str) -> AlertSummary:
        alerts = await self.dstate.get_alerts_since(since_dt)
        return AlertSummary(data=AlertSummaryData(since_dt=since_dt, label=label, alerts=alerts))

    async def _send_subscription_select(self, interaction: discord.Interaction) -> None:
        guild_roles = await self.bot.stock_data.alert_roles.get_all_for_guild(interaction.guild_id)
        member_role_ids = {r.id for r in interaction.user.roles}
        select = AlertSubscriptionSelect(guild_roles, member_role_ids)
        view = AlertSubscriptionView(select)
        await interaction.response.send_message(
            "Select the alerts you want to be notified about:",
            view=view,
            ephemeral=True,
        )

    @alert_group.command(name="summary", description="View a summary of recent alerts grouped by type")
    @app_commands.describe(
        since_when="Time period to summarize (defaults to since last close)",
        visibility="public posts to the alerts channel; private sends only to you",
    )
    @app_commands.choices(
        since_when=[
            app_commands.Choice(name="Since last close (default)", value="last_close"),
            app_commands.Choice(name="Since market open today",    value="market_open_today"),
            app_commands.Choice(name="Last 3 days",                value="last_3_days"),
            app_commands.Choice(name="Last 7 days",                value="last_7_days"),
        ],
        visibility=[
            app_commands.Choice(name="public",  value="public"),
            app_commands.Choice(name="private", value="private"),
        ],
    )
    async def alert_summary(
        self,
        interaction: discord.Interaction,
        since_when: app_commands.Choice[str] = None,
        visibility: app_commands.Choice[str] = None,
    ):
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/alert summary called by user '{interaction.user.name}'")
        since_dt, label = _resolve_since_dt(since_when.value if since_when else 'last_close')
        content = await self.build_alert_summary(since_dt, label)
        channel = await self.bot.get_channel_for_guild(interaction.guild_id, ALERTS)
        vis = visibility.value if visibility else "private"
        message = await send_report(content, channel, interaction=interaction, visibility=vis)
        if message is not None:
            await interaction.followup.send(f"[Alert summary posted]({message.jump_url})", ephemeral=True)
        else:
            await interaction.followup.send("Alert summary posted.", ephemeral=True)

    @alert_group.command(name="subscribe", description="Choose which alert types you want to be pinged for")
    async def alert_subscribe(self, interaction: discord.Interaction):
        await self._send_subscription_select(interaction)


#########
# Setup #
#########

async def setup(bot: commands.Bot):
    await bot.add_cog(Alerts(bot, bot.stock_data))
