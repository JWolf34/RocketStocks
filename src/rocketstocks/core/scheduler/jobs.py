import logging
import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from rocketstocks.data.stockdata import StockData
from rocketstocks.core.notifications import EventEmitter
from rocketstocks.core.notifications.event import NotificationEvent
from rocketstocks.core.notifications.config import NotificationLevel
from rocketstocks.core.analysis.classification import classify_ticker, compute_volatility, compute_return_stats

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def register_jobs(aio_sched: AsyncIOScheduler, stock_data: StockData, emitter: EventEmitter):
    """Add all scheduled jobs to *aio_sched*. Testable independently of asyncio.run()."""
    timezone = 'UTC'

    async def _update_daily():
        tickers = await stock_data.tickers.get_all_tickers()
        await stock_data.price_history.update_daily_price_history(tickers)

    async def _update_5m():
        tickers = await stock_data.tickers.get_all_tickers()
        await stock_data.price_history.update_5m_price_history(tickers)

    async def _classify_tickers():
        """Classify all tickers and update ticker_stats table."""
        logger.info("Running daily ticker classification job")
        tickers = await stock_data.tickers.get_all_tickers()

        # Batch fetch quotes for market cap data (25 at a time)
        all_quotes: dict = {}
        chunk_size = 25
        for i in range(0, len(tickers), chunk_size):
            chunk = tickers[i:i + chunk_size]
            try:
                chunk_quotes = await stock_data.schwab.get_quotes(tickers=chunk)
                chunk_quotes.pop('errors', None)
                all_quotes.update(chunk_quotes)
            except Exception as exc:
                logger.warning(f"Failed to fetch quotes for chunk {chunk}: {exc}")

        # Get classification overrides from watchlists
        overrides = await stock_data.watchlists.get_classification_overrides()

        # Get popularity data for meme detection (top-100 popular tickers)
        try:
            popular_df = stock_data.popularity.get_popular_stocks(num_stocks=100)
            popular_ranks = dict(zip(popular_df['ticker'], popular_df['rank']))
        except Exception as exc:
            logger.warning(f"Failed to fetch popularity data: {exc}")
            popular_ranks = {}

        for ticker in tickers:
            try:
                daily_prices = await stock_data.price_history.fetch_daily_price_history(ticker=ticker)
                if daily_prices.empty:
                    continue

                vol_20d = compute_volatility(daily_prices, period=20)
                mean_20d, std_20d = compute_return_stats(daily_prices, period=20)
                mean_60d, std_60d = compute_return_stats(daily_prices, period=60)

                quote_data = all_quotes.get(ticker, {})
                market_cap = (
                    quote_data.get('fundamental', {}).get('marketCapFloat')
                    or quote_data.get('fundamental', {}).get('marketCap')
                )
                if market_cap is not None:
                    try:
                        market_cap = int(float(market_cap))
                    except (TypeError, ValueError):
                        market_cap = None

                popularity_rank = popular_ranks.get(ticker)
                watchlist_override = overrides.get(ticker)

                stock_class = classify_ticker(
                    ticker=ticker,
                    market_cap=market_cap,
                    volatility_20d=vol_20d,
                    popularity_rank=popularity_rank,
                    watchlist_override=watchlist_override,
                )

                bb_upper = bb_lower = bb_mid = None
                if str(stock_class.value) == 'blue_chip':
                    try:
                        from rocketstocks.core.analysis.signals import signals as sig
                        bb_df = sig.bollinger_bands(daily_prices['close'])
                        if not bb_df.empty:
                            cols = bb_df.columns.tolist()
                            bbl = next((c for c in cols if c.startswith('BBL')), None)
                            bbm = next((c for c in cols if c.startswith('BBM')), None)
                            bbu = next((c for c in cols if c.startswith('BBU')), None)
                            if bbl:
                                bb_lower = float(bb_df[bbl].iloc[-1])
                            if bbm:
                                bb_mid = float(bb_df[bbm].iloc[-1])
                            if bbu:
                                bb_upper = float(bb_df[bbu].iloc[-1])
                    except Exception as exc:
                        logger.debug(f"Could not compute BB for '{ticker}': {exc}")

                stats = {
                    'market_cap': market_cap,
                    'classification': stock_class.value,
                    'volatility_20d': vol_20d,
                    'mean_return_20d': mean_20d,
                    'std_return_20d': std_20d,
                    'mean_return_60d': mean_60d,
                    'std_return_60d': std_60d,
                    'bb_upper': bb_upper,
                    'bb_lower': bb_lower,
                    'bb_mid': bb_mid,
                }
                await stock_data.ticker_stats.upsert_stats(ticker=ticker, stats_dict=stats)
            except Exception as exc:
                logger.warning(f"Failed to classify ticker '{ticker}': {exc}")

        logger.info(f"Ticker classification complete — processed {len(tickers)} tickers")

    async def _check_schwab_token_expiry():
        """Check Schwab token expiry and emit notifications if needed."""
        from rocketstocks.core.auth.token_manager import TokenStatus
        _JOB = "Check Schwab token expiry"
        _SRC = "rocketstocks.core.scheduler.jobs"
        try:
            info = await stock_data.schwab.get_token_info()

            if info.status == TokenStatus.HEALTHY:
                return

            elif info.status == TokenStatus.EXPIRING_SOON:
                hours = info.time_remaining.total_seconds() / 3600
                emitter.emit(NotificationEvent(
                    level=NotificationLevel.WARNING,
                    source=_SRC,
                    job_name=_JOB,
                    message=(
                        f"Schwab token expires in {hours:.1f} hours. "
                        "Run `/schwab-auth` to refresh before it expires."
                    ),
                ))

            elif info.status == TokenStatus.EXPIRED:
                emitter.emit(NotificationEvent(
                    level=NotificationLevel.FAILURE,
                    source=_SRC,
                    job_name=_JOB,
                    message="Schwab token has expired. Run `/schwab-auth` to re-authenticate.",
                ))

            elif info.status == TokenStatus.INVALID:
                emitter.emit(NotificationEvent(
                    level=NotificationLevel.FAILURE,
                    source=_SRC,
                    job_name=_JOB,
                    message=(
                        "Schwab token was rejected by Schwab (revoked or invalid). "
                        "Run `/schwab-auth` to re-authenticate."
                    ),
                ))

            elif info.status == TokenStatus.MISSING:
                emitter.emit(NotificationEvent(
                    level=NotificationLevel.FAILURE,
                    source=_SRC,
                    job_name=_JOB,
                    message=(
                        "Schwab token file is missing. "
                        "Run `/schwab-auth` to authenticate."
                    ),
                ))

        except Exception as exc:
            logger.error(f"Error checking Schwab token expiry: {exc}")
            emitter.emit(NotificationEvent(
                level=NotificationLevel.FAILURE,
                source=_SRC,
                job_name=_JOB,
                message=f"Error checking token expiry: {str(exc)}",
            ))

    async def _enrich_tickers():
        await stock_data.tickers.enrich_unenriched_batch(limit=240)

    async def _import_delisted():
        await stock_data.tickers.import_delisted_tickers()

    async def _load_delisted_prices():
        await stock_data.price_history.load_delisted_price_history_batch()

    # Triggers
    classify_tickers_trigger = CronTrigger(day_of_week="tue-sat", hour=5, minute=30, timezone=timezone)
    update_tickers_trigger = CronTrigger(day_of_week="mon-sun", hour=5, minute=0, timezone=timezone)
    insert_new_tickers_trigger = CronTrigger(day_of_week="sun", hour=6, minute=0, timezone=timezone)
    update_upcoming_earnings_trigger = CronTrigger(day_of_week="fri", hour=6, minute=0, timezone=timezone)
    remove_past_earnings_trigger = CronTrigger(day_of_week="tue-sat", hour=6, minute=0, timezone=timezone)
    update_historical_earnings_trigger = CronTrigger(day_of_week="tue-sat", hour=7, minute=0, timezone=timezone)
    update_daily_data_daily_trigger = CronTrigger(day_of_week="tue-sat", hour=3, minute=0, timezone=timezone)
    update_5m_data_daily_trigger = CronTrigger(day_of_week="tue-sat", hour=4, minute=0, timezone=timezone)
    update_politicians_trigger = CronTrigger(day_of_week="sun", hour=7, minute=0, timezone=timezone)
    check_schwab_token_expiry_trigger = CronTrigger(hour="*/6", minute=0, timezone=timezone, start_date=datetime.datetime(2000, 1, 1, 6, 0, 0))
    enrich_tickers_trigger = CronTrigger(hour="*/1", minute=5, timezone=timezone)
    import_delisted_trigger = CronTrigger(day_of_week="sun", hour=9, minute=0, timezone=timezone)
    load_delisted_prices_trigger = CronTrigger(hour=10, minute=0, timezone=timezone)

    # Jobs — each wrapped with emitter.job_wrapper for notification on success/failure
    aio_sched.add_job(emitter.job_wrapper("Update tickers data in DB", stock_data.tickers.update_tickers), trigger=update_tickers_trigger, name="Update tickers data in DB", timezone=timezone, replace_existing=True, misfire_grace_time=600)
    aio_sched.add_job(emitter.job_wrapper("Insert new tickers into DB", stock_data.tickers.insert_tickers), trigger=insert_new_tickers_trigger, name="Insert new tickers into DB", timezone=timezone, replace_existing=True, misfire_grace_time=600)
    aio_sched.add_job(emitter.job_wrapper("Update upcoming earnings", stock_data.earnings.update_upcoming_earnings), trigger=update_upcoming_earnings_trigger, name="Update upcoming earnings", timezone=timezone, replace_existing=True, misfire_grace_time=600)
    aio_sched.add_job(emitter.job_wrapper("Remove past earnings", stock_data.earnings.remove_past_earnings), trigger=remove_past_earnings_trigger, name="Remove past earnings", timezone=timezone, replace_existing=True, misfire_grace_time=600)
    aio_sched.add_job(emitter.job_wrapper("Update historical earnings", stock_data.earnings.update_historical_earnings), trigger=update_historical_earnings_trigger, name="Update historical earnings", timezone=timezone, replace_existing=True, misfire_grace_time=600)
    aio_sched.add_job(emitter.job_wrapper("Update daily price history (daily)", _update_daily), trigger=update_daily_data_daily_trigger, name="Update daily price history (daily)", timezone=timezone, replace_existing=True, misfire_grace_time=600)
    aio_sched.add_job(emitter.job_wrapper("Update 5m price history (daily)", _update_5m), trigger=update_5m_data_daily_trigger, name="Update 5m price history (daily)", timezone=timezone, replace_existing=True, misfire_grace_time=600)
    aio_sched.add_job(emitter.job_wrapper("Update politicians", stock_data.capitol_trades.update_politicians), trigger=update_politicians_trigger, name="Update politicians", timezone=timezone, replace_existing=True, misfire_grace_time=600)
    aio_sched.add_job(_check_schwab_token_expiry, trigger=check_schwab_token_expiry_trigger, name="Check Schwab token expiry", timezone=timezone, replace_existing=True, misfire_grace_time=60)
    aio_sched.add_job(emitter.job_wrapper("Classify tickers", _classify_tickers), trigger=classify_tickers_trigger, name="Classify tickers", timezone=timezone, replace_existing=True, misfire_grace_time=600)
    aio_sched.add_job(emitter.job_wrapper("Enrich tickers", _enrich_tickers), trigger=enrich_tickers_trigger, name="Enrich tickers", timezone=timezone, replace_existing=True, misfire_grace_time=600)
    aio_sched.add_job(emitter.job_wrapper("Import delisted tickers", _import_delisted), trigger=import_delisted_trigger, name="Import delisted tickers", timezone=timezone, replace_existing=True, misfire_grace_time=600)
    aio_sched.add_job(emitter.job_wrapper("Load delisted price history", _load_delisted_prices), trigger=load_delisted_prices_trigger, name="Load delisted price history", timezone=timezone, replace_existing=True, misfire_grace_time=600)
