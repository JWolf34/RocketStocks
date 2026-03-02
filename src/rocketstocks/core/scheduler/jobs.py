import logging
import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from rocketstocks.data.stockdata import StockData
from rocketstocks.core.notifications import EventEmitter
from rocketstocks.core.notifications.event import NotificationEvent
from rocketstocks.core.notifications.config import NotificationLevel

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def register_jobs(aio_sched: AsyncIOScheduler, stock_data: StockData, emitter: EventEmitter):
    """Add all scheduled jobs to *aio_sched*. Testable independently of asyncio.run()."""
    timezone = 'UTC'

    async def _update_daily():
        tickers = stock_data.tickers.get_all_tickers()
        await stock_data.price_history.update_daily_price_history(tickers)

    async def _update_5m():
        tickers = stock_data.tickers.get_all_tickers()
        await stock_data.price_history.update_5m_price_history(tickers)

    async def _check_schwab_token_expiry():
        """Check Schwab token expiry and emit notifications if needed."""
        try:
            expiry_time = stock_data.schwab.get_token_expiry()

            if expiry_time is None:
                emitter.emit(NotificationEvent(
                    level=NotificationLevel.FAILURE,
                    source="rocketstocks.core.scheduler.jobs",
                    job_name="Check Schwab token expiry",
                    message="Schwab token is not initialized. User authentication may be required.",
                ))
                return

            now = datetime.datetime.now()
            time_until_expiry = expiry_time - now
            one_day = datetime.timedelta(days=1)

            if time_until_expiry <= datetime.timedelta(0):
                # Token is expired
                emitter.emit(NotificationEvent(
                    level=NotificationLevel.FAILURE,
                    source="rocketstocks.core.scheduler.jobs",
                    job_name="Check Schwab token expiry",
                    message="Schwab token has expired. Please refresh the token.",
                ))
            elif time_until_expiry <= one_day:
                # Token expires within 1 day
                hours_until_expiry = time_until_expiry.total_seconds() / 3600
                emitter.emit(NotificationEvent(
                    level=NotificationLevel.WARNING,
                    source="rocketstocks.core.scheduler.jobs",
                    job_name="Check Schwab token expiry",
                    message=f"Schwab token will expire in {hours_until_expiry:.1f} hours. Plan to refresh soon.",
                ))
        except Exception as exc:
            logger.error(f"Error checking Schwab token expiry: {exc}")
            emitter.emit(NotificationEvent(
                level=NotificationLevel.FAILURE,
                source="rocketstocks.core.scheduler.jobs",
                job_name="Check Schwab token expiry",
                message=f"Error checking token expiry: {str(exc)}",
            ))

    # Triggers
    update_tickers_trigger = CronTrigger(day_of_week="mon-sun", hour=5, minute=0, timezone=timezone)
    insert_new_tickers_trigger = CronTrigger(day_of_week="sun", hour=6, minute=0, timezone=timezone)
    update_upcoming_earnings_trigger = CronTrigger(day_of_week="fri", hour=6, minute=0, timezone=timezone)
    remove_past_earnings_trigger = CronTrigger(day_of_week="tue-sat", hour=6, minute=0, timezone=timezone)
    update_historical_earnings_trigger = CronTrigger(day_of_week="tue-sat", hour=7, minute=0, timezone=timezone)
    update_daily_data_daily_trigger = CronTrigger(day_of_week="tue-sat", hour=3, minute=0, timezone=timezone)
    update_5m_data_daily_trigger = CronTrigger(day_of_week="tue-sat", hour=4, minute=0, timezone=timezone)
    update_politicians_trigger = CronTrigger(day_of_week="sun", hour=7, minute=0, timezone=timezone)
    check_schwab_token_expiry_trigger = CronTrigger(hour="*/6", minute=0, timezone=timezone, start_date=datetime.datetime(2000, 1, 1, 6, 0, 0))

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
