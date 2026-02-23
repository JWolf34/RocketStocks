import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from src.rocketstocks.data.stockdata import StockData

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def scheduler(stock_data: StockData):
    timezone = 'UTC'

    async def async_scheduler(stock_data: StockData):

        aio_sched = AsyncIOScheduler()

        async def _update_daily():
            tickers = stock_data.tickers.get_all_tickers()
            await stock_data.price_history.update_daily_price_history(tickers)

        async def _update_5m():
            tickers = stock_data.tickers.get_all_tickers()
            await stock_data.price_history.update_5m_price_history(tickers)

        # Triggers
        update_tickers_trigger = CronTrigger(day_of_week="mon-sun", hour=5, minute=0, timezone=timezone)
        insert_new_tickers_trigger = CronTrigger(day_of_week="sun", hour=6, minute=0, timezone=timezone)
        update_upcoming_earnings_trigger = CronTrigger(day_of_week="fri", hour=6, minute=0, timezone=timezone)
        remove_past_earnings_trigger = CronTrigger(day_of_week="tue-sat", hour=6, minute=0, timezone=timezone)
        update_historical_earnings_trigger = CronTrigger(day_of_week="tue-sat", hour=7, minute=0, timezone=timezone)
        update_daily_data_daily_trigger = CronTrigger(day_of_week="tue-sat", hour=3, minute=0, timezone=timezone)
        update_5m_data_daily_trigger = CronTrigger(day_of_week="tue-sat", hour=4, minute=0, timezone=timezone)
        update_politicians_trigger = CronTrigger(day_of_week="sun", hour=7, minute=0, timezone=timezone)

        # Jobs
        aio_sched.add_job(stock_data.tickers.update_tickers, trigger=update_tickers_trigger, name="Update tickers data in DB", timezone=timezone, replace_existing=True)
        aio_sched.add_job(stock_data.tickers.insert_tickers, trigger=insert_new_tickers_trigger, name="Insert new tickers into DB", timezone=timezone, replace_existing=True)
        aio_sched.add_job(stock_data.earnings.update_upcoming_earnings, trigger=update_upcoming_earnings_trigger, name="Update upcoming earnings", timezone=timezone, replace_existing=True)
        aio_sched.add_job(stock_data.earnings.remove_past_earnings, trigger=remove_past_earnings_trigger, name="Remove past earnings", timezone=timezone, replace_existing=True)
        aio_sched.add_job(stock_data.earnings.update_historical_earnings, trigger=update_historical_earnings_trigger, name="Update historical earnings", timezone=timezone, replace_existing=True)
        aio_sched.add_job(_update_daily, trigger=update_daily_data_daily_trigger, name="Update daily price history (daily)", timezone=timezone, replace_existing=True)
        aio_sched.add_job(_update_5m, trigger=update_5m_data_daily_trigger, name="Update 5m price history (daily)", timezone=timezone, replace_existing=True)
        aio_sched.add_job(stock_data.capitol_trades.update_politicians, trigger=update_politicians_trigger, name="Update politicians", timezone=timezone, replace_existing=True)

        aio_sched.start()

        while True:
            await asyncio.sleep(1000)

    asyncio.run(async_scheduler(stock_data))
