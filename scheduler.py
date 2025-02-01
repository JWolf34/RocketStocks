from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import stockdata as sd
import analysis as an
import logging
import sys
import asyncio

# Logging configuration
logger = logging.getLogger(__name__)


def scheduler():
    timezone = 'UTC'

    async def async_scheduler():
        # Scheduler
        aio_sched = AsyncIOScheduler()
        
        # Triggers
        update_tickers_trigger = CronTrigger(day_of_week="mon-sun", hour=5, minute=0, timezone=timezone)
        insert_new_tickers_trigger = CronTrigger(day_of_week="sun", hour=6, minute=0, timezone=timezone)
        update_upcoming_earnings_trigger = CronTrigger(day_of_week="fri", hour=6, minute=0, timezone=timezone)
        remove_past_earnings_trigger =  CronTrigger(day_of_week="tue-sat", hour=6, minute=0, timezone=timezone)
        update_historical_earnings_trigger = CronTrigger(day_of_week="tue-sat", hour=7, minute=0, timezone=timezone)
        update_daily_data_daily_trigger = CronTrigger(day_of_week="tue-sat", hour=6, minute=0, timezone=timezone)
        update_5m_data_daily_trigger = CronTrigger(day_of_week="tue-sat", hour=7, minute=0, timezone=timezone)
        update_politicians_trigger = CronTrigger(day_of_week="sun", hour=7, minute=0, timezone=timezone)
        #test_trigger = IntervalTrigger(seconds=10, timezone=timezone)

        # Jobs

        # Update tickers table in database with lastest NASDAQ data
        # Estimated runtime seconds
        aio_sched.add_job(sd.StockData.update_tickers, trigger=update_tickers_trigger, name = "Update tickers data in DB", timezone=timezone, replace_existing=True)

        # Insert new tickers from NASDAQ into table in database 
        # Estimated runtime seconds
        aio_sched.add_job(sd.StockData.insert_new_tickers, trigger=insert_new_tickers_trigger, name = "Insert new tickers into DB", timezone=timezone, replace_existing=True)

        # Update upcomingearnings table with newly reported earnings dates
        # Estimated runtime ~10 minutes
        aio_sched.add_job(sd.StockData.Earnings.update_upcoming_earnings, trigger=update_upcoming_earnings_trigger, name = "Update upcoming earnings", timezone=timezone, replace_existing=True)

        # Delete rows in upcomingearnings with a date earlier than today
        # Estimated runtime seconds
        aio_sched.add_job(sd.StockData.Earnings.remove_past_earnings, trigger=remove_past_earnings_trigger, name = "Remove past earnings", timezone=timezone, replace_existing=True)

        # Update historicalearnings table with newly release earnings
        # Estimated runtime seconds (once up-to-date)
        aio_sched.add_job(sd.StockData.Earnings.update_historical_earnings, trigger=update_historical_earnings_trigger, name = "Update historical earnings", timezone=timezone, replace_existing=True)

        # Update dailypricehistory table with today's market data (daily job)
        # Estimated runtime ~35 minutes
        aio_sched.add_job(sd.StockData.update_daily_price_history, trigger=update_daily_data_daily_trigger, name = "Update daily price history (daily)", timezone=timezone, replace_existing=True)

        # Update fiveminutepricehistorytable with recent market data (daily job)
        # Estimated runtime ~45 minutes
        aio_sched.add_job(sd.StockData.update_5m_price_history, trigger= update_5m_data_daily_trigger, name = "Update 5m price history (daily)", timezone=timezone, replace_existing=True)

        # Update ct_politicians table with new politicians added to Capitol Trades
        # Estimated runtime seconds
        aio_sched.add_job(sd.CapitolTrades.update_politicians, trigger=update_politicians_trigger, name = "Update politicians", timezone=timezone, replace_existing=True)

        aio_sched.start()

        while True:
            await asyncio.sleep(1000)

    # Async scheduler
    asyncio.run(async_scheduler())
 
if __name__ == '__main__':
    scheduler()


    