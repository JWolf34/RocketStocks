from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import stockdata as sd
import analysis as an
import logging
import sys

# Logging configuration
logger = logging.getLogger(__name__)


def scheduler():
    timezone = 'UTC'
    
    sched = BlockingScheduler()

    update_tickers_trigger = CronTrigger(day_of_week="mon-sun", hour=5, minute=0, timezone=timezone)
    insert_new_tickers_trigger = CronTrigger(day_of_week="sun", hour=6, minute=0, timezone=timezone)
    update_upcoming_earnings_trigger = CronTrigger(day_of_week="fri", hour=6, minute=0, timezone=timezone)
    remove_past_earnings_trigger =  CronTrigger(day_of_week="tue-sat", hour=6, minute=0, timezone=timezone)
    update_historical_earnings_trigger = CronTrigger(day_of_week="tue-sat", hour=7, minute=0, timezone=timezone)
    update_daily_data_daily_trigger = CronTrigger(day_of_week="tues-sat", hour=6, minute=0, timezone=timezone)
    update_5m_data_daily_trigger = CronTrigger(day_of_week="tues-sat", hour=7, minute=30, timezone=timezone)

    test_trigger = IntervalTrigger(minutes=5, timezone=timezone)

    
    # Update tickers table in database with lastest NASDAQ data
    # Estimated runtime seconds
    sched.add_job(sd.StockData.update_tickers, trigger=update_tickers_trigger, name = "Update tickers data in DB", timezone=timezone, replace_existing=True)

    # Insert new tickers from NASDAQ into table in database 
    # Estimated runtime seconds
    sched.add_job(sd.StockData.insert_new_tickers, trigger=insert_new_tickers_trigger, name = "Insert new tickers into DB", timezone=timezone, replace_existing=True)

    # Update upcomingearnings table with newly reported earnings dates
    # Estimated runtime ~10 minutes
    sched.add_job(sd.StockData.Earnings.update_upcoming_earnings, trigger=update_upcoming_earnings_trigger, name = "Update upcoming earnings", timezone=timezone, replace_existing=True)

    # Delete rows in upcomingearnings with a date earlier than today
    # Estimated runtime seconds
    sched.add_job(sd.StockData.Earnings.remove_past_earnings, trigger=remove_past_earnings_trigger, name = "Remove past earnings", timezone=timezone, replace_existing=True)

    # Update historicalearnings table with newly release earnings
    # Estimated runtime seconds (once up-to-date)
    sched.add_job(sd.StockData.Earnings.update_historical_earnings, trigger=update_historical_earnings_trigger, name = "Update historical earnings", timezone=timezone, replace_existing=True)

    # Update dailypricehistory table with today's market data (daily job)
    # Estimated runtime ~90 minutes
    sched.add_job(sd.StockData.update_daily_price_history, trigger=update_daily_data_daily_trigger, name = "Update daily price history (daily)", timezone=timezone, replace_existing=True)

    # Update fiveminutepricehistorytable with recent market data (daily job)
    # Estimated runtime unknown
    sched.add_job(sd.StockData.update_5m_price_history, trigger= update_5m_data_daily_trigger, name = "Update 5m price history (daily)", timezone=timezone, replace_existing=True)

    sched.start()

if __name__ == '__main__':
    scheduler()


    