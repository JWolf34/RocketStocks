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
    timezone = 'America/Chicago'
    
    sched = BlockingScheduler()

    update_tickers_trigger = CronTrigger(day_of_week="sun", hour=0, minute=0, timezone=timezone)
    update_upcoming_earnings_trigger = CronTrigger(day_of_week="fri", hour=0, minute=0, timezone=timezone)
    remove_past_earnings_trigger =  CronTrigger(day_of_week="tue-sat", hour=0, minute=0, timezone=timezone)
    update_historical_earnings_trigger = CronTrigger(day_of_week="mon", hour=2, minute=0, timezone=timezone)
    update_daily_data_daily_trigger = CronTrigger(day_of_week="mon-fri", hour=3, minute=30, timezone=timezone)
    update_5m_data_daily_trigger = IntervalTrigger(minutes=5, timezone=timezone)
    
    # Update tickers table in database with newest NASDAQ data
    # Estimated runtime 20-25 minutes
    sched.add_job(sd.StockData.update_tickers, trigger=update_tickers_trigger, name = "Update tickers", timezone=timezone, replace_existing=True)

    # Update upcomingearnings table with newly reported earnings dates
    # Estimated runtime ~10 minutes
    sched.add_job(sd.StockData.Earnings.update_upcoming_earnings, trigger=update_upcoming_earnings_trigger, name = "Update upcoming earnings", timezone=timezone, replace_existing=True)

    # Delete rows in upcomingearnings with a date earlier than today
    # Estimated runtime seconds
    sched.add_job(sd.StockData.Earnings.remove_past_earnings, trigger=remove_past_earnings_trigger, name = "Remove past earnings", timezone=timezone, replace_existing=True)

    # Update historicalearnings table with newly release earnings
    # Estimated runtime ~2 hours
    sched.add_job(sd.StockData.Earnings.update_historical_earnings, trigger=update_historical_earnings_trigger, name = "Update historical earnings", timezone=timezone, replace_existing=True)

    # Update dailypricehistory table with today's market data
    # Estimated runtime ~90 minutes
    sched.add_job(sd.StockData.update_daily_price_history, trigger=update_daily_data_daily_trigger, name = "Update daily price history", timezone=timezone, replace_existing=True)

    # Update fiveminutepricehistorytable with recent market data
    # Estimated runtime ~30 seconds, but scales with the number of tickers on a watchlist
    sched.add_job(lambda: sd.StockData.update_5m_price_history(only_last_hour=True), trigger= update_5m_data_daily_trigger, name = "Update 5m price history", timezone=timezone, replace_existing=True)

    sched.start()

if __name__ == '__main__':
    scheduler()


    