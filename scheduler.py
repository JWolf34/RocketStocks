from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import stockdata as sd
import analysis as an
import logging
import sys

# Logging configuration
logfile_handler = logging.FileHandler(filename="rocketstocks.log")
logfile_handler.setLevel(logging.DEBUG)
stderr_handler = logging.StreamHandler(stream=sys.stderr)
stderr_handler.setLevel(logging.ERROR)
handlers = [logfile_handler, stderr_handler]
format = '%(asctime)s [%(levelname)-8s] [%(thread)-5d] %(module)s.%(funcName)s: %(message)s'
logging.basicConfig(level = logging.DEBUG, format=format, handlers=handlers)
logger = logging.getLogger(__name__)
def scheduler():
    timezone = 'America/Chicago'
    
    sched = BlockingScheduler()

    daily_data_trigger = CronTrigger(day_of_week="mon-sat", hour=0, minute=0, timezone=timezone)
    minute_data_trigger = CronTrigger(day_of_week="sun", hour=0, minute=0, timezone=timezone)
    watchlist_analysis_trigger = CronTrigger(day_of_week="mon-fri", hour=5, minute=30, timezone=timezone)
    
    # Download daily data and generate indicator data on all tickers in masterlist daily
    # Estimated runtime ~4 hours
    sched.add_job(sd.daily_download_analyze_data, trigger=daily_data_trigger, name = 'Download data and generate indictor data for all tickers in the masterlist', timezone = timezone, replace_existing=True)

    # Download minute-by-minute data on all tickers in masterlist weekly
    # Estimated runtime ~4 hours
    sched.add_job(sd.minute_download_data, trigger=minute_data_trigger, name = 'Download minute-by-minute data for all tickers in the masterlist', timezone = timezone, replace_existing=True)

    # Run analaysis on the global watchlist tickers so they're ready to be posted by the bot
    sched.add_job(an.run_analysis, trigger=watchlist_analysis_trigger, name='Run analysis on data for tickers from the global watchlist', timezone=timezone, replace_existing=True)
     
    # Evaluate ticker scores on masterlist tickers
    #sched.add_job(an.generate_masterlist_scores, 'cron', name = "Calculate masterlist scores", timezone=timezone, hour = 7, minute = 30, replace_existing = True)

    sched.start()

if __name__ == '__main__':
    scheduler()


    