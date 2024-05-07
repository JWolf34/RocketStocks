from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import stockdata as sd
import analysis as an

def scheduler():
    timezone = 'America/Chicago'
    
    sched = BlockingScheduler()

    daily_data_trigger = CronTrigger(day_of_week="mon-sat", hour=0, minute=0)
    minute_data_trigger = CronTrigger(day_of_week="sun", hour = 0, minute = 0)
    
    # Download daily data and generate indicator data on all tickers in masterlist daily
    sched.add_job(sd.daily_download_analyze_data, trigger=daily_data_trigger, name = 'Download data and generate indictor data for all tickers in the masterlist', timezone = timezone, replace_existing=True)

    # Download minute-by-minute data on all tickers in masterlist weekly
    sched.add_job(sd.daily_download_analyze_data, trigger=minute_data_trigger, name = 'Download minute-by-minute data for all tickers in the masterlist', timezone = timezone, replace_existing=True)

    for ticker in sd.get_tickers():
        sched.add_job(an.run_analysis, 'cron', name='Run analysis on ' + ticker + ' data', timezone=timezone, hour = 7, minute=0, replace_existing=True)
     
    # Evaluate ticker scores on masterlist tickers
    sched.add_job(an.generate_masterlist_scores, 'cron', name = "Calculate masterlist scores", timezone=timezone, hour = 7, minute = 30, replace_existing = True)
    print('Ready!')

    sched.start()

if __name__ == '__main__':
    scheduler()


    