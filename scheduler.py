from apscheduler.schedulers.blocking import BlockingScheduler
import stockdata as sd
import analysis as an

def scheduler():
    timezone = 'America/New_York'
    
    sched = BlockingScheduler()

    for ticker in sd.get_tickers():
        sched.add_job(an.run_analysis, 'cron', name='Run analysis on ' + ticker + ' data', timezone=timezone, hour = 5, minute=0, replace_existing=True)
    
    # Download daily data on all tickers in masterlist
    sched.add_job(sd.download_masterlist_daily, 'cron', name = 'Download stock data for masterlist tickers', timezone = timezone, hour = 1, minute = 0, replace_existing=True)
    print('Ready!')

    sched.start()

if __name__ == '__main__':
    scheduler()


    