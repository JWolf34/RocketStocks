from apscheduler.schedulers.blocking import BlockingScheduler
import stockdata as sd
import analysis as an

def scheduler():
    timezone = 'America/Chicago'
    
    sched = BlockingScheduler()

    for ticker in sd.get_tickers():
        sched.add_job(an.run_analysis, 'cron', name='Run analysis on ' + ticker + ' data', timezone=timezone, hour = 5, minute=0, replace_existing=True)
    
    # Download daily data on all tickers in masterlist
    sched.add_job(sd.download_masterlist_daily, 'cron', name = 'Download stock data for masterlist tickers', timezone = timezone, hour = 0, minute = 0, replace_existing=True)

    # Evaluate ticker scores on masterlist tickers
    sched.add_job(an.generate_masterlist_scores, 'cron', name = "Calculate masterlist scores", timezone=timezone, hour = 6, minute = 0, replace_existing = True)
    print('Ready!')

    sched.start()

if __name__ == '__main__':
    scheduler()


    