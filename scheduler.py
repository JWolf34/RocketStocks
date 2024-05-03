from apscheduler.schedulers.blocking import BlockingScheduler
import stockdata as sd
import analysis as an

def scheduler():
    timezone = 'America/Chicago'
    
    sched = BlockingScheduler()
    
    # Download daily data and generate indicator data on all tickers in masterlist
    sched.add_job(sd.daily_download_data, 'cron', name = 'Download data and generate indictor data for all tickers in the masterlist', timezone = timezone, hour = 0, minute = 0, replace_existing=True)

    # Generate indicators on downloaded data and update the data file
    sched.add_job(an.generate_indicators, 'cron', name = 'Generate indicator data for masterlist tickers', timezone = timezone, hour = 4, minute = 0, replace_existing=True)

    for ticker in sd.get_tickers():
        sched.add_job(an.run_analysis, 'cron', name='Run analysis on ' + ticker + ' data', timezone=timezone, hour = 7, minute=0, replace_existing=True)
     
    # Evaluate ticker scores on masterlist tickers
    sched.add_job(an.generate_masterlist_scores, 'cron', name = "Calculate masterlist scores", timezone=timezone, hour = 7, minute = 30, replace_existing = True)
    print('Ready!')

    sched.start()

if __name__ == '__main__':
    scheduler()


    