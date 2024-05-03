from apscheduler.schedulers.blocking import BlockingScheduler
import stockdata as sd
import analysis as an

def scheduler():
    timezone = 'America/Chicago'
    
    sched = BlockingScheduler()
    
<<<<<<< HEAD
    # Download daily data and generate indicator data on all tickers in masterlist
    sched.add_job(sd.daily_download_analyze_scoring, 'cron', name = 'Download data and generate indictor data for all tickers in the masterlist', timezone = timezone, hour = 17, minute = 35, replace_existing=True)

    # Generate indicators on downloaded data and update the data file
    #sched.add_job(an.generate_indicators, 'cron', name = 'Download stock data for masterlist tickers', timezone = timezone, hour = 17, minute = 35, replace_existing=True)
=======
    # Download daily data on all tickers in masterlist
    sched.add_job(sd.download_masterlist_daily, 'cron', name = 'Download stock data for masterlist tickers', timezone = timezone, hour = 17, minute = 35, replace_existing=True)

    # Generate indicators on downloaded data and update the data file
    sched.add_job(an.generate_indicators, 'cron', name = 'Download stock data for masterlist tickers', timezone = timezone, hour = 17, minute = 35, replace_existing=True)
>>>>>>> 405501164043714887641f569eeb0ac5afde4e68

    #for ticker in sd.get_tickers():
    #    sched.add_job(an.run_analysis, 'cron', name='Run analysis on ' + ticker + ' data', timezone=timezone, hour = 17, minute=40, replace_existing=True)
    
    

    # Evaluate ticker scores on masterlist tickers
    sched.add_job(an.generate_masterlist_scores, 'cron', name = "Calculate masterlist scores", timezone=timezone, hour = 6, minute = 0, replace_existing = True)
    print('Ready!')

    sched.start()

if __name__ == '__main__':
    scheduler()


    