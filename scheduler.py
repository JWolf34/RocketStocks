from apscheduler.schedulers.blocking import BlockingScheduler
import stockdata as sd

def scheduler():
    timezone = 'America/New_York'
    
    sched = BlockingScheduler()

    for ticker in sd.get_tickers():
        sched.add_job(sd.download_data_and_update_csv, 'cron', args= [ticker, 'max', "1m"], name='Fetch' + ticker + 'data', timezone=timezone, hour = 16, minute=0, replace_existing=True)
        
    
    print('Ready!')

    sched.start()

if __name__ == '__main__':
    scheduler()


    