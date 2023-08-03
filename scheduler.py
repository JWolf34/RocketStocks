from apscheduler.schedulers.blocking import BlockingScheduler
import stockdata as sd

def scheduler():
    timezone = 'America/New_York'
    
    sched = BlockingScheduler()

    '''
    @sched.scheduled_job('cron', hour=9)
    def download_data_and_update_csv():
        with open("tickers.txt", 'r') as watchlist:
            tickers = watchlist.read().splitlines()
        for ticker in tickers:
            sd.download_data_and_update_csv(ticker, 'max')
    '''
    
    sched.add_job(sd.download_data_and_update_csv, 'cron', args= ['QQQ', 'max'], name='Fetch QQQ data', timezone=timezone, hour = 13, minute=1, replace_existing=True)
    
    print('Ready!')

    sched.start()

if __name__ == '__main__':
    scheduler()


    