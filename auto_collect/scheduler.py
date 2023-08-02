from apscheduler.schedulers.blocking import BlockingScheduler
import stockdata as sd

def scheduler():
    timezone = 'America/New_York'
    
    sched = BlockingScheduler()


    @sched.scheduled_job('cron', hour=9)
    def download_data_and_update_csv():
        with open("tickers.txt", 'r') as watchlist:
            tickers = watchlist.read().splitlines()
        for ticker in tickers:
            sd.download_data_and_update_csv(ticker, 'max')
        
    sched.start()
    