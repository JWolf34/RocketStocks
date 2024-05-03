import yfinance as yf
from pandas_datareader import data as pdr
import pandas as pd
import os
#import analysis as an
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter

yf.pdr_override()

# Paths for writing data
DAILY_DATA_PATH = "data/CSV/daily"
INTRADAY_DATA_PATH = "data/CSV/intraday"
FINANCIALS_PATH = "data/financials"
PLOTS_PATH = "data/plots"
ANALYSIS_PATH = "data/analysis"
ATTACHMENTS_PATH = "discord/attachments"

# Class for limiting requests to avoid hitting the rate limit when downloading data
class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass

session = CachedLimiterSession(
    limiter=Limiter(RequestRate(2, Duration.SECOND*5)),  # max 2 requests per 5 seconds
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache("yfinance.cache"),
)


def validate_ticker(ticker):
    data = yf.download(ticker, period="1d")
    if len(data) == 0:
        return False
    else:
        return True

# Return news articles from Yahoo finance relevant to input ticker
def get_news(ticker):
        message = ''
        stock = yf.Ticker(ticker)
        articles = stock.news
        uuids_txt = open("discord/tickers.txt", 'a')
        uuids_values = open("discord/tickers.txt", 'r').read().splitlines()
        titles = []
        links = []
        for article in articles:
            if article['uuid'] in uuids_values:
                pass
            else: 
                titles.append(article['title']) 
                links.append(article['link'])
        if len(titles) > 0:
            description = ''
            for i in range(0, len(titles)):
                description += "[" + titles[i] + "]" + "(" + links[i] + ")" + "\n"
            
            message += ticker + ": \n" + description + "\n"
        return message

 # Return tickers from watchlist - global by default, personal if chosen by user
def get_tickers(id = 0):

    watchlist_path = get_watchlist_path(id)
    
    try:
        with open("{}/watchlist.txt".format(watchlist_path), 'r+') as watchlist:
            tickers = watchlist.read().splitlines()
        return tickers
    except FileNotFoundError as e:
        validate_path(watchlist_path)
        return []
        
def download_data(ticker, period='max', interval='1d'):

    # Download data for the given ticker
    data = yf.download(tickers=ticker, 
                       period=period, 
                       interval=interval, 
                       prepost = True,
                       auto_adjust = False,
                       repair = True,
                       session=session)
    
    data.fillna(0)

    return data

def update_csv(data, ticker, path):

    validate_path(path)
    path += "/{}.csv".format(ticker)

    data.to_csv(path)
    '''

    # If the CSV file already exists, read the existing data
    if os.path.exists(path):
        existing_data = pd.read_csv(path, index_col=0, parse_dates=True)
        existing_data.fillna(0)
        
        # Append the new data to the existing data
        combined_data = pd.concat([existing_data, data])
    else:
        combined_data = data
    
    # Remove duplicate rows, if any
    combined_data = combined_data[~combined_data.index.duplicated(keep='first')]
    
    # Save the combined data to the CSV file
    combined_data.to_csv(path)
    '''

def download_data_and_update_csv(ticker, period, interval, path=DAILY_DATA_PATH):
    data = download_data(ticker, period, interval)
    update_csv(data, ticker, path)
    print('Done!')
'''
def combine_csv(ticker):

    # Load historical and current data into data frames
    historical_data = pd.read_csv("data/historical_data/" + ticker + ".csv", index_col=0, parse_dates=True)
    current_data = pd.read_csv("data/CSV/" + ticker + ".csv", index_col=0, parse_dates=True)
    
    # Append the historical data to the current data
    combined_data = pd.concat([historical_data, current_data])

     # Remove duplicate rows, if any
    combined_data = combined_data[~combined_data.index.duplicated(keep='first')]
    
    # Save the combined data to the CSV file
    combined_data.to_csv("data/CSV/" + ticker + ".csv")
    '''

# Return all filepaths of all charts for a given ticker
def fetch_charts(ticker):
    charts_path = "{}/{}/".format(PLOTS_PATH,ticker)
    charts = os.listdir(charts_path)
    for i in range(0, len(charts)):
        charts[i] = charts_path + charts[i]
    return charts

# Return the latest data with technical indicators as a Pandas datafram
def fetch_daily_data(ticker):
    data = pd.read_csv("{}/{}.csv".format(DAILY_DATA_PATH, ticker), parse_dates=True, index_col='Date').sort_index()
    return data

def fetch_analysis(ticker):
    analyis = ''
    path = "{}/{}/".format(ANALYSIS_PATH,ticker)
    for file in os.listdir(path):
        data = open(path + file)
        analyis += data.read() + "\n"

    return analyis

def get_list_from_tickers(tickers):
    ticker_list = tickers.split(" ")
    invalid_tickers = []
    for ticker in ticker_list:
        if not validate_ticker(ticker):
            invalid_tickers.append(ticker)
    for ticker in invalid_tickers:
        ticker_list.remove(ticker)
    return ticker_list, invalid_tickers

def get_watchlist_path(id = 0):
    if id == 0:
        return "data/watchlists/global"
    else:
        return "data/watchlists/{}".format(id)

def validate_path(path):
    if not (os.path.isdir(path)):
        os.makedirs(path) 
        return 
    else:
        return True

'''
def get_stock_data(ticker):
    try:
        return pd.read_csv("data/CSV/{}.csv".format(ticker))
    except FileNotFoundError as e:
        print(e)
        return pd.DataFrame()
        '''
        

def get_days_summary(ticker):
    # Assumes data has been pulled recently. Mainly called when running or fetching reports. 
    data  = fetch_daily_data(ticker)
    if len(data) > 0:
        summary = data[['Open', "Close", "High", "Low", "Volume"]].iloc[-1]
        return summary
    else:
        return None
    
def get_next_earnings_date(ticker):
    try:
        return yf.Ticker(ticker).calendar['Earnings Date'][0]
    except IndexError as e:
        print(e)
        return "Earnings date unavailable"
    except KeyError as e:
        print(e)
        return "Earnings Date unavailable"
    

def download_financials(ticker):
    path = "{}/{}".format(FINANCIALS_PATH,ticker)
    validate_path(path)
    
    stock = yf.Ticker(ticker)
    stock.income_stmt.to_csv("{}/income_stmt.csv".format(path))
    stock.quarterly_income_stmt.to_csv("{}/quarterly_income_stmt.csv".format(path))
    stock.balance_sheet.to_csv("{}/balance_sheet.csv".format(path))
    stock.quarterly_balance_sheet.to_csv("{}/quarterly_balance_sheet.csv".format(path))
    stock.cashflow.to_csv("{}/cashflow.csv".format(path))
    stock.quarterly_cashflow.to_csv("{}/quarterly_cashflow.csv".format(path))
    stock.get_earnings_dates(limit=8).to_csv("{}/earnings_dates.csv".format(path)
    )
    
def fetch_financials(ticker):
    path = "{}/{}".format(FINANCIALS_PATH,ticker)
    if not validate_path(path):
        download_financials(ticker)
    financials = os.listdir(path)
    for i in range(0, len(financials)):
        financials[i] = path + "/" + financials[i]
    return financials
    
def download_masterlist_daily():
    
    import time
    masterlist_file = "data/ticker_masterlist.txt"
    
    tickers = get_masterlist_tickers()

    if isinstance(tickers, list):
        print("Downloading masterlist data...")
        invalid_tickers = []
        num_requests = 0
        requests_limit = 1500
        for ticker in tickers:
            data = download_data(ticker, "max", "1d")
            if len(data) > 0:
                update_csv(data, ticker, DAILY_DATA_PATH)
            else:
                invalid_tickers.append(ticker)
            num_requests += 1
        for ticker in invalid_tickers:
            if ticker in tickers:
                tickers.remove(ticker)
        with open(masterlist_file,'w') as masterlist:
            masterlist.write("\n".join(tickers))

        print("Complete!")

    else:
        pass

def get_masterlist_tickers():
    masterlist_file = "data/ticker_masterlist.txt"

    if os.path.isfile(masterlist_file):
        with open(masterlist_file, 'r') as masterlist:
            tickers = masterlist.read().splitlines()
            return tickers
    else:
        print("No ticker masterlist available.")
        return ""

# Download data and generate indicator data on all tickers
def daily_download_data():
    import time
    masterlist_file = "data/ticker_masterlist.txt"
    
    tickers = get_masterlist_tickers()

    if isinstance(tickers, list):
        invalid_tickers = []
        num_ticker = 1
        for ticker in tickers:
            print("Downloading {}... {}/{}".format(ticker, num_ticker, len(tickers)))
            data = download_data(ticker)
            if data.size > 60 and data['Close'].iloc[-1] > 5.00:
                update_csv(data, ticker, DAILY_DATA_PATH)
            else:
                invalid_tickers.append(ticker)
                print("Invalid ticker {}, removing from list...".format(ticker))
            num_ticker += 1
        for ticker in invalid_tickers:
            if ticker in tickers:
                tickers.remove(ticker)
        with open(masterlist_file,'w') as masterlist:
            masterlist.write("\n".join(tickers))

        #an.generate_masterlist_scores()

        print("Complete!")

    else:
        pass


def test():
    daily_download_data()

if __name__ == "__main__":
    test()
    pass