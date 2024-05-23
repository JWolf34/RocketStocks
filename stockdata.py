import yfinance as yf
from pandas_datareader import data as pdr
import pandas as pd
import pandas_ta as ta
import os
import datetime
from datetime import timedelta
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter
import sys
import logging

# Logging configuration
logger = logging.getLogger(__name__)

# Override pandas data fetching with yfinance logic
yf.pdr_override()

# Paths for writing data
DAILY_DATA_PATH = "data/CSV/daily"
INTRADAY_DATA_PATH = "data/CSV/intraday"
FINANCIALS_PATH = "data/financials"
PLOTS_PATH = "data/plots"
ANALYSIS_PATH = "data/analysis"
ATTACHMENTS_PATH = "discord/attachments"
MINUTE_DATA_PATH = "data/CSV/minute"
WATCHLISTS_PATH = "data/watchlists"
UTILS_PATH = "utils"

# Class for limiting requests to avoid hitting the rate limit when downloading data

class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass


session = CachedLimiterSession(
    limiter=Limiter(RequestRate(2, Duration.SECOND*5)),  # max 2 requests per 5 seconds
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache("yfinance.cache"),
) 

#session = Session()


#########################
# Download and analysis #
#########################

# Download data for the given ticker        
def download_data(ticker, period='max', interval='1d'): 
    logger.info("Downloading data for ticker {} (period: {}, interval: {})".format(ticker, period, interval))
    data = pd.DataFrame()
    try: 
        data = yf.download(tickers=ticker, 
                        period=period, 
                        interval=interval, 
                        prepost = True,
                        auto_adjust = False,
                        repair = True,
                        session=session)
        
        data.fillna(0)

    # yfinance occassionally encounters KeyErrors if separately threaded downloads are initiated   
    except KeyError as e:
        logger.exception("Encountered KeyError when downloading data for {} \n{}".format(ticker, e))

    return data

# Write data to a CSV file at the path specifiede
def update_csv(data, ticker, path):

    validate_path(path)
    path += "/{}.csv".format(ticker)
    logger.info("Writing data for ticker {} to path '{}'".format(ticker, path))

    # If the CSV file already exists, read the existing data
    if os.path.exists(path):
        existing_data = pd.read_csv(path, index_col=0, parse_dates=True)
        existing_data.fillna(0)
        
        # Append the new data to the existing data
        combined_data = pd.concat([existing_data, data])
    else:
        combined_data = data
    
    # Remove duplicate rows, if any, and prefer most recent data
    combined_data = combined_data[~combined_data.index.duplicated(keep='last')]
    
    # Save the combined data to the CSV file
    combined_data.to_csv(path)

# Generate indicator data on the provided Dataframe per the specified strategy
# Can add or remove inidicators to the strategy as needed
def generate_indicators(data):#

    IndicatorStrategy = ta.Strategy(name = 'Indicator Strategy', ta = [
        {"kind": "sma", "length":10},
        {"kind": "sma", "length":30},
        {"kind": "sma", "length":50},
        {"kind": "sma", "length":200},
        {"kind": "obv"},
        {"kind": "macd"},
        {"kind": "rsi"},
        {"kind": "adx"},
        {"kind": "ad"}
    ]
    )
    logger.info("Generating indicator data...")
    
    data.ta.strategy(IndicatorStrategy)

# Download data and generate indicator data on specified ticker
def download_analyze_data(ticker):
    SIZE_THRESHOLD = 60
    CLOSE_THRESHOLD = 1.00

    data = download_data(ticker)
    generate_indicators(data)
    update_csv(data, ticker, DAILY_DATA_PATH)

# Daily process to download daily ticker data and generate indicator data on all 
# tickers in the masterlist
def daily_download_analyze_data():
    import time
    logging.info("********** [START DAILY DOWNLOAD TASK] **********")
    masterlist_file = "data/ticker_masterlist.txt"
    
    tickers = get_all_tickers()

    num_ticker = 1
    for ticker in tickers:
        logger.info("Processesing {}, {}/{}".format(ticker, num_ticker, len(tickers)))
        logger.debug("Validate if data file for {} is up-tp-date".format(ticker))
        if not daily_data_up_to_date(fetch_daily_data(ticker)):
            logger.debug("Data file for {} is either empty or not up-to-date".format(ticker))
            
            if validate_ticker(ticker):
                # Ticker is valid - download data
                
                data = download_data(ticker)

                # Validate quality of data
                if data.size < 30:
                    logger.warn("INVALID TICKER - Data of {} has size {} after download which is less than threshold 30".format(ticker, data.size))
                    remove_from_all_tickers(ticker)
                else:
                    generate_indicators(data)
                    update_csv(data, ticker, DAILY_DATA_PATH)
                    logger.debug("Download and analysis of {} complete.".format(ticker))
                



                """ # No CSV was written or data was bad:
                if data.size == 0:
                    logger.warn("INVALID TICKER - Data of {} has size 0 after download. Attempting to remove from masterlist".format(ticker))
                    remove_from_all_tickers(ticker)

                # Data downloaded and still not up-to-date means there is
                # no data for yesterday. Remove from masterlist
                elif not daily_data_up_to_date(fetch_daily_data(ticker)):
                    logger.warn("INVALID TICKER - No data for ticker {} available for yesterday. Attempting to remove from masterlist".format(ticker))
                    remove_from_all_tickers(ticker) """
            else:
                logger.info("Ticker '{}' is not valid. Removing from all tickers")
                remove_from_all_tickers(ticker)
        else:
            logger.info("Data for {} is up-to-date. Skipping...".format(ticker))
        
        num_ticker += 1
    logger.info("Daily data download task complete!")
   
# Weekly process to download minute-by-minute ticker data on all tickers
# in the masterlist
def minute_download_data():
    
    logging.info("********** [START WEEKLY DOWNLOAD TASK] **********")
    masterlist_file = "data/ticker_masterlist.txt"
    tickers = get_all_tickers()
    

    # Verify that ticker_masterlist.txt exists
    if isinstance(tickers, list):
        num_ticker = 1
        for ticker in tickers:    
            logger.info("Processesing {}, {}/{}".format(ticker, num_ticker, len(tickers)))
            data = download_data(ticker, period="7d", interval="1m")
            logger.debug("Download and analysis of {} complete. Validate that data is valid".format(ticker))

            # No CSV was written or data was bad:
            if data.size == 0:
                logger.warn("INVALID TICKER - Data of {} has size 0 after download. Attempting to remove from masterlist".format(ticker))
                remove_from_all_tickers(ticker)
            else: # Data downloaded 
                logger.debug("Data downloaded for {} is valid.".format(ticker))
                update_csv(data, ticker, MINUTE_DATA_PATH)
               
            num_ticker += 1

    else:
        pass

# Download and write financials for specified ticker to the financials folder
def download_financials(ticker):
    logger.info("Downloading financials for ticker {}".format(ticker))
    path = "{}/{}".format(FINANCIALS_PATH,ticker)
    validate_path(path)
    
    stock = yf.Ticker(ticker)
    logger.debug("Downloading income statement for {}".format(ticker))
    stock.income_stmt.to_csv("{}/income_stmt.csv".format(path))

    logger.debug("Downloading quarterly income statement for {}".format(ticker))
    stock.quarterly_income_stmt.to_csv("{}/quarterly_income_stmt.csv".format(path))

    logger.debug("Downloading balance sheet for {}".format(ticker))
    stock.balance_sheet.to_csv("{}/balance_sheet.csv".format(path))

    logger.debug("Downloading quarterly balance sheet for {}".format(ticker))
    stock.quarterly_balance_sheet.to_csv("{}/quarterly_balance_sheet.csv".format(path))

    logger.debug("Downloading cashflow for {}".format(ticker))
    stock.cashflow.to_csv("{}/cashflow.csv".format(path))

    logger.debug("Downloading quarterly cashflow for {}".format(ticker))
    stock.quarterly_cashflow.to_csv("{}/quarterly_cashflow.csv".format(path))

    logger.debug("Downloading earnings dates for {}".format(ticker))
    stock.get_earnings_dates(limit=8).to_csv("{}/earnings_dates.csv".format(path))
    

######################################################
# 'Fetch' functions for returning data saved to disk #
######################################################

# Return all filepaths of all charts for a given ticker
def fetch_charts(ticker):
    logger.info("Fetching charts for ticker {}".format(ticker))
    charts_path = "{}/{}/".format(PLOTS_PATH,ticker)
    charts = os.listdir(charts_path)
    for i in range(0, len(charts)):
        logger.debug("Appending {} chart: {}".format(ticker, charts_path + charts[i]))
        charts[i] = charts_path + charts[i]
    return charts

# Return the latest data with technical indicators as a Pandas dataframe.
# If CSV does not exist or is not up-to-date, return empty DataFrame
def fetch_daily_data(ticker):
    logger.info("Fetching daily data file for ticker {}".format(ticker))
    data = pd.DataFrame()
    data_path = "{}/{}.csv".format(DAILY_DATA_PATH, ticker)
    if not os.path.isfile(data_path):
        logger.warning("CSV file for {} does not exist.".format(ticker))
    else:
        logging.debug("Data file for {} exists: {}".format(ticker, data_path))
        data = pd.read_csv(data_path, parse_dates=True, index_col='Date').sort_index()
    
    return data

# Return the latest minute-by-minute data with technical indicators as a Pandas dataframe.
# If CSV does not exist or is not up-to-date, return empty DataFrame
def fetch_minute_data(ticker):
    logger.info("Fetching minute data file for ticker {}".format(ticker))
    data = pd.DataFrame()
    data_path = "{}/{}.csv".format(MINUTE_DATA_PATH, ticker)
    if not os.path.isfile(data_path):
        logger.warning("CSV file for {} does not exist.".format(ticker))
    else:
        logging.debug("Data file for {} exists: {}".format(ticker, data_path))
        data = pd.read_csv(data_path, parse_dates=True, index_col='Datetime').sort_index()
    
    return data

# Return analysis text for the given ticker
def fetch_analysis(ticker):
    logger.info("Fetching analysis for ticker {}".format(ticker))
    analyis = ''
    path = "{}/{}/".format(ANALYSIS_PATH,ticker)
    for file in os.listdir(path):
        logger.debug("Analysis for ticker {} found at {}".format(ticker, path))
        data = open(path + file)
        analyis += data.read() + "\n"

    return analyis

# Return list of files that contain financials data for the specified ticker
def fetch_financials(ticker):
    logger.info("Fetching financials for ticker {}".format(ticker))
    path = "{}/{}".format(FINANCIALS_PATH,ticker)
    if not validate_path(path):
        logger.debug("Path {} not found".format(path))
        download_financials(ticker)
    financials = os.listdir(path)
    for i in range(0, len(financials)):
        logger.debug("Appending path {} to list of financials".format(path + "/" + financials[i]))
        financials[i] = path + "/" + financials[i]
    return financials
    

###############################
# Helpers and data validation #
###############################

# Confirm we get valid data back when downloading data for ticker
def validate_ticker(ticker):
    logger.info("Verifying that ticker {} is valid".format(ticker))

    # Confirm if data file already exists
    if os.path.isfile("{}/{}.csv".format(DAILY_DATA_PATH, ticker)) or os.path.isfile("{}/{}.csv".format(MINUTE_DATA_PATH, ticker)):
        logger.info("Data files exixts for ticker '{}' - ticker is valid".format(ticker))
        return True
    data = download_data(ticker, period='1d')
    if data.size == 0:
        logger.warning("INVALID TICKER - Size of data for ticker {} is 0".format(ticker))
        return False
    else:
        logger.info("Data for ticker {} successfully downloaded - ticker is valid".format(ticker))
        return True

# Validate specified path exists and create it if needed
def validate_path(path):
    logger.debug("Validating that path {} exists".format(path))
    if not (os.path.isdir(path)):
        logger.warning("Path {} does not exist. Creating path...".format(path))
        os.makedirs(path) 
        return 
    else:
        logger.debug("Path {} exists in the filesystem".format(path))
        return True
       
# Return tickers from watchlist - global by default, personal if chosen by user
def get_tickers_from_watchlist(watchlist_id):
    logger.info("Fetching tickers from watchlist with ID '{}'".format(watchlist_id))
    watchlist_path = "{}/{}.txt".format(WATCHLISTS_PATH, watchlist_id)

    try:
        with open(watchlist_path, 'r+') as watchlist:
            tickers = watchlist.read().splitlines()
            logger.debug("Found file for watchlist with ID {} with tickers: '{}'".format(watchlist_id, tickers))
        return tickers
    except FileNotFoundError as e:
        logger.warning("Watchlist with ID {} does not exist".format(watchlist_id))
        return []

def get_tickers_from_all_watchlists():
    logger.debug("Fetching tickers from all available watchlists (besides personal)")
    watchlists = get_watchlists()
    logger.debug("Watchlists: {}".format(watchlists))
    watchlists.remove('personal')
    watchlists_tickers = []
    for watchlist in watchlists:
        watchlists_tickers = watchlists_tickers + get_tickers_from_watchlist(watchlist)
    return watchlists_tickers


# Format string of tickers into list
def get_list_from_tickers(tickers):
    logger.debug("Processing valid and invalid tickers from {}".format(tickers))
    ticker_list = tickers.split(" ")
    invalid_tickers = []
    for ticker in ticker_list:
        if not validate_ticker(ticker): 
            invalid_tickers.append(ticker)
    # Remove invalid tickers from list before returning
    logger.debug("Identified invalid tickers from original list {} - removing...".format(invalid_tickers))
    for ticker in invalid_tickers:
        ticker_list.remove(ticker)
    return ticker_list, invalid_tickers

def get_watchlists():
    watchlists = [x.split('.')[0] for x in os.listdir(WATCHLISTS_PATH)]
    watchlists = [x for x in watchlists if not x.isdigit()]
    watchlists.append('personal')
    watchlists.sort()
    return watchlists

    
def update_watchlist(watchlist_id, tickers):
    logger.info("Updating watchlist '{}': {}".format(watchlist_id, tickers))
    with open("{}/{}.txt".format(WATCHLISTS_PATH, watchlist_id), 'w') as watchlist:
        watchlist.write("\n".join(sorted(tickers)))
        watchlist.close()

def create_watchlist(watchlist_id, tickers):
    logger.info("Creating watchlist with ID '{}' and tickers {}".format(watchlist_id, tickers))
    with open("{}/{}.txt".format(WATCHLISTS_PATH, watchlist_id), 'w') as watchlist:
        watchlist.write("\n".join(tickers))
        watchlist.close()

def delete_watchlist(watchlist_id):
    logger.info("Deleting watchlist '{}'...".format(watchlist_id))
    os.remove("{}/{}.txt".format(WATCHLISTS_PATH, watchlist_id))


    
# Return Dataframe with the latest OHLCV of requested ticker
def get_days_summary(ticker):
    logger.info("Fetching today's OHLCV summary for ticker {}".format(ticker))
    # Assumes data has been pulled recently. Mainly called when running or fetching reports. 
    data = fetch_daily_data(ticker)
    if len(data) > 0:
        summary = data[['Open', "Close", "High", "Low", "Volume"]].iloc[-1]
        logger.debug("Today's summary for ticker {}: {}".format(ticker, summary))
        return summary
    else:
        logger.warning("Could not retrieve the day's summary for ticker {}".format(ticker))
        return None
    
# Return next earnings date of the specified ticker, if available
def get_next_earnings_date(ticker):
    logger.info("Finding next earnings date for ticker {}".format(ticker))
    try:
        earnings_date = yf.Ticker(ticker).calendar['Earnings Date'][0]
        logger.info("Next earnings date for ticker {} is {}".format(ticker, earnings_date))
        return earnings_date
    except IndexError as e:
        logger.exception("Encountered IndexError when fetching next earnings date for ticker {}:\n{}".format(ticker, e))
        return "Earnings date unavailable"
    except KeyError as e:
        logger.exception("Encountered KeyError when fetching next earnings date for ticker {}:\n{}".format(ticker, e))
        return "Earnings Date unavailable"
    
# Return dataframe containing data from 'all_tickers.csv'
def get_all_tickers_data():
    return pd.read_csv("{}/all_tickers.csv".format(UTILS_PATH), index_col='Symbol')
    
# Return list of tickers available in the masterlist
def get_all_tickers():
    logger.debug("Fetching tickers from masterlist")
    
    try:
        all_tickers_data = get_all_tickers_data()
        return all_tickers_data.index.to_list()
    except FileNotFoundError as e:
        logger.exception("Encountered FileNotFoundError when attempting to load data from 'all_tickers.csv:\n{}".format(e))
        logger.debug("'all_tickers.csv' file does not exist")
        return []

def remove_from_all_tickers(ticker):
    
    data = get_all_tickers_data()
    data = data.drop(index=ticker)
    data.to_csv("{}/all_tickers.csv".format(UTILS_PATH))
    logger.debug("Removed ticker '{}' from all tickers".format(ticker))

# Validate that data file for specified ticker has data up to yesterday
def daily_data_up_to_date(data):
    if data.size == 0:
        logger.debug("Provided data has size 0 - failed to validate if up-to-date")
        return False
    else:
        yesterday = datetime.date.today() - timedelta(days=1)
        data_dates = [date.date() for date in data.index]
        latest_date = data_dates[-1]
        if yesterday in data_dates:
            logger.debug("Provided data is up-to-date")
            return True
        else:
            logger.info("Provided data is not up-to-date")
            return False

def validate_columns(data, columns):
    logger.debug("Validating that columns {} exist in data".format(columns))
    for column in columns: 
        if column not in data.columns:
            logger.debug("Column {} does not exist in data".format(column))
            return False
    logger.debug("Columns {} exist in data".format(columns))
    return True

#########
# Tests #
#########

def test():
    logger.info("Running test case")
    data = download_data("ABLLW")
    print(data)

if __name__ == "__main__":
    logger.info("stockdata.py initialized")
    test()
    pass