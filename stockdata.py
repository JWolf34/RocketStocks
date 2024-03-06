import yfinance as yf
from pandas_datareader import data as pdr
import pandas as pd
import os

yf.pdr_override()

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
        
def download_data(ticker, period, interval):

    # Download data for the given ticker
    data = yf.download(tickers=ticker, 
                       period=period, 
                       interval=interval, 
                       prepost = True,
                       auto_adjust = False,
                       repair = True)
    
    data.fillna(0)

    return data

def update_csv(data, ticker):


    path = "data/CSV"
    validate_path(path)
    path += "/{}.csv".format(ticker)

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

def download_data_and_update_csv(ticker, period, interval):
    data = download_data(ticker, period, interval)
    update_csv(data, ticker)
    print('Done!')

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

# Return all filepaths of all charts for a given ticker
def fetch_charts(ticker):
    charts_path = "data/plots/{}/".format(ticker)
    charts = os.listdir(charts_path)
    for i in range(0, len(charts)):
        charts[i] = charts_path + charts[i]
    return charts

# Return the latest data with technical indicators as a Pandas datafram
def fetch_data(ticker):
    data = pd.read_csv("data/CSV/{}.csv".format(ticker), parse_dates=True, index_col='Date')
    return data

def fetch_analysis(ticker):
    analyis = ''
    path = "data/analysis/{}/".format(ticker)
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

def get_stock_data(ticker):
    try:
        return pd.read_csv("data/CSV/{}.csv".format(ticker))
    except FileNotFoundError as e:
        print(e)
        return pd.DataFrame()
        

def get_days_summary(ticker):
    # Assumes data has been pulled recently. Mainly called when running or fetching reports. 
    data  = get_stock_data(ticker)
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
    

def download_financials(ticker):
    financials_path = "data/financials/{}".format(ticker)
    validate_path(financials_path)
    
    stock = yf.Ticker(ticker)
    stock.income_stmt.to_csv("{}/income_stmt.csv".format(financials_path))
    stock.quarterly_income_stmt.to_csv("{}/quarterly_income_stmt.csv".format(financials_path))
    stock.balance_sheet.to_csv("{}/balance_sheet.csv".format(financials_path))
    stock.quarterly_balance_sheet.to_csv("{}/quarterly_balance_sheet.csv".format(financials_path))
    stock.cashflow.to_csv("{}/cashflow.csv".format(financials_path))
    stock.quarterly_cashflow.to_csv("{}/quarterly_cashflow.csv".format(financials_path))
    #stock.get_income_stmt().to_csv("{}/income_stmt.csv".format(financials_path))
    #stock.get_earnings_dates(limit=8)
    
def fetch_financials(ticker):
    financials_path = "data/financials/{}".format(ticker)
    if not validate_path(financials_path):
        download_financials(ticker)
    financials = os.listdir(financials_path)
    for i in range(0, len(financials)):
        financials[i] = financials_path + "/" + financials[i]
    return financials
    

def test():
    # Testing retrieving income statement
    downloaded_data = download_data("MSFT", "1y", "1d")
    download_data_and_update_csv("MSFT", "1y", "1d")
    fetched_data = fetch_data("MSFT")

    print('Done!')



    #Testing retrieving financials with yfinance
    '''
    ticker = yf.Ticker("ANF")
    print(ticker.info)
    print(ticker.income_stmt)
    print(ticker.quarterly_income_stmt)
    print(ticker.balance_sheet)
    print(ticker.quarterly_balance_sheet)
    print(ticker.cashflow)
    print(ticker.quarterly_cashflow)
    print(ticker.get_income_stmt())
    print(ticker.get_earnings_dates(limit=8))
    '''

if __name__ == "__main__":
    #test()
    pass