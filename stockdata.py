import yfinance as yf
from pandas_datareader import data as pdr
import pandas as pd
import os

yf.pdr_override()

def validate_ticker(ticker):
    stock = yf.Ticker(ticker)
    try:
        stock.info
        return True
    except Exception as e:
        return False

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
    
    with open("{}/watchlist.txt".format(get_watchlist_path(id)), 'r') as watchlist:
            tickers = watchlist.read().splitlines()
    return tickers
        
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

    path = "data/CSV/{}.csv".format(ticker)

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
    path = "data/plots/{}".format(ticker)
    charts = os.listdir("plots/" + ticker)
    for i in range(0, len(charts)):
        charts[i] = path + charts[i]
    return charts

# Return the latest data with technical indicators as a Pandas datafram
def fetch_data(ticker):
    data = pd.read_csv("data/{}.csv".format(ticker))
    return data

def fetch_analysis(ticker):
    analyis = ''
    path = "analysis/{}/".format(ticker)
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

                        

def test():
    ticker = yf.Ticker("NVDA")
    print(ticker.info)

if __name__ == "__main__":
    pass