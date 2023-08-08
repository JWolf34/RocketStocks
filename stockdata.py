import yfinance as yf
from pandas_datareader import data as pdr
import pandas as pd
from yahoo_fin import stock_info
import os


yf.pdr_override()


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

def get_tickers():
    with open("data/tickers.txt", 'r') as watchlist:
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
    
    return data

def update_csv(data, ticker):

    path = "data/" + ticker + ".csv"

    # If the CSV file already exists, read the existing data
    if os.path.exists(path):
        existing_data = pd.read_csv(path, index_col=0, parse_dates=True)
        
        # Append the new data to the existing data
        combined_data = pd.concat([existing_data, data])
    else:
        combined_data = data
    
    # Remove duplicate rows, if any
    combined_data = combined_data[~combined_data.index.duplicated(keep='first')]
    
    # Save the combined data to the CSV file
    combined_data.to_csv(path)

def download_data_and_update_csv(ticker, period):
    data = download_data(ticker, period)
    update_csv(data, ticker)
    print('Done!')

def combine_csv(ticker):

    # Load historical and current data into data frames
    historical_data = pd.read_csv("historical_data/" + ticker + ".csv", index_col=0, parse_dates=True)
    current_data = pd.read_csv("data/" + ticker + ".csv", index_col=0, parse_dates=True)
    
    # Append the historical data to the current data
    combined_data = pd.concat([historical_data, current_data])

     # Remove duplicate rows, if any
    combined_data = combined_data[~combined_data.index.duplicated(keep='first')]
    
    # Save the combined data to the CSV file
    combined_data.to_csv("data/" + ticker + ".csv")



if __name__ == "__main__":
    pass