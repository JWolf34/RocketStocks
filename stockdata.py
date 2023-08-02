import yfinance as yf
from pandas_datareader import data as pdr
from yahoo_fin import stock_info
import time


yf.pdr_override()


def get_news(ticker):
        message = ''
        stock = yf.Ticker(ticker)
        articles = stock.news
        uuids_txt = open("tickers.txt", 'a')
        uuids_values = open("tickers.txt", 'r').read().splitlines()
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
'''
def get_data():
    QQQ = yf.Ticker("QQQ")
    TQQQ = yf.Ticker("TQQQ")
    SQQQ = yf.Ticker("SQQQ")

    print("--------- QQQ ---------")
    print(QQQ.info)
    print()

    print("--------- TQQQ ---------")
    print(TQQQ.info)
    print()

    print("--------- SQQQ ---------")
    print(SQQQ.info)
    print()

def get_news():
    NASDAQ = yf.Ticker("Qr7H24986tgf")
    for article in NASDAQ.news:
        for link in article:
            print(link)

def download_data():
    data = pdr.get_data_yahoo("<ticker>", start="YYYY-MM-DD", end="YYYY-MM-DD")
    data.to_csv("output.csv")
    
def get_live_data():
    while True:
        price = stock_info.get_live_price("TQQQ")
        print(price)
        time.sleep(5)




'''
if __name__ == "__main__":
    pass