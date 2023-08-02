import yfinance as yf




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
    NASDAQ = yf.Ticker("NASDAQ")
    for article in NASDAQ.news:
        print(article)








if __name__ == "__main__":
    get_news()