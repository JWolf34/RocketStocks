import yfinance as yf




def get_data():
    QQQ = yf.Ticker("QQQ")
    TQQQ = yf.Ticker("TQQQ")
    SQQQ = yf.Ticker("SQQQ")

    print("--------- QQQ ---------")
    for line in QQQ.info:
        print(line)
    print()

    print("--------- TQQQ ---------")

    print("--------- SQQQ ---------")








if __name__ == "__main__":
    get_data()