import backtesting as bt
import stockdata as sd
import analysis as an
import pandas as pd


# Backtest Classes

class SMA_10_50_Strategy(bt.Strategy):

    def init(self):
        strategy = an.get_strategy("Simple Moving Average 10/50")
        self.signals = strategy.signals
        self.buy_threshold = strategy.buy_threshold
        self.sell_threshold = strategy.sell_threshold

    def next(self):
        #print(type(self.data))
        score = an.score_eval(an.signals_score(self.data.df, self.signals), self.buy_threshold, self.sell_threshold)

        if score == 'BUY':
            self.buy()
        elif score == 'SELL':
            self.position.close()

class SMA_ADX_Strategy(bt.Strategy):

    def init(self):
        strategy = an.get_strategy("SMA 10/50 & ADX")
        self.signals = strategy.signals
        self.buy_threshold = strategy.buy_threshold
        self.sell_threshold = strategy.sell_threshold

    def next(self):
        #print(type(self.data))
        score = an.score_eval(an.signals_score(self.data.df, self.signals), self.buy_threshold, self.sell_threshold)

        if score == 'BUY':
            self.buy()
        elif score == 'SELL':
            self.position.close()

class SMA_ADX_NDS_Strategy(bt.Strategy):

    def init(self):
        strategy = an.get_strategy("SMA 10/50 & ADX Next-Day Sell")
        self.signals = strategy.signals
        self.buy_threshold = strategy.buy_threshold
        self.sell_threshold = strategy.sell_threshold

    def next(self):
        #print(type(self.data))
        score = an.score_eval(an.signals_score(self.data.df, self.signals), self.buy_threshold, self.sell_threshold)

        if score == 'BUY':
            self.buy()
        elif score == 'SELL':
            self.position.close()

def backtester():
    tickers = sd.get_tickers_from_watchlist('volatile')
    avg_return = 0
    num_ticker = 1
    for ticker in tickers:
        print("**************** BACKTEST TICKER {}, {}/{} ****************".format(ticker, num_ticker, len(tickers)))
        backtest = bt.Backtest(sd.fetch_daily_data(ticker).tail(2358), SMA_ADX_NDS_Strategy, cash= 10_000)
        stats = backtest.run()
        avg_return += stats.get('Return [%]')
        print(stats)
        num_ticker += 1
        #backtest.plot()

    print("Average Return: {:2f}%".format(avg_return/len(tickers)))


if __name__ == '__main__':
    backtester()