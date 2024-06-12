import backtesting 
import stockdata as sd
import analysis as an
import pandas as pd
import strategies


# Backtest Classes

class Backtest (backtesting.Strategy):

    def init(self, signal_strategy):
        self.signals = signal_strategy.signals(self.data.df)

    def next(self):
        data_point = self.data.Close.shape[0] -1
        if self.signals.iloc[data_point]:
            self.buy()
        elif not self.signals.iloc[data_point]:
            self.position.close() 


def backtest():
    tickers = sd.get_tickers_from_watchlist('volatile')
    tickers += [x for x in sd.get_tickers_from_watchlist('meme-stocks') if x not in tickers]
    tickers += [x for x in sd.get_tickers_from_watchlist('crpyto') if x not in tickers]
    avg_return = 0
    num_ticker = 1
    highest_return = 0.0
    lowest_return  = 0.0

    strategy = strategies.get_strategy("ROC & OBV Strategy")()
    
    for ticker in tickers:
        print("**************** BACKTEST TICKER {}, {}/{} ****************".format(ticker, num_ticker, len(tickers)))
        data = sd.fetch_daily_data(ticker).tail(2358)
        if data.size == 0:
            print("Ticker data empty, skipping...")
       
        else: 
            bt = backtesting.Backtest(data, Backtest, cash= 10_000)
            stats = bt.run(signal_strategy = strategy)
            print(stats)
            return_value = stats.get('Return [%]')
            avg_return += return_value
            if return_value > highest_return:
                highest_return = return_value
            if return_value < lowest_return:
                lowest_return = return_value

            bt.plot()
        num_ticker += 1

    print("Average Return: {:2f}%".format(avg_return/len(tickers)))
    print("Highest Return: {:2f}%".format(highest_return))
    print("Lowest Return: {:2f}%".format(lowest_return))


if __name__ == '__main__':
    backtest()