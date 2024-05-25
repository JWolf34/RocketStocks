import stockdata as sd
import analysis as an
import backtesting as bt

# Backtest Classes

class SMA_10_50_Strategy():

    def init(self):
        strategy = an.get_strategy("Simple Moving Average 10/50")
        self.signals = strategy.signals
        self.buy_threshold = strategy.buy_threshold
        self.sell_threshold = strategy.sell_threshold

    def next(self):

        score = an.score_eval(an.signals_score(self.data, self.signals), self.buy_threshold, self.sell_threshold)

        if score == 'BUY':
            self.buy()
        elif score == 'SELL':
            self.position.close()

def backtest():
    backtest = bt.Backtest(sd.fetch_daily_data("GOOG"), SMA_10_50_Strategy, cash= 1_000)
    stats = backtest.run()
    print(stats)
    backtest.plot()

if __name__ == '__main__':
    backtest()