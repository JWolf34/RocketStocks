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
        
        score = an.score_eval(an.signals_score(self.data.df, self.signals), self.buy_threshold, self.sell_threshold)

        if score == 'BUY':
            self.buy()
        elif score == 'SELL':
            self.position.close()

class SMA_50_200_Strategy(bt.Strategy):

    def init(self):
        strategy = an.get_strategy("Simple Moving Average 50/200")
        self.signals = strategy.signals
        self.buy_threshold = strategy.buy_threshold
        self.sell_threshold = strategy.sell_threshold

    def next(self):
        
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
        
        score = an.score_eval(an.signals_score(self.data.df, self.signals), self.buy_threshold, self.sell_threshold)

        if score == 'BUY':
            self.buy()
        elif score == 'SELL':
            self.position.close()

class MACD_ADX_Strategy(bt.Strategy):

    def init(self):
        strategy = an.get_strategy("MACD & ADX")
        self.signals = strategy.signals
        self.buy_threshold = strategy.buy_threshold
        self.sell_threshold = strategy.sell_threshold

class ALL_SIGNALS_Strategy(bt.Strategy):

    def init(self):
        strategy = an.get_strategy("All Signals")
        self.signals = strategy.signals
        self.buy_threshold = strategy.buy_threshold
        self.sell_threshold = strategy.sell_threshold


    def next(self):
        
        score = an.score_eval(an.signals_score(self.data.df, self.signals), self.buy_threshold, self.sell_threshold)

        if score == 'BUY':
            self.buy()
        elif score == 'SELL':
            self.position.close()

class Volume_Boom_Strategy(bt.Strategy):

    def init(self):
        self.prev_volume = -1
        self.volume_threshold = 10000000
        self.volume_boom = False


    def next(self):
        
        curr_volume = self.data['Volume'][-1]
        close = self.data['Close'][-1]
        open = self.data['Open'][-1]
        
        

        if curr_volume > self.volume_threshold:
            volume_change = (curr_volume/self.prev_volume - 1) * 100
            if self.volume_boom and open > close:
                self.position.close()
                self.volume_boom = False
            if volume_change > 200.00 and not self.volume_boom:
                self.volume_boom = True
                self.buy()
            
        elif self.volume_boom:
            self.position.close()
            self.volume_boom = False

        self.prev_volume = curr_volume


def backtest():
    tickers = sd.get_tickers_from_watchlist('volatile')
    tickers +=  sd.get_tickers_from_watchlist('meme-stocks')
    tickers += sd.get_tickers_from_watchlist('crpyto')
    #tickers =  sd.get_tickers_from_watchlist('mag7')
    #tickers = sd.get_all_tickers()
    #tickers = sd.get_tickers_from_watchlist('restaurants')
    #tickers = ["FFIE"]
    avg_return = 0
    num_ticker = 1
    highest_return = 0.0
    lowest_return  = 0.0
    
    for ticker in tickers:
        print("**************** BACKTEST TICKER {}, {}/{} ****************".format(ticker, num_ticker, len(tickers)))
        data = sd.fetch_daily_data(ticker).tail(2358)
        if data.size == 0:
            print("Ticker data empty, skipping...")
       
        else: 
            backtest = bt.Backtest(data, Volume_Boom_Strategy, cash= 10_000)
            stats = backtest.run()
            print(stats)
            return_value = stats.get('Return [%]')
            avg_return += return_value
            if return_value > highest_return:
                highest_return = return_value
            if return_value < lowest_return:
                lowest_return = return_value

            #backtest.plot()
        num_ticker += 1

    print("Average Return: {:2f}%".format(avg_return/len(tickers)))
    print("Highest Return: {:2f}%".format(highest_return))
    print("Lowest Return: {:2f}%".format(lowest_return))


if __name__ == '__main__':
    backtest()