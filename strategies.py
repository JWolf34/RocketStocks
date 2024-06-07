import pandas_ta as ta
import analysis as an
import backtesting
import stockdata as sd

# Indicators
class SMA_10_20_Strategy(backtesting.Strategy, ta.Strategy):

    def init(self):
        super().init()
        self.name = "Simple Moving Average 10/20"
        self.short_name = "SMA_10_20"
        self.ta = [{"kind":"sma", "length":10}, {"kind":"sma", "length":20}] 
        self.indicators = ['sma_10_20']

    def signals(self, data):
        return an.signal_sma(data['Close'], 10, 20) 

    def next(self):
        signal = self.signals(data).iloc[-1]
        if signal:
            self.buy()
        else:
            self.position.close()
            
class SMA_10_50_Strategy(backtesting.Strategy, ta.Strategy):

    def init(self):
        super().init()
        self.name = "Simple Moving Average 10/50"
        self.short_name = "SMA_10_50"
        self.ta = [{"kind":"sma", "length":10}, {"kind":"sma", "length":50}] 
        self.indicators = ['sma_10_50']

    def signals(self, data):
        return an.signal_sma(data['Close'], 10, 50) 

    def next(self):
        signal = self.signals(data).iloc[-1]
        if signal:
            self.buy()
        else:
            self.position.close()
    
# Strategies
class SMA_10_50_ADX_Strategy(backtesting.Strategy, ta.Strategy):

    def init(self):
        super().init()
        self.name = "SMA 10/50 & ADX"
        self.short_name = "SMA_10_50_ADX"
        self.ta = [{"kind":"sma", "length":10}, {"kind":"sma", "length":50}, {"kind":"adx"}] 
        self.indicators = ['sma_10_50', 'adx']

    def signals(self, data):
        return an.signal_sma(data['Close'], 10, 50) & an.signal_adx(close=data['Close'], highs = data['High'], lows= data['Low'])

    def next(self):
        signal = self.signals(data).iloc[-1]
        if signal:
            self.buy()
        else:
            self.position.close()
            
    
    

    

strategies = {
    "Simple Moving Average 10/50": SMA_10_50_Strategy,
    "SMA 10/50 & ADX": SMA_10_50_ADX_Strategy
}

def get_strategies():
    return strategies


def get_strategy(name):
    return strategies.get(name)
    
if __name__ =='__main__':
    ticker = 'TSM'
    sd.download_analyze_data(ticker)
    data = sd.fetch_daily_data(ticker)

    bt = backtesting.Backtest(data, SMA_10_50_ADX_Strategy, cash=10000)
    stats = bt.run()
    print(stats)
    bt.plot()
