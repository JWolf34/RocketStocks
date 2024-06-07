import pandas_ta as ta
import analysis as an
import backtesting
import stockdata as sd

# Indicators
class SMA_10_50_Strategy(ta.Strategy):

    def __init__(self,
             name = "Simple Moving Average 10/50",
             short_name = "SMA_10_50",
             ta = [{"kind":"sma", "length":10}, {"kind":"sma", "length":50}],
             indicators = ['sma_10_50']
             ):
        
        self.name =  name
        self.short_name = short_name
        self.ta = ta
        self.indicators = indicators

    def next(self):
        pass
    
    def signals(self, data):
        return an.signal_sma(data['Close'], 10, 50)
    
# Strategies
class SMA_10_50_ADX_Strategy(ta.Strategy):

    def __init__(self,
             name = "SMA 10/50 & ADX",
             short_name = "SMA_10_50_ADX",
             ta = [{"kind":"sma", "length":10}, {"kind":"sma", "length":50}, {'kind':'adx'}],
             indicators = ['sma_10_50', 'adx']
             ):
        
        self.name =  name
        self.short_name = short_name
        self.ta = ta
        self.indicators = indicators

    def next(self):
        pass
    
    def signals(self, data):
        return an.signal_sma(data['Close'], 10, 50) & an.signal_adx(close=data['Close'], highs = data['High'], lows= data['Low'])

    

strategies = {
    "Simple Moving Average 10/50": SMA_10_50_Strategy,
    "SMA 10/50 & ADX": SMA_10_50_ADX_Strategy
}

def get_strategies():
    return strategies


def get_strategy(name):
    return strategies.get(name)
    
if __name__ =='__main__':
    pass
