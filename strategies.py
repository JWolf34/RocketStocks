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
             ):
        
        self.name =  name
        self.short_name = short_name
        self.ta = ta

    def next(self):
        pass
    
    def signals(self, data):
        return an.signal_sma(data['Close'], 10, 50)
    
# Strategies

    

strategies = {
    "Simple Moving Average 10/50": SMA_10_50_Strategy
}

def get_indicator_plots():
    return ""

def get_strategy_plots():
    return ""


def get_strategy(name):
    return strategies.get(name)
    
if __name__ =='__main__':
    pass
