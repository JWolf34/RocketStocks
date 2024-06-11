import pandas as pd
import pandas_ta as ta
import numpy as np
import analysis as an
import backtesting
import stockdata as sd
import sys, inspect

'''
Process for adding a new Strategy

- Create a new Strategy Class using boilerplate from an existing Class
- Add the Strategy to the `srtategies` dict
- Add chart for indicator(s) of new Strategy in Chart class in analysis.py
- Add signal method(s) to analysis.py

'''
# Candles

class Candles(ta.Strategy):

    def __init__(self):
        self.name = "Candles"
        self.short_name = "CANDLES"
        self.ta = [] 
        self.indicators = []

    def signals(self, data):
        try:
            return an.signal_sma(data['Close'], 10, 20) 
        except Exception as e:
            return pd.Series([False])  

    def override_chart_args(self, chart_args):
        chart_args['volume'] = True
        chart_args['tsignals'] = False
        return chart_args



# Indicators
class SMA_10_20_Strategy(ta.Strategy):

    def __init__(self):
        self.name = "Simple Moving Average 10/20"
        self.short_name = "SMA_10_20"
        self.ta = [{"kind":"sma", "length":10}, {"kind":"sma", "length":20}] 
        self.indicators = ['sma_10_20']

    def signals(self, data):
        try:
            return an.signal_sma(data['Close'], 10, 20) 
        except Exception as e:
            return pd.Series([False])  

    def override_chart_args(self, chart_args):
        return chart_args

class SMA_10_50_Strategy(ta.Strategy):

    def __init__(self):
        self.name = "Simple Moving Average 10/50"
        self.short_name = "SMA_10_50"
        self.ta = [{"kind":"sma", "length":10}, {"kind":"sma", "length":50}] 
        self.indicators = ['sma_10_50']

    def signals(self, data):
        try:
            return an.signal_sma(data['Close'], 10, 50) 
        except Exception as e:
            return pd.Series([False])

    def override_chart_args(self, chart_args):
        return chart_args

class SMA_50_200_Strategy(ta.Strategy):

    def __init__(self):
        self.name = "Simple Moving Average 50/200"
        self.short_name = "SMA_50_200"
        self.ta = [{"kind":"sma", "length":50}, {"kind":"sma", "length":200}] 
        self.indicators = ['sma_50_200']

    def signals(self, data):
        try:
            return an.signal_sma(data['Close'], 50, 200) 
        except Exception as e:
            return pd.Series([False])
    
    def override_chart_args(self, chart_args):
        return chart_args
        
class RSI_Strategy(ta.Strategy):

    def __init__(self):
        self.name = "Relative Strength Index"
        self.short_name = "RSI"
        self.ta = [{"kind":"rsi"}]
        self.indicators = ['rsi']

    def signals(self, data):
        try:
            return an.signal_rsi(close=data['Close'])
        except Exception as e:
            return pd.Series([False])

    def override_chart_args(self, chart_args):
        return chart_args
        
class OBV_Strategy(ta.Strategy):

    def __init__(self):
        self.name = "On-Balance Volume"
        self.short_name = "OBV"
        self.ta = [{"kind":"obv"}]
        self.indicators = ['obv']

    def signals(self, data):
        try:
            return an.signal_obv(close=data['Close'], volume=data['Volume'])
        except Exception as e:
            return pd.Series([False])

    def override_chart_args(chart_args):
        chart_args['volume'] = True
        return chart_args
        
class AD_Strategy(ta.Strategy):

    def __init__(self):
        self.name = "Accumulation/Distribution Index"
        self.short_name = "AD"
        self.ta = [{"kind":"ad"}]
        self.indicators = ['ad']

    def signals(self, data):
        try:
            return an.signal_ad(high=data['High'], low=data['Low'], close=data['Close'], open=data['Open'], volume=data['Volume'])
        except Exception as e:
            return pd.Series([False])

    def override_chart_args(self, chart_args):
        return chart_args
        
class AD_Strategy(ta.Strategy):

    def __init__(self):
        self.name = "Accumulation/Distribution Index"
        self.short_name = "AD"
        self.ta = [{"kind":"ad"}]
        self.indicators = ['ad']

    def signals(self, data):
        try:
            return an.signal_ad(high=data['High'], low=data['Low'], close=data['Close'], open=data['Open'], volume=data['Volume'])
        except Exception as e:
            return pd.Series([False])
        
    def override_chart_args(self, chart_args):
        return chart_args
        
class MACD_Strategy(ta.Strategy):

    def __init__(self):
        self.name = "Moving Average Convergence/Divergence"
        self.short_name = "MACD"
        self.ta = [{"kind":"macd"}]
        self.indicators = ['macd']

    def signals(self, data):
        try:
            return an.signal_macd(close=data['Close'])
        except Exception as e:
            return pd.Series([False])
    
    def override_chart_args(self, chart_args):
        return chart_args
        
class ADX_Strategy(ta.Strategy):

    def __init__(self):
        self.name = "Average Directional Index"
        self.short_name = "ADX"
        self.ta = [{"kind":"adx"}]
        self.indicators = ['adx']

    def signals(self, data):
        try:
            return an.signal_adx(close=data['Close'], highs=data['High'], lows=data['Low'])
        except Exception as e:
            return pd.Series([False])
    
    def override_chart_args(self, chart_args):
        return chart_args
    
class ZScore_Strategy(ta.Strategy):

    def __init__(self):
        self.name = "ZScore -3/-1"
        self.short_name = "zscore"
        self.ta = [{"kind":'zscore'}] 
        self.indicators = ['zscore']

    def signals(self, data):
        try:
            return an.signal_zscore(close=data['Close'], BUY_THRESHOLD= -3, SELL_THRESHOLD= -1)
        except Exception as e:
            return pd.Series([False])
        
    def override_chart_args(self, chart_args):
        return chart_args


# Strategies
class SMA_10_20_ADX_Strategy(ta.Strategy):

    def __init__(self):
        self.name = "SMA 10/20 & ADX Strategy"
        self.short_name = "SMA_10_20_ADX"
        self.ta = [{"kind":"sma", "length":10}, {"kind":"sma", "length":20}, {"kind":"adx"}] 
        self.indicators = ['sma_10_50', 'adx']
        self.long_position = False

    def signals(self, data):
        try:
            return an.signal_sma(data['Close'], 10, 20) & an.signal_adx(close=data['Close'], highs = data['High'], lows= data['Low'])
        except Exception as e:
            return pd.Series([False])
        
    def override_chart_args(self, chart_args):
        return chart_args
    
class ZScore_ADX_Strategy(ta.Strategy):

    def __init__(self):
        self.name = "ZScore 0/2 & ADX Strategy"
        self.short_name = "ZSCORE_ADX"
        self.ta = [[{"kind":'zscore'}], {"kind":"adx"}] 
        self.indicators = ['zscore', 'adx']
        self.long_position = False

    def signals(self, data):
        try:
            return an.signal_zscore(close=data['Close'], BUY_THRESHOLD=0, SELL_THRESHOLD=2) & an.signal_adx(close=data['Close'], highs = data['High'], lows= data['Low'])
        except Exception as e:
            return pd.Series([False])
        
    def override_chart_args(self, chart_args):
        return chart_args
    
class ZScore_ADX_SMA_10_50_Strategy(ta.Strategy):

    def __init__(self):
        self.name = "ZScore 0/2, ADX, & SMA 10/50 Strategy"
        self.short_name = "ZSCORE_ADX_SMA_10_50"
        self.ta = [[{"kind":'zscore'}], {"kind":"adx"}, {"kind":"sma", "length":10}, {"kind":"sma", "length":50}]
        self.indicators = ['zscore', 'adx', 'sma_10_50']
        self.long_position = False

    def signals(self, data):
        try:
            close = data['Close']
            position = False
            zscore_adx = an.signal_zscore(close=data['Close'], BUY_THRESHOLD=0, SELL_THRESHOLD=2) & an.signal_adx(close=data['Close'], highs = data['High'], lows= data['Low'])
            sma = an.signal_sma(close, 10, 50)
            signals = []
            for i in range(0, close.shape[0]):
                if i == 0:
                    signals.append(0)
                else:
                    if zscore_adx.iloc[i]:
                        signals.append(1)
                    elif not sma.iloc[i]:
                        signals.append(0)
                    else:
                        signals.append(signals[i-1])

            return pd.Series(signals).set_axis(close.index) 
            
        except Exception as e:
            return pd.Series([False])
        
    def override_chart_args(self, chart_args):
        return chart_args
    
""" class Backtest (backtesting.Strategy):

    def init(self):
        strategy = ZScore_ADX_SMA_10_50_Strategy()
        self.signals = strategy.signals(self.data.df)

    def next(self):
        data_point = self.data.Close.shape[0] -1
        if self.signals.iloc[data_point]:
            self.buy()
        elif not self.signals.iloc[data_point]:
            self.position.close() """
 



def get_strategies():
    strategies = {}
    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj):
            strategy = obj()
            strategies[strategy.name] = obj
    return strategies

def get_strategy(name):
    return get_strategies().get(name)

def get_combination_strategies():
    all_strategies = get_strategies()
    return [all_strategies.get(x) for x in all_strategies if "Strategy" in x]

def get_indicator_strategies():
    all_strategies = get_strategies()
    all_strategies.pop("Candles")
    return [all_strategies.get(x) for x in all_strategies if "Strategy" not in x]
    
if __name__ =='__main__':

    total_return = 0
    #tickers = sd.get_tickers_from_watchlist('volatile')
    tickers = ["ARM", 'CAVA', 'CELH', 'FMC', 'INTC', 'MRNA', 'NVDA', 'TLRY', 'TOST', 'TSM', 'YOLO']
    #strategy = ZScore_ADX_SMA_10_50_Strategy()
    num_tickers = 1
    for ticker in tickers:
        print("***** BACKTESTING {}, {}/{} *****".format(ticker, num_tickers, len(tickers)))
        sd.download_analyze_data(ticker)
        data = sd.fetch_daily_data(ticker)
        data = data.tail(an.recent_bars(data, tf='10y'))
        #signals = strategy.signals(data)

        bt = backtesting.Backtest(data, Backtest, cash=10000)
        stats = bt.run()
        strat_return = stats['Return [%]']
        print("Return over last {} days: {}".format(data.shape[0], strat_return))
        total_return += strat_return
        num_tickers += 1
        bt.plot() 
    print("***** END BACKTESTING *****")
    print("Average Return: {:.2f}%".format(total_return / len(tickers)))
