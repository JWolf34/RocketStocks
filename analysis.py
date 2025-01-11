import os
import pandas as pd
import stockdata as sd
import pandas_ta as ta
import stockdata as sd
import config
import datetime
import logging



# Logging configuration
logger = logging.getLogger(__name__)

class indicators:

    class volume:
        
        def avg_vol_at_time(data:pd.DataFrame, period:int = 10, dt:datetime.datetime = None):
            data = sd.StockData.fetch_daily_price_history(ticker=ticker)
            return data['volume'].tail(period).mean()
        
        def rvol(data:pd.DataFrame, period:int = 10, curr_volume:float = None):
            avg_volume = data['volume'].tail(period).mean()
            if curr_volume is None:
                curr_volume = sd.Schwab().get_quote(ticker)['quote']['totalVolume']      
            return curr_volume / avg_volume    

        def rvol_at_time(data:pd.DataFrame, period:int = 10, dt:datetime.datetime = None):
            # Round down to nearest 5m, looking for last complete 5m candle
            if dt is None:
                dt = config.date_utils.dt_round_down(datetime.datetime.now() - datetime.timedelta(minutes=5))
            time = datetime.time(hour=dt.hour, minute=dt.minute)

            # Filter data to include candles in specified interval
            # Calculate average volume over period
            filtered_data = data[data['datetime'].apply(lambda x: x.time()) == time]
            avg_vol_at_time = filtered_data['volume'].tail(period).mean()

            # Get latest complete 5m candle
            ticker = data['ticker'].iloc[0]
            curr_data = sd.Schwab().get_5m_price_history(ticker=ticker,
                                                        start_datetime=dt, 
                                                        end_datetime=dt)
            print(curr_data.tail(5))
            curr_vol_at_time = curr_data[curr_data['datetime'].apply(lambda x: x.time()) == time]['volume'].iloc[0]

            return curr_vol_at_time / avg_vol_at_time
        
    


###########
# Signals #
###########

class signals():

    @staticmethod
    def rsi(close, UPPER_BOUND=70, LOWER_BOUND=30):
        logger.debug("Calculating RSI signal...")

        return ta.rsi(close) < LOWER_BOUND

    def macd(close):
        logger.debug("Calculating MACD signal...")
        macds = ta.macd(close)
        macd = macds[macds.columns[0]]
        macd_sig = macds[macds.columns[1]]

        return macd > macd_sig
        
    def sma(close, short, long):
        logger.debug("Calculating SMA signal...")

        return ta.sma(close, short) > ta.sma(close, long)

    def adx(close, highs, lows, TREND_UPPER=25, TREND_LOWER=20):
        logger.debug("Calculating ADX signal...")

        adxs = ta.adx(close=close, high=highs, low=lows)
        adx = adxs[adxs.columns[0]]
        dip = adxs[adxs.columns[1]]
        din = adxs[adxs.columns[2]]
        
        return (adx > TREND_UPPER) & (dip > din)

    def obv(close, volume):
        logger.debug("Calculating OBV signal...")

        obv = ta.obv(close=close, volume=volume)
        return  ta.increasing(ta.sma(obv, 10))

    def ad(high, low, close, open, volume):
        logger.debug("Calculating AD signal...")

        ad = ta.ad(high=high, low=low, close=close, volume=volume, open=open)
        return  ta.increasing(ta.sma(ad, 10))
        
    def zscore(close, BUY_THRESHOLD, SELL_THRESHOLD):
        
        zscore = ta.zscore(close) 
        signals = []
        for i in range(0, zscore.shape[0]):
            zscore_i = zscore.iloc[i]
            if i == 0:
                signals.append(0)
            elif zscore.iloc[i] < BUY_THRESHOLD:
                signals.append(1)
            elif zscore.iloc[i] > SELL_THRESHOLD:
                signals.append(0)
            else:
                signals.append(signals[i-1])

        return pd.Series(signals).set_axis(close.index) 

    def roc(close, length=10):

        return ta.roc(close=close, length=length) > 0

def test():
    #tickers = ['ZVSA', 'AKYA', 'VMAR', 'IINN', 'GLXG']
    tickers = ['NVDA', 'MSFT']
    for ticker in tickers:
        data = sd.StockData.fetch_5m_price_history(ticker=ticker)
        print(f"{ticker} Average Volume at Time: {indicators.volume.rvol_at_time(data=data)}")

if __name__ == '__main__':
    test()
    pass
    
       
