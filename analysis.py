import os
import pandas as pd
import numpy as np
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
        
        def avg_vol_at_time(data:pd.DataFrame, periods:int = 10, dt:datetime.datetime = None):
            logger.debug(f"Calculating avg_vol_at_time {dt} over last {periods} periods")
            # Round down to nearest 5m, looking for last complete 5m candle
            if dt is None:
                dt = config.date_utils.dt_round_down(datetime.datetime.now() - datetime.timedelta(minutes=5))
            else:
                dt = config.date_utils.dt_round_down(dt)
            time = datetime.time(hour=dt.hour, minute=dt.minute)

            # Filter data to include candles in specified interval
            # Calculate average volume over periods
            filtered_data = data[data['datetime'].apply(lambda x: x.time()) == time]
            return filtered_data['volume'].tail(periods).mean(), time
        
        def rvol(data:pd.DataFrame, periods:int = 10, curr_volume:float = None):
            avg_volume = data['volume'].tail(periods).mean()
            if curr_volume is None:
                curr_volume = sd.Schwab().get_quote(ticker)['quote']['totalVolume']  

            logger.debug(f"Calculating rvol over last {periods} periods")
            rvol = curr_volume / avg_volume
            logger.debug(f"avg_volume ({avg_volume}) / curr_volume ({curr_volume}) = rvol ({rvol})")
            return rvol

        def rvol_at_time(data:pd.DataFrame, today_data:pd.DataFrame, periods:int = 10, dt:datetime.datetime = None):
            avg_vol_at_time, time = indicators.volume.avg_vol_at_time(data=data, periods=periods, dt=dt)

            # Get latest complete 5m candle
            logger.debug(f"Calculating rvol_at_time {dt} over last {periods} periods")
            try:
                curr_vol_at_time = today_data[today_data['datetime'].apply(lambda x: x.time()) == time]['volume'].iloc[0]
                rvol = curr_vol_at_time / avg_vol_at_time
                logger.debug(f"avg_vol_at_time ({avg_vol_at_time}) / curr_vol_at_time ({curr_vol_at_time}) = rvol ({rvol})")
                return rvol
            except IndexError as e:
                logger.debug(f"Could not process rvol_at_time - no volume data exists at time {time}. Latest row:\n{today_data.iloc[0]}")
                return np.nan
            
        
    


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
    
       
