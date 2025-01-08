import os
import pandas as pd
import stockdata as sd
import numpy as np
import pandas_ta as ta
import mplfinance as mpf
import stockdata as sd
import csv
import logging
import json
from itertools import zip_longest
import random as rnd
import strategies

# Logging configuration
logger = logging.getLogger(__name__)
         
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
    pass

if __name__ == '__main__':
    test()
    pass
    
       
