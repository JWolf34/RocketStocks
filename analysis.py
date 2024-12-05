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

#################################
# Generate analysis for reports #
#################################

# Top-level function to generate analysis and charts on select tickers
def run_analysis(tickers):
    logger.info("Running analysis on tickers {}".format(tickers))
    for ticker in tickers:
        data = sd.fetch_daily_data(ticker)

        # Verify that data is returned
        data_size = data.size
        data_up_to_date = sd.daily_data_up_to_date(data)
        ticker_in_all_tickers = True if ticker in sd.get_all_tickers() else False
        if data_size == 0 or not data_up_to_date or not ticker_in_all_tickers:
            logger.debug("Downloading data for ticker'{}':\n Data size: {} \n Data up-to-date: {}\n Ticker in all tickers: {}".format(ticker, data_size, data_up_to_date, ticker_in_all_tickers))
            if sd.validate_ticker(ticker):
                sd.download_analyze_data(ticker)
        data = sd.fetch_daily_data(ticker)
        generate_report_charts(data, ticker)
            
###########
# Signals #
###########

def get_signals():
    logger.debug("Building signals from JSON")
    with open("{}/signals.json".format(UTILS_PATH), 'r') as signals_json:
        signals = json.load(signals_json)
        return signals

def get_signal(signal):
    logger.debug("Fetching signals for '{}'".format(signal))
    return get_signals()[signal]

def signal_rsi(close, UPPER_BOUND=70, LOWER_BOUND=30):
    logger.debug("Calculating RSI signal...")

    return ta.rsi(close) < LOWER_BOUND

def signal_macd(close):
    logger.debug("Calculating MACD signal...")
    macds = ta.macd(close)
    macd = macds[macds.columns[0]]
    macd_sig = macds[macds.columns[1]]

    return macd > macd_sig
    
def signal_sma(close, short, long):
    logger.debug("Calculating SMA signal...")

    return ta.sma(close, short) > ta.sma(close, long)

def signal_adx(close, highs, lows, TREND_UPPER=25, TREND_LOWER=20):
    logger.debug("Calculating ADX signal...")

    adxs = ta.adx(close=close, high=highs, low=lows)
    adx = adxs[adxs.columns[0]]
    dip = adxs[adxs.columns[1]]
    din = adxs[adxs.columns[2]]
    
    return (adx > TREND_UPPER) & (dip > din)

def signal_obv(close, volume):
    logger.debug("Calculating OBV signal...")

    obv = ta.obv(close=close, volume=volume)
    return  ta.increasing(ta.sma(obv, 10))

def signal_ad(high, low, close, open, volume):
    logger.debug("Calculating AD signal...")

    ad = ta.ad(high=high, low=low, close=close, volume=volume, open=open)
    return  ta.increasing(ta.sma(ad, 10))
    
def signal_zscore(close, BUY_THRESHOLD, SELL_THRESHOLD):
    
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

def signal_roc(close, length=10):

    return ta.roc(close=close, length=length) > 0

###########
# Scoring #
###########

def get_ta_signals(signals_series):
    signals = pd.DataFrame()
    signals.ta.tsignals(signals_series, append=True)
    return signals

def signals_score(data, signals):
    logger.debug("Calculating score of data from signals {}".format(signals))
    #data = sd.fetch_daily_data(ticker)
    score = 0.0
    scores_legend = {
        'BUY':1.0,
        'HOLD':0.5,
        'SELL':0.0,
        'N/A':0.0
    }

    for signal in signals:
        logger.debug("Processing signal {}".format(signal))
        params = {'data':data} | get_signal(signal)['params']
        signal_function = globals()['signal_{}'.format(get_signal(signal)['signal_func'])]
        score += scores_legend.get(signal_function(**params))

    return score

def score_eval(score, buy_threshold, sell_threshold):
    logger.debug("Evaluating score ({}) against buy threshold ({}) and sell threshold({})".format(score, buy_threshold, sell_threshold))

    if score >= buy_threshold:
        return "BUY"
    elif score <= sell_threshold:
        return "SELL"
    else:
        return "HOLD"

def generate_strategy_scores(strategy):
    logger.info("Calculating scores for strategy '{}' on masterlist tickers".format(strategy.name))
    buys = []
    holds = []
    sells = []
    tickers = sd.get_all_tickers()
     
    num_tickers = 1
    for ticker in tickers:
        logger.debug("Evaluating score for ticker '{}', {}/{}".format(ticker, num_tickers, len(tickers)))
        data = sd.fetch_daily_data(ticker)
        try:
            signals = get_ta_signals(strategy.signals(data))
            if signals.TS_Entries.iloc[-1]:
                buys.append(ticker)
            elif signals.TS_Trends.iloc[-1]:
                holds.append(ticker)
            else:
                sells.append(ticker)
        except KeyError as e:
            logger.exception("Encountered KeyError generating '{}' signal for ticker '{}':\n{}".format(strategy.name, ticker, e))
        num_tickers += 1
    
    # Validate file path
    savefilepath_root ="{}/{}/{}".format(SCORING_PATH, "strategies", strategy.short_name)
    sd.validate_path(savefilepath_root)
    savefilepath = "{}/{}_scores.csv".format(savefilepath_root, strategy.short_name)

    # Prepare data to write to CSV
    signals = zip_longest(*[buys, holds, sells], fillvalue = '')
    
    # Write scores to CSV
    with open(savefilepath, 'w', newline='') as scores:
      wr = csv.writer(scores)
      wr.writerow(("BUY", "HOLD", "SELL"))
      wr.writerows(signals)
    
    logger.debug("Scores for strategy '{}' written to '{}'".format(strategy.name, savefilepath))
    
def get_strategy_scores(strategy):
    logger.debug("Fetching scores for strategy...{}")
    scores = pd.read_csv('{}/strategies/{}/{}_scores.csv'.format(SCORING_PATH, strategy.short_name, strategy.short_name))
    return scores

def get_strategy_score_filepath(strategy):
    return '{}/strategies/{}/{}_scores.csv'.format(SCORING_PATH, strategy.short_name, strategy.short_name)


#############
# Utilities #
#############

def all_values_are_nan(values):
    if np.isnan(values).all():
        return True
    else:
        return False

# Determine if there was a crossover in the values of indicator and signal Series
# over the last 5 data point
def recent_crossover(indicator, signal):

    for i in range (1, len(indicator)):
        curr_indicator = indicator[-i]
        prev_indicator = indicator[-i-1]
        curr_signal = signal[-i]
        prev_signal = signal[-i-1]

        if prev_indicator < prev_signal and curr_indicator > curr_signal:
            return 'UP'
        elif prev_indicator > prev_signal and curr_indicator < curr_signal:
            return'DOWN'

    return None

# Function to format Millions
def format_millions(x, pos):
    "The two args are the value and tick position"
    return "%1.1fM" % (x * 1e-6)

def ctitle(indicator_name, ticker="SPY", length=100):
    return f"{ticker}: {indicator_name} from {recent_startdate} to {recent_enddate} ({length})"

# # All Data: 0, Last Four Years: 0.25, Last Two Years: 0.5, This Year: 1, Last Half Year: 2, Last Quarter: 3
# yearly_divisor = 1
# recent = int(ta.RATE["TRADING_DAYS_PER_YEAR"] / yearly_divisor) if yearly_divisor > 0 else df.shape[0]
# print(recent)
def recent_bars(df, tf: str = "1y"):
    # All Data: 0, Last Four Years: 0.25, Last Two Years: 0.5, This Year: 1, Last Half Year: 2, Last Quarter: 4
    yearly_divisor = {"all": 0, "10y": 0.1, "5y": 0.2, "4y": 0.25, "3y": 1./3, "2y": 0.5, "1y": 1, "6mo": 2, "3mo": 4}
    yd = yearly_divisor[tf] if tf in yearly_divisor.keys() else 0
    return int(ta.RATE["TRADING_DAYS_PER_YEAR"] / yd) if yd > 0 else df.shape[0]

def get_plot_timeframes():
    return {"all": 0, "10y": 0.1, "5y": 0.2, "4y": 0.25, "3y": 1./3, "2y": 0.5, "1y": 1, "6mo": 2, "3mo": 4}

def ta_ylim(series: pd.Series, percent: float = 0.1):
    smin, smax = series.min(), series.max()
    if isinstance(percent, float) and 0 <= float(percent) <= 1:
        y_min = (1 + percent) * smin if smin < 0 else (1 - percent) * smin
        y_max = (1 - percent) * smax if smax < 0 else (1 + percent) * smax
        return (y_min, y_max)
    return (smin, smax)

def hline(size, value):
    hline = np.empty(size)
    hline.fill(value)
    return hline

def test():
    
    data = sd.fetch_daily_data("GOOG")
    for strategy_name in strategies.get_strategies():
        strategy = strategies.get_strategy(strategy_name)
        signals = pd.DataFrame()
        signals.ta.tsignals(strategy.signals(strategy, data), append=True)
        print(signals)
    """ data = sd.fetch_daily_data("AGBA")
    close = data['Close']
    high = data['High']
    low = data['Low'] 
    open = data['Open']
    volume = data['Volume']

    print(signal_zscore(close=close, BUY_THRESHOLD=-3, SELL_THRESHOLD=-1)) """

    # Create trends and see their returns
    #tsignals=True,
    # Example Trends or create your own. Trend must yield Booleans
    #long_trend=ta.sma(data['Close'],10) > ta.sma(data['Close'],20), # trend: sma(close,10) > sma(close,20) [Default Example]
#     long_trend=closedf > ta.ema(closedf,5), # trend: close > ema(close,5)
#     long_trend=ta.sma(closedf,10) > ta.ema(closedf,50), # trend: sma(close,10) > ema(close,50)
#     long_trend=ta.increasing(ta.ema(closedf), 10), # trend: increasing(ema, 10)
#     long_trend=macdh > 0, # trend: macd hist > 0
#     long_trend=macd_[macd_.columns[0]] > macd_[macd_.columns[-1]], # trend: macd > macd signal
#     long_trend=ta.increasing(ta.sma(ta.rsi(closedf), 10), 5, asint=False), # trend: rising sma(rsi, 10) for the previous 5 periods
#     long_trend=ta.squeeze(highdf, lowdf, closedf, lazybear=True, detailed=True).SQZ_PINC > 0,
#     long_trend=ta.amat(closedf, 50, 200, mamode="sma").iloc[:,0], # trend: amat(50, 200) long signal using sma
    #show_nontrading=False, # Intraday use if needed
    #verbose=True, # More detail
   

if __name__ == '__main__':
    test()
    pass
    
       
