import os
import pandas as pd
import stockdata as sd
import numpy as np
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas_ta as ta
import pandas_ta.momentum as tamomentum
import pandas_ta.volume as tavolume
import pandas_ta.trend as tatrend
import mplfinance as mpf
import stockdata as sd
import yfinance as yfupdayte
import csv
import logging
import sys
import json

# Logging configuration
logger = logging.getLogger(__name__)

# Paths for writing data
DAILY_DATA_PATH = "data/CSV/daily"
INTRADAY_DATA_PATH = "data/CSV/intraday"
FINANCIALS_PATH = "data/financials"
PLOTS_PATH = "data/plots"
ANALYSIS_PATH = "data/analysis"
ATTACHMENTS_PATH = "discord/attachments"
MINUTE_DATA_PATH = "data/CSV/minute"
UTILS_PATH = "utils"

##############
# Strategies #
##############

# Strategy class for storing indicator data, signal calculators, and buy/sell thresholds
# Inherits from ta.Strategy
class Strategy(ta.Strategy):

    def __init__(self, name, ta, signals, buy_threshold, sell_threshold):
        super(Strategy, self).__init__(name, ta)
        self.signals=signals
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold

    def run_strategy(self, data):
        data.ta.strategy(self)\

# Return list of strategies
def get_strategies():
    strategies = []
    strategies_path = "{}/strategies.csv".format(UTILS_PATH)
    if os.path.exists(strategies_path):
        strategies_df = pd.read_csv(strategies_path)
        for index, row in strategies_df.iterrows():
            name = row['Name']
            ta = eval(row['TA'])
            signals = eval(row['Signals'])
            buy_threshold = float(row['Buy Threshold'])
            sell_threshold = float(row['Sell Threshold'])
            
            strategies.append(Strategy(name, ta, signals, buy_threshold, sell_threshold))

        return strategies
    else:
        print('No strategies available')
        return []
            

############
# Plotting #
############

def get_plots():
    with open("utils/plots.json", 'r') as plots_json:
        plots = json.load(plots_json)
        return plots

def get_plot(indicator_name):
    return get_plots()[indicator_name]

def get_plot_types():
    return ['line', 'candle', 'ohlc', 'renko', 'pnf']

def get_plot_styles():
    return mpf.available_styles()

def generate_basic_charts(data, ticker):
    
    if not (os.path.isdir("data/plots/" + ticker)):
            os.makedirs("data/plots/" + ticker)

    plot(ticker=ticker, data=data, indicator_name="Simple Moving Average 10/50")
    plot(ticker=ticker, data=data, indicator_name="Volume", title="{} 30-Day Candlestick".format(ticker), num_days=30, plot_type="candle", show_volume=True)
    plot(ticker=ticker, data=data, indicator_name="Relative Strength Index", plot_type="candle")
    plot(ticker=ticker, data=data, indicator_name="On-Balance Volume", plot_type="candle", num_days=90)
    plot(ticker=ticker, data=data, indicator_name="Accumulation/Distrtibution Index", plot_type="candle", num_days=90)
    plot(ticker=ticker, data=data, indicator_name="Moving Average Convergence/Divergence", plot_type="candle")
    plot(ticker=ticker, data=data, indicator_name="Average Directional Index", plot_type="candle")

def generate_all_charts(data, ticker):

    if not (os.path.isdir("data/plots/" + ticker)):
            os.makedirs("data/plots/" + ticker)

    # Generate technical indicator charts

    for indicator in get_plots():
        plot(ticker, data, indicator_name=indicator)

def plot(ticker, data, indicator_name, title = '', display_signals=True, num_days=365, plot_type = 'line', style='tradingview', show_volume= False, savefilepath_root = PLOTS_PATH):

    def buy_sell_signals(signals):
        buy_signals = [np.nan] * data['Close'].shape[0]
        sell_signals = [np.nan] * data['Close'].shape[0]
        position = False
        
        for i in range(5, data.size):
            score = signals_score(data.head(i), signals)
            score_evaluation = score_eval(score, 1.00, 0.00)
            if score_evaluation == 'BUY' and position == False:
                buy_signals[i-1] = (data['Close'].iloc[i-1]*0.99)
                position = True
            elif score_evaluation == 'SELL' and position == True:
                sell_signals[i-1] = (data['Close'].iloc[i-1]*1.01)
                position = False
            
        return buy_signals, sell_signals

    # Validate title
    if title == '':
        # Set default title
        title = '\n\n{} {}'.format(ticker, indicator_name)
    # Validate num_days
    if num_days > data.shape[0]:
        num_days =  data.shape[0]

    # Validate show_volume
    if indicator_name == 'Volume':
        show_volume= True

    chart = get_plot(indicator_name)
    indicator_abbr = chart['abbreviation']
    addplots = chart['addplots']
    signals =  chart['signals']

    savefilepath_root = '{}/{}'.format(savefilepath_root,ticker)
    sd.validate_path(savefilepath_root)
    savefilepath = "{}/{}.png".format(savefilepath_root, indicator_abbr)
    
    save      = dict(fname=savefilepath,dpi=500,pad_inches=0.25)
    data      = data.tail(num_days)


    apds = []

    for addplot in addplots:
        if addplot['kind'] == 'column':
            column = addplot['column']
            if column not in data.columns:
                return False, "Data column needed to generate this chart ({}) does not exist. There may not be enough data to populate this data column yet.".format(column)

            kwargs = addplot['params']
            apds.append(mpf.make_addplot(data[column], **kwargs))
        elif addplot['kind'] == "hline":
            hline = [addplot['value']] * data.shape[0]
            kwargs = addplot['params']
            apds.append(mpf.make_addplot(hline, **kwargs))
        elif addplot['kind'] == "vline":
            # TODO
            pass
        elif addplot['kind'] == "fill-between":
            # TODO
            pass
    
    if len(signals) == 0:
        display_signals = False

    if (display_signals):
        buy_signal, sell_signal = buy_sell_signals(signals)
        if not all_values_are_nan(buy_signal):
            apds.append(mpf.make_addplot(buy_signal,color='g',type='scatter',markersize=50,marker='^',label='Buy Signal'))
        if not all_values_are_nan(sell_signal):
            apds.append(mpf.make_addplot(sell_signal,color='r',type='scatter',markersize=50,marker='v',label='Sell Signal'))
    
    mpf.plot(data,type=plot_type,ylabel='Close Price',addplot=apds,figscale=1.6,figratio=(6,5),title=title,
            style=style, volume=show_volume, savefig=save)#,show_nontrading=True),fill_between=fb  
    return True, "{} for ticker {} over {} days".format(indicator_name, ticker, num_days)

def generate_charts(data, ticker):
    
    if not (os.path.isdir("data/plots/" + ticker)):
            os.makedirs("data/plots/" + ticker)

    # Generate technical indicator charts

    for indicator in get_plots():
        plot(ticker, data, indicator_name=indicator)
    #plot_volume(data,ticker)
    #plot_macd(data,ticker)
    #plot_rsi(data,ticker)
    #plot_sma(data,ticker)
    #plot_obv(data,ticker)
    #plot_adx(data,ticker)
    #plot_strategy(data, ticker)

#################################
# Generate analysis for reports #
#################################

# Top-level function to generate analysis and charts on select tickers
def run_analysis(tickers=sd.get_tickers()):
    logger.info("Running analysis on tickers {}".format(tickers))
    for ticker in tickers:
        data = sd.fetch_daily_data(ticker)

        # Verify that data is returned
        if data.size == 0:
            if sd.validate_ticker(ticker):
                sd.download_analyze_data(ticker)
        data = sd.fetch_daily_data(ticker)
        generate_basic_charts(data, ticker)
        generate_analysis(data, ticker)

# Running analysis on techincal indicators to generate buy/sell signals
def generate_analysis(data, ticker):
    if not (os.path.isdir("data/analysis/" + ticker)):
        os.makedirs("data/analysis/" + ticker)
    
    analyze_macd(data, ticker)
    analyze_rsi(data, ticker)
    analyze_sma(data,ticker)
    analyze_adx(data,ticker)
    #analyze_obv(data,ticker)
    

def analyze_rsi(data, ticker):

    
    signal_data = get_signal("rsi")['params']
    signal_columns = [signal_data.get(x) for x in signal_data if isinstance(signal_data.get(x), str)]
    if sd.validate_columns(data, signal_columns):
        signal = signal_rsi(**({'data':data} | signal_data))
        rsi_col = signal_data['rsi_col']
        UPPER_BOUND = signal_data['UPPER_BOUND']
        LOWER_BOUND = signal_data ['LOWER_BOUND']
        curr_rsi = data[rsi_col].values[-1]
    else:
        signal = "N/A"

    

    with open("data/analysis/{}/RSI.txt".format(ticker),'w') as rsi_analysis: 
        if signal == "BUY":
            analysis = "RSI: **{}** - The RSI value ({:,.2f}) is below {}, indicating the stock is currently pversold and could see an increase in price soon".format(signal, curr_rsi, LOWER_BOUND)
        elif signal == "SELL":
            analysis = "RSI: **{}** - The RSI value ({:,.2f}) is above {}, indicating the stock is currently overbought and could see an incline in price soon".format(signal, curr_rsi, UPPER_BOUND)
        elif signal == "HOLD":
            analysis = "RSI: **{}** - The RSI value ({:,.2f}) is between {} and {} , giving no indication as to where the price will move".format(signal, curr_rsi, LOWER_BOUND, UPPER_BOUND)
        elif signal == "N/A":
            analysis = "RSI: **N/A**"
        rsi_analysis.write(analysis)

def analyze_macd(data, ticker):

    signal_data = get_signal("macd")['params']
    signal_columns = [signal_data.get(x) for x in signal_data if isinstance(signal_data.get(x), str)]
    if sd.validate_columns(data, signal_columns):
        signal = signal_macd(**({'data':data} | signal_data))
        macd = data[signal_data['macd_col']].iloc[-1]
        macd_signal = data[signal_data['macd_signal_col']].iloc[-1]
    else:
        signal = "N/A"
    
    

    with open("data/analysis/{}/MACD.txt".format(ticker),'w') as macd_analysis: 
        if (signal == "BUY"):
            analysis = "MACD: **{}** - The MACD line ({:,.2f}) is above the MACD signal line ({:,.2f}) and has recently crossed over 0, indicating an upcoming or continuing uptrend".format(signal, macd, macd_signal)
        elif (signal == "HOLD"):
            analysis = "MACD: **{}** - The MACD line ({:,.2f}) is above the MACD signal line ({:,.2f}), which can indicate an upcoming uptrend".format(signal, macd, macd_signal)
        elif (signal == "SELL"):
            analysis = "MACD: **{}** - The MACD line ({:,.2f}) is below the MACD signal line ({:,.2f}) an 0, indicating an upcoming or continuing downtrend".format(signal, macd, macd_signal)
        elif signal == "N/A":
            analysis = "MACD: **N/A**"
        macd_analysis.write(analysis)

def analyze_sma(data, ticker):
    signal_data = get_signal("sma_10_50")['params']
    signal_columns = [signal_data.get(x) for x in signal_data if isinstance(signal_data.get(x), str)]
    if sd.validate_columns(data, signal_columns):
        signal = signal_sma(**({'data':data} | signal_data))
        sma_10 = data[signal_data['short']].iloc[-1]
        sma_50 = data[signal_data['long']].iloc[-1]
    else:
        signal = "N/A"
    
    with open("data/analysis/{}/SMA.txt".format(ticker),'w') as sma_analysis: 
        if (signal == "BUY"):
            analysis = "SMA: **{}** - The SMA_10 line ({:,.2f}) is above the SMA_50 line ({:,.2f}) and has recently crossed over the SMA_50 line, indicating an upcoming uptrend".format(signal, sma_10, sma_50)
        elif (signal == "HOLD"):
            analysis = "SMA: **{}** - The SMA_10 line ({:,.2f}) is above the SMA_50 line ({:,.2f}), indicating a current uptrend".format(signal, sma_10, sma_50)
        elif (signal == "SELL"):
            analysis = "SMA: **{}** - The SMA_10 line ({:,.2f}) is below the SMA_50 line ({:,.2f}), indicating an upcoming or continuing downtrend".format(signal, sma_10, sma_50)
        elif signal == "N/A":
            analysis = "SMA: **N/A**"
        sma_analysis.write(analysis)

def analyze_adx(data, ticker):
    signal_data = get_signal("adx")['params']
    signal_columns = [signal_data.get(x) for x in signal_data if isinstance(signal_data.get(x), str)]
    if sd.validate_columns(data, signal_columns):
        signal = signal_adx(**({'data':data} | signal_data))
        adx = data[signal_data['adx_col']].iloc[-1]
        dip = data[signal_data['dip_col']].iloc[-1]
        din = data[signal_data['din_col']].iloc[-1]
        TREND_UPPER = signal_data["TREND_UPPER"]
        TREND_LOWER = signal_data["TREND_LOWER"]
    else:
        signal = "N/A"

    with open("data/analysis/{}/ADX.txt".format(ticker),'w') as adx_analysis: 
        # BUY SIGNAL - ADX crosses above TREND_UPPER and DI+ > DI-
        if (signal == "BUY"):
            analysis = "ADX: **{}** - The ADX line ({:,.2f}) has recently crossed {} and DI+ ({:,.2f}) is above DI- ({:,.2f}), indicating the stock is strong uptrend".format(signal, adx, TREND_UPPER, dip, din)
        # HOLD SIGNAL - ADX > TREND_LOWER and DI+ > DI-
        elif (signal == "HOLD"):
            analysis = "ADX: **{}** - The ADX line  ({:,.2f}) has stayed above {} and DI+ ({:,.2f}) is above DI- ({:,.2f}), indicating the stock is in an uptrend.".format(signal, adx, TREND_LOWER, dip, din)
        # SELL SIGNAL - ADX > TREND_UPPER and DI- > DI+
        elif (signal == "SELL"):
            analysis = "ADX: **{}** - The ADX line ({:,.2f}) has recently crossed {} and DI+ ({:,.2f}) is below DI- ({:,.2f}), indicating the stock is strong downtrend".format(signal, adx, TREND_UPPER, dip, din)
        elif signal == "N/A":
            analysis = "ADX: **N/A**"
        adx_analysis.write(analysis)
            
###########
# Signals #
###########

def get_signals():
    with open("utils/signals.json", 'r') as signals_json:
        signals = json.load(signals_json)
        return signals

def get_signal(signal):
    return get_signals()[signal]

def signal_rsi(data, rsi_col, UPPER_BOUND, LOWER_BOUND):
    
    curr_rsi = data[rsi_col].iloc[-1]

    # BUY SIGNAL - RSI is below lower bound
    if curr_rsi < LOWER_BOUND:
        return "BUY"
    
    # SELL SIGNAL - RSI is above upper bound
    if curr_rsi > UPPER_BOUND:
        return "SELL"

    # HOLD SIGNAL - RSI is between upper bound and lower bound
    else:
        return "HOLD"

def signal_macd(data, macd_col, macd_signal_col):
    signal = ''
    macd = data[macd_col].iloc[-1]
    macd_signal = data[macd_signal_col].iloc[-1]
    prev_macd = data[macd_col].tail(5).to_list()
    prev_macd_signal = data[macd_signal_col].tail(5).to_list()
    #compare_0 = [0]*5
    #cross_0 = recent_crossover(prev_macd, compare_0)
    cross_signal = recent_crossover(prev_macd, prev_macd_signal)


    # BUY SIGNAL - MACD is above MACD_SIGNAL and has recently crossed over MACD_SIGNAL
    if macd > macd_signal and cross_signal == 'UP':
        return "BUY"

    # SELL SIGNAL - MACD is below signal 
    elif macd < macd_signal:
        return "SELL"
    
    # HOLD SIGNAL - MACD is above the signal but has not recently crossed the signal
    elif macd > macd_signal:
        return "HOLD"
    
    else:
        logger.debug("MACD values likely NaN for specified range. MACD: {}, MACD_SIGNAL: {}. Return 'N/A'".format(macd, macd_signal))
        return 'N/A'
    
def signal_sma(data, short, long):

    sma_short = data[short].iloc[-1]
    sma_long = data[long].iloc[-1]

    prev_short = data[short].tail(5).to_list()
    prev_long = data[long].tail(5).to_list()

    recent_cross = recent_crossover(prev_short, prev_long)
    
    # BUY SIGNAL - SHORT above SMA_50 and SHORT recently crossed over SMA_50
    if sma_short > sma_long and recent_cross == 'UP':
        return 'BUY'

    # SELL SIGNAL - SHORT is below SMA_50
    elif sma_short < sma_long:
        return "SELL"
    
    # HOLD SIGNAL - SHORT is above SMA_50 and no recent crossover
    elif sma_short > sma_long:
        return "HOLD"

    else:
        print("How did we get here?")
        return "HOLD"

def signal_adx(data, adx_col, dip_col, din_col, TREND_UPPER, TREND_LOWER):

    adx = data[adx_col]
    dip = data[dip_col]
    din = data[din_col]

    prev_adx = adx.tail(5).to_list()
    prev_dip = dip.tail(5).to_list()
    prev_din = din.tail(5).to_list()
    prev_trend_upper = [TREND_UPPER] * 5
    prev_trend_lower = [TREND_LOWER] * 5

    # BUY SIGNAL - ADX crosses above TREND_LOWER and DI+ > DI-
    if recent_crossover(prev_adx, prev_trend_lower) == 'UP' and dip.iloc[-1] > din.iloc[-1]:
        return 'BUY'

    # SELL SIGNAL - ADX > TREND_LOWER and DI- > DI+
    elif adx.iloc[-1] > TREND_LOWER and din.iloc[-1] > dip.iloc[-1]:
        return "SELL"
    
    # HOLD SIGNAL - ADX > TREND_LOWER and DI+ > DI-
    elif adx.iloc[-1] > TREND_LOWER and dip.iloc[-1] > din.iloc[-1]:
        return "HOLD"

    # HOLD SIGNAL - ADX < TREND_LOWER
    elif adx.iloc[-1] < TREND_LOWER:
        return "HOLD"
    
    else:
        logger.debug("ADX values likely NaN for specified range. ADX: {}, DIP: {}, DINL: {}. Return 'N/A'".format(adx.iloc[-1], dip.iloc[-1], din.iloc[-1]))
        return "N/A"

###########
# Scoring #
###########

def signals_score(data, signals):
    #data = sd.fetch_daily_data(ticker)
    score = 0.0
    scores_legend = {
        'BUY':1.0,
        'HOLD':0.5,
        'SELL':0.0,
        'N/A':0.0
    }

    for signal in signals:
        params = {'data':data} | get_signal(signal)['params']
        signal_function = globals()['signal_{}'.format(get_signal(signal)['signal_func'])]
        score += scores_legend.get(signal_function(**params))

    return score

def score_eval(score, buy_threshold, sell_threshold):
    
    if score >= buy_threshold:
        return "BUY"
    elif score <= sell_threshold:
        return "SELL"
    else:
        return "HOLD"

def generate_masterlist_scores():
    print("Generating scores for all tickers in masterlist...")
    scores = {}

    tickers = sd.get_masterlist_tickers()
    num_ticker = 1
    for ticker in tickers:
        print("Evaluating {}... {}/{}".format(ticker, num_ticker, len(tickers)))
        try:
            score = signals_score(ticker)
            if score in scores.keys():
                score_tickers = scores.get(score)
                score_tickers.append(ticker)
                scores[score] = score_tickers
            else:
                scores[score] = [ticker]
        except Exception as e:
            print(e)
            print("Skipping {}".format(ticker))
        num_ticker += 1
            
    scores = dict(sorted(scores.items()))
    scores_df = pd.DataFrame.from_dict(scores, orient='index').T
    scores_df.to_csv('{}/daily_rankings.csv'.format(ATTACHMENTS_PATH))
    
    '''
    with open('{}/daily_rankings.csv'.format(ATTACHMENTS_PATH), 'w+') as f:  
        w = csv.DictWriter(f, sorted(scores.keys(), reverse=True))
        w.writeheader()
        w.writerow(scores)
        '''

def get_masterlist_scores():
    return pd.read_csv('{}/daily_rankings.csv'.format(ATTACHMENTS_PATH))


#############
# Utilities #
#############

def all_values_are_nan(values):
    if np.isnan(values).all():
        return True
    else:
        return False

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

def test():
    ticker = 'A'
    data = sd.fetch_daily_data(ticker)
    plot_info = get_plots("Simple Moving Average 10/50")
    
    plot('SPY', data, 'Simple Moving Average 50/200', True, num_days=760)



if __name__ == '__main__':
    #test()
    pass
    
       
