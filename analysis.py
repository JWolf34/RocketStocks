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
import yfinance as yf


# Plotting Technical Indicators
def plot_volume(data, ticker):
    NUM_DAYS = 30
    save      = dict(fname='data/plots/{}/{}_VOLUME.png'.format(ticker, ticker),dpi=500,pad_inches=0.25)
    data = data.tail(NUM_DAYS)
    mpf.plot(data,volume=True,type='candle',ylabel='Close Price',figscale=1.6,figratio=(6,5),title='\n\n{} {}-Day Candlestick'.format(ticker,NUM_DAYS),
            style='tradingview', savefig=save)#,show_nontrading=True)   

def plot_rsi(data, ticker):
    def buy_sell_signals(rsi, close):
        buy_signals = []
        sell_signals = []
        UPPER_BOUND  = 70
        LOWER_BOUND  = 30 
        previous_rsi = 50
        for date, rsi_value in rsi.items():
            if rsi_value < LOWER_BOUND: #and previous_rsi > LOWER_BOUND:
                buy_signals.append(close[date])
            else:
                buy_signals.append(np.nan)
            if rsi_value > UPPER_BOUND: #and previous_rsi < UPPER_BOUND:
                sell_signals.append(close[date])
            else:
                sell_signals.append(np.nan)
            previous_rsi = rsi_value
        return buy_signals, sell_signals

    save      = dict(fname='data/plots/{}/{}_RSI.png'.format(ticker, ticker),dpi=500,pad_inches=0.25)
    data      = get_rsi(data)
    data      = data.tail(365)
    rsi       = data['RSI']
    close     = data['Close']
    hline_70  = [70] * data.shape[0]
    hline_30  = [30] * data.shape[0]
    buy_signal, sell_signal = buy_sell_signals(rsi, close)

    fb_green = dict(y1=buy_signal,y2=0,where=rsi<30,color="#93c47d",alpha=0.6,interpolate=True)
    fb_red   = dict(y1=sell_signal,y2=0,where=rsi>70,color="#e06666",alpha=0.6,interpolate=True)
    fb_red['panel'] = 0
    fb_green['panel'] = 0
    fb       = [fb_green,fb_red]

    apds = [
        #mpf.make_addplot(buy_signal, color='b', type='scatter', label="Buy Signal"),
        #mpf.make_addplot(sell_signal,color='orange',type='scatter', label="Sell Signal"),
        mpf.make_addplot(hline_70,type='line',panel=1,color='r',secondary_y=False,label='Overbought', linestyle="--"),
        mpf.make_addplot(hline_30,type='line',panel=1,color='g',secondary_y=False,label='Overbought', linestyle="--"),
        mpf.make_addplot(rsi,panel=1,color='orange',secondary_y=False,label='RSI', ylabel= 'Relative Strength \nIndex'),
        ]

    #s = mpf.make_mpf_style(base_mpf_style='classic',rc={'figure.facecolor':'lightgray'})

    mpf.plot(data,type='candle',ylabel='Close Price',addplot=apds,figscale=1.6,figratio=(6,5),title='\n\n{} Relative Strength Index'.format(ticker),
            style='tradingview',panel_ratios=(1,1),fill_between=fb, savefig=save)#,show_nontrading=True),fill_between=fb   

def analyze_rsi(data, ticker):
    analysis = ''
    signal = ''
    rsi = data['RSI'].values[-1]

    with open("data/analysis/{}/RSI.txt".format(ticker),'w') as rsi_analysis: 
        if rsi >= 70:
            signal = "SELL"
            analysis = "RSI: **{}** - The RSI value is above 70 ({:,.2f}) indicating the stock is currently overbought and could see a decline in price soon".format(signal, rsi)
            rsi_analysis.write(analysis)
        elif rsi <= 30:
            signal = "BUY"
            analysis = "RSI: **{}** - The RSI value is below 30 ({:,.2f}) indicating the stock is currently oversold and could see an incline in price soon".format(signal, rsi)
            rsi_analysis.write(analysis)
        else:
            signal = "NEUTRAL"
            analysis = "RSI: **{}** - The RSI value is between 30 and 70 ({:,.2f}), giving no indication as to where the price will move".format(signal, rsi)
            rsi_analysis.write(analysis)

def get_rsi(data):
    # Run Relative Stength Index (RSI) analysis
    data['RSI'] = ta.rsi(data['Close'])
    return data
        
def plot_macd(data, ticker):

    def buy_sell_signals(macd,macd_signal,close):
        buy_signals = []
        sell_signals = []
        previous_signal = -1
        previous_macd = 0
        for date, value in macd.items():
            if value > macd_signal[date] and previous_macd < previous_signal:
                buy_signals.append(close[date])
            else:
                buy_signals.append(np.nan)
            if value < macd_signal[date] and previous_macd > previous_signal:
                sell_signals.append(close[date])
            else:
                sell_signals.append(np.nan)
            previous_macd = value
            previous_signal =  macd_signal[date]
        return buy_signals, sell_signals

    save      = dict(fname='data/plots/{}/{}_MACD.png'.format(ticker, ticker),dpi=500,pad_inches=0.25)
    data      = get_macd(data)
    data      = data.tail(365)
    exp12     = data['Close'].ewm(span=12, adjust=False).mean()
    exp26     = data['Close'].ewm(span=26, adjust=False).mean()
    macd      = data['MACD']
    signal    = data['MACD_SIGNAL']
    histogram = data['MACD_HISTOGRAM']
    buy_signal, sell_signal = buy_sell_signals(macd, signal, data['Close'])


    fb_green = dict(y1=macd.values,y2=signal.values,where=signal<macd,color="#93c47d",alpha=0.6,interpolate=True)
    fb_red   = dict(y1=macd.values,y2=signal.values,where=signal>macd,color="#e06666",alpha=0.6,interpolate=True)
    fb_green['panel'] = 1
    fb_red['panel'] = 1
    fb       = [fb_green,fb_red]

    apds = [#mpf.make_addplot(exp12,color='lime', label='MACD'),
        #mpf.make_addplot(exp26,color='c',label="MACD_SIGNAL"),
        mpf.make_addplot(buy_signal, color='blue', type='scatter', label="Buy Signal"),
        mpf.make_addplot(sell_signal,color='orange',type='scatter', label="Sell Signal"),
        mpf.make_addplot(macd,panel=1,color='green',secondary_y=False,label='MACD', ylabel='Moving Average\nConvergence Divergence'),
        mpf.make_addplot(signal,panel=1,color='yellow',secondary_y=False,label="MACD_SIGNAL"),#,fill_between=fb),
        mpf.make_addplot(histogram,type='bar',width=0.7,panel=1,color='lightgray',secondary_y=True)
        ]

    #s = mpf.make_mpf_style(base_mpf_style='classic',rc={'figure.facecolor':'lightgray'})

    mpf.plot(data,type='candle',ylabel='Close Price',addplot=apds,figscale=1.6,figratio=(6,5),title='\n\n{} Moving Average\nConvergence Divergence'.format(ticker),
            style='tradingview',panel_ratios=(1,1),fill_between=fb, savefig=save)#,show_nontrading=True)   
    
def analyze_macd(data, ticker):
   signal = signal_macd(data)
   macd = data['MACD'].iloc[-1]
   macd_signal = data['MACD_SIGNAL'].iloc[-1]

   with open("data/analysis/{}/MACD.txt".format(ticker),'w') as sma_analysis: 
        if (signal == "BUY"):
            analysis = "SMA: **{}** - The SMA_10 line ({:,.2f}) is above the SMA_50 line ({:,.2f}) and has recently crossed over the SMA_50 line, indicating an upcoming uptrend".format(signal, sma_10, sma_50)
            sma_analysis.write(analysis)
        elif (signal == "WEAK BUY"):
            analysis = "SMA: **{}** - The SMA_10 line ({:,.2f}) is above the SMA_30 line ({:,.2f}) but below the SMA_50 line ({:,.2f}). Watch for a cross over ths SMA_50 line.".format(signal, sma_10, sma_30, sma_50)
            sma_analysis.write(analysis)
        elif (signal == "HOLD"):
            analysis = "SMA: **{}** - The SMA_10 line ({:,.2f}) is above the SMA_50 line ({:,.2f}), indicating a current uptrend".format(signal, sma_10, sma_50)
            sma_analysis.write(analysis)
        elif (signal == "WEAK SELL"):
            analysis = "SMA: **{}** - The SMA_10 line ({:,.2f}) is below the SMA_30 line ({:,.2f}) but above the SMA_50 line ({:,.2f}), indicating an upcoming downtrend".format(signal, sma_10, sma_30, sma_50)
            sma_analysis.write(analysis)
        if (signal == "SELL"):
            analysis = "SMA: **{}** - The SMA_10 line ({:,.2f}) is below the SMA_50 line ({:,.2f}), indicating an upcoming or continuing downtrend".format(signal, sma_10, sma_50)
            sma_analysis.write(analysis)


def signal_macd(data):
    signal = ''
    macd = data['MACD']
    macd_signal = data['MACD_SIGNAL']
    prev_macd = macd.tail(5).to_list()
    prev_macd_signal = macd_signal.tail(5).to_list()
    compare_0 = [0]*5
    cross_0 = recent_crossover(prev_macd, compare_0)
    cross_signal = recent_crossover(prev_macd, prev_macd_signal)


    # BUY SIGNAL - MACD is above MACD_SIGNAL and has recently crossed over 0
    if macd > macd_signal and cross_0 == 'UP':
        return "BUY"
    
    # WEAK BUY SIGNAL - MACD has recently crossed over signal
    elif cross_signal == 'UP':
        return "WEAK BUY"
    
    # WEAK SELL SIGNAL - MACD is below signal but above 0
    elif macd < macd_signal and macd > 0:
        return "WEAK SELL"

    # SELL SIGNAL - MACD is below signal and 0
    elif macd < 0 and macd < macd_signal:
        return "SELL"
    
    # HOLD SIGNAL - MACD is above signal and 0 but has not recently crossed the signal or 0
    elif macd > macd_signal and macd > 0 and cross_signal == None and cross_0 == None:
        return "HOLD"
    


def get_macd(data):
    # Run Moving Average Convergence/Divergence (MACD) analysis 
    macd = ta.macd(data['Close'])
    data['MACD'], data['MACD_HISTOGRAM'], data['MACD_SIGNAL'] = macd['MACD_12_26_9'], macd['MACDh_12_26_9'], macd['MACDs_12_26_9'] 
    return data

def plot_sma(data,ticker):
    def buy_sell_signals(sma_10, sma_50,close):
        buy_signals = [np.nan] * 365
        sell_signals = [np.nan] * 365
        
        for i in range(1,sma_10.size):
            sma_10_values = [sma_10.iloc[i-1], sma_10.iloc[i]]
            sma_50_values = [sma_50.iloc[i-1], sma_50.iloc[i]]
            cross = recent_crossover(sma_10_values, sma_50_values)
            if cross == 'UP':
                buy_signals[i] = close.iloc[i]*0.95
            elif cross == 'DOWN':
                sell_signals[i] = close.iloc[i]*1.05
            else:
                buy_signals[i] = np.nan
                sell_signals[i] = np.nan
        return buy_signals, sell_signals

    save      = dict(fname='data/plots/{}/{}_SMA.png'.format(ticker, ticker),dpi=500,pad_inches=0.25)
    data      = get_sma(data)
    data      = data.tail(365)
    sma_10    = data['SMA_10']
    sma_30    = data['SMA_30']
    sma_50    = data['SMA_50']
    buy_signal, sell_signal = buy_sell_signals(sma_10, sma_50, data['Close'])
    apds  = [
        mpf.make_addplot(buy_signal,color='g',type='scatter',markersize=100,marker='^',label='Buy Signal'),
        mpf.make_addplot(sell_signal,color='r',type='scatter',markersize=100,marker='v',label='Sell Signal'),
        mpf.make_addplot(sma_10, color='blue', label = 'SMA 10'),
        mpf.make_addplot(sma_30, color='purple', label = 'SMA 30'),
        mpf.make_addplot(sma_50, color='red', label = 'SMA 50')
    ]

    mpf.plot(data,type='candle',ylabel='Close Price',addplot=apds,figscale=1.6,figratio=(6,5),title='\n\n{} Simple Moving Average'.format(ticker),
            style='tradingview',savefig=save)
    
def analyze_sma(data, ticker):
    signal = signal_sma(data)
    sma_10 = data['SMA_10'].iloc[-1]
    sma_30 = data['SMA_30'].iloc[-1]
    sma_50 = data['SMA_50'].iloc[-1]

    with open("data/analysis/{}/SMA.txt".format(ticker),'w') as sma_analysis: 
        if (signal == "BUY"):
            analysis = "SMA: **{}** - The SMA_10 line ({:,.2f}) is above the SMA_50 line ({:,.2f}) and has recently crossed over the SMA_50 line, indicating an upcoming uptrend".format(signal, sma_10, sma_50)
            sma_analysis.write(analysis)
        elif (signal == "WEAK BUY"):
            analysis = "SMA: **{}** - The SMA_10 line ({:,.2f}) is above the SMA_30 line ({:,.2f}) but below the SMA_50 line ({:,.2f}). Watch for a cross over ths SMA_50 line.".format(signal, sma_10, sma_30, sma_50)
            sma_analysis.write(analysis)
        elif (signal == "HOLD"):
            analysis = "SMA: **{}** - The SMA_10 line ({:,.2f}) is above the SMA_50 line ({:,.2f}), indicating a current uptrend".format(signal, sma_10, sma_50)
            sma_analysis.write(analysis)
        elif (signal == "WEAK SELL"):
            analysis = "SMA: **{}** - The SMA_10 line ({:,.2f}) is below the SMA_30 line ({:,.2f}) but above the SMA_50 line ({:,.2f}), indicating an upcoming downtrend".format(signal, sma_10, sma_30, sma_50)
            sma_analysis.write(analysis)
        if (signal == "SELL"):
            analysis = "SMA: **{}** - The SMA_10 line ({:,.2f}) is below the SMA_50 line ({:,.2f}), indicating an upcoming or continuing downtrend".format(signal, sma_10, sma_50)
            sma_analysis.write(analysis)


def signal_sma(data):
    signal = ''
    sma_10 = data['SMA_10'].iloc[-1]
    sma_30 = data['SMA_30'].iloc[-1]
    sma_50 = data['SMA_50'].iloc[-1]

    prev_sma_10 = data['SMA_10'].tail(5).to_list()
    prev_sma_50 = data['SMA_50'].tail(5).to_list()

    recent_cross_50 = recent_crossover(prev_sma_10, prev_sma_50)
    
    # BUY SIGNAL - SMA_10 above SMA_50 and SMA_10 recently crossed over SMA_50
    if sma_10 > sma_50 and recent_cross_50 == 'UP':
        return 'BUY'
    
    # WEAK BUY SIGNAL - SMA_10 above SMA_30
    elif sma_10 > sma_30 and sma_10 < sma_50:
        return "WEAK BUY"
    
    # WEAK SELL SIGNAL - SMA_10 is below SMA_30 and above SMA_50
    elif sma_10 < sma_30 and sma_10 > sma_50:
        return "WEAK SELL"

    # SELL SIGNAL - SMA_10 is below SMA_50
    elif sma_10 < sma_50:
        return "SELL"
    
    # HOLD SIGNAL - SMA_10 is above SMA_50 and no recent crossover
    elif sma_10 > sma_50 and recent_cross_50 == None:
        return "HOLD"

def get_sma(data):
    sma_10 = ta.sma(data['Close'],length=10, append = True)
    sma_30 = ta.sma(data['Close'],length=30, append = True)
    sma_50 = ta.sma(data['Close'],length=50, append = True)

    data['SMA_10'], data['SMA_30'], data['SMA_50'] = sma_10, sma_30, sma_50
    return data

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

def generate_charts(data, ticker):
    
    if not (os.path.isdir("data/plots/" + ticker)):
            os.makedirs("data/plots/" + ticker)

    # Generate technical indicator charts

    plot_volume(data, ticker)
    plot_macd(data, ticker)
    plot_rsi(data, ticker)
    plot_sma(data,ticker)

    
# Running analysis on techincal indicators to generate buy/sell signals
def generate_analysis(data, ticker):
    if not (os.path.isdir("data/analysis/" + ticker)):
        os.makedirs("data/analysis/" + ticker)

    analyze_macd(data, ticker)
    analyze_rsi(data, ticker)
    analyze_sma(data,ticker)

def run_analysis(tickers=sd.get_tickers()):
    for ticker in tickers:
        sd.download_data_and_update_csv(ticker=ticker, period="max", interval="1d")
        #generate_indicators(ticker)
        data = sd.fetch_data(ticker)
        generate_charts(data, ticker)
        generate_analysis(data, ticker)


def test():
    # Testing mplfinance plot styles:

    styles = mpf.available_styles()
    data = sd.fetch_data('MSFT')
    data = get_macd(data)
    for style in styles:
        mpf.plot(data, style=style, volume=True, title=style)

if __name__ == '__main__':  
    #test()
    pass



       
