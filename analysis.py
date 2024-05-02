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

# Paths for writing data
DAILY_DATA_PATH = "data/CSV/daily"
INTRADAY_DATA_PATH = "data/CSV/intraday"
FINANCIALS_PATH = "data/financials"
PLOTS_PATH = "data/plots"
ANALYSIS_PATH = "data/analysis"
ATTACHMENTS_PATH = "discord/attachments"

# Plotting Technical Indicators
def plot_volume(data, ticker):
    NUM_DAYS = 30
    save      = dict(fname='data/plots/{}/{}_VOLUME.png'.format(ticker, ticker),dpi=500,pad_inches=0.25)
    data = data.tail(NUM_DAYS)
    mpf.plot(data,volume=True,type='candle',ylabel='Close Price',figscale=1.6,figratio=(6,5),title='\n\n{} {}-Day Candlestick'.format(ticker,NUM_DAYS),
            style='tradingview', savefig=save)#,show_nontrading=True)   

def plot_rsi(data, ticker):
    UPPER_BOUND  = 80
    LOWER_BOUND  = 20
    def buy_sell_signals(rsi, close):
        buy_signals = []
        sell_signals = []
        
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
    hline_upper  = [UPPER_BOUND] * data.shape[0]
    hline_lower  = [UPPER_BOUND] * data.shape[0]
    buy_signal, sell_signal = buy_sell_signals(rsi, close)

    fb_green = dict(y1=buy_signal,y2=0,where=rsi<30,color="#93c47d",alpha=0.6,interpolate=True)
    fb_red   = dict(y1=sell_signal,y2=0,where=rsi>70,color="#e06666",alpha=0.6,interpolate=True)
    fb_red['panel'] = 0
    fb_green['panel'] = 0
    fb       = [fb_green,fb_red]

    apds = [
        #mpf.make_addplot(buy_signal, color='b', type='scatter', label="Buy Signal"),
        #mpf.make_addplot(sell_signal,color='orange',type='scatter', label="Sell Signal"),
        mpf.make_addplot(hline_upper,type='line',panel=1,color='r',secondary_y=False,label='Overbought', linestyle="--"),
        mpf.make_addplot(hline_lower,type='line',panel=1,color='g',secondary_y=False,label='Overbought', linestyle="--"),
        mpf.make_addplot(rsi,panel=1,color='orange',secondary_y=False,label='RSI', ylabel= 'Relative Strength \nIndex'),
        ]
    '''
    if not all_values_are_nan(buy_signal):
        apds.append(mpf.make_addplot(buy_signal, color='b', type='scatter', label="Buy Signal"))
    if not all_values_are_nan(sell_signal):
        apds.append(mpf.make_addplot(sell_signal,color='orange',type='scatter', label="Sell Signal"))
        '''

    
    mpf.plot(data,type='candle',ylabel='Close Price',addplot=apds,figscale=1.6,figratio=(6,5),title='\n\n{} Relative Strength Index'.format(ticker),
            style='tradingview',panel_ratios=(1,1),fill_between=fb, savefig=save)#,show_nontrading=True),fill_between=fb   

def analyze_rsi(data, ticker):
    UPPER_BOUND = 80
    LOWER_BOUND = 20
    signal = signal_rsi(data)
    rsi = data['RSI'].values[-1]

    with open("data/analysis/{}/RSI.txt".format(ticker),'w') as rsi_analysis: 
        if signal == "BUY":
            analysis = "RSI: **{}** - The RSI value ({:,.2f}) is below {}, indicating the stock is currently pversold and could see an increase in price soon".format(signal, rsi, LOWER_BOUND)
            rsi_analysis.write(analysis)
        elif signal == "SELL":
            analysis = "RSI: **{}** - The RSI value ({:,.2f}) is above {}, indicating the stock is currently overbought and could see an incline in price soon".format(signal, rsi, UPPER_BOUND)
            rsi_analysis.write(analysis)
        elif signal == "HOLD":
            analysis = "RSI: **{}** - The RSI value ({:,.2f}) is between {} and {} , giving no indication as to where the price will move".format(signal, rsi, LOWER_BOUND, UPPER_BOUND)
            rsi_analysis.write(analysis)

def signal_rsi(data):
    
    UPPER_BOUND = 80
    LOWER_BOUND = 20
    rsi = data['RSI'].iloc[-1]

    # BUY SIGNAL - RSI is below lower bound
    if rsi < LOWER_BOUND:
        return "BUY"
    
    # SELL SIGNAL - RSI is above upper bound
    if rsi > UPPER_BOUND:
        return "SELL"

    # HOLD SIGNAL - RSI is between upper bound and lower bound
    else:
        return "HOLD"


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

    apds = [
        mpf.make_addplot(macd,panel=1,color='green',secondary_y=False,label='MACD', ylabel='Moving Average\nConvergence Divergence'),
        mpf.make_addplot(signal,panel=1,color='yellow',secondary_y=False,label="MACD_SIGNAL"),#,fill_between=fb),
        mpf.make_addplot(histogram,type='bar',width=0.7,panel=1,color='lightgray',secondary_y=True)
        ]

    if not all_values_are_nan(buy_signal):
        apds.append(mpf.make_addplot(buy_signal, color='blue', type='scatter', label="Buy Signal"))
    if not all_values_are_nan(sell_signal):
        apds.append(mpf.make_addplot(sell_signal,color='orange',type='scatter', label="Sell Signal"))



    mpf.plot(data,type='candle',ylabel='Close Price',addplot=apds,figscale=1.6,figratio=(6,5),title='\n\n{} Moving Average\nConvergence Divergence'.format(ticker),
            style='tradingview',panel_ratios=(1,1),fill_between=fb, savefig=save)#,show_nontrading=True)   
    
def analyze_macd(data, ticker):
   signal = signal_macd(data)
   macd = data['MACD'].iloc[-1]
   macd_signal = data['MACD_SIGNAL'].iloc[-1]

   with open("data/analysis/{}/MACD.txt".format(ticker),'w') as macd_analysis: 
        if (signal == "BUY"):
            analysis = "MACD: **{}** - The MACD line ({:,.2f}) is above the MACD signal line ({:,.2f}) and has recently crossed over 0, indicating an upcoming or continuing uptrend".format(signal, macd, macd_signal)
            macd_analysis.write(analysis)
        elif (signal == "WEAK BUY"):
            analysis = "MACD: **{}** - The MACD line ({:,.2f}) has recently crossed above the MACD signal line ({:,.2f}), indicating that the price is rising".format(signal, macd, macd_signal)
            macd_analysis.write(analysis)
        elif (signal == "HOLD"):
            analysis = "MACD: **{}** - The MACD line ({:,.2f}) is above the MACD signal line ({:,.2f}), which can indicate an upcoming uptrend".format(signal, macd, macd_signal)
            macd_analysis.write(analysis)
        elif (signal == "WEAK SELL"):
            analysis = "MACD: **{}** - The MACD line ({:,.2f}) is below the MACD signal line ({:,.2f}) but above the 0, which can indicate an upcoming downtrend".format(signal, macd, macd_signal)
            macd_analysis.write(analysis)
        if (signal == "SELL"):
            analysis = "MACD: **{}** - The MACD line ({:,.2f}) is below the MACD signal line ({:,.2f}) an 0, indicating an upcoming or continuing downtrend".format(signal, macd, macd_signal)
            macd_analysis.write(analysis)


def signal_macd(data):
    signal = ''
    macd = data['MACD'].iloc[-1]
    macd_signal = data['MACD_SIGNAL'].iloc[-1]
    prev_macd = data['MACD'].tail(5).to_list()
    prev_macd_signal = data['MACD_SIGNAL'].tail(5).to_list()
    compare_0 = [0]*5
    cross_0 = recent_crossover(prev_macd, compare_0)
    cross_signal = recent_crossover(prev_macd, prev_macd_signal)


    # BUY SIGNAL - MACD is above MACD_SIGNAL and has recently crossed over 0
    if macd > macd_signal and cross_0 == 'UP':
        return "BUY"
    
    # WEAK BUY SIGNAL - MACD has recently crossed over signal
    elif cross_signal == 'UP' or (macd > macd_signal and macd < 0):
        return "WEAK BUY"
    
    # WEAK SELL SIGNAL - MACD is below signal but above 0
    elif macd < macd_signal and macd > 0:
        return "WEAK SELL"

    # SELL SIGNAL - MACD is below signal and 0
    elif macd < 0 and macd < macd_signal:
        return "SELL"
    
    # HOLD SIGNAL - MACD is above the signal but has not recently crossed the signal or 0
    elif macd > macd_signal and cross_signal == None and cross_0 == None:
        return "HOLD"
    
    else:
        print("How did we get here?")
        return 'HOLD'
    


def get_macd(data):
    # Run Moving Average Convergence/Divergence (MACD) analysis 
    macd = ta.macd(data['Close'])
    data['MACD'], data['MACD_HISTOGRAM'], data['MACD_SIGNAL'] = macd['MACD_12_26_9'], macd['MACDh_12_26_9'], macd['MACDs_12_26_9'] 
    return data

def plot_sma(data,ticker):
    def buy_sell_signals(sma_10, sma_50,close):
        buy_signals = [np.nan] * close.shape[0]
        sell_signals = [np.nan] * close.shape[0]
        
        for i in range(1,sma_10.size):
            sma_10_values = [sma_10.iloc[i-1], sma_10.iloc[i]]
            sma_50_values = [sma_50.iloc[i-1], sma_50.iloc[i]]
            cross = recent_crossover(sma_10_values, sma_50_values)
            if cross == 'UP':
                buy_signals[i] = close.iloc[i]*0.95
            elif cross == 'DOWN':
                sell_signals[i] = close.iloc[i]*1.05
            #else:
            #   buy_signals[i] = np.nan
            #    sell_signals[i] = np.nan
        return buy_signals, sell_signals

    save      = dict(fname='data/plots/{}/{}_SMA.png'.format(ticker, ticker),dpi=500,pad_inches=0.25)
    data      = get_sma(data)
    data      = data.tail(365)
    sma_10    = data['SMA_10']
    sma_30    = data['SMA_30']
    sma_50    = data['SMA_50']
    buy_signal, sell_signal = buy_sell_signals(sma_10, sma_50, data['Close'])

    
    apds  = [
        mpf.make_addplot(sma_10, color='blue', label = 'SMA 10'),
        mpf.make_addplot(sma_30, color='purple', label = 'SMA 30'),
        mpf.make_addplot(sma_50, color='red', label = 'SMA 50')
    ]



    if not all_values_are_nan(buy_signal):
        apds.append(mpf.make_addplot(buy_signal,color='g',type='scatter',markersize=100,marker='^',label='Buy Signal'))
    if not all_values_are_nan(sell_signal):
        apds.append(mpf.make_addplot(sell_signal,color='r',type='scatter',markersize=100,marker='v',label='Sell Signal'))


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

    else:
        print("How did we get here?")
        return "HOLD"

def get_sma(data):
    sma_10 = ta.sma(data['Close'],length=10, append = True)
    sma_30 = ta.sma(data['Close'],length=30, append = True)
    sma_50 = ta.sma(data['Close'],length=50, append = True)

    data['SMA_10'], data['SMA_30'], data['SMA_50'] = sma_10, sma_30, sma_50
    return data

def plot_obv(data, ticker):

    save      = dict(fname='data/plots/{}/{}_OBV.png'.format(ticker, ticker),dpi=500,pad_inches=0.25)
    data      = get_obv(data)
    data      = data.tail(60)
    close     = data['Close']
    obv       = data['OBV']
    #buy_signal, sell_signal = buy_sell_signals(sma_10, sma_50, data['Close'])
    close_slope = ta.slope(data['Close'], 60)#,as_angle=True, to_degress=True)
    obv_slope = ta.slope(obv, 60)#,as_angle=True, to_degree=True)
    #print("{} Close Slope: {}".format(ticker, close_slope))
    #print("{} OBV Slope: {}".format(ticker, obv_slope))
    apds  = [
        #mpf.make_addplot(close_slope, color= 'purple', label='CLOSE_SLOPE'),
        #mpf.make_addplot(obv_slope, color = 'orange',panel=1,label="OVB_SLOPE"),
        mpf.make_addplot(obv, color='lightblue',panel=1,label="OBV",ylabel='On-Balance Volume')
    ]

    mpf.plot(data,type='candle',ylabel='Close Price',addplot=apds,figscale=1.6,figratio=(6,5),title='\n\n{} On-Balance Volume'.format(ticker),
            style='tradingview',panel_ratios=(1,1),savefig=save)

def get_obv(data):
    data['OBV'] = ta.obv(data['Close'], data['Volume'])
    return data

def plot_adx(data,ticker):
    TREND_UPPER = 25
    TREND_LOWER = 20

    def buy_sell_signals(adx, dip, din, close):
        buy_signals = [np.nan] * adx.shape[0]
        sell_signals = [np.nan] * adx.shape[0]

        
        for i in range(1, adx.size):
            prev_trend_upper = [TREND_UPPER] * 2
            prev_trend_lower = [TREND_UPPER] * 2
            prev_adx = [adx.iloc[i-1], adx.iloc[i]]
            prev_dip = []
            prev_din = []

            if adx.iloc[i] > TREND_LOWER:
                if dip.iloc[i] > din.iloc[i]:
                    buy_signals[i] = close.iloc[i]
                else:
                    sell_signals[i] = close.iloc[i]

            '''
            for j in range(0,5):
                prev_dip.append(dip.iloc[i-j])
            
            for j in range(0,5):
                prev_din.append(din.iloc[i-j])
                
            if recent_crossover(prev_adx, prev_trend_upper) == 'UP':
                if dip.iloc[i] > din.iloc[i]:
                    buy_signals[i] = close.iloc[i]
                else:
                    sell_signals[i] = close.iloc[i]
            elif adx.iloc[i] > TREND_LOWER:
                if recent_crossover(prev_dip, prev_din) == 'UP':
                    buy_signals[i] = close.iloc[i]
                elif recent_crossover(prev_dip,prev_din) == 'DOWN':
                    sell_signals[i] = close.iloc[i]
                    '''
        
        return buy_signals, sell_signals


    
    save      = dict(fname='data/plots/{}/{}_ADX.png'.format(ticker, ticker),dpi=500,pad_inches=0.25)
    data      = get_adx(data)
    data      = data.tail(365)
    adx    = data['ADX']
    dip    = data['DI+']
    din    = data['DI-']
    hline_upper  = [TREND_UPPER] * data.shape[0]
    hline_lower  = [TREND_LOWER] * data.shape[0]

    buy_signal, sell_signal = buy_sell_signals(adx, dip, din, data['Close'])

    fb_green = dict(y1=buy_signal,y2=0,where=adx > TREND_LOWER,color="#93c47d",alpha=0.6,interpolate=True)
    fb_red   = dict(y1=sell_signal,y2=0,where=adx > TREND_LOWER,color="#e06666",alpha=0.6,interpolate=True)
    fb_red['panel'] = 0
    fb_green['panel'] = 0
    fb       = [fb_green,fb_red]
    
    apds  = [
        mpf.make_addplot(adx,type='line',color='purple',label='ADX',panel=1),
        mpf.make_addplot(dip,type='line',color='blue',label='DI+',panel=1),
        mpf.make_addplot(din,type='line',color='red',label ='DI-',panel=1),
        mpf.make_addplot(hline_upper,type='line',linestyle='--',color='g',panel=1),
        mpf.make_addplot(hline_lower,type='line',linestyle='--',color='r',panel=1)
    ]

    '''
    if not all_values_are_nan(buy_signal):
        apds.append(mpf.make_addplot(buy_signal,color='blue',type='scatter',label='Buy Signal'))
    if not all_values_are_nan(sell_signal):
        apds.append(mpf.make_addplot(sell_signal,color='orange',type='scatter',label='Sell Signal'))
        '''


    mpf.plot(data,type='candle',ylabel='Close Price',addplot=apds,figscale=1.6,figratio=(6,5),title='\n\n{} Average Direction Index'.format(ticker),
    panel_ratios=(1,1), fill_between=fb,style='tradingview',savefig=save)

def analyze_adx(data, ticker):
    TREND_UPPER = 25
    TREND_LOWER = 20
    signal = signal_adx(data)
    adx = data['ADX'].iloc[-1]
    dip = data['DI+'].iloc[-1]
    din = data['DI-'].iloc[-1]

    with open("data/analysis/{}/ADX.txt".format(ticker),'w') as adx_analysis: 
        #adx_analysis.write('ADX: **{}**'.format(signal))

        # BUY SIGNAL - ADX crosses above TREND_UPPER and DI+ > DI-
        if (signal == "BUY"):
            analysis = "ADX: **{}** - The ADX line ({:,.2f}) has recently crossed {} and DI+ ({:,.2f}) is above DI- ({:,.2f}), indicating the stock is strong uptrend".format(signal, adx, TREND_UPPER, dip, din)
            adx_analysis.write(analysis)

        # WEAK BUY SIGNAL - ADX between TREND_UPPER and TREND_LOWER and DI+ > DI-    
        elif (signal == "WEAK BUY"):
            analysis = "ADX: **{}** - The ADX line ({:,.2f}) is between {} and {} and DI+ ({:,.2f}) is above DI- ({:,.2f}), indicating the stock is in an uptrend. Look for ADX to cross {} for a strong buy signal".format(signal, adx, TREND_LOWER, TREND_UPPER, dip, din, TREND_UPPER)
            adx_analysis.write(analysis)

        # HOLD SIGNAL - ADX > TREND_LOWER and DI+ > DI-
        elif (signal == "HOLD"):
            analysis = "ADX: **{}** - The ADX line  ({:,.2f}) has stayed above {} and DI+ ({:,.2f}) is above DI- ({:,.2f}), indicating the stock is in an uptrend.".format(signal, adx, TREND_LOWER, dip, din)
            adx_analysis.write(analysis)

        # WEAK SELL SIGNAL - ADX between TREND_UPPER and TREND_LOWER and DI- > DI+ OR ADX < TREND_LOWER
        elif (signal == "WEAK SELL"):
            analysis = "ADX: **{}** - The ADX line ({:,.2f}) is between {} and {} and DI+ ({:,.2f}) is below DI- ({:,.2f}), indicating the stock is in a downtrend. Otherwise, ADX is below {} which does not indcate a current trend".format(signal, adx, TREND_LOWER, TREND_UPPER, dip, din, TREND_LOWER)
            adx_analysis.write(analysis)

        # SELL SIGNAL - ADX > TREND_UPPER and DI- > DI+
        if (signal == "SELL"):
            analysis = "ADX: **{}** - The ADX line ({:,.2f}) has recently crossed {} and DI+ ({:,.2f}) is below DI- ({:,.2f}), indicating the stock is strong downtrend".format(signal, adx, TREND_UPPER, dip, din)
            adx_analysis.write(analysis)
            

def signal_adx(data):

    TREND_UPPER = 25
    TREND_LOWER = 20
    adx = data['ADX']
    dip = data['DI+']
    din = data['DI-']

    prev_adx = adx.tail(5).to_list()
    prev_dip = dip.tail(5).to_list()
    prev_din = din.tail(5).to_list()
    prev_trend_upper = [TREND_UPPER] * 5
    prev_trend_lower = [TREND_LOWER] * 5

    # BUY SIGNAL - ADX crosses above TREND_UPPER and DI+ > DI-
    if recent_crossover(prev_adx, prev_trend_upper) == 'UP' and dip.iloc[-1] > din.iloc[-1]:
        return 'BUY'
    
    # WEAK BUY SIGNAL - ADX between TREND_UPPER and TREND_LOWER and DI+ > DI-
    elif recent_crossover(prev_adx, prev_trend_lower) == 'UP' and adx.iloc[-1] < TREND_UPPER and dip.iloc[-1] > din.iloc[-1]:
        return "WEAK BUY"
    
    # WEAK SELL SIGNAL - ADX between TREND_UPPER and TREND_LOWER and DI- > DI+ OR ADX < TREND_LOWER
    elif (adx.iloc[-1] < TREND_UPPER and adx.iloc[-1] > TREND_LOWER and din.iloc[-1] > dip.iloc[-1]) or adx.iloc[-1] < TREND_LOWER:
        return "WEAK SELL"

    # SELL SIGNAL - ADX > TREND_UPPER and DI- > DI+
    elif adx.iloc[-1] > TREND_UPPER and din.iloc[-1] > dip.iloc[-1]:
        return "SELL"
    
    # HOLD SIGNAL - ADX > TREND_LOWER and DI+ > DI-
    elif adx.iloc[-1] > TREND_LOWER and dip.iloc[-1] > din.iloc[-1]:
        return "HOLD"

def get_adx(data):
    adx = ta.adx(data['High'], data['Low'], data['Close'])
    data['ADX'], data["DI+"], data["DI-"] = adx['ADX_14'], adx['DMP_14'], adx['DMN_14']
    return data

def plot_strategy(data, ticker):

    def buy_sell_signals(data):

        BUY_THRESHOLD = 2.00
        SELL_THRESHOLD = 1.50

        buy_signals = [np.nan] * data['Close'].shape[0]
        sell_signals = [np.nan] * data['Close'].shape[0]

        position = False
        
        for i in range(5, data['Close'].size):
            score = signals_score(data.head(i))
            score = float(score)
            if score >= BUY_THRESHOLD and position == False:
                buy_signals[i] = data['Close'].iloc[i]*0.999999
                position = True
            elif score <= SELL_THRESHOLD and position == True:
                sell_signals[i] = data['Close'].iloc[i]*1.000001
                position = False
        return buy_signals, sell_signals

    save      = dict(fname='data/plots/{}/{}_STRATEGY.png'.format(ticker, ticker),dpi=500,pad_inches=0.25)
    data      = data.tail(365)
    buy_signal, sell_signal = buy_sell_signals(data)

    
    apds  = []



    if not all_values_are_nan(buy_signal):
        apds.append(mpf.make_addplot(buy_signal,color='g',type='scatter',markersize=50,marker='^',label='Buy Signal'))
    if not all_values_are_nan(sell_signal):
        apds.append(mpf.make_addplot(sell_signal,color='r',type='scatter',markersize=50,marker='v',label='Sell Signal'))


    mpf.plot(data,type='line',ylabel='Close Price',addplot=apds,figscale=1.6,figratio=(6,5),title='\n\n{} Strategy'.format(ticker),
            style='tradingview',savefig=save)
    pass


#Utilities
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

def signals_score(data):
    #data = sd.fetch_daily_data(ticker)
    score = 0.0
    scores_legend = {
        'BUY':1.0,
        'WEAK BUY':0.75,
        'HOLD':0.5,
        'WEAK SELL':0.0,
        'SELL':0.0
    }

    score += scores_legend.get(signal_rsi(data))    #(get_rsi(data)))
    score += scores_legend.get(signal_macd(data))   #(get_macd(data)))
    score += scores_legend.get(signal_sma(data))    #(get_sma(data)))
    score += scores_legend.get(signal_adx(data))    #(get_adx(data)))
    return score

def generate_charts(data, ticker):
    
    if not (os.path.isdir("data/plots/" + ticker)):
            os.makedirs("data/plots/" + ticker)

    # Generate technical indicator charts

    plot_volume(data,ticker)
    plot_macd(data,ticker)
    plot_rsi(data,ticker)
    plot_sma(data,ticker)
    plot_obv(data,ticker)
    plot_adx(data,ticker)
    plot_strategy(data, ticker)

    
# Running analysis on techincal indicators to generate buy/sell signals
def generate_analysis(data, ticker):
    if not (os.path.isdir("data/analysis/" + ticker)):
        os.makedirs("data/analysis/" + ticker)
    
    analyze_macd(data, ticker)
    analyze_rsi(data, ticker)
    analyze_sma(data,ticker)
    #analyze_obv(data,ticker)
    analyze_adx(data,ticker)

def get_masterlist_scores():
    return pd.read_csv('{}/daily_rankings.csv'.format(ATTACHMENTS_PATH))

def generate_masterlist_scores():
    print("Generating scores for all tickers in masterlist...")
    scores = {}

    tickers = sd.get_masterlist_tickers()
    num_ticker = 1
    for ticker in tickers:
        print("Evaluating {}... {}/{}".format(ticker, num_ticker, len(tickersss)))
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
        



def run_analysis(tickers=sd.get_tickers()):
    for ticker in tickers:
        #sd.download_data_and_update_csv(ticker=ticker, period="max", interval="1d", path=DAILY_DATA_PATH)
        #generate_indicators(ticker)
        data = sd.fetch_daily_data(ticker)
        generate_charts(data, ticker)
        generate_analysis(data, ticker)

def generate_indicators(data):
    data = get_rsi(data)
    data = get_sma(data)
    data = get_macd(data)
    data = get_obv(data)
    data = get_adx(data)
    return data



def test():
    #sd.download_masterlist_daily()
    generate_masterlist_scores()

if __name__ == '__main__':  
    #test()
    pass



       
