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
    mpf.plot(data,volume=True,type='candle',ylabel='Close Price',figscale=1.6,figratio=(6,5),title='\n\n{} {}-Day'.format(ticker,NUM_DAYS),
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

    mpf.plot(data,type='candle',ylabel='Close Price',addplot=apds,figscale=1.6,figratio=(6,5),title='\n\n{} RSI'.format(ticker),
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
        mpf.make_addplot(buy_signal, color='b', type='scatter', label="Buy Signal"),
        mpf.make_addplot(sell_signal,color='orange',type='scatter', label="Sell Signal"),
        mpf.make_addplot(macd,panel=1,color='green',secondary_y=False,label='MACD', ylabel='Moving Average\nConvergence Divergence'),
        mpf.make_addplot(signal,panel=1,color='yellow',secondary_y=False,label="MACD_SIGNAL"),#,fill_between=fb),
        mpf.make_addplot(histogram,type='bar',width=0.7,panel=1,alpha=1,color='dimgray', secondary_y=True)
        ]

    #s = mpf.make_mpf_style(base_mpf_style='classic',rc={'figure.facecolor':'lightgray'})

    mpf.plot(data,type='candle',ylabel='Close Price',addplot=apds,figscale=1.6,figratio=(6,5),title='\n\n{} MACD'.format(ticker),
            style='tradingview',panel_ratios=(1,1),fill_between=fb, savefig=save)#,show_nontrading=True)   
    
def analyze_macd(data, ticker):
    data = get_macd(data)
    signal = ''
    macd = data['MACD'].values[-1]
    macd_signal = data['MACD_SIGNAL'].values[-1]

    with open("data/analysis/{}/MACD.txt".format(ticker),'w') as macd_analysis: 
        if (macd > 0 and macd > macd_signal):
            signal = "BUY"
            analysis = "MACD: **{}** - The MACD value is above 0 ({:,.2f}) and greater than the MACD signal line ({:,.2f}), indicating an uptrend".format(signal, macd, macd_signal)
            macd_analysis.write(analysis)
        elif (macd > 0 and macd <= macd_signal):
            signal = "NEUTRAL"
            analysis = "MACD: **{}** - The MACD value is above 0 ({:,.2f}) but less than the MACD signal line ({:,.2f}). Wait until it crosses the signal line to buy, or consider selling .".format(signal, macd, macd_signal)
            macd_analysis.write(analysis)
        elif (macd <= 0):
            signal = "SELL"
            analysis = "MACD: **{}** - The MACD value is below 0 ({:,.2f}), indicating a downtrend".format(signal, macd)
            macd_analysis.write(analysis)

def get_macd(data):
    # Run Moving Average Convergence/Divergence (MACD) analysis 
    macd = ta.macd(data['Close'])
    data['MACD'], data['MACD_HISTOGRAM'], data['MACD_SIGNAL'] = macd['MACD_12_26_9'], macd['MACDh_12_26_9'], macd['MACDs_12_26_9'] 
    return data

def plot_sma(data,ticker):

    save      = dict(fname='data/plots/{}/{}_SMA.png'.format(ticker, ticker),dpi=500,pad_inches=0.25)
    data      = get_sma(data)
    sma_50    = data['SMA_50']
    sma_200   = data['SMA_200']

    apds  = [
        mpf.make_addplot(sma_50, color='blue', label = 'SMA 50'),
        mpf.make_addplot(sma_200, color='purple', label = 'SMA 200')
    ]

    mpf.plot(data,type='candle',ylabel='Close Price',addplot=apds,figscale=1.6,figratio=(6,5),title='\n\n{} SMA'.format(ticker),
            style='tradingview',savefig=save)

def get_sma(data):
    sma_50 = ta.sma(data['Close'], length=50)
    sma_200 = ta.sma(data['Close'], length=200)

    data['SMA_50'], data['SMA_200'] = sma_50, sma_200
    return data

def format_plot(plot):
    pass

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
    #analyse_sma(data,ticker)
    
def get_obv(data):
    # Run On-Balance Volume (OBV) analysis 
    data ['OBV'] = ta.obv(data['Close'], data['Volume'])
    return data

def get_adi(data):
    # Run Accumulation/Distribution Index (ADI) analysis 
    data['ADI'] = tavolume.ad(data['High'], data['Low'], data['Close'], data['Volume'])
    return data

def get_adx(data):
    # Run Average Directional Index (ADX) analysis  
    adx = tatrend.adx(data['High'], data['Low'], data['Close'])
    data['ADX'], data['ADX_DI+'], data['ADX_DI-'] = adx['ADX_14'], adx['DMP_14'], adx['DMN_14'] 
    return data

def get_aroon(data):
    # Run Aroon Oscillator (AROON) analysis 
    aroon = tatrend.aroon(data['High'], data ['Low'])
    data['AROON_DOWN'], data['AROON_INDICATOR'], data['AROON_UP'] = aroon['AROOND_14'], aroon['AROONOSC_14'], aroon['AROONU_14'] 
    return data

def get_stoch(data):
    # Run Stochastic Oscillator (STOCH) analysis 
    stoch = ta.stoch(close = data['Close'], high = data['High'], low = data['Low'])
    data['STOCH'], data['STOCH_SIGNAL']= stoch['STOCHk_14_3_3'], stoch['STOCHd_14_3_3'] 
    return data

def run_analysis(tickers=sd.get_tickers()):
    for ticker in tickers:
        sd.download_data_and_update_csv(ticker=ticker, period="1y", interval="1d")
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



       
