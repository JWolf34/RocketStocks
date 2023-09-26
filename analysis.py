import os
import pandas as pd
import stockdata as sd
import numpy as np
import datetime
import matplotlib.pyplot as plt
import pandas_ta as ta
import pandas_ta.momentum as tamomentum
import pandas_ta.volume as tavolume
import pandas_ta.trend as tatrend

# Plotting Technical Indicators

def plot_rsi(data, ticker):

    # Create two charts on the same figure.
    ax1 = plt.subplot2grid((10,1), (0,0), rowspan = 4, colspan = 1)
    ax2 = plt.subplot2grid((10,1), (5,0), rowspan = 4, colspan = 1, sharex=ax1)

    # First chart:
    # Plot the closing price on the first chart
    ax1.plot(data['Close'], linewidth=2)
    ax1.set_title(ticker.upper() + ' Close Price')

    # Second chart
    # Plot the RSI
    ax2.set_title('Relative Strength Index (' + ticker + ')')
    ax2.plot(data['RSI'], color='orange', linewidth=1)
    # Add two horizontal lines, signalling the buy and sell ranges.
    # Oversold
    ax2.axhline(30, linestyle='--', linewidth=1.5, color='green')
    # Overbought
    ax2.axhline(70, linestyle='--', linewidth=1.5, color='red')

    plt.savefig("plots/" + ticker + "/" + ticker + "_RSI.png", dpi=1000)
    plt.close()

def plot_obv(data, ticker):
    # Create two charts on the same figure.
        ax1 = plt.subplot2grid((10,1), (0,0), rowspan = 4, colspan = 1)
        ax2 = plt.subplot2grid((10,1), (5,0), rowspan = 4, colspan = 1, sharex=ax1)

        period = 60

        obv_curr = data['OBV'].values[-1]
        obv_old = data['OBV'].values[-period]

        close_curr = data['Close'].values[-1]
        close_old = data['Close'].values[-period]

        obv_slope = (obv_curr - obv_old) / period
        close_slope = (close_curr - close_old) / period


        xstart = len(data['Close']) - period

        # First chart:
        # Plot the closing price on the first chart
        ax1.plot(data['Close'], linewidth=2)
        ax1.set_title(ticker.upper() + ' Close Price')

        # Create line indicating start of analysis period
        ax1.axvline(x = xstart, linestyle = '--', color = 'red')
        ax1.axline(xy1 = (xstart, close_old), slope  = close_slope, color = 'red')

        # Second chart
        # Plot the OBV
        ax2.set_title('On-Balance Volume (' + ticker + ')')
        ax2.plot(data['OBV'], color='purple', linewidth=1)

        # Create line indicating start of analysis period
        ax2.axvline(x = xstart, linestyle = '--', color = 'red')
        ax2.axline(xy1 = (xstart, obv_old), slope  = obv_slope, color = 'red')

        plt.savefig("plots/" + ticker + "/" + ticker + "_OBV.png", dpi=1000)
        plt.close()

def plot_adi(data, ticker):
    # Create two charts on the same figure.
        ax1 = plt.subplot2grid((10,1), (0,0), rowspan = 4, colspan = 1)
        ax2 = plt.subplot2grid((10,1), (5,0), rowspan = 4, colspan = 1, sharex=ax1)

        period = 60

        adi_curr = data['ADI'].values[-1]
        adi_old = data['ADI'].values[-period]

        close_curr = data['Close'].values[-1]
        close_old = data['Close'].values[-period]

        adi_slope = (adi_curr - adi_old) / period
        close_slope = (close_curr - close_old) / period

        xstart = len(data['Close']) - period

        # First chart:
        # Plot the closing price on the first chart
        ax1.plot(data['Close'], linewidth=2)
        ax1.set_title(ticker.upper() + ' Close Price')

        # Create line indicating start of analysis period
        ax1.axvline(x = xstart, linestyle = '--', color = 'red')
        ax1.axline(xy1 = (xstart, close_old), slope  = close_slope, color = 'red')

        # Second chart
        # Plot the OBV
        ax2.set_title('Accumulation/Distribution Index (' + ticker + ')')
        ax2.plot(data['ADI'], color='green', linewidth=1)

        # Create line indicating start of analysis period
        ax2.axvline(x = xstart, linestyle = '--', color = 'red')
        ax2.axline(xy1 = (xstart, adi_old), slope  = adi_slope, color = 'red')

        plt.savefig("plots/" + ticker + "/" + ticker + "_ADI.png", dpi=1000)
        plt.close()

def plot_adx(data, ticker):
    # Create two charts on the same figure.
        ax1 = plt.subplot2grid((10,1), (0,0), rowspan = 4, colspan = 1)
        ax2 = plt.subplot2grid((10,1), (5,0), rowspan = 4, colspan = 1, sharex=ax1)

        # First chart:
        # Plot the closing price on the first chart
        ax1.plot(data['Close'], linewidth=2)
        ax1.set_title(ticker.upper() + ' Close Price')

        # Second chart
        # Plot the OBV
        ax2.set_title('Average Directional Index (' + ticker + ')')
        ax2.plot(data['ADX'], color='purple', linewidth=1, label = 'ADX')
        ax2.plot(data['ADX_DI+'], color='blue', linewidth=1, label = 'DI+')
        ax2.plot(data['ADX_DI-'], color = 'red', linewidth=1, label = 'DI-')
        ax2.legend()

        # Add two horizontal lines, signalling the uptrend and downrtend ranges.
        # Uptrend
        ax2.axhline(40, linestyle='--', linewidth=1.5, color='green')
        # Downtrend
        ax2.axhline(20, linestyle='--', linewidth=1.5, color='orange')

        plt.savefig("plots/" + ticker + "/" + ticker + "_ADX.png", dpi=1000)
        plt.close()

def plot_aroon(data, ticker):
    # Create two charts on the same figure.
        ax1 = plt.subplot2grid((10,1), (0,0), rowspan = 4, colspan = 1)
        ax2 = plt.subplot2grid((10,1), (5,0), rowspan = 4, colspan = 1, sharex=ax1)

        # First chart:
        # Plot the closing price on the first chart
        ax1.plot(data['Close'], linewidth=2)
        ax1.set_title(ticker.upper() + ' Close Price')

        # Second chart
        # Plot the OBV
        ax2.set_title('Aroon Oscillator (' + ticker + ')')
        ax2.plot(data['AROON_DOWN'], color='green', linewidth=1, label = 'AROON_DOWN')
        #ax2.plot(data['AROON_INDICATOR'], color='blue', linewidth=1, label = 'AROON_INDICATOR')
        ax2.plot(data['AROON_UP'], color = 'orange', linewidth=1, label = 'AROON_UP')
        ax2.legend()

        plt.savefig("plots/" + ticker + "/" + ticker + "_AROON.png", dpi=1000)
        plt.close()

def plot_macd(data, ticker):
    # Create two charts on the same figure.
        ax1 = plt.subplot2grid((10,1), (0,0), rowspan = 4, colspan = 1)
        ax2 = plt.subplot2grid((10,1), (5,0), rowspan = 4, colspan = 1, sharex=ax1)

        # First chart:
        # Plot the closing price on the first chart
        ax1.plot(data['Close'], linewidth=2)
        ax1.set_title(ticker.upper() + ' Close Price')

        # Second chart
        # Plot the OBV
        ax2.set_title('Moving Average Convergence/Divergence (' + ticker + ')')
        ax2.plot(data['MACD'], color='green', linewidth=1, label = 'MACD')
        ax2.plot(data['MACD_SIGNAL'], color = 'orange', linewidth=1, label = 'MACD_SIGNAL')
        ax2.legend()
        
        # Plot zero-line
        ax2.axhline(0, linestyle='--', linewidth=1.5, color='blue')

        plt.savefig("plots/" + ticker + "/" + ticker + "_MACD.png", dpi=1000)
        plt.close()

def plot_stoch(data, ticker):
    # Create two charts on the same figure.
        ax1 = plt.subplot2grid((10,1), (0,0), rowspan = 4, colspan = 1)
        ax2 = plt.subplot2grid((10,1), (5,0), rowspan = 4, colspan = 1, sharex=ax1)

        # First chart:
        # Plot the closing price on the first chart
        ax1.plot(data['Close'], linewidth=2)
        ax1.set_title(ticker.upper() + ' Close Price')

        # Second chart
        # Plot the OBV
        ax2.set_title('Stochastic Oscillator (' + ticker + ')')
        ax2.plot(data['STOCH'], color='purple', linewidth=1, label = 'STOCH')
        ax2.plot(data['STOCH_SIGNAL'], color='blue', linewidth=1, label = 'STOCH_SIGNAL')
        ax2.legend()

        # Add two horizontal lines, signalling the buy and sell ranges.
        # Oversold
        ax2.axhline(20, linestyle='--', linewidth=1.5, color='green')
        # Overbought
        ax2.axhline(80, linestyle='--', linewidth=1.5, color='red')

        plt.savefig("plots/" + ticker + "/" + ticker + "_STOCH.png", dpi=1000)
        plt.close()

def generate_charts(data, ticker):
    
    if not (os.path.isdir("plots/" + ticker)):
            os.makedirs("plots/" + ticker)

    # Generate technical indicator charts

    plot_obv(data, ticker)
    plot_adi(data, ticker)
    plot_adx(data, ticker)
    plot_aroon(data, ticker)
    plot_macd(data, ticker)
    plot_rsi(data, ticker)
    plot_stoch(data, ticker)
    
# Running analysis on techincal indicators to generate buy/sell signals

def analyze_obv(data, ticker):
    obv_curr = data['OBV'].values[-1]
    obv_old = data['OBV'].values[-60]

    close_curr = data['Close'].values[-1]
    close_old = data['Close'].values[-60]

    obv_slope = (obv_curr - obv_old) / 60.0
    close_slope = (close_curr - close_old) / 60.0


    with open("analysis/{}/OBV.txt".format(ticker),'w') as obv_analysis:
        if obv_slope >= 0.25 and close_slope > 0: 
            signal = "BUY"
            analysis = "OBV: **{}** - Over the last 60 days, the slope of OBV ({:.2f}) and the slope of Close ({:.2f}) are positive, indicating a continuing uptrend".format(signal, obv_slope, close_slope)
            obv_analysis.write(analysis)
        elif obv_slope >= 0.25 and close_slope <= 0.25:
            signal = "BUY"
            analysis = "OBV: **{}** - Over the last 60 days, the slope of OBV ({:.2f}) is positive and the slope of Close ({:.2f}) is flat or negative, indicating an upcoming uptrend".format(signal, obv_slope, close_slope)
            obv_analysis.write(analysis)
        elif obv_slope <= 0.25 and close_slope > 0: 
            signal = "WEAK SELL"
            analysis = "OBV: **{}** - Over the last 60 days, the slope of OBV ({:.2f}) is flat or negative and the slope of Close ({:.2f}) is positive, indicating the end of an uptrend".format(signal, obv_slope, close_slope)
            obv_analysis.write(analysis)
        elif obv_slope <= 0.25 and close_slope < 0: 
            signal = "SELL"
            analysis = "OBV: **{}** - Over the last 60 days, the slope of OBV ({:.2f}) and the slope of Close ({:.2f}) are flat or negative, indicating a continuing downtrend".format(signal, obv_slope, close_slope)
            obv_analysis.write(analysis)
        

def analyze_adi(data, ticker):
    adi_curr = data['ADI'].values[-1]
    adi_old = data['ADI'].values[-60]

    close_curr = data['Close'].values[-1]
    close_old = data['Close'].values[-60]

    adi_slope = (adi_curr - adi_old) / 60.0
    close_slope = (close_curr - close_old) / 60.0


    with open("analysis/{}/ADI.txt".format(ticker),'w') as adi_analysis:
        if adi_slope >= 0.25 and close_slope > 0: 
            signal = "BUY"
            analysis = "ADI: **{}** - Over the last 60 days, the slope of ADI ({:.2f}) and the slope of Close ({:.2f}) are positive, indicating a continuing uptrend".format(signal, adi_slope, close_slope)
            adi_analysis.write(analysis)
        elif adi_slope >= 0.25 and close_slope <= 0.25:
            signal = "BUY"
            analysis = "ADI: **{}** - Over the last 60 days, the slope of ADI ({:.2f}) is positive and the slope of Close ({:.2f}) is flat or negative, indicating an upcoming uptrend".format(signal, adi_slope, close_slope)
            adi_analysis.write(analysis)
        elif adi_slope <= 0.25 and close_slope > 0: 
            signal = "WEAK SELL"
            analysis = "ADI: **{}** - Over the last 60 days, the slope of ADI ({:.2f}) is flat or negative and the slope of Close ({:.2f}) is positive, indicating the end of an uptrend".format(signal, adi_slope, close_slope)
            adi_analysis.write(analysis)
        elif adi_slope <= -0.25 and close_slope < 0: 
            signal = "SELL"
            analysis = "ADI: **{}** - Over the last 60 days, the slope of ADI ({:.2f}) and the slope of Close ({:.2f}) are negative, indicating a continuing downtrend".format(signal, adi_slope, close_slope)
            adi_analysis.write(analysis)
        
def analyze_adx(data, ticker):
    analysis = ''
    signal = ''
    adx = data['ADX'].values[-1]
    DIplus = data['ADX_DI+'].values[-1]
    DIminus = data['ADX_DI-'].values[-1]

    with open("analysis/{}/ADX.txt".format(ticker),'w') as adx_analysis: 
        if (adx >= 20 and DIplus > DIminus):
            signal = "BUY"
            analysis = "ADX: **{}** - The ADX value is above 20 ({:.2f}) and DI+ ({}) is greater than DI- ({}), indicating an uptrend".format(signal, adx, DIplus, DIminus)
            adx_analysis.write(analysis)
        elif (adx >= 20 and DIplus < DIminus):
            signal = "SELL"
            analysis = "ADX: **{}** - The ADX value is above 20 ({:.2f}) and DI+ ({}) is less than DI- ({}), indicating a downtrend".format(signal, adx, DIplus, DIminus)
            adx_analysis.write(analysis)
        elif (adx < 20):
            signal = "NEUTRAL"
            analysis = "ADX: **{}** - The ADX value is below 20 ({:.2f}), indicating no trend in either direction".format(signal, adx)
            adx_analysis.write(analysis)

def analyze_aroon(data, ticker):
    analysis = ""
    return analysis

def analyze_macd(data, ticker):
    signal = ''
    macd = data['MACD'].values[-1]
    macd_signal = data['MACD_SIGNAL'].values[-1]

    with open("analysis/{}/MACD.txt".format(ticker),'w') as macd_analysis: 
        if (macd > 0 and macd > macd_signal):
            signal = "BUY"
            analysis = "MACD: **{}** - The MACD value is above 0 ({:.2f}) and greater than the MACD signal line ({}), indicating an uptrend".format(signal, macd, macd_signal)
            macd_analysis.write(analysis)
        elif (macd > 0 and macd <= macd_signal):
            signal = "NEUTRAL"
            analysis = "MACD: **{}** - The MACD value is above 0 ({:.2f}) but less than the MACD signal line ({}). Wait until it crosses the signal line to buy, or consider selling .".format(signal, macd, macd_signal)
            macd_analysis.write(analysis)
        elif (macd <= 0):
            signal = "SELL"
            analysis = "MACD: **{}** - The MACD value is below 0 ({:.2f}), indicating a downtrend".format(signal, macd)
            macd_analysis.write(analysis)

def analyze_rsi(data, ticker):
    analysis = ''
    signal = ''
    rsi = data['RSI'].values[-1]

    with open("analysis/{}/RSI.txt".format(ticker),'w') as rsi_analysis: 
        if rsi >= 70:
            signal = "SELL"
            analysis = "RSI: **{}** - The RSI value is above 70 ({:.2f}) indicating the stock is currently overbought and could see a decline in price soon".format(signal, rsi)
            rsi_analysis.write(analysis)
        elif rsi <= 30:
            signal = "BUY"
            analysis = "RSI: **{}** - The RSI value is below 30 ({:.2f}) indicating the stock is currently oversold and could see an incline in price soon".format(signal, rsi)
            rsi_analysis.write(analysis)
        else:
            signal = "NEUTRAL"
            analysis = "RSI: **{}** - The RSI value is between 30 and 70 ({:.2f}), giving no indication as to where the price will move".format(signal, rsi)
            rsi_analysis.write(analysis)

def analyze_stoch(data, ticker):
    stoch = data['STOCH'].values[-1]
    last_5_days_stoch = data['STOCH'].values[[-1, -2, -3, -4, -5]]
    with open("analysis/{}/STOCH.txt".format(ticker),'w') as stoch_analysis: 
        if (stoch < 80 and max(last_5_days_stoch) > 80):
            signal = "SELL"
            analysis = "STOCH: **{}** - The STOCH value ({:.2f}) has recently dropped below 80, indicating a potential decrease in price soon".format(signal, stoch)
            stoch_analysis.write(analysis)
        elif (stoch > 80 and min(last_5_days_stoch) < 80):
            signal = "NEUTRAL"
            analysis = "STOCH: **{}** - The STOCH value ({:.2f}) has recently crossed above 80. This is not always a sell signal, but look to sell soon".format(signal, stoch)
            stoch_analysis.write(analysis)
        elif (stoch > 20 and min(last_5_days_stoch) < 20):
            signal = "BUY"
            analysis = "STOCH: **{}** - The STOCH value ({:.2f}) has recently risen above 20, indicating a potential increase in price soon".format(signal, stoch)
            stoch_analysis.write(analysis)
        elif (stoch < 20 and max(last_5_days_stoch) > 20):
            signal = "NEUTRAL"
            analysis = "STOCH: **{}** - The STOCH value ({:.2f}) has recently crossed below 20. This is not always a buy signal, but look for buying opportunities".format(signal, stoch)
            stoch_analysis.write(analysis)
        else: 
            signal = "NEUTRAL"
            analysis = "STOCH: **{}** - The STOCH value ({:.2f}) gives no indication of a trend or this behavior is not documented yet".format(signal, stoch)
            stoch_analysis.write(analysis) 
    
def generate_analysis(data, ticker):
    if not (os.path.isdir("analysis/" + ticker)):
        os.makedirs("analysis/" + ticker)

    analyze_obv(data, ticker)
    analyze_adi(data, ticker)
    analyze_adx(data, ticker)
    analyze_aroon(data, ticker)
    analyze_macd(data, ticker)
    analyze_rsi(data, ticker)
    analyze_stoch(data, ticker)

def generate_indicators(ticker):
        
    # Load the data into a dataframe
    data = sd.download_data(ticker=ticker, period="1y", interval="1d")

    # Run On-Balance Volume (OBV) analysis 
    data ['OBV'] = ta.obv(data['Close'], data['Volume'])
    
    # Run Accumulation/Distribution Index (ADI) analysis 
    data['ADI'] = tavolume.ad(data['High'], data['Low'], data['Close'], data['Volume'])
    
    # Run Average Directional Index (ADX) analysis  
    adx = tatrend.adx(data['High'], data['Low'], data['Close'])
    data['ADX'], data['ADX_DI+'], data['ADX_DI-'] = adx['ADX_14'], adx['DMP_14'], adx['DMN_14'] 
    
    # Run Aroon Oscillator (AROON) analysis 
    aroon = tatrend.aroon(data['High'], data ['Low'])
    data['AROON_DOWN'], data['AROON_INDICATOR'], data['AROON_UP'] = aroon['AROOND_14'], aroon['AROONOSC_14'], aroon['AROONU_14'] 
    
    # Run Moving Average Convergence/Divergence (MACD) analysis 
    macd = ta.macd(data['Close'])
    data['MACD'], data['MACD_HISTOGRAM'], data['MACD_SIGNAL'] = macd['MACD_12_26_9'], macd['MACDh_12_26_9'], macd['MACDs_12_26_9'] 
    
    # Run Stochastic Oscillator (STOCH) analysis 
    stoch = ta.stoch(close = data['Close'], high = data['High'], low = data['Low'])
    data['STOCH'], data['STOCH_SIGNAL']= stoch['STOCHk_14_3_3'], stoch['STOCHd_14_3_3'] 
    
    # Run Relative Stength Index (RSI) analysis
    data ['RSI'] = ta.rsi(data['Close'])
        
    sd.update_csv(data, ticker)

def run_analysis():
    for ticker in sd.get_tickers():
        generate_indicators(ticker)
        data = sd.fetch_data(ticker)
        generate_charts(data, ticker)
        generate_analysis(data, ticker)


if __name__ == '__main__':  
    run_analysis()

        

       
