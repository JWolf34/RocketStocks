import pandas as pd
import stockdata as sd
import numpy as np
import datetime
import matplotlib.pyplot as plt
import pandas_ta as ta
import pandas_ta.momentum as tamomentum
import pandas_ta.volume as tavolume
import pandas_ta.trend as tatrend


def rsi(data):

    change = data["Close"].diff()
    change.dropna(inplace=True)

    # Create two copies of the Closing price Series
    change_up = change.copy()
    change_down = change.copy()

    # 
    change_up[change_up<0] = 0
    change_down[change_down>0] = 0

    # Verify that we did not make any mistakes
    change.equals(change_up+change_down)

    # Calculate the rolling average of average up and average down
    avg_up = change_up.rolling(14).mean()
    avg_down = change_down.rolling(14).mean().abs()

    rsi = 100 * avg_up / (avg_up + avg_down)

    # Take a look at the 20 oldest datapoints
    rsi.head(20)

    # Create two charts on the same figure.
    ax1 = plt.subplot2grid((10,1), (0,0), rowspan = 4, colspan = 1)
    ax2 = plt.subplot2grid((10,1), (5,0), rowspan = 4, colspan = 1)

    # First chart:
    # Plot the closing price on the first chart
    ax1.plot(data['Close'], linewidth=2)
    ax1.set_title(ticker.upper() + ' Close Price')

    # Second chart
    # Plot the RSI
    ax2.set_title('Relative Strength Index')
    ax2.plot(rsi, color='orange', linewidth=1)
    # Add two horizontal lines, signalling the buy and sell ranges.
    # Oversold
    ax2.axhline(30, linestyle='--', linewidth=1.5, color='green')
    # Overbought
    ax2.axhline(70, linestyle='--', linewidth=1.5, color='red')

    plt.show()


def obv(data):
    obv = []
    obv.append(0)

    #Loop through data set to record volume and close price each day
    #Append to OBV accordingly
    for i in range(1, len(data.Close)):
        if data.Close[i] > data.Close[i-1]:
            obv.append(obv[-1] + data.Volume[i])
        elif data.Close[i] < data.Close[i-1]:
            obv.append(obv[-1] - data.Volume[i])
        else:
            obv.append(obv[-1])
    
    #Include OBV and OBV Exponential Moving Average (EMA) as columns in data
    data['OBV'] = obv
    data['OBV_EMA'] = data['OBV'].ewm(span=20).mean()

    # Create two charts on the same figure.
    ax1 = plt.subplot2grid((10,1), (0,0), rowspan = 4, colspan = 1)
    ax2 = plt.subplot2grid((10,1), (5,0), rowspan = 4, colspan = 1)

    # First chart:
    # Plot the closing price on the first chart
    ax1.plot(data['Close'], linewidth=2)
    ax1.set_title(ticker.upper() + ' Close Price')

    # Second chart
    # Plot the OBV
    ax2.set_title('OBV / OBV_EMA')
    ax2.plot(data['OBV'], label='OBV', color='orange')
    ax2.plot(data['OBV_EMA'], label='OBV_EMA', color='purple')
    

    plt.show()

#Return the Simple Moving Average (SMA) of a given stock
def sma(data, period=30, column='Close'):

    return data[column].rolling(window=period).mean()
    
#Return the Exponential Moving Average (EMA) of a given stock
def ema(data, period=20, column = 'Close'):

    return data[column].ewm(span=period, adjust = False).mean()

#Calculate the Moving Average Convergence/Divergence (MACD)
def MACD(data, period_long=26, period_short=12, period_signal=9, column='Close'):

    #Calculate Short-Term EMA
    shortEMA = ema(data, period_short, column=column)

    #Calculate the Long Term EMA
    longEMA = ema(data, period_long, column=column)

    #Calculate the MACD
    data['MACD'] = shortEMA - longEMA 

    #Calculate the signal line
    data['signal_line'] = ema(data, period_signal, column='MACD')

    return data

def plot_rsi(data, ticker):
    # Create two charts on the same figure.
        ax1 = plt.subplot2grid((10,1), (0,0), rowspan = 4, colspan = 1)
        ax2 = plt.subplot2grid((10,1), (5,0), rowspan = 4, colspan = 1)

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

        plt.show()

def plot_obv(data, ticker):
    # Create two charts on the same figure.
        ax1 = plt.subplot2grid((10,1), (0,0), rowspan = 4, colspan = 1)
        ax2 = plt.subplot2grid((10,1), (5,0), rowspan = 4, colspan = 1)

        # First chart:
        # Plot the closing price on the first chart
        ax1.plot(data['Close'], linewidth=2)
        ax1.set_title(ticker.upper() + ' Close Price')

        # Second chart
        # Plot the OBV
        ax2.set_title('On-Balance Volume (' + ticker + ')')
        ax2.plot(data['OBV'], color='purple', linewidth=1)

        plt.show()

def plot_adi(data, ticker):
    # Create two charts on the same figure.
        ax1 = plt.subplot2grid((10,1), (0,0), rowspan = 4, colspan = 1)
        ax2 = plt.subplot2grid((10,1), (5,0), rowspan = 4, colspan = 1)

        # First chart:
        # Plot the closing price on the first chart
        ax1.plot(data['Close'], linewidth=2)
        ax1.set_title(ticker.upper() + ' Close Price')

        # Second chart
        # Plot the OBV
        ax2.set_title('Accumulation/Distribution Index (' + ticker + ')')
        ax2.plot(data['ADI'], color='green', linewidth=1)

        plt.show()

def plot_adx(data, ticker):
    # Create two charts on the same figure.
        ax1 = plt.subplot2grid((10,1), (0,0), rowspan = 4, colspan = 1)
        ax2 = plt.subplot2grid((10,1), (5,0), rowspan = 4, colspan = 1)

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

        plt.show()

def plot_aroon(data, ticker):
    # Create two charts on the same figure.
        ax1 = plt.subplot2grid((10,1), (0,0), rowspan = 4, colspan = 1)
        ax2 = plt.subplot2grid((10,1), (5,0), rowspan = 4, colspan = 1)

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

        plt.show()



def plot_macd(data, ticker):
    # Create two charts on the same figure.
        ax1 = plt.subplot2grid((10,1), (0,0), rowspan = 4, colspan = 1)
        ax2 = plt.subplot2grid((10,1), (5,0), rowspan = 4, colspan = 1)

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

        plt.show()

def plot_stoch(data, ticker):
    # Create two charts on the same figure.
        ax1 = plt.subplot2grid((10,1), (0,0), rowspan = 4, colspan = 1)
        ax2 = plt.subplot2grid((10,1), (5,0), rowspan = 4, colspan = 1)

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

        plt.show()  


if __name__ == '__main__':

    
    #help(ta.macd)

    
    for ticker in sd.get_tickers():
        # Load the data into a dataframe
        data = sd.download_data(ticker=ticker, period="1y", interval="1d")

        # Run On-Balance Volume (OBV) analysis and generate charts
        data ['OBV'] = ta.obv(data['Close'], data['Volume'])
        plot_obv(data, ticker)

        # Run Accumulation/Distribution Index (ADI) analysis and generate charts
        data['ADI'] = tavolume.ad(data['High'], data['Low'], data['Close'], data['Volume'])
        plot_adi(data, ticker)

        # Run Average Directional Index (ADX) analysis and generate charts 
        adx = tatrend.adx(data['High'], data['Low'], data['Close'])
        data['ADX'], data['ADX_DI+'], data['ADX_DI-'] = adx['ADX_14'], adx['DMP_14'], adx['DMN_14'] 
        plot_adx(data, ticker)

        # Run Aroon Oscillator (AROON) analysis and generate charts 
        aroon = tatrend.aroon(data['High'], data ['Low'])
        data['AROON_DOWN'], data['AROON_INDICATOR'], data['AROON_UP'] = aroon['AROOND_14'], aroon['AROONOSC_14'], aroon['AROONU_14'] 
        plot_aroon(data, ticker)

        # Run Moving Average Convergence/Divergence (MACD) analysis and generate charts 
        macd = ta.macd(data['Close'])
        data['MACD'], data['MACD_HISTOGRAM'], data['MACD_SIGNAL'] = macd['MACD_12_26_9'], macd['MACDh_12_26_9'], macd['MACDs_12_26_9'] 
        plot_macd(data, ticker)

        # Run Stochastic Oscillator (STOCH) analysis and generate charts 
        stoch = ta.stoch(close = data['Close'], high = data['High'], low = data['Low'])
        data['STOCH'], data['STOCH_SIGNAL']= stoch['STOCHk_14_3_3'], stoch['STOCHd_14_3_3'] 
        plot_stoch(data, ticker)
        
        # Run Relative Stength Index (RSI) analysis and generate charts
        data ['RSI'] = ta.rsi(data['Close'])
        plot_rsi(data, ticker)

       
