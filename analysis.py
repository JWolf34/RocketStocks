import pandas as pd
import stockdata as sd
#from ta import add_all_ta_features
#from ta.utils import dropna
#from ta.momentum import *
import datetime
import matplotlib.pyplot as plt


def rsi(ticker):
    # Load the data into a dataframe
    data = sd.download_data(ticker=ticker, period="1y", interval="1d")

    # Filter the data by date
    #data = data[data.index > datetime.date(2022,8,7)]
    #data = data[data.index < datetime.date(2023,8,7)]

    # Print the result
    #print(data)

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


def obv(ticker):
    obv = []
    obv.append(0)

    #Fetch yearly data for ticker
    data = sd.download_data(ticker=ticker, period="1y", interval="1d")

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
    # Plot the RSI
    ax2.set_title('OBV / OBV_EMA')
    ax2.plot(data['OBV'], label='OBV', color='orange')
    ax2.plot(data['OBV_EMA'], label='OBV_EMA', color='purple')
    

    plt.show()
        
    
    


    

if __name__ == '__main__':
    obv("GOOG")