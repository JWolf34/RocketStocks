import pandas as pd
import yfinance as yf
#from ta import add_all_ta_features
#from ta.utils import dropna
#from ta.momentum import *
import datetime
import matplotlib.pyplot as plt


def rsi():
    # Load the data into a dataframe
    symbol = yf.Ticker('QQQ')
    qqq = symbol.history(interval="1d",period="1y")

    # Filter the data by date
    #qqq = qqq[qqq.index > datetime.date(2022,8,7)]
    #qqq = qqq[qqq.index < datetime.date(2023,8,7)]

    # Print the result
    print(qqq)

    del qqq["Dividends"]
    del qqq["Stock Splits"]

    change = qqq["Close"].diff()
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
    ax1.plot(qqq['Close'], linewidth=2)
    ax1.set_title('QQQ Close Price')

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

'''
def obv():

    np.where(df['close'] > df['close'].shift(1), df['volume'], 
    np.where(df['close'] < df['close'].shift(1), -df['volume'], 0)).cumsum()
'''

if __name__ == '__main__':
    rsi()