import yfinance as yf
import pandas as pd
import os

def download_and_update_csv(ticker, period, csv_file_path):
    # Download data for the given ticker
    data = yf.download(tickers=ticker, 
                       period=Period, 
                       interval="1m", 
                       prepost = True,
                       auto_adjust = False,
                       repair = True)

    # If the CSV file already exists, read the existing data
    if os.path.exists(csv_file_path):
        existing_data = pd.read_csv(csv_file_path, index_col=0, parse_dates=True)
        
        # Append the new data to the existing data
        combined_data = existing_data.append(data)
    else:
        combined_data = data
    
    # Remove duplicate rows, if any
    combined_data = combined_data[~combined_data.index.duplicated(keep='first')]
    
    # Save the combined data to the CSV file
    combined_data.to_csv(csv_file_path)

if __name__ == "__main__":
    # Set your ticker and CSV file path. Repeat for QQQ, TQQQ, and SQQQ.

    # set your period
    Period = "max"

    # QQQ
    ticker = "QQQ"
    csv_file_path = r"C:\Users\kangb\OneDrive\Desktop\Stock data\Yahoo Finance\auto_collect\QQQ.csv"

    # Call the function to download data and update the CSV file
    download_and_update_csv(ticker, Period, csv_file_path)

    # TQQQ
    ticker = "TQQQ"
    csv_file_path = r"C:\Users\kangb\OneDrive\Desktop\Stock data\Yahoo Finance\auto_collect\TQQQ.csv"

    # Call the function to download data and update the CSV file
    download_and_update_csv(ticker, Period, csv_file_path)

    # SQQQ
    ticker = "SQQQ"
    csv_file_path = r"C:\Users\kangb\OneDrive\Desktop\Stock data\Yahoo Finance\auto_collect\SQQQ.csv"

    # Call the function to download data and update the CSV file
    download_and_update_csv(ticker, Period, csv_file_path)