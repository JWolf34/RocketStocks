import sys
sys.path.append('../RocketStocks/discord')
import yfinance as yf
from pandas_datareader import data as pdr
import pandas as pd
import pandas_ta as ta
import psycopg2
from psycopg2 import sql
from newsapi import NewsApiClient
import os
import datetime
from datetime import timedelta
import requests
from ratelimit import limits, sleep_and_retry
import utils
import logging
from tradingview_screener import Query, Column
import schwab
import time
import httpx
from bs4 import BeautifulSoup

# Logging configuration
logger = logging.getLogger(__name__)



           




       

#########
# Tests #
#########

def test():
    pass
    

if __name__ == "__main__":#
    test()
    

    
    

    