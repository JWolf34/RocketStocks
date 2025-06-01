import logging
from ratelimit import limits, sleep_and_retry
import requests
import pandas as pd

# Logging configuration
logger = logging.getLogger(__name__)

class Nasdaq():
    def __init__(self):
        self.url_base = "https://api.nasdaq.com/api"
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'}
        self.MAX_CALLS = 10
        self.MAX_PERIOD = 60

    # Retrieve latest tickers and their properties on the NASDAQ
    @sleep_and_retry
    @limits(calls = 5, period = 60)
    def get_all_tickers(self):
        logger.debug("Retrieving latest tickers and their properties from NASDAQ")
        url = f"{self.url_base}/screener/stocks?tableonly=false&limit=25&download=true"
        data = requests.get(url, headers=self.headers).json()
        tickers = pd.DataFrame(data['data']['rows'])
        return tickers

    # Retrieve all earnings on a given date
    @sleep_and_retry
    @limits(calls = 5, period = 60) 
    def get_earnings_by_date(self, date):
        logger.debug(f"Retrieving earnings on date {date}")
        url = f"{self.url_base}/calendar/earnings"
        params = {'date':date}
        data = requests.get(url, headers=self.headers, params=params).json()
        if data['data'] is None:
            return pd.DataFrame()
        else:
            return pd.DataFrame(data['data']['rows'])
    
    # Retrieve earnings forecast for target tickert from NASDAQ
    @sleep_and_retry
    @limits(calls = 5, period = 60) 
    def get_earnings_forecast(self, ticker):
        logger.debug(f"Retrieving earnings forecast for ticker '{ticker}'")
        url = f"https://api.nasdaq.com/api/analyst/{ticker}/earnings-forecast"
        data = requests.get(url, headers=self.headers).json()
        return data['data']

    def get_earnings_forecast_quarterly(self, ticker):
        return pd.DataFrame.from_dict(self.get_earnings_forecast(ticker)['quarterlyForecast']['rows'])
    
    def get_earnings_forecast_yearly(self, ticker):
        return pd.DataFrame.from_dict(self.get_earnings_forecast(ticker)['yearlyForecast']['rows'])

    # Retrieve historical EPS for target ticker from NASDAQ
    @sleep_and_retry
    @limits(calls = 5, period = 60) 
    def get_eps(self, ticker):
        logger.debug(F"Retrieving eps for ticker '{ticker}'")
        url = f"https://api.nasdaq.com/api/quote/{ticker}/eps"
        eps_request = requests.get(url, headers=self.headers)
        eps = pd.DataFrame.from_dict(eps_request.json()['data']['earningsPerShare'])
        return eps
        
    def get_prev_eps(self, ticker):
        eps = self.get_eps(ticker)
        return eps[eps['type'] == 'PreviousQuarter']

    def get_future_eps(self, ticker):
        eps = self.get_eps(ticker)
        return eps[eps['type'] == 'UpcomingQuarter']
  
