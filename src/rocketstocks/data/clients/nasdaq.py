import logging

import pandas as pd
import requests
from ratelimit import limits, sleep_and_retry

logger = logging.getLogger(__name__)


class Nasdaq:
    def __init__(self):
        self.url_base = "https://api.nasdaq.com/api"
        self.headers = {
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/96.0.4664.45 Safari/537.36'
            )
        }

    @sleep_and_retry
    @limits(calls=5, period=60)
    def get_all_tickers(self) -> pd.DataFrame:
        """Retrieve latest tickers and their properties from NASDAQ."""
        logger.debug("Retrieving latest tickers and their properties from NASDAQ")
        url = f"{self.url_base}/screener/stocks?tableonly=false&limit=25&download=true"
        resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()
        data = resp.json()
        # B10 fix: guard against None data
        if data.get('data') is None or data['data'].get('rows') is None:
            logger.warning("NASDAQ returned no ticker data")
            return pd.DataFrame()
        return pd.DataFrame(data['data']['rows'])

    @sleep_and_retry
    @limits(calls=5, period=60)
    def get_earnings_by_date(self, date) -> pd.DataFrame:
        """Retrieve all earnings on a given date."""
        logger.debug(f"Retrieving earnings on date {date}")
        url = f"{self.url_base}/calendar/earnings"
        params = {'date': date}
        resp = requests.get(url, headers=self.headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        if data.get('data') is None or data['data'].get('rows') is None:
            return pd.DataFrame()
        return pd.DataFrame(data['data']['rows'])

    @sleep_and_retry
    @limits(calls=5, period=60)
    def get_earnings_forecast(self, ticker):
        """Retrieve earnings forecast for *ticker* from NASDAQ."""
        logger.debug(f"Retrieving earnings forecast for ticker '{ticker}'")
        url = f"https://api.nasdaq.com/api/analyst/{ticker}/earnings-forecast"
        resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()
        data = resp.json().get('data')
        if data is None:
            logger.warning(f"NASDAQ returned no forecast data for '{ticker}'")
            return None
        return data

    def get_earnings_forecast_quarterly(self, ticker) -> pd.DataFrame:
        data = self.get_earnings_forecast(ticker)
        if data is None:
            return pd.DataFrame()
        qf = data.get('quarterlyForecast') or {}
        rows = qf.get('rows')
        if not rows:
            logger.debug(f"No quarterly forecast rows for '{ticker}'")
            return pd.DataFrame()
        return pd.DataFrame.from_dict(rows)

    def get_earnings_forecast_yearly(self, ticker) -> pd.DataFrame:
        data = self.get_earnings_forecast(ticker)
        if data is None:
            return pd.DataFrame()
        yf = data.get('yearlyForecast') or {}
        rows = yf.get('rows')
        if not rows:
            logger.debug(f"No yearly forecast rows for '{ticker}'")
            return pd.DataFrame()
        return pd.DataFrame.from_dict(rows)

    @sleep_and_retry
    @limits(calls=5, period=60)
    def get_eps(self, ticker) -> pd.DataFrame:
        """Retrieve historical EPS for *ticker* from NASDAQ."""
        logger.debug(f"Retrieving eps for ticker '{ticker}'")
        url = f"https://api.nasdaq.com/api/quote/{ticker}/eps"
        resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()
        return pd.DataFrame.from_dict(resp.json()['data']['earningsPerShare'])

    def get_prev_eps(self, ticker) -> pd.DataFrame:
        eps = self.get_eps(ticker)
        return eps[eps['type'] == 'PreviousQuarter']

    def get_future_eps(self, ticker) -> pd.DataFrame:
        eps = self.get_eps(ticker)
        return eps[eps['type'] == 'UpcomingQuarter']
