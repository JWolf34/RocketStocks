import logging
import schwab
import httpx
import datetime
from RocketStocks.utils import secrets
import pandas as pd

# Logging configuration
logger = logging.getLogger(__name__)


class Schwab():
    def __init__(self):
        self.client = schwab.auth.easy_client(
            api_key=secrets.schwab_api_key,
            app_secret=secrets.schwab_api_secret,
            callback_url="https://127.0.0.1:8182",
            token_path="data/schwab-token.json",
            asyncio=True
        )

    # Request daily price history from Schwab between start_datetime and end_datetime
    async def get_daily_price_history(self, ticker, start_datetime=None, end_datetime=datetime.datetime.now(datetime.timezone.utc)):
        logger.debug(f"Requesting daily price history from Schwab for ticker: '{ticker}' - start: {start_datetime}, end: {end_datetime}")
        if start_datetime is None: # If no start time, get data as far back as 2000
            start_datetime = datetime.datetime(
                                year = 2000,
                                month = 1,
                                day = 1,
                                hour = 0,
                                minute = 0,
                                second = 0
                                ).astimezone(datetime.timezone.utc)
        resp = await self.client.get_price_history_every_day(
            symbol=ticker, 
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )
        logger.debug(f"Reponse status code is {resp.status_code}")
        try:
            assert resp.status_code == httpx.codes.OK, resp.raise_for_status()
            data = resp.json()
            price_history = pd.DataFrame.from_dict(data['candles'])
            if price_history.size > 0:
                price_history['datetime'] = price_history['datetime'].apply(lambda x: datetime.date.fromtimestamp(x/1000))
                price_history = price_history.rename(columns={'datetime':'date'})
                price_history.insert(loc=0, column='ticker', value=ticker)
                return price_history
            else:
                return None
        except httpx.HTTPStatusError as e:
            logger.error(f"Enountered HTTPStatusError when downloading daily price history for ticker {ticker}\n{e}")
            return pd.DataFrame()

    # Request 5m price history from Schwab between start_datetime and end_datetime
    async def get_5m_price_history(self, ticker, start_datetime=None, end_datetime=None):
        logger.debug(f"Requesting 5m price history from Schwab for ticker: '{ticker}' - start: {start_datetime}, end: {end_datetime}")
        resp =  await self.client.get_price_history_every_five_minutes(
            symbol=ticker, 
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )
        logger.debug(f"Reponse status code is {resp.status_code}")
        assert resp.status_code == httpx.codes.OK, resp.raise_for_status()
        data = resp.json()
        price_history = pd.DataFrame.from_dict(data['candles'])
        if price_history.size > 0:
            price_history['datetime'] = price_history['datetime'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000))
            price_history.insert(loc=0, column='ticker', value=ticker)
            return price_history
        else:
            return None

    # Get latest quote for ticker from Schwab
    async def get_quote(self, ticker):
        logger.debug(f"Retrieving quote for ticker '{ticker}' from Schwab")
        resp = await self.client.get_quote(
            symbol=ticker
        )
        logger.debug(f"Reponse status code is {resp.status_code}")
        assert resp.status_code == httpx.codes.OK, resp.raise_for_status()
        data = resp.json()
        return data[ticker]

    # Get quotes for multiple tickers from Schwab
    async def get_quotes(self, tickers):
        logger.debug(f"Retrieving quotes for tickers {tickers} from Schwab")
        resp = await self.client.get_quotes(
            symbols=tickers
        )
        logger.debug(f"Reponse status code is {resp.status_code}")
        assert resp.status_code == httpx.codes.OK, resp.raise_for_status()
        data = resp.json()
        return data
    
    # Get latest fundamental data from Schwab
    async def get_fundamentals(self, tickers):
        logger.debug(f"Retrieving latest fundamental data for tickers {tickers}")
        resp = await self.client.get_instruments(symbols=tickers, 
                                           projection=self.client.Instrument.Projection.FUNDAMENTAL)
        logger.debug(f"Reponse status code is {resp.status_code}") 
        assert resp.status_code == httpx.codes.OK, resp.raise_for_status()
        data = resp.json()
        return data
    
    # Get latest option chain for target ticker
    async def get_options_chain(self, ticker):
        logger.debug(f"Retreiving latest options chain for ticker '{ticker}'")
        resp = await self.client.get_option_chain(ticker)
        assert resp.status_code == httpx.codes.OK, resp.raise_for_status()
        data = resp.json()
        return data

    # Get top 10 price movers for the day
    async def get_movers(self):
        logger.debug("Retrieving top 10 price movers from Schwab")
        resp = await self.client.get_movers(index=self.client.Movers.Index.EQUITY_ALL, 
                                      sort_order=self.client.Movers.SortOrder.PERCENT_CHANGE_UP,
                                      frequency=self.client.Movers.Frequency.TEN)
        logger.debug(f"Reponse status code is {resp.status_code}")
        assert resp.status_code == httpx.codes.OK, resp.raise_for_status()
        data = resp.json()
        return data
  