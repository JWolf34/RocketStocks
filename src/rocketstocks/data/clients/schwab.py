import logging
import datetime

import httpx
import schwab

from rocketstocks.core.config.secrets import secrets

logger = logging.getLogger(__name__)


class Schwab:
    def __init__(self):
        self.client = schwab.auth.easy_client(
            api_key=secrets.schwab_api_key,
            app_secret=secrets.schwab_api_secret,
            callback_url="https://127.0.0.1:8182",
            token_path="data/schwab-token.json",
            asyncio=True,
        )

    async def get_daily_price_history(self, ticker, start_datetime=None, end_datetime=None):
        """Request daily price history from Schwab between start_datetime and end_datetime."""
        # B3 fix: resolve end_datetime here, not at import/class-definition time
        if end_datetime is None:
            end_datetime = datetime.datetime.now(datetime.timezone.utc)
        if start_datetime is None:
            start_datetime = datetime.datetime(2000, 1, 1, 0, 0, 0).astimezone(datetime.timezone.utc)

        logger.debug(
            f"Requesting daily price history from Schwab for ticker: '{ticker}' "
            f"- start: {start_datetime}, end: {end_datetime}"
        )
        resp = await self.client.get_price_history_every_day(
            symbol=ticker,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )
        logger.debug(f"Response status code is {resp.status_code}")
        try:
            resp.raise_for_status()
            data = resp.json()
            import pandas as pd
            price_history = pd.DataFrame.from_dict(data['candles'])
            if price_history.size > 0:
                price_history['datetime'] = price_history['datetime'].apply(
                    lambda x: datetime.date.fromtimestamp(x / 1000)
                )
                price_history = price_history.rename(columns={'datetime': 'date'})
                price_history.insert(loc=0, column='ticker', value=ticker)
                return price_history
            else:
                return pd.DataFrame()
        except httpx.HTTPStatusError as e:
            logger.error(
                f"Encountered HTTPStatusError when downloading daily price history "
                f"for ticker {ticker}\n{e}"
            )
            import pandas as pd
            return pd.DataFrame()

    async def get_5m_price_history(self, ticker, start_datetime=None, end_datetime=None):
        """Request 5-minute price history from Schwab between start_datetime and end_datetime."""
        logger.debug(
            f"Requesting 5m price history from Schwab for ticker: '{ticker}' "
            f"- start: {start_datetime}, end: {end_datetime}"
        )
        resp = await self.client.get_price_history_every_five_minutes(
            symbol=ticker,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )
        logger.debug(f"Response status code is {resp.status_code}")
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTPStatusError fetching 5m price history for {ticker}\n{e}")
            import pandas as pd
            return pd.DataFrame()
        data = resp.json()
        import pandas as pd
        price_history = pd.DataFrame.from_dict(data['candles'])
        if price_history.size > 0:
            price_history['datetime'] = price_history['datetime'].apply(
                lambda x: datetime.datetime.fromtimestamp(x / 1000)
            )
            price_history.insert(loc=0, column='ticker', value=ticker)
            return price_history
        else:
            return pd.DataFrame()

    async def get_quote(self, ticker):
        """Get latest quote for ticker from Schwab."""
        logger.debug(f"Retrieving quote for ticker '{ticker}' from Schwab")
        resp = await self.client.get_quote(symbol=ticker)
        logger.debug(f"Response status code is {resp.status_code}")
        resp.raise_for_status()
        data = resp.json()
        return data[ticker]

    async def get_quotes(self, tickers):
        """Get quotes for multiple tickers from Schwab."""
        logger.debug(f"Retrieving quotes for tickers {tickers} from Schwab")
        resp = await self.client.get_quotes(symbols=tickers)
        logger.debug(f"Response status code is {resp.status_code}")
        resp.raise_for_status()
        return resp.json()

    async def get_fundamentals(self, tickers):
        """Get latest fundamental data from Schwab."""
        logger.debug(f"Retrieving latest fundamental data for tickers {tickers}")
        resp = await self.client.get_instruments(
            symbols=tickers,
            projection=self.client.Instrument.Projection.FUNDAMENTAL,
        )
        logger.debug(f"Response status code is {resp.status_code}")
        resp.raise_for_status()
        return resp.json()

    async def get_options_chain(self, ticker):
        """Get latest option chain for target ticker."""
        logger.debug(f"Retrieving latest options chain for ticker '{ticker}'")
        resp = await self.client.get_option_chain(ticker)
        resp.raise_for_status()
        return resp.json()

    async def get_movers(self):
        """Get top 10 price movers for the day."""
        logger.debug("Retrieving top 10 price movers from Schwab")
        resp = await self.client.get_movers(
            index=self.client.Movers.Index.EQUITY_ALL,
            sort_order=self.client.Movers.SortOrder.PERCENT_CHANGE_UP,
            frequency=self.client.Movers.Frequency.TEN,
        )
        logger.debug(f"Response status code is {resp.status_code}")
        resp.raise_for_status()
        return resp.json()
