import logging
import datetime
from typing import Callable

import httpx
import schwab
from aiolimiter import AsyncLimiter
from authlib.integrations.base_client.errors import OAuthError

from rocketstocks.core.config.settings import settings
from rocketstocks.core.auth.token_manager import get_token_info, TokenInfo, TokenStatus

logger = logging.getLogger(__name__)


class SchwabTokenError(Exception):
    """Raised when the Schwab client is unavailable due to a token problem."""


class SchwabRateLimitError(Exception):
    """Raised when the Schwab API returns HTTP 429 Too Many Requests."""


class Schwab:
    def __init__(self, client=None, token_store=None, limiter=None,
                 on_token_invalid: Callable | None = None):
        self._token_store = token_store
        self._token_invalid: bool = False
        self._limiter = limiter or AsyncLimiter(120, 60)  # 120 req/min
        self._on_token_invalid = on_token_invalid

        if client is not None:
            self.client = client
        else:
            self.client = None

    async def init_client(self) -> None:
        """Load the Schwab client from the token stored in the database.

        Sets ``self.client = None`` if the token is missing or invalid rather
        than raising, so the bot can start without a valid token.
        """
        self._token_invalid = False
        token_dict = await self._token_store.load_token()
        if token_dict is None:
            logger.warning("No Schwab token in DB. Run /schwab auth to authenticate.")
            self.client = None
            return
        try:
            self.client = schwab.auth.client_from_access_functions(
                api_key=settings.schwab_api_key,
                app_secret=settings.schwab_api_secret,
                token_read_func=lambda: token_dict,
                token_write_func=self._token_store.schedule_save,
                asyncio=True,
            )
            logger.info("Schwab client loaded from database token")
        except Exception as exc:
            logger.error(f"Failed to load Schwab client: {exc}")
            self.client = None

    async def reload_client(self) -> None:
        """Re-load the Schwab client from the database token.

        Call this after a successful OAuth flow to activate the new token.
        """
        await self.init_client()

    async def get_token_info(self) -> TokenInfo:
        """Return the current token status, including runtime invalidity."""
        if self._token_invalid:
            return TokenInfo(status=TokenStatus.INVALID, expires_at=None, time_remaining=None)
        token_dict = await self._token_store.load_token()
        return get_token_info(token_dict)

    async def get_token_expiry(self):
        """Return the Schwab token expiry as a naive local datetime, or None."""
        token_dict = await self._token_store.load_token()
        info = get_token_info(token_dict)
        return info.expires_at

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_client(self):
        """Raise SchwabTokenError if the client is unavailable."""
        if self.client is None:
            raise SchwabTokenError(
                "Schwab client is not available. Run /schwab-auth to authenticate."
            )

    def _check_rate_limit(self, resp) -> None:
        """Raise SchwabRateLimitError if the response is HTTP 429."""
        if resp.status_code == 429:
            logger.error(
                "Schwab API rate limit exceeded (HTTP 429). "
                "Reduce request frequency or wait before retrying."
            )
            raise SchwabRateLimitError(
                "Schwab API rate limit exceeded (HTTP 429 Too Many Requests). "
                "Reduce request frequency or wait before retrying."
            )

    def _handle_oauth_error(self, exc: OAuthError) -> None:
        """Mark token as invalid and re-raise as SchwabTokenError."""
        logger.error(
            f"Schwab OAuth error — token is invalid or has been revoked: {exc}. "
            "Run /schwab-auth to re-authenticate."
        )
        self._token_invalid = True
        self.client = None
        if self._on_token_invalid is not None:
            self._on_token_invalid()
        raise SchwabTokenError(
            f"Schwab token was rejected: {exc}. Run /schwab-auth to re-authenticate."
        ) from exc

    # ------------------------------------------------------------------
    # API methods
    # ------------------------------------------------------------------

    async def get_daily_price_history(self, ticker, start_datetime=None, end_datetime=None):
        """Request daily price history from Schwab between start_datetime and end_datetime."""
        self._require_client()
        # B3 fix: resolve end_datetime here, not at import/class-definition time
        if end_datetime is None:
            end_datetime = datetime.datetime.now(datetime.timezone.utc)
        if start_datetime is None:
            start_datetime = datetime.datetime(2000, 1, 1, 0, 0, 0).astimezone(datetime.timezone.utc)

        logger.debug(
            f"Requesting daily price history from Schwab for ticker: '{ticker}' "
            f"- start: {start_datetime}, end: {end_datetime}"
        )
        async with self._limiter:
            try:
                resp = await self.client.get_price_history_every_day(
                    symbol=ticker,
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
                )
            except OAuthError as exc:
                self._handle_oauth_error(exc)

        logger.debug(f"Response status code is {resp.status_code}")
        self._check_rate_limit(resp)
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
        self._require_client()
        logger.debug(
            f"Requesting 5m price history from Schwab for ticker: '{ticker}' "
            f"- start: {start_datetime}, end: {end_datetime}"
        )
        async with self._limiter:
            try:
                resp = await self.client.get_price_history_every_five_minutes(
                    symbol=ticker,
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
                )
            except OAuthError as exc:
                self._handle_oauth_error(exc)

        logger.debug(f"Response status code is {resp.status_code}")
        self._check_rate_limit(resp)
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
        self._require_client()
        logger.debug(f"Retrieving quote for ticker '{ticker}' from Schwab")
        async with self._limiter:
            try:
                resp = await self.client.get_quote(symbol=ticker)
            except OAuthError as exc:
                self._handle_oauth_error(exc)
        logger.debug(f"Response status code is {resp.status_code}")
        self._check_rate_limit(resp)
        resp.raise_for_status()
        data = resp.json()
        return data[ticker]

    async def get_quotes(self, tickers, fields=None):
        """Get quotes for multiple tickers from Schwab.

        Args:
            tickers: List of ticker symbols.
            fields: Optional list of field groups to include in the response
                (e.g. ``['fundamental']``). When omitted the Schwab API returns
                quote/regular/reference data only.
        """
        self._require_client()
        logger.debug(f"Retrieving quotes for tickers {tickers} from Schwab")
        async with self._limiter:
            try:
                resp = await self.client.get_quotes(symbols=tickers, fields=fields)
            except OAuthError as exc:
                self._handle_oauth_error(exc)
        logger.debug(f"Response status code is {resp.status_code}")
        self._check_rate_limit(resp)
        resp.raise_for_status()
        return resp.json()

    async def get_fundamentals(self, tickers):
        """Get latest fundamental data from Schwab."""
        self._require_client()
        logger.debug(f"Retrieving latest fundamental data for tickers {tickers}")
        async with self._limiter:
            try:
                resp = await self.client.get_instruments(
                    symbols=tickers,
                    projection=self.client.Instrument.Projection.FUNDAMENTAL,
                )
            except OAuthError as exc:
                self._handle_oauth_error(exc)
        logger.debug(f"Response status code is {resp.status_code}")
        self._check_rate_limit(resp)
        resp.raise_for_status()
        return resp.json()

    async def get_options_chain(self, ticker):
        """Get latest option chain for target ticker."""
        self._require_client()
        logger.debug(f"Retrieving latest options chain for ticker '{ticker}'")
        async with self._limiter:
            try:
                resp = await self.client.get_option_chain(ticker)
            except OAuthError as exc:
                self._handle_oauth_error(exc)
        self._check_rate_limit(resp)
        resp.raise_for_status()
        return resp.json()

    async def get_movers(self, sort_order=None):
        """Get top 10 price movers for the day.

        Args:
            sort_order: Schwab Movers.SortOrder enum value. Defaults to PERCENT_CHANGE_UP.
                        Pass ``self.client.Movers.SortOrder.PERCENT_CHANGE_DOWN`` for losers.
        """
        self._require_client()
        logger.debug("Retrieving top 10 price movers from Schwab")
        effective_sort = sort_order if sort_order is not None else self.client.Movers.SortOrder.PERCENT_CHANGE_UP
        async with self._limiter:
            try:
                resp = await self.client.get_movers(
                    index=self.client.Movers.Index.EQUITY_ALL,
                    sort_order=effective_sort,
                    frequency=self.client.Movers.Frequency.TEN,
                )
            except OAuthError as exc:
                self._handle_oauth_error(exc)
        logger.debug(f"Response status code is {resp.status_code}")
        self._check_rate_limit(resp)
        resp.raise_for_status()
        return resp.json()
