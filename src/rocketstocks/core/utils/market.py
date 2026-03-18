import datetime
import logging
import pandas as pd
import pandas_market_calendars as mcal

logger = logging.getLogger(__name__)


class MarketUtils:

    def __init__(self):
        try:
            self._calendar = mcal.get_calendar('NYSE')
        except Exception as e:
            logger.error(f"Failed to load NYSE calendar: {e}")
            self._calendar = None
        self._schedule: pd.DataFrame = pd.DataFrame()
        self._cached_date: datetime.date | None = None

    @property
    def calendar(self):
        return self._calendar

    @property
    def schedule(self) -> pd.DataFrame:
        return self._schedule

    @schedule.setter
    def schedule(self, new_sched: pd.DataFrame) -> None:
        self._schedule = new_sched

    def get_market_schedule(self, date: datetime.date) -> pd.DataFrame:
        try:
            return self.calendar.schedule(start_date=date, end_date=date, start='pre', end='post')
        except Exception as e:
            logger.error(f"get_market_schedule failed for {date}: {e}")
            return pd.DataFrame()

    def _refresh_schedule_if_needed(self) -> None:
        today = datetime.datetime.now(datetime.UTC).date()
        if self._cached_date != today:
            self._schedule = self.get_market_schedule(today)
            self._cached_date = today

    def market_open_today(self) -> bool:
        today = datetime.datetime.now(datetime.UTC).date()
        valid_days = self.calendar.valid_days(start_date=today, end_date=today)
        return today in valid_days.date

    def market_open_on_date(self, date: datetime.date) -> bool:
        return date in self.calendar.valid_days(start_date=date, end_date=date).date

    def in_premarket(self) -> bool:
        now = datetime.datetime.now(datetime.UTC)
        if self._schedule.size > 0:
            premarket_start = self._schedule['pre'].iloc[0]
            intraday_start = self._schedule['market_open'].iloc[0]
            return now > premarket_start and now < intraday_start
        return False

    def in_intraday(self) -> bool:
        now = datetime.datetime.now(datetime.UTC)
        if self._schedule.size > 0:
            intraday_start = self._schedule['market_open'].iloc[0]
            aftermarket_start = self._schedule['market_close'].iloc[0]
            return now > intraday_start and now < aftermarket_start
        return False

    def in_aftermarket(self) -> bool:
        now = datetime.datetime.now(datetime.UTC)
        if self._schedule.size > 0:
            aftermarket_start = self._schedule['market_close'].iloc[0]
            market_end = self._schedule['post'].iloc[0]
            return now > aftermarket_start and now < market_end
        return False

    def get_market_period(self) -> str:
        self._refresh_schedule_if_needed()

        if self.in_premarket():
            return "premarket"
        elif self.in_intraday():
            return "intraday"
        elif self.in_aftermarket():
            return "aftermarket"
        else:
            return "EOD"

    def get_current_price(self, quote: dict) -> float:
        """Get the appropriate current price based on market period."""
        period = self.get_market_period()

        if period in ["premarket", "aftermarket"]:
            extended_price = quote.get('extended', {}).get('lastPrice')
            if extended_price and extended_price > 0:
                return extended_price

        quote_price = quote.get('quote', {}).get('lastPrice')
        if quote_price and quote_price > 0:
            return quote_price

        regular_price = quote.get('regular', {}).get('regularMarketLastPrice')
        if regular_price and regular_price > 0:
            return regular_price

        logger.debug(f"get_current_price: no valid price found in quote, returning 0.0")
        return 0.0

"""
# Backward-compat alias
market_utils = MarketUtils
"""