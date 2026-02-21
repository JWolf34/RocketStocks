import datetime
import logging
import pandas_market_calendars as mcal

logger = logging.getLogger(__name__)


class market_utils():

    def __init__(self):
        self._calendar = mcal.get_calendar('NYSE')
        now = datetime.datetime.now()
        self._schedule = self.get_market_schedule(now.date())

    @property
    def calendar(self):
        return self._calendar

    @property
    def schedule(self):
        return self._schedule

    @schedule.setter
    def schedule(self, new_sched):
        self._schedule = new_sched

    def get_market_schedule(self, date):
        schedule = self.calendar.schedule(start_date=date, end_date=date, start='pre', end='post')
        return schedule

    def market_open_today(self):
        today = datetime.datetime.now(datetime.UTC).date()
        valid_days = self.calendar.valid_days(start_date=today, end_date=today)
        return today in valid_days.date

    def market_open_on_date(self, date):
        return date in self.calendar.valid_days(start_date=date, end_date=date).date

    def in_extended_hours(self):
        return self.get_market_period() in ['premarket', 'aftermarket']

    def in_market_hours(self):
        return self.get_market_period() != 'EOD'

    def in_premarket(self):
        now = datetime.datetime.now(datetime.UTC)
        if self.schedule.size > 0:
            premarket_start = self.schedule['pre'].iloc[0]
            intraday_start = self.schedule['market_open'].iloc[0]
            return now > premarket_start and now < intraday_start
        else:
            return False

    def in_intraday(self):
        now = datetime.datetime.now(datetime.UTC)
        if self.schedule.size > 0:
            intraday_start = self.schedule['market_open'].iloc[0]
            aftermarket_start = self.schedule['market_close'].iloc[0]
            return now > intraday_start and now < aftermarket_start
        else:
            return False

    def in_aftermarket(self):
        now = datetime.datetime.now(datetime.UTC)
        if self.schedule.size > 0:
            aftermarket_start = self.schedule['market_close'].iloc[0]
            market_end = self.schedule['post'].iloc[0]
            return now > aftermarket_start and now < market_end
        else:
            return False

    def get_market_period(self):
        now = datetime.datetime.now()
        self._schedule = self.get_market_schedule(now.date())

        if self.in_premarket():
            return "premarket"
        elif self.in_intraday():
            return "intraday"
        if self.in_aftermarket():
            return "aftermarket"
        else:
            return "EOD"
