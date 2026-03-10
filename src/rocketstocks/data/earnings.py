import datetime
import logging
import pandas as pd
from rocketstocks.core.utils.dates import date_utils
from rocketstocks.core.utils.market import market_utils

logger = logging.getLogger(__name__)

_UPCOMING_COLS = [
    'date', 'ticker', 'time', 'fiscal_quarter_ending',
    'eps_forecast', 'no_of_ests', 'last_year_eps', 'last_year_rpt_dt',
]
_HISTORICAL_COLS = ['date', 'ticker', 'eps', 'surprise', 'epsforecast', 'fiscalquarterending']


class Earnings:
    def __init__(self, nasdaq, db):
        self.nasdaq = nasdaq
        self.db = db
        self.mutils = market_utils()

    async def update_upcoming_earnings(self):
        """Identify upcoming earnings dates for all tickers and add to database"""
        logger.info("Updating upcoming earnings in database")

        column_map = {
            'symbol': 'ticker',
            'date': 'date',
            'time': 'time',
            'fiscalQuarterEnding': 'fiscal_quarter_ending',
            'epsForecast': 'eps_forecast',
            'noOfEsts': 'no_of_ests',
            'lastYearEPS': 'last_year_eps',
            'lastYearRptDt': 'last_year_rpt_dt',
        }

        for i in range(0, 50):
            date = datetime.datetime.today() + datetime.timedelta(days=i)
            if date.weekday() < 5:
                date_string = date_utils.format_date_ymd(date=date)
                earnings_data = self.nasdaq.get_earnings_by_date(date_string)
                logger.debug(f"Identified {len(earnings_data)} earnings on date {date_string}")

                if not earnings_data.empty:
                    earnings_data['date'] = date_string
                    earnings_data = earnings_data.filter(list(column_map.keys()))
                    earnings_data = earnings_data.rename(columns=column_map)

                    values = [tuple(row) for row in earnings_data.values]
                    cols = earnings_data.columns.to_list()
                    placeholders = ', '.join(['%s'] * len(cols))
                    col_list = ', '.join(cols)
                    await self.db.execute_batch(
                        f"INSERT INTO upcoming_earnings ({col_list}) VALUES ({placeholders}) "
                        "ON CONFLICT DO NOTHING",
                        values,
                    )
                    logger.info(f'Updated earnings for {date_string}')
        logger.info("Upcoming earnings have been updated!")

    async def fetch_upcoming_earnings(self) -> pd.DataFrame:
        rows = await self.db.execute(
            f"SELECT {', '.join(_UPCOMING_COLS)} FROM upcoming_earnings"
        )
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=_UPCOMING_COLS)

    async def get_next_earnings_date(self, ticker):
        """Retrieve next earnings date for the input ticker"""
        row = await self.db.execute(
            "SELECT date FROM upcoming_earnings WHERE ticker = %s",
            [ticker],
            fetchone=True,
        )
        return row[0] if row else None

    async def get_next_earnings_info(self, ticker):
        """Retrieve information on upcoming earnings report for input ticker"""
        row = await self.db.execute(
            f"SELECT {', '.join(_UPCOMING_COLS)} FROM upcoming_earnings WHERE ticker = %s",
            [ticker],
            fetchone=True,
        )
        if row is None:
            return None
        return dict(zip(_UPCOMING_COLS, row))

    async def remove_past_earnings(self):
        """Remove previous earnings from database"""
        logger.info("Removing upcoming earnings that have past")
        await self.db.execute(
            "DELETE FROM upcoming_earnings WHERE date < %s",
            [datetime.date.today()],
        )
        logger.info("Previous upcoming earnings removed from database")

    async def update_historical_earnings(self):
        """Update database with historical earnings records from the NASDAQ"""
        logger.info("Updating historical earnings in database...")
        column_map = {
            'date': 'date',
            'symbol': 'ticker',
            'eps': 'eps',
            'surprise': 'surprise',
            'epsForecast': 'epsforecast',
            'fiscalQuarterEnding': 'fiscalquarterending',
        }
        today = datetime.date.today()

        row = await self.db.execute(
            "SELECT date FROM historical_earnings ORDER BY date DESC LIMIT 1",
            fetchone=True,
        )
        if row is None:
            logger.info("No date found in historical_earnings table - use default 1/3/2008")
            start_date = datetime.date(year=2008, month=1, day=3)
        else:
            start_date = row[0]
            logger.info(f"Last earnings date recorded is {date_utils.format_date_mdy(start_date)}")

        num_days = (today - start_date).days
        for i in range(1, num_days):
            date = start_date + datetime.timedelta(days=i)
            if self.mutils.market_open_on_date(date):
                date_string = date_utils.format_date_ymd(date)
                earnings = self.nasdaq.get_earnings_by_date(date_string)

                if earnings.size > 0:
                    earnings = earnings.rename(columns=column_map)
                    earnings = earnings.drop(
                        columns=[x for x in earnings.columns.to_list() if x not in column_map.values()]
                    )
                    earnings['date'] = date
                    earnings = earnings[column_map.values()]

                    earnings['eps'] = earnings['eps'].apply(
                        lambda x: float(x.replace('(', '-').replace(")", "").replace('$', "").replace(',', ""))
                        if (len(x) > 0 and x != "N/A") else None
                    )
                    earnings['epsforecast'] = earnings['epsforecast'].apply(
                        lambda x: float(x.replace('(', '-').replace(")", "").replace('$', "").replace(',', ""))
                        if (len(x) > 0 and x != "N/A") else None
                    )
                    earnings['surprise'] = earnings['surprise'].apply(
                        lambda x: float(x) if x != 'N/A' else None
                    )

                    values = [tuple(row) for row in earnings.values]
                    await self.db.execute_batch(
                        "INSERT INTO historical_earnings (date, ticker, eps, surprise, epsforecast, fiscalquarterending) "
                        "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                        values,
                    )
                    logger.info(f"Updated historical earnings for {date_string}")
                else:
                    logger.info(f"No earnings reported on date {date_string}")
            else:
                logger.info(f"Market is not open on {date} - no earning to pull")

    async def get_historical_earnings(self, ticker) -> pd.DataFrame:
        """Return earnings reports for input ticker"""
        logger.info(f"Fetching historical earnings for ticker '{ticker}' from database")
        rows = await self.db.execute(
            f"SELECT {', '.join(_HISTORICAL_COLS)} FROM historical_earnings WHERE ticker = %s",
            [ticker],
        )
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=_HISTORICAL_COLS)

    async def get_earnings_on_date(self, date: datetime.date) -> pd.DataFrame:
        """Return contents of all earnings that are due to release today"""
        logger.info(f"Fetching all earnings reported on date {date}")
        rows = await self.db.execute(
            f"SELECT {', '.join(_UPCOMING_COLS)} FROM upcoming_earnings WHERE date = %s",
            [date],
        )
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=_UPCOMING_COLS)
