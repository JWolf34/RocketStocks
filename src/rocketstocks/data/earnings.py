import datetime
import logging
import pandas as pd
from rocketstocks.core.utils.dates import date_utils
from rocketstocks.core.utils.market import market_utils

# Logging configuration
logger = logging.getLogger(__name__)


class Earnings:
    def __init__(self, nasdaq, db):
        self.nasdaq = nasdaq
        self.db = db
        self.mutils = market_utils()

    def update_upcoming_earnings(self):
        """Identify upcoming earnings dates for all tickers and add to database"""
        logger.info("Updating upcoming earnings in database")

        # Columns map
        column_map = {'symbol':'ticker',
                      'date':'date',
                      'time':'time',
                      'fiscalQuarterEnding':'fiscal_quarter_ending',
                      'epsForecast':'eps_forecast',
                      'noOfEsts':'no_of_ests',
                      'lastYearEPS':'last_year_eps',
                      'lastYearRptDt':'last_year_rpt_dt'}

        for i in range(0, 50): # Look at next 50 days of earnings
            date = datetime.datetime.today() + datetime.timedelta(days=i)
            # Only iterate through weekdays since earnings won't be published on weekends
            if date.weekday() < 5:
                date_string = date_utils.format_date_ymd(date=date)
                earnings_data = self.nasdaq.get_earnings_by_date(date_string)
                logger.debug(f"Identified {len(earnings_data)} earnings on date {date_string}")

                # Earnings data found - cleanup data and write to db
                if not earnings_data.empty:
                    # Create date column
                    earnings_data['date'] = date_string

                    # Filter out unwanted columns and rename remaining columns
                    earnings_data = earnings_data.filter(list(column_map.keys()))
                    earnings_data = earnings_data.rename(columns=column_map)

                    values = [tuple(row) for row in earnings_data.values]
                    self.db.insert(table='upcoming_earnings', fields=earnings_data.columns.to_list(), values=values)
                    logger.info(f'Updated earnings for {date_string}')
        logger.info("Upcoming earnings have been updated!")

    def fetch_upcoming_earnings(self):

        # Query
        '''SELECT * FROM upcoming_earnings;'''
        columns = self.db.get_table_columns(table='upcoming_earnings')
        results = self.db.select(table='upcoming_earnings',
                                 fields=columns)

        if not results:
            return pd.DataFrame()
        else:
            return pd.DataFrame(results, columns=columns)

    def get_next_earnings_date(self,ticker):
        """Retrieve next earnings date for the input ticker"""
        result = self.db.select(table='upcoming_earnings',
                                        fields=['date'],
                                        where_conditions=[('ticker', ticker)],
                                        fetchall=False)
        if not result:
            return None
        else:
            return result[0]

    def get_next_earnings_info(self, ticker):
        '''Retrieve information on upcoming earnings report for input ticker'''
        columns = self.db.get_table_columns('upcoming_earnings')

        result = self.db.select(table='upcoming_earnings',
                                        fields=columns,
                                        where_conditions=[('ticker', ticker)],
                                        fetchall=False)
        if result is None:
            return None
        else:
            return {field:value for field, value in zip(columns, result)}

    def remove_past_earnings(self):
        """Remove previous earnigs from database"""
        logger.info("Removing upcoming earnings that have past")
        self.db.delete(table='upcoming_earnings',
                            where_conditions=[('date', '<', datetime.date.today())])
        logger.info("Previous upcoming earnings removed from database")

    async def update_historical_earnings(self):
        """Update database with historical earnings records from the NASDAQ"""
        logger.info("Updating historical earnings in database...")
        column_map = {'date':'date',
                        'symbol':'ticker',
                        'eps':'eps',
                        'surprise':'surprise',
                        'epsForecast':'epsForecast',
                        'fiscalQuarterEnding':'fiscalQuarterEnding'}
        today = datetime.date.today()

        # Get most recently inserted date in database
        result = self.db.select(table='historical_earnings',
                                        fields=['date'],
                                        order_by=('date', 'DESC'),
                                        fetchall=False)

        if result is None:
            logger.info("No date found in historical_earnings table - use default 1/3/2008")
            start_date = datetime.date(year=2008, month=1, day=3) # Earliest day I can find earnings for on Nasdaq 1/3/2008
        else:
            start_date = result[0]
            logger.info(f"Last earnings date recorded is {date_utils.format_date_mdy(start_date)}")

        # Iterate over each day to find earnings reported on that day and write to database
        num_days = (today - start_date).days
        for i in range(1, num_days):
            date = start_date + datetime.timedelta(days=i)
            if self.mutils.market_open_on_date(date):
                date_string = date_utils.format_date_ymd(date)
                earnings = self.nasdaq.get_earnings_by_date(date_string)

                # At least one earnings report found
                if earnings.size > 0:
                    # Format df columns and add date column
                    earnings = earnings.rename(columns=column_map)
                    earnings = earnings.drop(columns=[x for x in earnings.columns.to_list() if x not in column_map.values()])
                    earnings['date'] = date
                    earnings = earnings[column_map.values()]

                    # Format EPS and surprise columns
                    earnings ['eps'] = earnings['eps'].apply(lambda x: float(x.replace('(', '-')
                                                                            .replace(")", "")
                                                                            .replace('$', "")
                                                                            .replace(',',""))
                                                                            if (len(x) > 0 and x != "N/A") else None)
                    earnings ['epsForecast'] = earnings['epsForecast'].apply(lambda x: float(x.replace('(', '-')
                                                                            .replace(")", "")
                                                                            .replace('$', "")
                                                                            .replace(',',""))
                                                                            if (len(x) > 0 and x != "N/A") else None)
                    earnings ['surprise'] = earnings['surprise'].apply(lambda x: float(x) if x != 'N/A' else None)

                    # Identify values and write to database
                    values = [tuple(row) for row in earnings.values]
                    self.db.insert(table='historical_earnings', fields=earnings.columns.to_list(), values=values)
                    logger.info(f"Updated historical earnings for {date_string}")
                else: # No earnings recorded on target date
                    logger.info(f"No earnings reported on date {date_string}")
            else: # Market is not open on target date
                logger.info(f"Market is not open on {date} - no earning to pull")

    def get_historical_earnings(self, ticker):
        """Return earnings reports for input ticker"""
        logger.info(f"Fetching historical earnings for ticker '{ticker}' from database")
        columns = self.db.get_table_columns('historical_earnings')
        results = self.db.select(table='historical_earnings',
                                    fields=columns,
                                    where_conditions=[('ticker', ticker)],
                                    fetchall=True)
        if not results:
            return pd.DataFrame()
        else:
            return pd.DataFrame(results, columns=columns)

    def get_earnings_on_date(self, date:datetime.date):
        """Return contents of all earnings that are due to release today"""
        logger.info(f"Fetching all earnings reported on date {date}")
        columns = self.db.get_table_columns('upcoming_earnings')
        results = self.db.select(table='upcoming_earnings',
                                    fields=columns,
                                    where_conditions=[('date', date)],
                                    fetchall=True)
        # B14 fix: always return DataFrame for consistent return type
        if not results:
            return pd.DataFrame()
        else:
            return pd.DataFrame(results, columns=columns)
