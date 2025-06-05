import datetime
from db import Postgres
from nasdaq import Nasdaq
from capitol_trades import CapitolTrades
from stockdata.watchlists import Watchlists
from trading_view import TradingView
from ape_wisdom import ApeWisdom
import logging
import pandas as pd
from RocketStocks.utils import date_utils, market_utils
from sec import SEC
from schwab_client import Schwab
import time
import yfinance as yf

# Logging configuration
logger = logging.getLogger(__name__)


class Earnings:
    def __init__(self, nasdaq:Nasdaq, db:Postgres):
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

    def update_historical_earnings(self):
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
                logger.info(f"Market is not open on {date_string} - no earning to pull")

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
        if not results:
            return results
        else:
            return pd.DataFrame(results, columns=columns)

class StockData():
    def __init__(self):
        self.db = Postgres()
        self.sec = SEC(sd=self)
        self.schwab = Schwab()
        self.nasdaq = Nasdaq() 
        self.earnings = Earnings(nasdaq=self.nasdaq, db=self.db)  
        self.capitol_trades = CapitolTrades(db=self.db)
        self.watchlists = Watchlists(self.db)
        self.trading_view = TradingView()
        self.popularity = ApeWisdom()
        self._alert_tickers = {}

    @property
    def alert_tickers(self):
        return self._alert_tickers
    
    def update_alert_tickers(self, tickers:list, source:str):
        self._alert_tickers[source] = tickers

        

    def update_tickers(self):
        """Update tickers table with the most up-to-date information from the NASDAQ"""
        logger.info("Updating tickers database table with up-to-date ticker data")

        start_time = time.time()

        # Map columns names and mark columns to drop from df
        column_map = {'name':'name',
                      'marketCap':'marketcap',
                      'country':'country',
                      'ipoyear':'ipoyear',
                      'industry':'industry',
                      'sector':'sector',
                      'url':'nasdaqendpoint',
                      'symbol':'ticker'}
        drop_columns = ['lastsale',
                        'netchange',
                        'pctchange',
                        'volume']
        tickers_data = self.nasdaq.get_all_tickers()
        logger.debug(f"Found {len(tickers_data)} tickers on NASDAQ")
        tickers_data = tickers_data[tickers_data['symbol'].isin(self.get_all_tickers())]
        tickers_data = tickers_data.drop(columns=drop_columns)
        tickers_data = tickers_data.rename(columns=column_map)
        tickers_data = tickers_data[column_map.values()]

        
        # Update info for each ticker in database, if applicable
        for row in tickers_data.values:
            ticker = row[0]
            set_fields = [(tickers_data.columns.to_list()[i], row[i]) for i in range(0, row.size)]
            self.db.update(table='tickers',
                            set_fields=set_fields,
                            where_conditions=[('ticker', ticker)])
            logger.info(f"Updated ticker '{ticker}' in database:\n{row}")

        end_time = time.time()
        logger.info("Tickers have been updated!")
        logger.debug(f"Updating tickers completed in {time.strftime("H:M:S", end_time-start_time)}")
    
    async def insert_tickers(self):
        """Identify data on all tickers from the SEC to update the database with"""
        logger.info("Updating tickers database table with new tickers from SEC")

        start_time = time.time()

        # Get tickers from SEC and format
        sec_tickers = self.sec.get_company_tickers()
        sec_column_map = {'ticker':'ticker',
                          'cik_str':'cik'}
        sec_tickers = sec_tickers.filter(list(sec_column_map.keys()))
        sec_tickers = sec_tickers.rename(columns=sec_column_map)
        sec_tickers['cik'] = sec_tickers['cik'].apply(lambda cik: str(cik).zfill(10))
                               
        # Get tickers from NASDAQ and format
        nasdaq_tickers = self.nasdaq.get_all_tickers()
        nasdaq_column_map = {'symbol':'ticker',
                      'name':'name',
                      'country':'country',
                      'ipoyear':'ipoyear',
                      'industry':'industry',
                      'sector':'sector',
                      'url':'url'}
        nasdaq_tickers = nasdaq_tickers.filter(list(nasdaq_column_map.keys()))
        nasdaq_tickers = nasdaq_tickers.rename(columns=nasdaq_column_map)

        # Merge
        all_tickers = pd.merge(sec_tickers, nasdaq_tickers, on='ticker', how='left')
        all_tickers.set_index('ticker')
        
        # Identify values and append to database
        values = [tuple(row) for row in all_tickers.values]
        self.db.insert(table='tickers', fields=all_tickers.columns.to_list(), values=values)

        end_time = time.time()
        logger.info("Tickers have been updated!")
        logger.debug(f"Insert new tickers completed in {start_time-end_time} seconds")
    
    
    async def update_daily_price_history(self):
        """"Update database with latest daily price data on all tickers"""
        logger.info("Updating daily price history for all tickers")

        start_time = time.time()

        tickers = self.get_all_tickers()
        num_tickers = len(tickers)
        curr_ticker = 1
        for ticker in tickers:
            logger.info(f"Inserting daily price data for ticker {ticker}, {curr_ticker}/{num_tickers}")
            await self.update_daily_price_history_by_ticker(ticker)
            curr_ticker += 1

        end_time = time.time()
        logger.info("Completed update to daily price history in database")
        logger.debug(f"Updating daily price history completed in {time.strftime("H:M:S", end_time-start_time)}")

    async def update_daily_price_history_by_ticker(self,ticker):
        """Update database with latest daily price data for input ticker"""

        # Query
        """SELECT date FROM daily_price_history
           WHERE ticker = '{ticker}'
           ORDER BY date DESC;
           """
        result = self.db.select(table='daily_price_history',
                                   fields=['date'],
                                   where_conditions=[('ticker', ticker)],
                                   order_by=('date', 'DESC'),
                                   fetchall=False)
        if not result:
            start_datetime = datetime.datetime(year=2000, month=1, day=1) # No data found
            logger.debug(f"No daily price history for ticker {ticker} in database, fetching price history from default date {date_utils.format_date_mdy(start_datetime.date())}")
        else:
            start_datetime = datetime.datetime.combine(result[0], datetime.time(hour=0, minute=0, second=0))
            logger.debug(f"Latest recorded daily price history for {ticker} is {start_datetime.date()}")
        price_history = await self.schwab.get_daily_price_history(ticker, start_datetime=start_datetime)

        # Found price history for ticker, insert into database
        if not price_history.empty:
            fields = price_history.columns.to_list()
            values = [tuple(row) for row in price_history.values]
            self.db.insert(table='daily_price_history', fields=fields, values=values)
        # No daily price history found
        else:
            logger.warning(f"No daily price history found for ticker {ticker}")
    

    async def update_5m_price_history(self):
        """"Update database with latest 5m price data on all tickers"""
        logger.info("Updating 5m price history for all tickers")

        start_time = time.time()


        tickers = self.get_all_tickers()
        num_tickers = len(tickers)
        curr_ticker = 1
        for ticker in tickers:
            logger.debug(f"Inserting 5m price data for ticker {ticker}, {curr_ticker}/{num_tickers}")
            await self.update_5m_price_history_by_ticker(ticker)
            curr_ticker += 1

        end_time = time.time()
        logger.info("Completed update to 5m price history in database")
        logger.debug(f"Updating 5m price history completed in {time.strftime("H:M:S", end_time-start_time)}")
    
    async def update_5m_price_history_by_ticker(self, ticker):
        """Update database with latest 5m price data for input ticker"""

        # Query
        """SELECT datetime FROM five_minute_price_history
           WHERE ticker = '{ticker}'
           ORDER BY datetime DESC;
            """
        
        # Find latest date 5m price history is recorded for ticker
        # Use default date if no rows found in database
        result = self.db.select(table='five_minute_price_history',
                                   fields=['datetime'],
                                   where_conditions=[('ticker', ticker)],
                                   order_by=('datetime', 'DESC'),
                                   fetchall=False)
        if not result:
            start_datetime = result # No data found
            logger.debug(f"No 5m price history for ticker {ticker} in database, fetching price history from default date")
        else:
            start_datetime = result[0]
            logger.debug(f"Latest recorded 5m price history for {ticker} is {start_datetime.date}")

        price_history = await self.schwab.get_5m_price_history(ticker, start_datetime=start_datetime)

        # Found price history for ticker, insert into database
        if price_history:
            fields = price_history.columns.to_list()
            values = [tuple(row) for row in price_history.values]
            self.db.insert(table='five_minute_price_history', fields=fields, values=values)
        # No 5m price history found
        else:
            logger.warning(f"No 5m price history found for ticker {ticker}")
    
    def fetch_daily_price_history(self, ticker, start_date:datetime.date = None, end_date:datetime.date = None):
        """Return daily price history for input ticker from database"""
        logger.info(f"Fetching daily price history for ticker '{ticker}' from database")

        # Query
        """SELECT * FROM daily_price_history
           WHERE ticker = '{ticker}';
           """
        where_conditions = [('ticker', ticker)]

        if start_date is not None:
            where_conditions.append(('date', '>', start_date))
        if end_date is not None:
            where_conditions.append(('date', '<', end_date))

        results = self.db.select(table='daily_price_history',
                                    fields=['ticker', 'open', 'high', 'low', 'close', 'volume', 'date'],
                                    where_conditions=where_conditions,
                                    fetchall=True)
        if not results:
            logger.warning(f"No daily price history available for ticker '{ticker}'")
            return pd.DataFrame()
        else:
            logger.debug(f"Returned {len(results)} row(s) for ticker '{ticker}'")
            columns = self.db.get_table_columns("daily_price_history")
            return pd.DataFrame(results, columns=columns)

    def fetch_5m_price_history(self, ticker, start_datetime:datetime.datetime = None, end_datetime:datetime.datetime = None):
        """Return 5m price history for input ticker from daytabase"""
        logger.info(f"Fetching 5m price history for ticker '{ticker}' from database")

        # Query
        """SELECT * FROM five_minute_price_history
           WHERE ticker = '{ticker}';
           """

        where_conditions = [('ticker', ticker)]

        if start_datetime is not None:
            where_conditions.append(('datetime', '>=', start_datetime))
        if end_datetime is not None:
            where_conditions.append(('datetime', '<=', end_datetime))

        results = self.db.select(table='five_minute_price_history',
                                    fields=['ticker', 'open', 'high', 'low', 'close', 'volume', 'datetime'],
                                    where_conditions=where_conditions,
                                    fetchall=True)
        if not results:
            logger.warning(f"No 5m price history available for ticker '{ticker}'")
            return pd.DataFrame()
        else:
            logger.debug(f"Returned {len(results)} row(s) for ticker '{ticker}'")
            columns = self.db.get_table_columns('five_minute_price_history')
            return pd.DataFrame(results, columns=columns)

    
    @staticmethod
    def fetch_financials(ticker):

        """Return latest available financial statements for input ticker from Yahoo Finance"""
        logger.info(f"Fetching financials for ticker {ticker}")
        financials = {}
        stock = yf.Ticker(ticker)
        financials['income_statment'] = stock.income_stmt
        financials['quarterly_income_statement'] = stock.quarterly_income_stmt
        financials['balance_sheet'] = stock.balance_sheet
        financials['quarterly_income_statement'] = stock.quarterly_balance_sheet
        financials['cash_flow']=stock.cashflow
        financials['quarterly_cash_flow'] = stock.quarterly_cashflow

        return financials

    def get_ticker_info(self, ticker):
        """Return ticker row from database"""
        logger.info(f"Fetching info for ticker '{ticker}' from database")

        # Query
        """SELECT * FROM tickers
           WHERE ticker = '{ticker}';
           """
        
        fields = self.db.get_table_columns('tickers')
        result = self.db.select(table='tickers',
                                 fields=fields,
                                 where_conditions=[('ticker', ticker)],
                                 fetchall=False)
        
        return {field:value for field, value in zip(fields, result)}
    
    def get_all_ticker_info(self):
        """Return information (df) on all tickers from database"""
        logger.info(f"Fetching info for all tickers in database")
        columns = self.db.get_table_columns('tickers')
        data = self.db.select(table='tickers',
                                 fields=columns,
                                 fetchall=True)
        data = pd.DataFrame(data, columns=columns)
        data.set_index('ticker')
        logger.debug(f"Found data for {len(data)} tickers in database")
        return data
    
    def get_all_tickers(self):
        '''Return list of all tickers in database'''
        logger.info('Fetching all tickers in database')

        # Query
        """SELECT ticker FROM tickers;
        """
        results = self.db.select(table='tickers',
                                    fields=['ticker'],
                                    fetchall=True)
        return [result[0] for result in results]

    def get_all_tickers_by_market_cap(self, market_cap):
        """Return list of tickers with market cap greater than input market cap"""
        logger.info(f"Fetching all tickers with market cap > {market_cap} from database")

        # Query
        """SELECT ticker, marketcap FROM tickers;
        """

        results = self.db.select(table='tickers',
                                    fields=['ticker', 'marketcap'],
                                    fetchall=True)
        tickers = []
        for result in results:
            if result[1]: # Market cap is not empty string
                if float(result[1]) >= market_cap:
                    tickers.append(result[0])
        return tickers

    def get_all_tickers_by_sector(self, sector):
        """Return list of tickers whose sector matches the input sector"""
        logger.info(f"Fetching all tickers in sector {sector} from database")

        # Query
        """SELECT ticker FROM tickers
           WHERE sector LIKE '%{sector}%';
           
           """
        results = self.db.select(table='tickers',
                                    fields=['tickers'],
                                    where_conditions=[('sector', 'LIKE', f"%{sector}%")])
        
        return [result[0] for result in results] if results else None

    def get_cik(self, ticker):
        """Return CIK number of input ticker from database"""
        logger.info(f"Retreiving CIK value for ticker '{ticker}' from database")

        # Query
        """SELECT cik from tickers
           WHERE ticker = '{ticker}';
           """
        result = self.db.select(table='tickers',
                                   fields=['cik'],
                                   where_conditions=[('ticker', ticker)], 
                                   fetchall=False)
        return result[0] if result else None

    def get_market_cap(self, ticker):
        """Return market cap of input ticker in database"""

        logger.info(f"Retrieving market cap for ticker '{ticker}' from database")

        # Query
        """SELECT marketcap from tickers
           WHERE ticker = '{ticker}';
           """
        
        result = self.db.select(table='tickers',
                                   fields=['marketcap'],
                                   where_conditions=[('ticker', ticker)], 
                                   fetchall=False)
        return float(result[0]) if result else None

    def fetch_popularity(self, ticker=None):
        """Return historical popularity of input ticker from database"""
        logger.info(f"Retrieving historical popularity {ticker} from database" if ticker else "Retrieving all historical popularity from database")

        columns = self.db.get_table_columns('popularity')
        where_conditions = []
        order_by = ('datetime', 'DESC')
        if ticker:
            where_conditions.append(('ticker', ticker))
        
    
        results = self.db.select(table='popularity',
                                    fields=columns,
                                    where_conditions=where_conditions,
                                    order_by=order_by,
                                    fetchall=True)
        return pd.DataFrame(results, columns=columns) if results else pd.DataFrame()
    
    
    def insert_popularity(self, popular_stocks:pd.DataFrame):
        """Import new rows into popularity table"""
        logger.debug(f"Inserting new popularity data into database - {popular_stocks.shape[0]} rows")

        values = [tuple(row) for row in popular_stocks.values]
        self.db.insert(table='popularity', fields=popular_stocks.columns.to_list(), values=values)

        




    async def validate_ticker(self, ticker):
        """Returns true if ticker exists in database, else False"""
        logger.info(f"Verifying that ticker '{ticker}' is valid")

    
        '''Logic puling from database'''
     
        # Query
        
        """SELECT ticker FROM tickers
           WHERE ticker = '{ticker}';
           """
        ticker = self.db.select(table='tickers',
                                       fields=['ticker'],
                                       where_conditions=[('ticker', ticker)],
                                       fetchall=False)
        return True if ticker else False
        

        '''Logic checking from Schwab'''
        '''
        data = await self.schwab.get_daily_price_history(ticker=ticker,start_datetime=datetime.datetime.now() - datetime.timedelta(days=7))
        return True if not data.empty else False
        '''
    
    # Get list of valid tickers from string
    async def parse_valid_tickers(self, ticker_string:str):
        """Return list of valid tickers from string of comma-separated tickers"""
        logger.info(f"Parsing valid tickers from string: '{ticker_string}'")
        tickers = ticker_string.upper().split()
        valid_tickers = []
        invalid_tickers = []
        for ticker in tickers:
            if await self.validate_ticker(ticker):
                valid_tickers.append(ticker)
            else:
                invalid_tickers.append(ticker)
        logger.info(f"Parsed {len(valid_tickers)} valid tickers: {valid_tickers}, {len(invalid_tickers)} invalid tickers: {invalid_tickers}")
        return valid_tickers, invalid_tickers
    


if __name__ == '__main__':

    import asyncio
    sd = StockData()
    #sd.db.drop_all_tables()
    sd.db.create_tables()

    start = time.time()
    
    asyncio.run(sd.insert_tickers())
    print('hi')

    #sd.update_popularity(popular_stocks=popular_stocks)
    end = time.time()

    print(f"Function competed in {end-start} seconds")
        
   
