import datetime
from db import Postgres
from nasdaq import Nasdaq
import logging
import pandas as pd
from utils import date_utils, market_utils
from sec import SEC
from schwab import Schwab
import time
import yfinance as yf

# Logging configuration
logger = logging.getLogger(__name__)


class Earnings:
    def __init__(self, nasdaq:Nasdaq, db:Postgres):
        self.nasdaq = nasdaq
        self.db = db

    def update_upcoming_earnings(self):
        """Identify upcoming earnings dates for all tickers and add to database"""
        logger.info("Updating upcoming earnings in database")

        columns = ['symbol',
                    'date',
                    'time',
                    'fiscalQuarterEnding',
                    'epsForecast',
                    'noOfEsts',
                    'lastYearRptDt', 
                    'lastYearEPS']
        
        for i in range(0, 50): # Look at next 50 days of earnings
            date = datetime.datetime.today() + datetime.timedelta(days=i)
            if date.weekday() < 5:
                date_string = date.strftime("%Y-%m-%d")
                earnings_data = self.nasdaq.get_earnings_by_date(date_string)
                if earnings_data.size > 0:
                    earnings_data['date'] = date_string
                    earnings_data = earnings_data[columns]
                    earnings_data = earnings_data.rename(columns={'symbol':'ticker'})
                    values = [tuple(row) for row in earnings_data.values]
                    self.db.insert(table='upcoming_earnings', fields=earnings_data.columns.to_list(), values=values)
                    logger.debug(f'Updated earnings for {date_string}')
        logger.info("Upcoming earnings have been updated!")

    @staticmethod
    def get_next_earnings_date(ticker):
        """Retrieve next earnings date for the input ticker"""
        result = Postgres().select(table='upcoming_earnings',
                                        fields=['date'],
                                        where_conditions=[('ticker', ticker)], 
                                        fetchall=False)
        if result is None:
            return "N/A"
        else:
            return result[0]

    @staticmethod
    def get_next_earnings_info(self, ticker):
        columns = self.db.get_table_columns('upcoming_earnings')

        result = Postgres().select(table='upcoming_earnings',
                                        fields=columns, 
                                        where_conditions=[('ticker', ticker)], 
                                        fetchall=False)
        if result is None:
            return pd.DataFrame()
        else:
            return pd.DataFrame([result], columns=columns)

    @staticmethod
    def remove_past_earnings():
        logger.info("Removing upcoming earnings that have past")

        Postgres().delete(table='upcoming_earnings',
                            where_conditions=[('date', '<', datetime.date.today())])
        logger.info("Previous upcoming earnings removed from database")

    @staticmethod
    def update_historical_earnings():
        logger.info("Updating historical earnings in database...")
        column_map = {'date':'date',
                        'symbol':'ticker',
                        'eps':'eps',
                        'surprise':'surprise',
                        'epsForecast':'epsForecast',
                        'fiscalQuarterEnding':'fiscalQuarterEnding'}
        today = datetime.date.today()
        
        # Get most recently inserted date in database
        select_script = """SELECT date FROM historical_earnings
                            ORDER BY date DESC;
                            """
        result = Postgres().select(table='historical_earnings',
                                        fields=['date'],
                                        order_by=('date', 'DESC'),
                                        fetchall=False)

        if result is None:
            start_date = datetime.date(year=2008, month=1, day=3) # Earliest day I can find earnings for on Nasdaq 1/3/2008
        else:
            start_date = result[0]

        num_days = (today - start_date).days
        for i in range(1, num_days):
            date = start_date + datetime.timedelta(days=i)
            if market_utils.market_open_on_date(date):
                date_string = date_utils.format_date_ymd(date)
                earnings = self.nasdaq.get_earnings_by_date(date_string)
                if earnings.size > 0:
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

                    values = [tuple(row) for row in earnings.values]
                    Postgres().insert(table='historical_earnings', fields=earnings.columns.to_list(), values=values)
                    logger.debug(f"Updated historical earnings for {date_string}")
                else: # No earnings recorded on target date
                    logger.debug(f"No earnings reported on date {date_string}")
            else: # Market is not open on target date
                logger.debug(f"Market is not open on {date_string} - no earning to pull")


    @staticmethod
    def get_historical_earnings(ticker):
        logger.debug(f"Fetching historical earnings for ticker '{ticker}' from database")
        columns = Postgres().get_table_columns('historical_earnings')
        results = Postgres().select(table='historical_earnings',
                                    fields=columns,
                                    where_conditions=[('ticker', ticker)], 
                                    fetchall=True)
        if results is None:
            return pd.DataFrame()
        else:
            return pd.DataFrame(results, columns=columns)

    @staticmethod
    def get_earnings_today(date):
        logger.debug(f"Fetching all earnings reported on date {date}")
        columns = Postgres().get_table_columns('upcoming_earnings')
        results = Postgres().select(table='upcoming_earnings',
                                    fields=columns, 
                                    where_conditions=[('date', date)],
                                    fetchall=True)
        if results is None:
            return results
        else:
            return pd.DataFrame(results, columns=columns)



    
class StockData():
    def __init__(self):
        self.db = Postgres
        self.sec = SEC()
        self.schwab = Schwab()
        self.nasdaq = Nasdaq()       

    @staticmethod
    def update_tickers():
        logger.info("Updating tickers database table with up-to-date ticker data")
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
        logger.debug("Fetched latest tickers from NASDAQ")
        tickers_data = tickers_data[tickers_data['symbol'].isin(StockData.get_all_tickers())]
        tickers_data = tickers_data.drop(columns=drop_columns)
        tickers_data = tickers_data.rename(columns=column_map)
        tickers_data = tickers_data[column_map.values()]
        
        update_script = f"""UPDATE tickers SET
                            {",".join(f"{column} = (%s)" for column in tickers_data.columns.to_list()[:-1])}
                            WHERE ticker = (%s);
                            """
        values = [tuple(row) for row in tickers_data.values]

        for row in tickers_data.values:
            ticker = row[0]
            set_fields = [(tickers_data.columns.to_list()[i], row[i]) for i in range(0, row.size)]
            Postgres().update(table='tickers',
                            set_fields=set_fields,
                            where_conditions=[('ticker', ticker)])
            logger.debug(f"Updated ticker '{ticker}' in database")
        logger.info("Tickers have been updated!")
    
    @staticmethod
    async def insert_new_tickers(self):
        logger.info("Updating tickers database table with up-to-date ticker data")
        column_map = {'symbol':'ticker',
                      'name':'name',
                      'marketCap':'marketcap',
                      'country':'country',
                      'ipoyear':'ipoyear',
                      'industry':'industry',
                      'sector':'sector',
                      'url':'nasdaqendpoint',
                      'cik':'cik'}
        drop_columns = ['lastsale',
                        'netchange',
                        'pctchange',
                        'volume']
        tickers_data = self.nasdaq.get_all_tickers()
        logger.debug("Fetched latest tickers from NASDAQ")
        tickers_data = tickers_data[~tickers_data['symbol'].isin(StockData.get_all_tickers())]
        tickers_data = tickers_data.drop(columns=drop_columns)
        tickers_data = tickers_data.rename(columns=column_map)
        cik_series = pd.Series(name='cik', index=tickers_data.index)
        for i in range(0, tickers_data['ticker'].size):
            ticker = tickers_data['ticker'].iloc[i]
            logger.debug(f"Getting CIK value for ticker '{ticker}'")
            cik_series[i] = self.sec.get_cik_from_ticker(ticker)
        tickers_data = tickers_data.join(cik_series)
        values = [tuple(row) for row in tickers_data.values]
        Postgres().insert(table='tickers', fields=tickers_data.columns.to_list(), values=values)
        logger.info("Tickers have been updated!")
    
    @staticmethod
    async def update_daily_price_history():
        logger.info(f"Updating daily price history for all tickers")
        tickers = StockData.get_all_tickers()
        num_tickers = len(tickers)
        curr_ticker = 1
        for ticker in tickers:
            logger.debug(f"Inserting daily price data for ticker {ticker}, {curr_ticker}/{num_tickers}")
            await StockData.update_daily_price_history_by_ticker(ticker)
            curr_ticker += 1
        logger.info("Completed update to daily price history in database")

    @staticmethod
    async def update_daily_price_history_by_ticker(ticker):
        """SELECT date FROM daily_price_history
           WHERE ticker = '{ticker}'
           ORDER BY date DESC;
           """
        result = Postgres().select(table='daily_price_history',
                                   fields=['date'],
                                   where_conditions=[('ticker', ticker)],
                                   order_by=('date', 'DESC'),
                                   fetchall=False)
        if not result:
            start_datetime = datetime.datetime(year=2000, month=1, day=1) # No data found
        else:
            start_datetime = datetime.datetime.combine(result[0], datetime.time(hour=0, minute=0, second=0))
        price_history = await self.schwab.get_daily_price_history(ticker, start_datetime=start_datetime)
        if price_history.size > 0:
            
            #price_history['date'] = price_history['date'].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d").date())
            fields = price_history.columns.to_list()
            values = [tuple(row) for row in price_history.values]
            Postgres().insert(table='daily_price_history', fields=fields, values=values)
        else:
            logger.warning(f"No daily price history found for ticker {ticker}")
    

    @staticmethod
    async def update_5m_price_history():
        logger.info(f"Updating 5m price history for all tickers")

        tickers = StockData.get_all_tickers()
        num_tickers = len(tickers)
        curr_ticker = 1
        start =  time.time()
        for ticker in tickers:
            logger.debug(f"Inserting 5m price data for ticker {ticker}, {curr_ticker}/{num_tickers}")
            await StockData.update_5m_price_history_by_ticker(ticker)
            curr_ticker += 1

        end = time.time()
        elapsed = end-start
        logger.info(f"Done! Time elapsed: {end-start} seconds")
        logger.info("Completed update to 5m price history in database")
    
    @staticmethod
    async def update_5m_price_history_by_ticker(ticker):
        # Get datetime of most recently inserted data
        """SELECT datetime FROM five_minute_price_history
           WHERE ticker = '{ticker}'
           ORDER BY datetime DESC;
            """
        result = Postgres().select(table='five_minute_price_history',
                                   fields=['datetime'],
                                   where_conditions=[('ticker', ticker)],
                                   order_by=('datetime', 'DESC'),
                                   fetchall=False)
        if result is None:
            start_datetime = result # No data found
        else:
            start_datetime = result[0]
        price_history = await self.schwab.get_5m_price_history(ticker, start_datetime=start_datetime)
        if price_history.size > 0:
            fields = price_history.columns.to_list()
            values = [tuple(row) for row in price_history.values]
            Postgres().insert(table='five_minute_price_history', fields=fields, values=values)
        else:
            logger.warning(f"No 5m price history found for ticker {ticker}")
    
    
    @staticmethod
    def fetch_daily_price_history(ticker, start_date:datetime.date = None, end_date:datetime.date = None):
        logger.debug(f"Fetching daily price history for ticker '{ticker}' from database")
        """SELECT * FROM daily_price_history
           WHERE ticker = '{ticker}';
           """
        where_conditions = [('ticker', ticker)]

        if start_date is not None:
            where_conditions.append(('date', '>', start_date))
        if end_date is not None:
            where_conditions.append(('date', '<', end_date))

        results = Postgres().select(table='daily_price_history',
                                    fields=['ticker', 'open', 'high', 'low', 'close', 'volume', 'date'],
                                    where_conditions=where_conditions,
                                    fetchall=True)
        if results is None:
            logger.debug(f"No daily price history available for ticker '{ticker}'")
            return pd.DataFrame()
        else:
            logger.debug(f"Returned {len(results)} row(s) for ticker '{ticker}'")
            columns = Postgres().get_table_columns("daily_price_history")
            return pd.DataFrame(results, columns=columns)

    @staticmethod
    def fetch_5m_price_history(ticker, start_datetime:datetime.datetime = None, end_datetime:datetime.datetime = None):
        logger.debug(f"Fetching 5m price history for ticker '{ticker}' from database")
        """SELECT * FROM five_minute_price_history
           WHERE ticker = '{ticker}';
           """

        where_conditions = [('ticker', ticker)]

        if start_datetime is not None:
            where_conditions.append(('datetime', '>=', start_datetime))
        if end_datetime is not None:
            where_conditions.append(('datetime', '<=', end_datetime))

        results = Postgres().select(table='five_minute_price_history',
                                    fields=['ticker', 'open', 'high', 'low', 'close', 'volume', 'datetime'],
                                    where_conditions=where_conditions,
                                    fetchall=True)
        if results is None:
            logger.debug(f"No 5m price history available for ticker '{ticker}'")
            return pd.DataFrame()
        else:
            logger.debug(f"Returned {len(results)} row(s) for ticker '{ticker}'")
            columns = Postgres().get_table_columns('five_minute_price_history')
            return pd.DataFrame(results, columns=columns)

    # Download and write financials for specified ticker to the financials folder
    @staticmethod
    def fetch_financials(ticker):
        logger.debug(f"Fetching financials for ticker {ticker}")
        financials = {}
        stock = yf.Ticker(ticker)
        financials['income_statment'] = stock.income_stmt
        financials['quarterly_income_statement'] = stock.quarterly_income_stmt
        financials['balance_sheet'] = stock.balance_sheet
        financials['quarterly_income_statement'] = stock.quarterly_balance_sheet
        financials['cash_flow']=stock.cashflow
        financials['quarterly_cash_flow'] = stock.quarterly_cashflow

        return financials


    @staticmethod
    def get_ticker_info(ticker):
        logger.debug(f"Fetching info for ticker '{ticker}' from database")
        """SELECT * FROM tickers
           WHERE ticker = '{ticker}';
           """
        
        fields = Postgres().get_table_columns('tickers')
        return Postgres().select(table='tickers',
                                 fields=fields,
                                 where_conditions=[('ticker', ticker)],
                                 fetchall=False)
    
    @staticmethod
    def get_all_ticker_info():
        logger.debug(f"Fetching info for all tickers in database")
        columns = Postgres().get_table_columns('tickers')
        data = Postgres().select(table='tickers',
                                 fields=columns,
                                 fetchall=True)
        data = pd.DataFrame(data, columns=columns)
        data.index = data['ticker']
        return data
    
    @staticmethod
    def get_all_tickers():
        logger.debug('Fetching all tickers in database')
        """SELECT ticker FROM tickers;
        """
        results = Postgres().select(table='tickers',
                                    fields=['ticker'],
                                    fetchall=True)
        return [result[0] for result in results]

    @staticmethod
    def get_all_tickers_by_market_cap(market_cap):
        logger.debug(f"Fetching all tickers with market cap > {market_cap} from database")
        """SELECT ticker, marketcap FROM tickers;
        """
        results = Postgres().select(table='tickers',
                                    fields=['ticker', 'marketcap'],
                                    fetchall=True)
        tickers = []
        for result in results:
            if len(result[1]) > 0: # Market cap is not empty string
                if float(result[1]) >= market_cap:
                    tickers.append(result[0])
        return tickers

    @staticmethod
    def get_all_tickers_by_sector(sector):
        logger.debug(f"Fetching all tickers in sector {sector} from database")
        select_script = f"""SELECT ticker FROM tickers
                           WHERE sector LIKE '%{sector}%';
                        """
        results = Postgres().select(table='tickers',
                                    fields=['tickers'],
                                    where_conditions=[('sector', 'LIKE', f"%{sector}%")])
        if results is None:
            return results
        else:
            return [result[0] for result in results]

    @staticmethod
    def get_cik(ticker):
        logger.debug(f"Retreiving CIK value for ticker '{ticker}' from database")
        """SELECT cik from tickers
           WHERE ticker = '{ticker}';
           """
        result = Postgres().select(table='tickers',
                                   fields=['cik'],
                                   where_conditions=[('ticker', ticker)], 
                                   fetchall=False)
        if result is None:
            return None
        else:
            return result[0]

    @staticmethod
    def get_market_cap(ticker):
        logger.debug(f"Retreiving market cap for ticker '{ticker}' from database")
        """SELECT marketcap from tickers
           WHERE ticker = '{ticker}';
           """
        result = Postgres().select(table='tickers',
                                   fields=['marketcap'],
                                   where_conditions=[('ticker', ticker)], 
                                   fetchall=False)
        if result is None:
            return None
        else:
            return float(result[0])

    @staticmethod
    def get_historical_popularity(ticker=None):
        logger.debug(f"Retrieving historical popularity {f"for ticker '{ticker}'" if ticker is not None else ''} from database")
        columns = Postgres().get_table_columns('popular_stocks')
        where_conditions = []
        order_by = ('date', 'DESC')
        select_script = """SELECT * from popular_stocks\n"""
        if ticker != None:
            where_conditions.append(('ticker', ticker))
        
    
        results = Postgres().select(table='popular_stocks',
                                    fields=columns,
                                    where_conditions=where_conditions,
                                    order_by=order_by,
                                    fetchall=True)
        if results is None:
            return results
        else:
            return pd.DataFrame(results, columns=columns)



    # Confirm we get valid data back when downloading data for ticker
    @staticmethod
    def validate_ticker(ticker):
        logger.debug("Verifying that ticker {} is valid".format(ticker))
        """SELECT ticker FROM tickers
           WHERE ticker = '{ticker}';
           """
        ticker = Postgres().select(table='tickers',
                                       fields=['ticker'],
                                       where_conditions=[('ticker', ticker)],
                                       fetchall=False)
        if ticker is None:
            return False
        else:
            return True
    
    # Get list of valid tickers from string
    @staticmethod
    def get_valid_tickers(ticker_string:str):
        logger.debug(f"Parsing valid tickers from string: '{ticker_string}'")
        tickers = ticker_string.split()
        valid_tickers = []
        invalid_tickers = []
        for ticker in tickers:
            if StockData.validate_ticker(ticker):
                valid_tickers.append(ticker)
            else:
                invalid_tickers.append(ticker)
        logger.debug(f"Parsed valid tickers: {valid_tickers}, invalid tickers: {invalid_tickers}")
        return valid_tickers, invalid_tickers

    @staticmethod
    # Return supported exchanges
    def get_supported_exchanges():
        return ['NASDAQ', 'NYSE', 'AMEX']
