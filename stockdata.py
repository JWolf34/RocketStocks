import sys
sys.path.append('../RocketStocks/discord')
import yfinance as yf
from pandas_datareader import data as pdr
import pandas as pd
import pandas_ta as ta
import psycopg2
import strategies
from newsapi import NewsApiClient
import os
import datetime
from datetime import timedelta
import requests
from ratelimit import limits, sleep_and_retry
import config
from config import utils
import logging
from tradingview_screener import Scanner, Query, Column
import schwab

import httpx

# Logging configuration
logger = logging.getLogger(__name__)

class News():
    def __init__(self):
        self.token = config.get_news_api_token()
        self.news = NewsApiClient(api_key=config.get_news_api_token())
        self.categories= {'Business':'business',
                          'Entertainment':'entertainment',
                          'General':'eeneral',
                          'Health':'health',
                          'Science':'science',
                          'Sports':'sports',
                          'Technology':'technology'}
        self.sort_by = {'Relevancy':'relevancy',
                        'Popularity':'popularity',
                        'Publication Time':'publishedAt'}

    def get_sources(self):
        return self.news.get_sources()

    def get_news(self, query, **kwargs):
        return self.news.get_everything(q=query, language='en', **kwargs)
    
    def get_breaking_news(self, query, **kwargs):
        return self.news.get_top_headlines(q=query, **kwargs)

    def format_article_date(self, date):
        new_date = datetime.datetime.fromisoformat(date)
        return new_date.strftime("%m/%d/%y %H:%M:%S EST")

class Nasdaq():
    def __init__(self):
        self.url_base = "https://api.nasdaq.com/api"
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'}
        self.MAX_CALLS = 10
        self.MAX_PERIOD = 60

    @sleep_and_retry
    @limits(calls = 5, period = 60)
    def get_all_tickers(self):
        url = f"{self.url_base}/screener/stocks?tableonly=false&limit=25&download=true"
        data = requests.get(url, headers=self.headers).json()
        tickers = pd.DataFrame(data['data']['rows'])
        return tickers

    @sleep_and_retry
    @limits(calls = 5, period = 60) 
    def get_earnings_by_date(self, date):
        url = f"{self.url_base}/calendar/earnings"
        params = {'date':date}
        data = requests.get(url, headers=self.headers, params=params).json()
        if data['data'] is None:
            return pd.DataFrame()
        else:
            return pd.DataFrame(data['data']['rows'])
    
    @sleep_and_retry
    @limits(calls = 5, period = 60) 
    def get_earnings_forecast(self, ticker):
        url = f"https://api.nasdaq.com/api/analyst/{ticker}/earnings-forecast"
        data = requests.get(url, headers=self.headers).json()
        return data['data']

    def get_earnings_forecast_quarterly(self, ticker):
        return pd.DataFrame.from_dict(self.get_earnings_forecast(ticker)['quarterlyForecast']['rows'])
    
    def get_earnings_forecast_yearly(self, ticker):
        return pd.DataFrame.from_dict(self.get_earnings_forecast(ticker)['yearlyForecast']['rows'])

    @sleep_and_retry
    @limits(calls = 5, period = 60) 
    def get_eps(self, ticker):
        url = f"https://api.nasdaq.com/api/quote/{ticker}/eps"
        eps_request = requests.get(url, headers=self.headers)
        eps = pd.DataFrame.from_dict(eps_request.json()['data']['earningsPerShare'])
        return eps
        
    def get_prev_eps(self, ticker):
        eps = self.get_eps(ticker)
        return eps[eps['type'] == 'PreviousQuarter']

    def get_future_eps(self, ticker):
        eps = self.get_eps(ticker)
        return eps[eps['type'] == 'UpcomingQuarter']

class Postgres():
    def __init__(self):
        self.user = config.get_db_user()
        self.pwd = config.get_db_password()
        self.db = config.get_db_name()
        self.host = config.get_db_host()
        self.conn = None
        self.cur = None
        
    # Open connection to PostgreSQL database
    def open_connection(self):
        self.conn = psycopg2.connect(
            host =self.host,
            dbname = self.db,
            user = self.user,
            password = self.pwd,
            port = 5432)

        self.cur = self.conn.cursor()

    # Close connection to PostgreSQL database
    def close_connection(self):
        if self.cur is not None:
            self.cur.close()
            self.cur = None
        if self.conn is not None:
            self.conn.close()
            self.conn = None
    
    # Create database tables
    def create_tables(self):
        self.open_connection()
        create_script = """ CREATE TABLE IF NOT EXISTS tickers (
                            ticker          varchar(8) PRIMARY KEY,
                            name            varchar(255) NOT NULL,
                            marketCap       varchar(20) NOT NULL, 
                            country         varchar(40), 
                            ipoyear         char(4),
                            industry        varchar(64),
                            sector          varchar(64),
                            nasdaqEndpoint  varchar(64),
                            cik             char(10)
                            );

                            CREATE TABLE IF NOT EXISTS upcomingEarnings (
                            ticker              varchar(8) PRIMARY KEY,
                            date                date NOT NULL,
                            time                varchar(32),
                            fiscalQuarterEnding varchar(10),
                            epsForecast         varchar(8),
                            noOfEsts            varchar(8),
                            lastYearRptDt       varchar(10),
                            lastYearEPS         varchar(8)
                            );

                            CREATE TABLE IF NOT EXISTS watchlists (
                            ID                  varchar(255) PRIMARY KEY,
                            tickers             varchar(255),
                            systemGenerated     boolean
                            );
                            
                            CREATE TABLE IF NOT EXISTS popularstocks (
                            date                date,
                            ticker              varchar(8),
                            rank                int,
                            mentions            int,
                            upvotes             int,
                            PRIMARY KEY (date, ticker)
                            );

                            CREATE TABLE IF NOT EXISTS historicalearnings (
                            date                date,
                            ticker              varchar(8),
                            eps                 float,
                            surprise            float,
                            epsForecast         float,
                            fiscalQuarterEnding varchar(10),            
                            PRIMARY KEY (date, ticker)
                            );

                            CREATE TABLE IF NOT EXISTS reports(
                            type                varchar(64) PRIMARY KEY,
                            messageid           bigint
                            );

                            CREATE TABLE IF NOT EXISTS alerts(
                            date                date,
                            ticker              varchar(8),
                            alert_type          varchar(64),
                            messageid           bigint,
                            PRIMARY KEY (date, ticker, alert_type)
                            );

                            CREATE TABLE IF NOT EXISTS dailypricehistory(
                            ticker              varchar(8),
                            open                float,
                            high                float,
                            low                 float,
                            close               float,
                            volume              bigint,
                            datetime            date,
                            PRIMARY KEY (ticker, datetime)
                            );

                            CREATE TABLE IF NOT EXISTS fiveminutepricehistory(
                            ticker              varchar(8),
                            open                float,
                            high                float,
                            low                 float,
                            close               float,
                            volume              bigint,
                            datetime            timestamp,
                            PRIMARY KEY (ticker, datetime)
                            );
                            """
        logger.debug("Running script to create tables in database...")
        self.cur.execute(create_script)
        self.conn.commit()
        logger.debug("Create script completed successfully!")

        self.close_connection()
    
    def init_tables(self):
        logger.debug("Initlializing database tables")
        # Init reports
        table = 'reports'
        fields = ['type', 'messageid']
        values = [('PREMARKET_GAINER_REPORT', 0),
                  ('INTRADAY_GAINER_REPORT', 0),
                  ('AFTERHOURS_GAINER_REPORT', 0),
                  ('UNUSUAL_VOLUME_REPORT', 0)]
        self.insert(table=table, fields=fields, values=values)
    
    # Drop database tables
    def drop_all_tables(self):
        self.open_connection()
        drop_script = """DROP TABLE upcomingearnings;
                         DROP TABLE tickers;
                         DROP TABLE watchlists;
                        """

        self.cur.execute(drop_script)
        self.conn.commit()
        self.close_connection()
        logger.debug("All database tables dropped")
    
    def drop_table(self, table:str):
        self.open_connection()
        drop_script = f"""DROP TABLE IF EXISTS {table};
                       """
        self.cur.execute(drop_script)
        self.conn.commit()
        self.close_connection()
        logger.debug(f"Dropped table '{table}' from database")

    # Insert row(s) into database
    def insert(self, table:str, fields:list, values:list):
        self.open_connection()
        insert_script = f"""INSERT INTO {table} ({",".join(fields)})
                            VALUES({",".join(["%s"]*len(fields))})
                            ON CONFLICT DO NOTHING;
                            """
        for row in values:
            self.cur.execute(insert_script, row)

        self.conn.commit()
        logger.debug(f"Inserted new row in table {table}")
        self.close_connection()

    # Select a sigle row from database
    def select_one(self, query:str, ):
        self.open_connection()
        self.cur.execute(query)
        result = self.cur.fetchone()
        self.close_connection()
        return result

    # Select multiple rows from database
    def select_many(self, query:str):
        self.open_connection()
        self.cur.execute(query)
        results = self.cur.fetchall()
        self.close_connection()
        return results
    
    # Update row(s) in database
    def update(self, query:str, values):
        self.open_connection()
        for row in values:
            self.cur.execute(query, row)
        self.conn.commit()
        self.close_connection()
    
    # Delete row(s) from database
    def delete(self, table:str, where_condition:str):
        self.open_connection()
        delete_script = f"""DELETE FROM {table}
                            WHERE {where_condition};
                            """
        self.cur.execute(delete_script)
        self.conn.commit()
        self.close_connection()
        logger.debug(f"Deleted from table '{table}' where {where_condition}")

    # Return list of columns from selected table
    def get_table_columns(self, table):
        self.open_connection()
        select_script = f"""SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_NAME = '{table}'
                            ORDER BY ordinal_position;
                            """
        self.cur.execute(select_script)
        columns = [column[0] for column in self.cur.fetchall()]
        self.close_connection()
        return columns
  
class Watchlists():
    def __init__(self):
        self.db_table = 'watchlists'
        self.db_fields = ['id', 'tickers', 'systemGenerated']
        
    # Return tickers from watchlist - global by default, personal if chosen by user
    def get_tickers_from_watchlist(self, watchlist_id):
        logger.debug("Fetching tickers from watchlist with ID '{}'".format(watchlist_id))
        
        select_script = f"""SELECT tickers FROM {self.db_table}
                            WHERE id = '{watchlist_id}';
                            """
        tickers = Postgres().select_one(query=select_script)
        if tickers is None:
            return tickers
        else:
            return sorted(tickers[0].split())
       

    # Return tickers from all available watchlists
    def get_tickers_from_all_watchlists(self, no_personal=True, no_systemGenerated=True):
        logger.debug("Fetching tickers from all available watchlists (besides personal)")
        select_script = f"""SELECT * FROM {self.db_table};
                            """
        watchlists = Postgres().select_many(query=select_script)
        tickers = []
        for watchlist in watchlists:
            watchlist_id, watchlist_tickers, is_systemGenerated = watchlist[0], watchlist[1].split(), watchlist[2]
            if watchlist_id.isdigit() and no_personal:
                pass
            elif is_systemGenerated and no_systemGenerated:
                pass
            else: 
                tickers += watchlist_tickers
                    
        return sorted(set(tickers))

    # Return list of existing watchlists
    def get_watchlists(self, no_personal=True, no_systemGenerated=True):
        logger.debug("Fetching all watchlists")
        select_script = f"""SELECT * FROM {self.db_table}"""
        filtered_watchlists = []
        watchlists = Postgres().select_many(query=select_script)
        for i in range(len(watchlists)):
            watchlist = watchlists[i]
            watchlist_id = watchlist[0]
            is_systemGenerated = watchlist[2]
            if watchlist_id.isdigit() and no_personal:
                pass
            elif is_systemGenerated and no_systemGenerated:
                pass
            else: 
                filtered_watchlists.append(watchlist_id)
        if no_personal:
            filtered_watchlists.append("personal")
        return sorted(filtered_watchlists)

    # Set content of watchlist to provided tickers
    def update_watchlist(self, watchlist_id, tickers):
        logger.info("Updating watchlist '{}': {}".format(watchlist_id, tickers))
        update_script = f""" UPDATE {self.db_table}
                             SET tickers = '{" ".join(tickers)}'
                             WHERE id = '{watchlist_id}';
                             """
        Postgres().update(query=update_script)

    # Create a new watchlist with id 'watchlist_id'
    def create_watchlist(self, watchlist_id, tickers, systemGenerated):
        logger.debug("Creating watchlist with ID '{}' and tickers {}".format(watchlist_id, tickers))
        Postgres().insert(table=self.db_table, fields=self.db_fields, values=[(watchlist_id, " ".join(tickers), systemGenerated)])

    # Delete watchlist with id 'watchlist_id'
    def delete_watchlist(self, watchlist_id):
        logger.debug("Deleting watchlist '{}'...".format(watchlist_id))
        Postgres().delete(table=self.db_table, where_condition=f"id = '{watchlist_id}'")

    # Validate watchlist exists in the database
    def validate_watchlist(self, watchlist_id):
        logger.info(f"Validating watchlist '{watchlist_id}' exists")
        select_script = f"""SELECT id FROM {self.db_table}
                            WHERE id = '{watchlist_id}';
                            """
        result = Postgres().select_one(select_script)
        if result is None:
            logger.warning(f"Watchlist '{watchlist_id}' does not exist")
            return False
        else:
            return True

    

class SEC():
    def __init__(self):
        self.headers = {"User-Agent":"johnmwolf34@gmail.com"}
        self.MAX_CALLS = 10
        self.MAX_CALLS = 1

    @sleep_and_retry
    @limits(calls = 5, period = 1) # 5 calls per 1 second
    def get_cik_from_ticker(self, ticker):
        logger.debug(f"Fetching CIK number for ticker {ticker}")
        tickers_data = requests.get("https://www.sec.gov/files/company_tickers.json", headers=self.headers).json()
        for company in tickers_data.values():
            if company['ticker'] == ticker:
                cik = str(company['cik_str']).zfill(10)
                return cik

    @sleep_and_retry
    @limits(calls = 5, period = 1) # 5 calls per 1 second
    def get_submissions_data(self, ticker):
        submissions_json = requests.get(f"https://data.sec.gov/submissions/CIK{StockData.get_cik(ticker)}.json", headers=self.headers).json()
        return submissions_json
    
    def get_recent_filings(self, ticker):
        return pd.DataFrame.from_dict(self.get_submissions_data(ticker)['filings']['recent'])

    def get_filings_from_today(self, ticker):
        recent_filings = self.get_recent_filings(ticker)
        today_string = datetime.datetime.today().strftime("%Y-%m-%d")
        return recent_filings.loc[recent_filings['filingDate'] == today_string]

    def get_link_to_filing(self, ticker, filing):
        return f"https://sec.gov/Archives/edgar/data/{StockData.get_cik(ticker).lstrip("0")}/{filing['accessionNumber'].replace("-","")}/{filing['primaryDocument']}"

    @sleep_and_retry
    @limits(calls = 5, period = 1) # 5 calls per second
    def get_accounts_payable(self, ticker):
        json = requests.get(f"https://data.sec.gov/api/xbrl/companyconcept/CIK{StockData.get_cik(ticker)}/us-gaap/AccountsPayableCurrent.json", headers=self.headers).json()
        return pd.DataFrame.from_dict(json)

    @sleep_and_retry
    @limits(calls = 5, period = 1) # 5 calls per second
    def get_company_facts(self, ticker):
        json = requests.get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{StockData.get_cik(ticker)}.json", headers=self.headers).json()
        return pd.DataFrame.from_dict(json)
    
class StockData():
    def __init__(self):
        pass

    class Earnings():
        def __init__(self):
            pass
    
        @staticmethod
        def update_upcoming_earnings():
            logger.info("Updating upcoming earnings in database")
            nasdaq = Nasdaq()
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
                    earnings_data = nasdaq.get_earnings_by_date(date_string)
                    if earnings_data.size > 0:
                        earnings_data['date'] = date_string
                        earnings_data = earnings_data[columns]
                        earnings_data = earnings_data.rename(columns={'symbol':'ticker'})
                        values = [tuple(row) for row in earnings_data.values]
                        Postgres().insert(table='upcomingearnings', fields=earnings_data.columns.to_list(), values=values)
                        logger.debug(f'Updated earnings for {date_string}')
            logger.info("Upcoming earnings have been updated!")

        @staticmethod
        def get_next_earnings_date(ticker):
            select_script = f"""SELECT date FROM upcomingearnings
                               WHERE ticker = '{ticker}'
                               """
            result = Postgres().select_one(select_script)
            if result is None:
                return "N/A"
            else:
                return result[0]

        @staticmethod
        def get_next_earnings_info(ticker):
            columns = Postgres().get_table_columns('upcomingearnings')
            select_script = f"""SELECT * FROM upcomingearnings
                               WHERE ticker = '{ticker}'
                               """
            result = Postgres().select_one(select_script)
            if result is None:
                return pd.DataFrame()
            else:
                return pd.DataFrame([result], columns=columns)

        @staticmethod
        def remove_past_earnings():
            logger.info("Removing upcoming earnings that have past")
            where = "date < CURRENT_DATE"
            Postgres().delete(table='upcomingearnings', where_condition=where)
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
            select_script = """SELECT date FROM historicalearnings
                               ORDER BY date DESC;
                               """
            result = Postgres().select_one(select_script)
            if result is None:
                start_date = datetime.date(year=2008, month=1, day=3) # Earliest day I can find earnings for on Nasdaq 1/3/2008
            else:
                start_date = result[0]

            num_days = (today - start_date).days
            for i in range(1, num_days):
                date = start_date + datetime.timedelta(days=i)
                if utils.market_open_on_date(date):
                    date_string = utils.format_date_ymd(date)
                    earnings = Nasdaq().get_earnings_by_date(date_string)
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
                                                                                if len(x) > 0 else None)
                        earnings ['epsForecast'] = earnings['epsForecast'].apply(lambda x: float(x.replace('(', '-')
                                                                                .replace(")", "")
                                                                                .replace('$', "")
                                                                                .replace(',',"")) 
                                                                                if len(x) > 0 else None)
                        earnings ['surprise'] = earnings['surprise'].apply(lambda x: float(x) if x != 'N/A' else None)

                        values = [tuple(row) for row in earnings.values]
                        Postgres().insert(table='historicalearnings', fields=earnings.columns.to_list(), values=values)
                        print(f"Updated historical earnings for {date_string}")
                    else: # No earnings recorded on target date
                        pass
                else: # Market is not open on target date
                    pass


        @staticmethod
        def get_historical_earnings(ticker):
            columns = Postgres().get_table_columns('historicalearnings')
            select_script = f"""SELECT * FROM historicalearnings
                               WHERE ticker = '{ticker}';
                               """
            results = Postgres().select_many(select_script)
            if results is None:
                return pd.DataFrame()
            else:
                return pd.DataFrame(results, columns=columns)

        @staticmethod
        def get_earnings_today(date):
            select_script = f"""SELECT * FROM upcomingearnings
                               WHERE date = '{date}';
                               """
            results = Postgres().select_many(select_script)
            if results is None:
                return results
            else:
                columns = Postgres().get_table_columns('upcomingearnings')
                return pd.DataFrame(results, columns=columns)

        


    @staticmethod
    def update_tickers():
        logger.info("Updating tickers database table with up-to-date ticker data")
        column_map = {'name':'name',
                      'marketCap':'marketCap',
                      'country':'country',
                      'ipoyear':'ipoyear',
                      'industry':'industry',
                      'sector':'sector',
                      'url':'nasdaqEndpoint',
                      'symbol':'ticker'}
        drop_columns = ['lastsale',
                        'netchange',
                        'pctchange',
                        'volume']
        tickers_data = Nasdaq().get_all_tickers()
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
        Postgres().update(update_script, values)
        logger.info("Tickers have been updated!")
    
    @staticmethod
    def insert_new_tickers():
        logger.info("Updating tickers database table with up-to-date ticker data")
        column_map = {'symbol':'ticker',
                      'name':'name',
                      'marketCap':'marketCap',
                      'country':'country',
                      'ipoyear':'ipoyear',
                      'industry':'industry',
                      'sector':'sector',
                      'url':'nasdaqEndpoint',
                      'cik':'cik'}
        drop_columns = ['lastsale',
                        'netchange',
                        'pctchange',
                        'volume']
        tickers_data = Nasdaq().get_all_tickers()
        logger.debug("Fetched latest tickers from NASDAQ")
        tickers_data = tickers_data[~tickers_data['symbol'].isin(StockData.get_all_tickers())]
        tickers_data = tickers_data.drop(columns=drop_columns)
        tickers_data = tickers_data.rename(columns=column_map)
        cik_series = pd.Series(name='cik', index=tickers_data.index)
        for i in range(0, tickers_data['ticker'].size):
            ticker = tickers_data['ticker'].iloc[i]
            logger.debug(f"Getting CIK value for ticker '{ticker}'")
            cik_series[i] = SEC().get_cik_from_ticker(ticker)
        tickers_data = tickers_data.join(cik_series)
        values = [tuple(row) for row in tickers_data.values]
        Postgres().insert(table='tickers', fields=tickers_data.columns.to_list(), values=values)
        logger.info("Tickers have been updated!")
    
    @staticmethod
    def update_daily_price_history(override_schedule = False, only_today = False):
        if config.utils.market_open_today() or override_schedule:
            start_date = None
            if only_today:
                start_date = datetime.datetime.today()
            logger.info(f"Updating daily price history for watchlist tickers")
            tickers = StockData.get_all_tickers()
            num_tickers = len(tickers)
            curr_ticker = 1
            for ticker in tickers:
                price_history = Schwab().get_daily_price_history(ticker, start_datetime=start_date)
                if price_history is not None:
                    fields = price_history.columns.to_list()
                    values = [tuple(row) for row in price_history.values]
                    logger.debug(f"Inserting daily price data for ticker {ticker}, {curr_ticker}/{num_tickers}")
                    Postgres().insert(table='dailypricehistory', fields=fields, values=values)
                    curr_ticker += 1
                else:
                    logger.warning(f"No daily price history found for ticker {ticker}, {curr_ticker}/{num_tickers}")
                    curr_ticker += 1
            logger.info("Completed update to daily price history in database")

    @staticmethod
    def update_5m_price_history(override_schedule=False, only_last_hour=False):
        if override_schedule or (config.utils.in_extended_hours() or config.utils.in_intraday()):
            logger.info(f"Updating 5m price history for watchlist tickers")
            start_datetime = None
            if only_last_hour:
                start_datetime = datetime.datetime.now() - datetime.timedelta(hours=1)
            tickers = StockData.get_all_tickers()
            num_tickers = len(tickers)
            curr_ticker = 1
            for ticker in tickers:
                price_history = Schwab().get_5m_price_history(ticker, start_datetime=start_datetime)
                if price_history is not None:
                    fields = price_history.columns.to_list()
                    values = [tuple(row) for row in price_history.values]
                    print(f"Inserting 5m price data for ticker {ticker}, {curr_ticker}/{num_tickers}")
                    Postgres().insert(table='fiveminutepricehistory', fields=fields, values=values)
                    curr_ticker += 1
                else:
                    logger.warning(f"No 5m price history found for ticker {ticker}, {curr_ticker}/{num_tickers}")
                    curr_ticker += 1
            logger.info("Completed update to 5m price history in database")
    
    
    @staticmethod
    def fetch_daily_price_history(ticker):
        select_script = f"""SELECT * FROM dailypricehistory
                           WHERE ticker = '{ticker}';
                           """
        results = Postgres().select_many(select_script)
        if results is None:
            return pd.DataFrame()
        else:
            columns = Postgres().get_table_columns("dailypricehistory")
            return pd.DataFrame(results, columns=columns)

    @staticmethod
    def fetch_5m_price_history(ticker):
        select_script = f"""SELECT * FROM fiveminutepricehistory
                           WHERE ticker = '{ticker}';
                           """
        results = Postgres().select_many(select_script)
        if results is None:
            return pd.DataFrame()
        else:
            columns = Postgres().get_table_columns('fiveminutepricehistory')
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
        select_script = f"""SELECT * FROM tickers
                            WHERE ticker = '{ticker}';
                            """
        return Postgres().select_one(select_script)
    
    @staticmethod
    def get_all_ticker_info():
        columns = Postgres().get_table_columns('tickers')
        data = Postgres().select_many('SELECT * FROM tickers;')
        data = pd.DataFrame(data, columns=columns)
        data.index = data['ticker']
        return data
    
    @staticmethod
    def get_all_tickers():
        select_script = """SELECT ticker FROM tickers;
                        """
        results = Postgres().select_many(select_script)
        return [result[0] for result in results]

    @staticmethod
    def get_all_tickers_by_market_cap(market_cap):
        select_script = """SELECT ticker, marketCap FROM tickers;
                        """
        results = Postgres().select_many(select_script)
        tickers = []
        for result in results:
            if len(result[1]) > 0: # Market cap is not empty string
                if float(result[1]) >= market_cap:
                    tickers.append(result[0])
        return tickers

    @staticmethod
    def get_all_tickers_by_sector(sector):
        select_script = f"""SELECT ticker FROM tickers
                           WHERE sector LIKE '%{sector}%';
                        """
        results = Postgres().select_many(select_script)
        if results is None:
            return results
        else:
            return [result[0] for result in results]

    @staticmethod
    def get_cik(ticker):
        logger.debug(f"Retreiving CIK value for ticker '{ticker}' from database")
        select_script = f"""SELECT cik from tickers
                            WHERE ticker = '{ticker}';
                            """
        result = Postgres().select_one(select_script)
        if result is None:
            return None
        else:
            return result[0]

    # Confirm we get valid data back when downloading data for ticker
    @staticmethod
    def validate_ticker(ticker):
        logger.debug("Verifying that ticker {} is valid".format(ticker))

        select_script = f"""SELECT ticker FROM tickers
                            WHERE ticker = '{ticker}';
                            """
        ticker = Postgres().select_one(select_script)
        if ticker is None:
            return False
        else:
            return True
    
    # Get list of valid tickers from string
    @staticmethod
    def get_valid_tickers(ticker_string:str):
        tickers = ticker_string.split()
        valid_tickers = []
        invalid_tickers = []
        for ticker in tickers:
            if StockData.validate_ticker(ticker):
                valid_tickers.append(ticker)
            else:
                invalid_tickers.append(ticker)
        return valid_tickers, invalid_tickers

    @staticmethod
    # Return supported exchanges
    def get_supported_exchanges():
        return ['NASDAQ', 'NYSE', 'AMEX']

class TradingView():
    def __init__(self):
        pass

    @staticmethod
    def get_premarket_gainers():
        logger.info("Fetching premarket gainers")
        num_rows, gainers = (Query()
                    .select('name', 'close', 'volume', 'market_cap_basic', 'premarket_change', 'premarket_volume', 'exchange')
                    .order_by('premarket_change', ascending=False)
                    .where(
                            Column('exchange').isin(StockData.get_supported_exchanges()))
                    .limit(100)
                    .get_scanner_data())
        gainers = gainers.drop(columns='exchange')
        gainers = gainers.drop(columns='ticker')
        heaaders = ['Ticker', 'Close', 'Volume', 'Market Cap', 'Premarket Change', "Premarket Volume"]
        gainers.columns = headers
        return gainers

    @staticmethod
    def get_premarket_gainers_by_market_cap(market_cap):
        logger.info("Fetching intraday gainers")
        num_rows, gainers = (Query()
                .select('name', 'close', 'volume', 'market_cap_basic', 'premarket_change', 'premarket_volume', 'exchange')
                .order_by('premarket_change', ascending=False)
                .where(
                    Column('market_cap_basic') >= market_cap,
                    Column('exchange').isin(StockData.get_supported_exchanges()))
                .limit(100)
                .get_scanner_data())
        gainers = gainers.drop(columns='exchange')
        gainers = gainers.drop(columns='ticker')
        headers = ['Ticker', 'Close', 'Volume', 'Market Cap', 'Premarket Change', 'Premarket Volume']
        gainers.columns = headers
        return gainers

    @staticmethod
    def get_intraday_gainers():
        logger.info("Fetching intraday gainers")
        gainers = (Query()
                .select('name', 'close', 'volume', 'market_cap_basic', 'change', 'exchange' )
                .order_by('change', ascending=False)
                .where(
                            Column('exchange').isin(StockData.get_supported_exchanges()))
                .limit(100)
                .get_scanner_data())
        gainers = gainers.drop(columns='exchange')
        gainers = gainers.drop(columns='ticker')
        headers = ['Ticker', 'Close', 'Volume', 'Market Cap', '% Change']
        gainers.columns = headers
        return gainers

    @staticmethod
    def get_intraday_gainers_by_market_cap(market_cap):
        logger.info("Fetching intrday gainers by market cap")
        num_rows, gainers = (Query()
                .select('name', 'close', 'volume', 'market_cap_basic', 'change', 'exchange')
                .set_markets('america')
                .order_by('change', ascending=False)
                .where(
                            Column('market_cap_basic') >= market_cap,
                            Column('exchange').isin(StockData.get_supported_exchanges()))
                .limit(100)
                .get_scanner_data())
        gainers = gainers.drop(columns='exchange')
        gainers = gainers.drop(columns='ticker')
        headers = ['Ticker', 'Close', 'Volume', 'Market Cap', '% Change']
        gainers.columns = headers
        return gainers
                
    @staticmethod
    def get_postmarket_gainers():
        logger.info("Fetching after hours gainers")
        num_rows, gainers = (Query()
                .select('name', 'close', 'volume', 'market_cap_basic', 'postmarket_change', 'postmarket_volume', 'exchange')
                .order_by('postmarket_change', ascending=False)
                .where(
                            Column('exchange').isin(StockData.get_supported_exchanges()))
                .limit(100)
                .get_scanner_data())
        gainers = gainers.drop(columns='exchange')
        gainers = gainers.drop(columns='ticker')
        headers = ['Ticker', 'Close', 'Volume', 'Market Cap', 'After Hours Change', 'After Hours Volume']
        gainers.columns = headers
        return gainers

    @staticmethod
    def get_postmarket_gainers_by_market_cap(market_cap):
        logger.info("Fetching after hours gainers by market cap")
        num_rows, gainers = (Query()
                .select('name', 'close', 'volume', 'market_cap_basic', 'postmarket_change', 'postmarket_volume', 'exchange')
                .order_by('postmarket_change', ascending=False)
                .where(
                            Column('market_cap_basic') >= market_cap,
                            Column('exchange').isin(StockData.get_supported_exchanges()))
                .limit(100)
                .get_scanner_data())
        gainers = gainers.drop(columns='exchange')
        gainers = gainers.drop(columns='ticker')
        headers = ['Ticker', 'Close', 'Volume', 'Market Cap', 'After Hours Change', 'After Hours Volume']
        gainers.columns = headers
        return gainers

    
    @staticmethod
    def get_unusual_volume_movers():
        logger.info("Fetching stocks with ununsual volume")
        columns = ['Ticker', 'Close', '% Change', 'Volume', 'Relative Volume', 'Average Volume (10 Day)', 'Market Cap']
        num_rows, unusual_volume = (Query()
                            .select('name','Price', 'Change %', 'Volume', 'Relative Volume', 'Average Volume (10 day)','Market Capitalization')
                            .set_markets('america')
                            .where(
                                Column('Volume') > 1000000
                            )
                            .limit(100)
                            .order_by('Relative Volume', ascending=False)
                            .get_scanner_data())
        unusual_volume = unusual_volume.drop(columns = "ticker")
        unusual_volume.columns = columns
        return unusual_volume

class ApeWisdom():
    def __init__(self):
        self.base_url = "https://apewisdom.io/api/v1.0/filter"
        self.filters_map = {
                    "all subreddits":"all",  # All subreddits combined
                    'all stock subreddits':'all-stocks',  #  Only subreddits focusing on stocks such as r/wallstreetbets or r/stocks
                    'all crypto subreddits':'all-crypto',  #  Only subreddits focusing on cryptocurrencies such as r/CryptoCurrency or r/SatoshiStreetBets
                    '4chan':'4chan', 
                    'r/Cryptocurrency':'CryptoCurrency', 
                    'r/CryptoCurrencies':'CryptoCurrencies', 
                    'r/Bitcoin':'Bitcoin', 
                    'r/SatoshiStreetBets':'SatoshiStreetBets', 
                    'r/CryptoMoonShots':'CryptoMoonShots', 
                    'r/CryptoMarkets':'CryptoMarkets', 
                    'r/stocks':'stocks', 
                    'r/wallstreetbets':'wallstreetbets', 
                    'r/options':'options', 
                    'r/WallStreetbetsELITE':'WallStreetbetsELITE', 
                    'r/Wallstreetbetsnew':'Wallstreetbetsnew', 
                    'r/SPACs':'SPACs', 
                    'r/investing':'investing', 
                    'r/Daytrading':'Daytrading', 
                    'r/Shortsqueeze':'Shortsqueeze',
                    'r/SqueezePlays':"SqueezePlays"
        }
    
    def get_filter(self, filter_name):
        return self.filters_map[filter_name]

    def get_top_stocks(self, filter_name = 'all stock subreddits'):
        logger.debug(f"Fetching top stocks from source: '{filter_name}'")
        filter = self.get_filter(filter_name=filter_name)
        if filter is not None:
            top_stocks_json = requests.get(f"{self.base_url}/{filter}").json()
            if top_stocks_json is not None:
                top_stocks = pd.DataFrame(top_stocks_json['results'])
                return top_stocks
        else:
            return None

class Dolthub():
    def __init__(self):
        self.token = config.get_dolthub_api_token()
        self.headers={"authorization": f"token {self.token}" }
        
    @sleep_and_retry
    @limits(calls = 5, period = 1) # 5 calls per 1 second
    def get_historical_earnings_by_ticker(self, ticker):
        db_owner = "post-no-preference"
        db_repo = "earnings"
        db_branch = "master"
        select_query = f"""SELECT * FROM eps_history WHERE act_symbol = '{ticker}'"""
        earnings_http = requests.get(url=f"https://www.dolthub.com/api/v1alpha1/{db_owner}/{db_repo}/{db_branch}",
                           params={"q": select_query},
                           headers=self.headers)
        earnings_json = earnings_http.json()
        columns = [column.get('columnName') for column in earnings_json['schema']]
        earnings_data = earnings_json['rows']
        return pd.DataFrame(earnings_data, columns=columns)

class Schwab():
    def __init__(self):
        self.client = schwab.auth.easy_client(
            api_key=config.get_schwab_api_key(),
            app_secret=config.get_schwab_api_secret(),
            callback_url="https://127.0.0.1:8182",
            token_path="data/schwab-token.json"
        )

    def get_daily_price_history(self, ticker, start_datetime=None, end_datetime=datetime.datetime.now(datetime.timezone.utc)):
        if start_datetime is None: # If no start time, get data as far back as 2000
            start_datetime = datetime.datetime(
                                year = 2000,
                                month = 1,
                                day = 1,
                                hour = 0,
                                minute = 0,
                                second = 0
                                ).astimezone(datetime.timezone.utc)
        resp = self.client.get_price_history_every_day(
            symbol=ticker, 
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )
        try:
            assert resp.status_code == httpx.codes.OK, resp.raise_for_status()
            data = resp.json()
            price_history = pd.DataFrame.from_dict(data['candles'])
            if price_history.size > 0:
                price_history['datetime'] = price_history['datetime'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000))
                price_history.insert(loc=0, column='ticker', value=ticker)
                return price_history
            else:
                return None
        except httpx.HTTPStatusError as e:
            print(f"Enountered HTTPStatusError when downloading daily price history for ticker {ticker}\n{e}")
            return None

    # This reports live market data!
    def get_5m_price_history(self, ticker, start_datetime=None, end_datetime=None):
        resp = self.client.get_price_history_every_five_minutes(
            symbol=ticker, 
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )
        assert resp.status_code == httpx.codes.OK, resp.raise_for_status()
        data = resp.json()
        price_history = pd.DataFrame.from_dict(data['candles'])
        price_history['datetime'] = price_history['datetime'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000))
        price_history.insert(loc=0, column='ticker', value=ticker)
        return price_history

    def get_quote(self, ticker):
        resp = self.client.get_quote(
            symbol=ticker
        )
        assert resp.status_code == httpx.codes.OK, resp.raise_for_status()
        data = resp.json()
        return data[ticker]

    def get_quotes(self, tickers):
        resp = self.client.get_quotes(
            symbols=tickers
        )
        assert resp.status_code == httpx.codes.OK, resp.raise_for_status()
        data = resp.json()
        return data
    
    def get_fundamentals(self, tickers):
        resp = self.client.get_instruments(symbols=tickers, 
                                           projection=self.client.Instrument.Projection.FUNDAMENTAL)
        assert resp.status_code == httpx.codes.OK, resp.raise_for_status()
        data = resp.json()
        return data
    
    def get_options_chain(self, ticker):
        resp = self.client.get_option_chain(ticker)
        assert resp.status_code == httpx.codes.OK, resp.raise_for_status()
        data = resp.json()
        return data

    def get_movers(self):
        resp = self.client.get_movers(index=self.client.Movers.Index.EQUITY_ALL, 
                                      sort_order=self.client.Movers.SortOrder.PERCENT_CHANGE_UP,
                                      frequency=self.client.Movers.Frequency.TEN)
        assert resp.status_code == httpx.codes.OK, resp.raise_for_status()
        data = resp.json()
        return data
    

###############################
# Helpers and data validation #
###############################

# Validate specified path exists and create it if needed
def validate_path(path):
    logger.debug("Validating that path {} exists".format(path))
    if not (os.path.isdir(path)):
        logger.warning("Path {} does not exist. Creating path...".format(path))
        os.makedirs(path) 
        return 
    else:
        logger.debug("Path {} exists in the filesystem".format(path))
        return True
       

#########
# Tests #
#########

def test():
    #tickers = StockData.get_all_tickers_by_market_cap(100000000)
    #quotes = Schwab().get_quotes(tickers[:1000])
    #movers = Schwab().get_movers()
    
    #StockData.Earnings.update_historical_earnings()
    #tickers = StockData.get_all_tickers_by_sector('Technology')
    #print(tickers)

    StockData.update_tickers()
    

if __name__ == "__main__":
    #test    
    # Initilaize database
    #Postgres().create_tables()
    #Postgres().init_tables()
    test()
    pass

    
    

    