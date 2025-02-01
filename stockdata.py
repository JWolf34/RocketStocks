import sys
sys.path.append('../RocketStocks/discord')
import yfinance as yf
from pandas_datareader import data as pdr
import pandas as pd
import pandas_ta as ta
import psycopg2
from psycopg2 import sql
from newsapi import NewsApiClient
import os
import datetime
from datetime import timedelta
import requests
from ratelimit import limits, sleep_and_retry
import config
import logging
from tradingview_screener import Query, Column
import schwab
import time
import httpx
from bs4 import BeautifulSoup

# Logging configuration
logger = logging.getLogger(__name__)

class News():
    def __init__(self):
        self.token = config.secrets.news_api_token
        self.news = NewsApiClient(api_key=self.token)
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
        logger.debug(f"Fetching news with query '{query}'")
        return self.news.get_everything(q=query, language='en', **kwargs)
    
    def get_breaking_news(self, query, **kwargs):
        logger.debug(f"Fetching breaking news with query '{query}'")
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

    # Retrieve latest tickers and their properties on the NASDAQ
    @sleep_and_retry
    @limits(calls = 5, period = 60)
    def get_all_tickers(self):
        logger.debug("Retrieving latest tickers and their properties from NASDAQ")
        url = f"{self.url_base}/screener/stocks?tableonly=false&limit=25&download=true"
        data = requests.get(url, headers=self.headers).json()
        tickers = pd.DataFrame(data['data']['rows'])
        return tickers

    # Retrieve all earnings on a given date
    @sleep_and_retry
    @limits(calls = 5, period = 60) 
    def get_earnings_by_date(self, date):
        logger.debug(f"Retrieving earnings on date {date}")
        url = f"{self.url_base}/calendar/earnings"
        params = {'date':date}
        data = requests.get(url, headers=self.headers, params=params).json()
        if data['data'] is None:
            return pd.DataFrame()
        else:
            return pd.DataFrame(data['data']['rows'])
    
    # Retrieve earnings forecast for target tickert from NASDAQ
    @sleep_and_retry
    @limits(calls = 5, period = 60) 
    def get_earnings_forecast(self, ticker):
        logger.debug(f"Retrieving earnings forecast for ticker '{ticker}'")
        url = f"https://api.nasdaq.com/api/analyst/{ticker}/earnings-forecast"
        data = requests.get(url, headers=self.headers).json()
        return data['data']

    def get_earnings_forecast_quarterly(self, ticker):
        return pd.DataFrame.from_dict(self.get_earnings_forecast(ticker)['quarterlyForecast']['rows'])
    
    def get_earnings_forecast_yearly(self, ticker):
        return pd.DataFrame.from_dict(self.get_earnings_forecast(ticker)['yearlyForecast']['rows'])

    # Retrieve historical EPS for target ticker from NASDAQ
    @sleep_and_retry
    @limits(calls = 5, period = 60) 
    def get_eps(self, ticker):
        logger.debug(F"Retrieving eps for ticker '{ticker}'")
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
        self.user = config.secrets.db_user
        self.pwd = config.secrets.db_password
        self.db = config.secrets.db_name
        self.host = config.secrets.db_host
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
                            marketcap       varchar(20) NOT NULL, 
                            country         varchar(40), 
                            ipoyear         char(4),
                            industry        varchar(64),
                            sector          varchar(64),
                            nasdaqendpoint  varchar(64),
                            cik             char(10)
                            );

                            CREATE TABLE IF NOT EXISTS upcoming_earnings (
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
                            systemgenerated     boolean
                            );
                            
                            CREATE TABLE IF NOT EXISTS popular_stocks (
                            date                date,
                            ticker              varchar(8),
                            rank                int,
                            mentions            int,
                            upvotes             int,
                            PRIMARY KEY (date, ticker)
                            );

                            CREATE TABLE IF NOT EXISTS historical_earnings (
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
                            alert_data          json,
                            PRIMARY KEY (date, ticker, alert_type)
                            );

                            CREATE TABLE IF NOT EXISTS daily_price_history(
                            ticker              varchar(8),
                            open                float,
                            high                float,
                            low                 float,
                            close               float,
                            volume              bigint,
                            date                date,
                            PRIMARY KEY (ticker, date)
                            );

                            CREATE TABLE IF NOT EXISTS five_minute_price_history(
                            ticker              varchar(8),
                            open                float,
                            high                float,
                            low                 float,
                            close               float,
                            volume              bigint,
                            datetime            timestamp,
                            PRIMARY KEY (ticker, datetime)
                            );

                            CREATE TABLE IF NOT EXISTS ct_politicians(
                            politician_id       char(7) PRIMARY KEY,
                            name                varchar(64),
                            party               varchar(16),
                            state               varchar(32)
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
        drop_script = """DROP TABLE alerts;
                         DROP TABLE daily_price_history;
                         DROP TABLE five_minute_price_history;
                         DROP TABLE historical_earnings;
                         DROP TABLE popular_stocks;
                         DROP TABLE reports;
                         DROP TABLE tickers;
                         DROP TABLE upcoming_earnings;
                         DROP TABLE watchlists;
                        """

        self.cur.execute(drop_script)
        self.conn.commit()
        self.close_connection()
        logger.debug("All database tables dropped")
    
    def drop_table(self, table:str):
        self.open_connection()
        drop_script = sql.SQL("DROP TABLE IF EXISTS {sql_table};").format(
                                                                sql_table = sql.Identifier(table)
        )
        self.cur.execute(drop_script)
        self.conn.commit()
        self.close_connection()
        logger.debug(f"Dropped table '{table}' from database")

  
    def insert(self, table:str, fields:list, values:list):
        self.open_connection()
        
        # Insert into
        insert_script =  sql.SQL("INSERT INTO {sql_table} ({sql_fields})").format(
                                    sql_table = sql.Identifier(table),
                                    sql_fields = sql.SQL(',').join([
                                        sql.Identifier(field) for field in fields
                                    ]))

        # Values
        values_string = ','.join(["%s" for i in range(0, len(fields))])
        insert_script += sql.SQL(f"VALUES ({values_string})")

        # On conflict, do nothing
        insert_script += sql.SQL("ON CONFLICT DO NOTHING;")
        
        for row in values:
            self.cur.execute(insert_script, row)

        self.conn.commit()
        self.close_connection()

    # Select row(s) from database
    def select(self, table:str, fields:list, where_conditions:list = [], order_by:tuple = tuple(), fetchall:bool = True):
        self.open_connection()
        values = tuple()
        
        # Select
        select_script = sql.SQL("SELECT {sql_fields} FROM {sql_table} ").format(
                                                                    sql_fields = sql.SQL(",").join([
                                                                        sql.Identifier(field) for field in fields
                                                                    ]),
                                                                    sql_table = sql.Identifier(table)
        )

        # Where conditions
        if len(where_conditions) > 0:
            where_script, where_values = self.where_clauses(where_conditions=where_conditions)
           
            # Update script and values
            select_script += where_script
            values += where_values
        

        # Order by
        if len(order_by) > 0:
            select_script += sql.SQL("ORDER BY {sql_field} {sql_order}").format(
                                                                sql_field = sql.Identifier(order_by[0]),
                                                                sql_order = sql.SQL(order_by[1])
            )

        
        # End script
        select_script += sql.SQL(';')

        self.cur.execute(select_script, values)
        if fetchall:
            result = self.cur.fetchall()
        else:
            result = self.cur.fetchone()
        self.close_connection()
        return result
    
    # Update row(s) in database
    def update(self, table:str, set_fields:list, where_conditions:list = []):
        self.open_connection()

        values = tuple()
        # Update
        update_script = sql.SQL("UPDATE {sql_table} ").format(
                                                            sql_table = sql.Identifier(table)
        )

        # Set
        set_columns = [field for (field, value) in set_fields]
        set_values = tuple([value for (field, value) in set_fields])
        values += set_values
        

        update_script += sql.SQL("SET ")
        update_script += sql.SQL(',').join([
            sql.SQL("{sql_field} = %s").format(
                sql_field = sql.Identifier(field)
            ) for field in set_columns
        ])

        # Where conditions
        if len(where_conditions) > 0:
            where_script, where_values = self.where_clauses(where_conditions=where_conditions)
           
            # Update script and values
            update_script += where_script
            values += where_values
        
        
        # End script
        update_script += sql.SQL(';')

        self.cur.execute(update_script, values)
        
        self.conn.commit()
        self.close_connection()
    
    # Delete row(s) from database
    def delete(self, table:str, where_conditions:list):
        self.open_connection()

        values = tuple()
        # Delete
        delete_script = sql.SQL("DELETE FROM {sql_table} ").format(
                                                            sql_table = sql.Identifier(table)
        )

        # Where conditions
        if len(where_conditions) > 0:
            where_script, where_values = self.where_clauses(where_conditions=where_conditions)
           
            # Update script and values
            delete_script += where_script
            values += where_values
        
        # End script
        delete_script += sql.SQL(';')

        self.cur.execute(delete_script, values)
        self.conn.commit()
        self.close_connection()

    # Generate where clauses SQL
    def where_clauses(self, where_conditions:list):
        
        where_script = sql.SQL(" WHERE ")
        values = tuple()

        where_clauses = []
        for condition in where_conditions:
            if len(condition) == 2:
                # Only field and value; use = operator
                where_clauses.append(sql.SQL("{sql_field} = %s").format(
                                    sql_field = sql.Identifier(condition[0])))
                values += (condition[1],)

            elif len(condition) == 3:
                # Operator specified
                where_clauses.append(sql.SQL("{sql_field} {sql_operator} %s").format(
                                    sql_field = sql.Identifier(condition[0]),
                                    sql_operator = sql.SQL(condition[1])))
                values += (condition[2],)
        where_script += sql.SQL(" AND ").join(where_clauses)

        return where_script, values

    # Return list of columns from selected table
    def get_table_columns(self, table):
        self.open_connection()

        # Select 
        select_script = sql.SQL("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS")

        # Where
        select_script += sql.SQL(f" WHERE TABLE_NAME = '{table}'")
                                                                
        # Order by
        select_script += sql.SQL(" ORDER BY ordinal_position;")

        self.cur.execute(select_script)
        result = self.cur.fetchall()
        columns = [column[0] for column in result]
        self.close_connection()
        return columns
  
class Watchlists():
    def __init__(self):
        self.db_table = 'watchlists'
        self.db_fields = ['id', 'tickers', 'systemgenerated']
        
    # Return tickers from watchlist - global by default, personal if chosen by user
    def get_tickers_from_watchlist(self, watchlist_id):
        logger.debug("Fetching tickers from watchlist with ID '{}'".format(watchlist_id))
        
        tickers = Postgres().select(table='watchlists',
                                        fields=['tickers'],
                                        where_conditions=[('id', watchlist_id)],
                                        fetchall=False)
        if tickers is None:
            return tickers
        else:
            return sorted(tickers[0].split())
       

    # Return tickers from all available watchlists
    def get_tickers_from_all_watchlists(self, no_personal=True, no_systemGenerated=True):
        logger.debug("Fetching tickers from all available watchlists (besides personal)")
        
        watchlists = Postgres().select(table='watchlists',
                                       fields=['id', 'tickers', 'systemgenerated'],
                                       fetchall=True)
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
        filtered_watchlists = []
        watchlists = Postgres().select(table='watchlists',
                                       fields = ['id', 'tickers', 'systemgenerated'],
                                       fetchall=True)
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
        logger.debug("Updating watchlist '{}': {}".format(watchlist_id, tickers))

        Postgres().update(table='watchlists', 
                          set_fields=[('tickers', ' '.join(tickers))], 
                          where_conditions=[('id', watchlist_id)])

    # Create a new watchlist with id 'watchlist_id'
    def create_watchlist(self, watchlist_id, tickers, systemGenerated):
        logger.debug("Creating watchlist with ID '{}' and tickers {}".format(watchlist_id, tickers))
        Postgres().insert(table=self.db_table, fields=self.db_fields, values=[(watchlist_id, " ".join(tickers), systemGenerated)])

    # Delete watchlist with id 'watchlist_id'
    def delete_watchlist(self, watchlist_id):
        logger.debug("Deleting watchlist '{}'...".format(watchlist_id))
        Postgres().delete(table=self.db_table, where_conditions=[('id', watchlist_id)])

    # Validate watchlist exists in the database
    def validate_watchlist(self, watchlist_id):
        logger.debug(f"Validating watchlist '{watchlist_id}' exists")

        result = Postgres().select(table='watchlists',
                                       fields=['id'],
                                       where_conditions=[('id', watchlist_id)],
                                       fetchall=False)
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
        logger.debug(f"Fetching  SEC submissions for ticker {ticker}")
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
        logger.debug(f"Fetching accounts payable from SEC for ticker {ticker}")
        json = requests.get(f"https://data.sec.gov/api/xbrl/companyconcept/CIK{StockData.get_cik(ticker)}/us-gaap/AccountsPayableCurrent.json", headers=self.headers).json()
        return pd.DataFrame.from_dict(json)

    @sleep_and_retry
    @limits(calls = 5, period = 1) # 5 calls per second
    def get_company_facts(self, ticker):
        logger.debug(f"Fetching company facts from SEC for ticker {ticker}")
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
                        Postgres().insert(table='upcoming_earnings', fields=earnings_data.columns.to_list(), values=values)
                        logger.debug(f'Updated earnings for {date_string}')
            logger.info("Upcoming earnings have been updated!")

        @staticmethod
        def get_next_earnings_date(ticker):
            result = Postgres().select(table='upcoming_earnings',
                                           fields=['date'],
                                           where_conditions=[('ticker', ticker)], 
                                           fetchall=False)
            if result is None:
                return "N/A"
            else:
                return result[0]

        @staticmethod
        def get_next_earnings_info(ticker):
            columns = Postgres().get_table_columns('upcoming_earnings')

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
                if config.market_utils.market_open_on_date(date):
                    date_string = config.date_utils.format_date_ymd(date)
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

        for row in tickers_data.values:
            ticker = row[0]
            set_fields = [(tickers_data.columns.to_list()[i], row[i]) for i in range(0, row.size)]
            Postgres().update(table='tickers',
                            set_fields=set_fields,
                            where_conditions=[('ticker', ticker)])
            logger.debug(f"Updated ticker '{ticker}' in database")
        logger.info("Tickers have been updated!")
    
    @staticmethod
    async def insert_new_tickers():
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
        price_history = await Schwab().get_daily_price_history(ticker, start_datetime=start_datetime)
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
        price_history = await Schwab().get_5m_price_history(ticker, start_datetime=start_datetime)
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

class TradingView():
    def __init__(self):
        pass

    @staticmethod
    def get_premarket_gainers():
        logger.debug("Fetching premarket gainers from TradingView")
        num_rows, gainers = (Query()
                    .select('name', 'close', 'volume', 'market_cap_basic', 'premarket_change', 'premarket_volume', 'exchange')
                    .order_by('premarket_change', ascending=False)
                    .where(
                            Column('exchange').isin(StockData.get_supported_exchanges()))
                    .limit(100)
                    .get_scanner_data())
        gainers = gainers.drop(columns='exchange')
        gainers = gainers.drop(columns='ticker')
        headers = ['Ticker', 'Close', 'Volume', 'Market Cap', 'Premarket Change', "Premarket Volume"]
        gainers.columns = headers
        logger.debug(f"Returned gainers dataframe with shape {gainers.shape}")
        return gainers

    @staticmethod
    def get_premarket_gainers_by_market_cap(market_cap):
        logger.debug(f"Fetching premarket gainers from TradingView with market cap  > {market_cap}")
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
        logger.debug(f"Returned gainers dataframe with shape {gainers.shape}")
        return gainers

    @staticmethod
    def get_intraday_gainers():
        logger.debug(f"Fetching intraday gainers from TradingView")
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
        logger.debug(f"Returned gainers dataframe with shape {gainers.shape}")
        return gainers

    @staticmethod
    def get_intraday_gainers_by_market_cap(market_cap):
        logger.debug(f"Fetching intrday gainers with market cap > {market_cap} from TradingView")
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
        logger.debug(f"Returned gainers dataframe with shape {gainers.shape}")
        return gainers
                
    @staticmethod
    def get_postmarket_gainers():
        logger.debug(f"Fetching after hours gainers from TradingView")
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
        logger.debug(f"Returned gainers dataframe with shape {gainers.shape}")
        return gainers

    @staticmethod
    def get_postmarket_gainers_by_market_cap(market_cap):
        logger.debug(f"Fetching after hours gainers with market cap > {market_cap} from TradingView")
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
        logger.debug(f"Returned gainers dataframe with shape {gainers.shape}")
        return gainers

    
    @staticmethod
    def get_unusual_volume_movers():
        logger.debug("Fetching stocks with ununsual volume from TradingView")
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
        logger.debug(f"Returned gainers dataframe with shape {unusual_volume.shape}")
        return unusual_volume

    @staticmethod
    def get_unusual_volume_at_time_movers():
        logger.debug("Fetching stocks with ununsual volume at time form TradingView")
        columns = ['Ticker', 'Close', '% Change', 'Volume', 'Relative Volume At Time', 'Average Volume (10 Day)', 'Market Cap']
        num_rows, unusual_volume_at_time = (Query()
                            .select('name','Price', 'Change %', 'Volume', 'relative_volume_intraday|5', 'Average Volume (10 day)','Market Capitalization')
                            .set_markets('america')
                            .where(
                                Column('Volume') > 1000000
                            )
                            .limit(100)
                            .order_by('relative_volume_intraday|5', ascending=False)
                            .get_scanner_data())
        unusual_volume_at_time = unusual_volume_at_time.drop(columns = "ticker")
        unusual_volume_at_time.columns = columns
        logger.debug(f"Returned volume moovers dataframe with shape {gainers.shape}")
        return unusual_volume_at_time

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

    def get_top_stocks(self, filter_name = 'all stock subreddits', page = 1):
        logger.debug(f"Fetching top stocks from source: '{filter_name}', page {page}")
        filter = self.get_filter(filter_name=filter_name)
        if filter is not None:
            top_stocks_json = requests.get(f"{self.base_url}/{filter}/page/{page}").json()
            if top_stocks_json is not None:
                top_stocks = pd.DataFrame(top_stocks_json['results'])
                return top_stocks
        else:
            return None


class Schwab():
    def __init__(self):
        self.client = schwab.auth.easy_client(
            api_key=config.secrets.schwab_api_key,
            app_secret=config.secrets.schwab_api_secret,
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
                return pd.DataFrame()
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
            return price_history

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

class CapitolTrades:

    def politician(name:str=None, politician_id:str=None):
        logger.debug(f"Fetching politician with id '{politician_id}' and name '{name}'")
        if not name and not politician_id:
            logger.debug("No politician found with provided criteria")
            return None
        else:
            fields = Postgres().get_table_columns('ct_politicians')
            where_conditions = []
            if name:
                where_conditions.append(('name', name))
            if politician_id:
                where_conditions.append(('politician_id', politician_id))
            data = Postgres().select(table='ct_politicians',
                                    fields=fields,
                                    where_conditions=where_conditions,
                                    fetchall=False)
            politician = dict(zip(fields, data))                    
            logger.debug(f"Returning politician data: {politician}")
            return politician
    
    def all_politicians():
        logger.debug("Retrieving all politicians from database")
        fields = Postgres().get_table_columns('ct_politicians')
        data = Postgres().select(table='ct_politicians',
                                    fields=fields,
                                    fetchall=True)
        politicians = [dict(zip(fields, data[index])) for index in range(0, len(data))]
        logger.debug(f"Returning data on {len(politicians)} politicians")
        return politicians


    def update_politicians():
        logger.info("Updating politicians in the database")
        politicians = []
        page_num = 1
        while True: 
            params = {'page':page_num, 'pageSize':96}
            politicians_r = requests.get(url='https://www.capitoltrades.com/politicians', params=params)
            logger.debug(f"Requesting politicians on page {page_num}, status code is {politicians_r.status_code}")
            html = politicians_r.content
            politicians_soup = BeautifulSoup(html, 'html.parser')
            cards = politicians_soup.find_all('a', class_="index-card-link")
            if cards:
                for card in cards:
                    politician_id = card['href'].split('/')[-1]
                    name = card.find('h2').text
                    party = card.find('span', class_=lambda c: "q-field party" in c).text
                    state = card.find('span', class_=lambda c: "q-field us-state-full" in c).text
                    politician = (politician_id, name, party, state)
                    logger.debug(f"Identified politician with data {politician}")
                    politicians.append(politician)
                    
                page_num += 1
            else:
                postgres = Postgres()
                columns = postgres.get_table_columns(table='ct_politicians')
                logger.debug("Inserting politicians into database")
                Postgres().insert(table='ct_politicians',
                                  fields=columns,
                                  values=politicians)
                break
        logger.info("Updating politicians complete!")


    def trades(pid:str):
        logger.debug(f"Requesting trades information for politician with id '{pid}")
        trades = []
        page_num = 1
        while True: 
            params = {'page':page_num, 'pageSize':96}
            trades_r = requests.get(url=f'https://www.capitoltrades.com/politicians/{pid}', params=params)
            logger.debug(f"Requesting trades on page {page_num}, status code is {trades_r.status_code}")
            html = trades_r.content
            trades_soup = BeautifulSoup(html, 'html.parser')
            table = trades_soup.find('tbody')
            rows = table.find_all('tr')
            if len(rows) > 1:
                for row in rows:

                    # Ticker
                    ticker = row.find('span', class_='q-field issuer-ticker').text
                    if ":" in ticker:
                        ticker = ticker.split(":")[0]

                    # Published and Filed Dates
                    # Special case for September since datetime uses "Sep" but CT uses "Sept"
                    dates = row.find_all('div', class_ = "text-size-3 font-medium")
                    years = row.find_all('div', class_ = "text-size-2 text-txt-dimmer")
                    published_date = datetime.datetime.strptime(f"{dates[0].text.replace('Sept','Sep')} {years[0].text}", "%d %b %Y").date()
                    filed_date = datetime.datetime.strptime(f"{dates[1].text.replace('Sept','Sep')} {years[1].text}", "%d %b %Y").date()

                    # Filed after
                    filed_after = f"{row.find('span', class_= lambda c: 'reporting-gap-tier' in c).text} days"

                    # Order Type and Size
                    order_type = row.find('span', class_ = lambda c: "q-field tx-type" in c).text.replace('"','').upper()
                    order_size = row.find('span', class_ = "mt-1 text-size-2 text-txt-dimmer hover:text-foreground").text

                    # Add to DF and increment page_num
                    trade = (ticker, config.date_utils.format_date_mdy(published_date), config.date_utils.format_date_mdy(filed_date), filed_after, order_type, order_size)
                    logger.debug(f"Identified trade with data {trade}")
                    trades.append()
                page_num += 1
            else:
                logger.debug(f"Returning data on {len(trades)} trades")
                return pd.DataFrame(trades,columns=['Ticker', 'Published Date', 'Filed Dated', 'Filed After', 'Order Type', 'Order Size'])
                




       

#########
# Tests #
#########

def test():
    pass
    

if __name__ == "__main__":#
    test()
    

    
    

    