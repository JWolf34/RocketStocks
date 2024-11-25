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
import logging
from tradingview_screener import Scanner, Query, Column

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
    @limits(calls = 5, period = 60) # 10 calls per minute
    def get_all_tickers(self):
        url = f"{self.url_base}/screener/stocks?tableonly=false&limit=25&download=true"
        data = requests.get(url, headers=self.headers).json()
        tickers = pd.DataFrame(data['data']['rows'])
        return tickers

    @sleep_and_retry
    @limits(calls = 5, period = 60) # 10 calls per 10 minutes
    def get_earnings_by_date(self, date):
        url = f"{self.url_base}/calendar/earnings"
        params = {'date':date}
        data = requests.get(url, headers=self.headers, params=params).json()
        if data is None:
            return data
        else:
            return pd.DataFrame(data['data']['rows'])
    
    @sleep_and_retry
    @limits(calls = 5, period = 60) # 10 calls per 10 minutes
    def get_earnings_forecast(self, ticker):
        url = f"https://api.nasdaq.com/api/analyst/{ticker}/earnings-forecast"
        data = requests.get(url, headers=self.headers).json()
        return data['data']

    def get_earnings_forecast_quarterly(self, ticker):
        return pd.DataFrame.from_dict(self.get_earnings_forecast(ticker)['quarterlyForecast']['rows'])
    
    def get_earnings_forecast_yearly(self, ticker):
        return pd.DataFrame.from_dict(self.get_earnings_forecast(ticker)['yearlyForecast']['rows'])

    @sleep_and_retry
    @limits(calls = 5, period = 60) # 10 calls per 10 minutes
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
                            """
        logger.debug("Running script to create tables in database...")
        self.cur.execute(create_script)
        self.conn.commit()
        logger.debug("Create script completed successfully!")

        self.close_connection()
    
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
        logger.debug(f"Inserted values {values} into table {table}")
        self.close_connection()

    # Select a sigle row from database
    def select_one(self, query:str):
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
    def update(self, query:str):
        self.open_connection()
        self.cur.execute(query)
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
                            WHERE TABLE_NAME = '{table}';
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
        submissions_json = requests.get(f"https://data.sec.gov/submissions/CIK{self.get_cik_from_ticker(ticker)}.json", headers=self.headers).json()
        return submissions_json
    
    def get_recent_filings(self, ticker):
        return pd.DataFrame.from_dict(self.get_submissions_data(ticker)['filings']['recent'])

    def get_link_to_filing(self, ticker, filing):
        return f"https://sec.gov/Archives/edgar/data/{StockData.get_cik(ticker).lstrip("0")}/{filing['accessionNumber'].replace("-","")}/{filing['primaryDocument']}"

    @sleep_and_retry
    @limits(calls = 5, period = 1) # 10 calls per second
    def get_accounts_payable(self, ticker):
        json = requests.get(f"https://data.sec.gov/api/xbrl/companyconcept/CIK{StockData.get_cik(ticker)}/us-gaap/AccountsPayableCurrent.json", headers=self.headers).json()
        return pd.DataFrame.from_dict(json)

    @sleep_and_retry
    @limits(calls = 5, period = 1) # 10 calls per second
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
                return result[0].strftime("%m/%d/%Y")
        
        @staticmethod
        def format_earnings_date(date_string):
            earnings_date_fmt = "%Y-%m-%d"
            desired_date_fmt = "%m/%d/%Y"
            date = datetime.datetime.strptime(date_string, earnings_date_fmt)
            new_date_string = date.strftime(desired_date_fmt)
            return new_date_string

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
    def update_tickers():
        logger.info("Updating tickers database table with up-to-date tickers")
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
        tickers_data = tickers_data.drop(columns=drop_columns)
        tickers_data = tickers_data.rename(columns=column_map)
        cik_series = pd.Series(name='cik', index=tickers_data.index)
        for i in range(0, tickers_data['ticker'].size):
            logger.debug(f"Getting CIK value for ticker '{ticker}'")
            ticker = tickers_data['ticker'].iloc[i]
            cik_series[i] = SEC().get_cik_from_ticker(ticker)
        tickers_data = tickers_data.join(cik_series)
        values = [tuple(row) for row in tickers_data.values]
        Postgres().insert(table='tickers', fields=tickers_data.columns.to_list(), values=values)
        logger.info("Tickers have been updated!")

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
    def get_cik(ticker):
        logger.debug(f"Retreiving CIK value for ticker '{ticker}' from database")
        select_script = f"""SELECT cik from tickers
                            WHERE ticker = '{ticker}';
                            """
        return Postgres().select_one(select_script)[0]

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
    def get_list_from_tickers(ticker_string:str):
        tickers = ticker_string.split()
        valid_tickers = []
        invalid_tickers = []
        for ticker in tickers:
            if StockData.validate_ticker(ticker):
                valid_tickers.append(ticker)
            else:
                invalid_tickers.append(ticker)
        return valid_tickers, invalid_tickers

class TradingView():
    def __init__(self):
        pass

    @staticmethod
    def get_premarket_gainers():
        logger.info("Fetching premarket gainers")
        num_rows, gainers = Scanner.premarket_gainers.get_scanner_data()
        return gainers

    @staticmethod
    def get_premarket_gainers_by_market_cap(market_cap):
        logger.info("Fetching premarket gainers by market cap")
        num_rows, gainers = Scanner.premarket_gainers.get_scanner_data()
        return gainers.loc[gainers['market_cap_basic'] >= market_cap]    

    @staticmethod
    def get_intraday_gainers():
        logger.info("Fetching intraday gainers")
        num_rows, gainers = (Query()
                            .select('name','close', 'change', 'volume')
                            .get_scanner_data())
        return gainers

    @staticmethod
    def get_intraday_gainers_by_market_cap(market_cap):
        logger.info("Fetching intrday gainers by market cap")
        num_rows, gainers = (Query()
                            .select('name','close', 'volume', 'market_cap_basic', 'change', 'exchange')
                            .set_markets('america')
                            .where(
                                Column('market_cap_basic') > market_cap,
                                Column('exchange').isin(get_supported_exchanges())
                            )
                            .order_by('change', ascending=False)
                            .get_scanner_data())
        return gainers
                
    @staticmethod
    def get_postmarket_gainers():
        logger.info("Fetching after hours gainers")

        num_rows, gainers = Scanner.postmarket_gainers.get_scanner_data()
        return gainers

    @staticmethod
    def get_postmarket_gainers_by_market_cap(market_cap):
        logger.info("Fetching after hours gainers by market cap")
        num_rows, gainers = Scanner.postmarket_gainers.get_scanner_data()
        return gainers.loc[gainers['market_cap_basic'] >= market_cap]    

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
    
#########################
# Download and analysis #
#########################

# Download data for the given ticker        
def download_data(ticker, period='max', interval='1d'): 
    logger.info("Downloading data for ticker {} (period: {}, interval: {})".format(ticker, period, interval))
    data = pd.DataFrame()
    try: 
        data = yf.download(tickers=ticker, 
                        period=period, 
                        interval=interval, 
                        prepost = True,
                        auto_adjust = False,
                        repair = True,
                        session=session)
        
        data.fillna(0)

    # yfinance occassionally encounters KeyErrors if separately threaded downloads are initiated   
    except KeyError as e:
        logger.exception("Encountered KeyError when downloading data for {} \n{}".format(ticker, e))

    return data

# Write data to a CSV file at the path specifiede
def update_csv(data, ticker, path):

    validate_path(path)
    path += "/{}.csv".format(ticker)
    logger.info("Writing data for ticker {} to path '{}'".format(ticker, path))

    # If the CSV file already exists, read the existing data
    if os.path.exists(path):
        existing_data = pd.read_csv(path, index_col=0, parse_dates=True)
        existing_data.fillna(0)
        
        # Append the new data to the existing data
        combined_data = pd.concat([existing_data, data])
    else:
        combined_data = data
    
    # Remove duplicate rows, if any, and prefer most recent data
    combined_data = combined_data[~combined_data.index.duplicated(keep='last')]
    
    # Save the combined data to the CSV file
    combined_data.to_csv(path)

# Generate indicator data on the provided Dataframe per the specified strategy
# Can add or remove inidicators to the strategy as needed

def generate_indicators(data):

    ta_list = []
    for strategy in strategies.get_indicator_strategies():
        strategy = strategy()
        ta_list += [x for x in strategy.ta if x not in ta_list]

    IndicatorStrategy = ta.Strategy(name="Indicator Strategy", ta=ta_list)
        
    logger.info("Generating indicator data...")
    
    data.ta.strategy(IndicatorStrategy)

# Download data and generate indicator data on specified ticker
def download_analyze_data(ticker):
    
    data = download_data(ticker)
    generate_indicators(data)
    update_csv(data, ticker, config.get_daily_data_path())

# Daily process to download daily ticker data and generate indicator data on all 
# tickers in the masterlist
def daily_download_analyze_data():
    
    logging.info("********** [START DAILY DOWNLOAD TASK] **********")
    DATA_SHAPE_THRESHOLD = 60
    
    tickers = get_all_tickers()

    num_ticker = 1
    for ticker in tickers:
        logger.info("Processesing {}, {}/{}".format(ticker, num_ticker, len(tickers)))
        logger.debug("Validate if data file for {} is up-tp-date".format(ticker))
        if not daily_data_up_to_date(fetch_daily_data(ticker)):
            logger.debug("Data file for {} is either empty or not up-to-date".format(ticker))
            
            if validate_ticker(ticker):
                # Ticker is valid - download data
                
                data = download_data(ticker)

                # Validate quality of data
                if data.shape[0] < DATA_SHAPE_THRESHOLD:
                    logger.warn("INVALID TICKER - Data of {} has shape {} after download which is less than threshold {}".format(ticker, data.shape[0], DATA_SHAPE_THRESHOLD))
                    remove_from_all_tickers(ticker)
                else:
                    generate_indicators(data)
                    update_csv(data, ticker, config.get_daily_data_path())
                    logger.debug("Download and analysis of {} complete.".format(ticker))
            else:
                logger.info("Ticker '{}' is not valid. Removing from all tickers".format(ticker))
                remove_from_all_tickers(ticker)
        else:
            logger.info("Data for {} is up-to-date. Skipping...".format(ticker))
        
        num_ticker += 1
    logger.info("Daily data download task complete!")
   
# Weekly process to download minute-by-minute ticker data on all tickers
# in the masterlist
def minute_download_data():
    
    logging.info("********** [START WEEKLY DOWNLOAD TASK] **********")
    masterlist_file = "data/ticker_masterlist.txt"
    tickers = get_all_tickers()
    

    # Verify that ticker_masterlist.txt exists
    if isinstance(tickers, list):
        num_ticker = 1
        for ticker in tickers:    
            logger.info("Processesing {}, {}/{}".format(ticker, num_ticker, len(tickers)))
            data = download_data(ticker, period="7d", interval="1m")
            logger.debug("Download and analysis of {} complete. Validate that data is valid".format(ticker))

            # No CSV was written or data was bad:
            if data.size == 0:
                logger.warn("INVALID TICKER - Data of {} has size 0 after download. Attempting to remove from masterlist".format(ticker))
                remove_from_all_tickers(ticker)
            else: # Data downloaded 
                logger.debug("Data downloaded for {} is valid.".format(ticker))
                update_csv(data, ticker, config.get_minute_data_path())
               
            num_ticker += 1

    else:
        pass

# Download and write financials for specified ticker to the financials folder
def download_financials(ticker):
    logger.info("Downloading financials for ticker {}".format(ticker))
    path = "{}/{}".format(config.get_financials_path(),ticker)
    validate_path(path)
    
    stock = yf.Ticker(ticker)
    logger.debug("Downloading income statement for {}".format(ticker))
    stock.income_stmt.to_csv("{}/income_stmt.csv".format(path))

    logger.debug("Downloading quarterly income statement for {}".format(ticker))
    stock.quarterly_income_stmt.to_csv("{}/quarterly_income_stmt.csv".format(path))

    logger.debug("Downloading balance sheet for {}".format(ticker))
    stock.balance_sheet.to_csv("{}/balance_sheet.csv".format(path))

    logger.debug("Downloading quarterly balance sheet for {}".format(ticker))
    stock.quarterly_balance_sheet.to_csv("{}/quarterly_balance_sheet.csv".format(path))

    logger.debug("Downloading cashflow for {}".format(ticker))
    stock.cashflow.to_csv("{}/cashflow.csv".format(path))

    logger.debug("Downloading quarterly cashflow for {}".format(ticker))
    stock.quarterly_cashflow.to_csv("{}/quarterly_cashflow.csv".format(path))

    logger.debug("Downloading earnings dates for {}".format(ticker))
    stock.get_earnings_dates(limit=8).to_csv("{}/earnings_dates.csv".format(path))
    

######################################################
# 'Fetch' functions for returning data saved to disk #
######################################################

# Return all filepaths of all charts for a given ticker
def fetch_charts(ticker):
    logger.info("Fetching charts for ticker {}".format(ticker))
    charts_path = "{}/{}/".format(config.get_plots_path(),ticker)
    charts = os.listdir(charts_path)
    for i in range(0, len(charts)):
        logger.debug("Appending {} chart: {}".format(ticker, charts_path + charts[i]))
        charts[i] = charts_path + charts[i]
    return charts

# Return the latest data with technical indicators as a Pandas dataframe.
# If CSV does not exist or is not up-to-date, return empty DataFrame
def fetch_daily_data(ticker):
    logger.debug("Fetching daily data file for ticker {}".format(ticker))
    data = pd.DataFrame()
    data_path = "{}/{}.csv".format(config.get_daily_data_path(), ticker)
    if not os.path.isfile(data_path):
        logger.warning("CSV file for {} does not exist.".format(ticker))
    else:
        logging.debug("Data file for {} exists: {}".format(ticker, data_path))
        data = pd.read_csv(data_path, parse_dates=True, index_col='Date').sort_index()
    
    return data

# Return the latest minute-by-minute data with technical indicators as a Pandas dataframe.
# If CSV does not exist or is not up-to-date, return empty DataFrame
def fetch_minute_data(ticker):
    logger.info("Fetching minute data file for ticker {}".format(ticker))
    data = pd.DataFrame()
    data_path = "{}/{}.csv".format(config.get_minute_data_path(), ticker)
    if not os.path.isfile(data_path):
        logger.warning("CSV file for {} does not exist.".format(ticker))
    else:
        logging.debug("Data file for {} exists: {}".format(ticker, data_path))
        data = pd.read_csv(data_path, parse_dates=True, index_col='Datetime').sort_index()
    
    return data

# Return analysis text for the given ticker
def fetch_analysis(ticker):
    logger.info("Fetching analysis for ticker {}".format(ticker))
    analyis = ''
    path = "{}/{}/".format(config.get_analysis_path(),ticker)
    for file in os.listdir(path):
        logger.debug("Analysis for ticker {} found at {}".format(ticker, path))
        data = open(path + file)
        analyis += data.read() + "\n"

    return analyis

# Return list of files that contain financials data for the specified ticker
def fetch_financials(ticker):
    logger.info("Fetching financials for ticker {}".format(ticker))
    path = "{}/{}".format(config.get_financials_path(),ticker)
    if not validate_path(path):
        logger.debug("Path {} not found".format(path))
        download_financials(ticker)
    financials = os.listdir(path)
    for i in range(0, len(financials)):
        logger.debug("Appending path {} to list of financials".format(path + "/" + financials[i]))
        financials[i] = path + "/" + financials[i]
    return financials
    

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
       
# Return Dataframe with the latest OHLCV of requested ticker
def get_days_summary(data):
    logger.info("Fetching today's OHLCV summary for provided data")
    # Assumes data has been pulled recently. Mainly called when running or fetching reports. 
    if len(data) > 0:
        summary = data[["Open","High", "Low","Close","Volume"]].iloc[-1]
        return summary
    else:
        logger.warning("Could not retrieve the day's summary for provided data")
        return None
    
# Return next earnings date of the specified ticker, if available
def get_next_earnings_date(ticker):
    logger.info("Finding next earnings date for ticker {}".format(ticker))
    try:
        earnings_date = yf.Ticker(ticker).calendar['Earnings Date'][0]
        logger.info("Next earnings date for ticker {} is {}".format(ticker, earnings_date))
        return earnings_date
    except IndexError as e:
        logger.exception("Encountered IndexError when fetching next earnings date for ticker {}:\n{}".format(ticker, e))
        return "Earnings date unavailable"
    except KeyError as e:
        logger.exception("Encountered KeyError when fetching next earnings date for ticker {}:\n{}".format(ticker, e))
        return "Earnings Date unavailable"

def get_ticker_info(ticker):
    logger.debug("Retrieving info for ticker '{}'".format(ticker))
    stock = yf.Ticker(ticker)
    return stock.info
    
# Return dataframe containing data from 'all_tickers.csv'
def get_all_tickers_data():
    return pd.read_csv("{}/all_tickers.csv".format(config.get_utils_path()), index_col='Symbol')
    
# Return list of tickers available in the masterlist
def get_all_tickers():
    logger.debug("Fetching tickers from masterlist")
    
    try:
        all_tickers_data = get_all_tickers_data()
        return all_tickers_data.index.to_list()
    except FileNotFoundError as e:
        logger.exception("Encountered FileNotFoundError when attempting to load data from 'all_tickers.csv:\n{}".format(e))
        logger.debug("'all_tickers.csv' file does not exist")
        return []
    
def add_to_all_tickers(ticker):
    ticker_info = get_ticker_info(ticker)
                
    #Init dict with columns to be added to all_tickers
    columns = ["Name","Last","Sale","Net Change","% Change","Market Cap","Country","IPO Year","Volume","Sector","Industry"]
    ticker_data = dict.fromkeys(columns, "N/A")

    ticker_data['Name'] = ticker_info.get("longName")
    ticker_data['Sector'] = ticker_info.get("sector")
    ticker_data['Industry'] = ticker_info.get("industry")
    ticker_data['Market Cap'] = ticker_info.get("marketCap")
    ticker_data['Country'] = ticker_info.get('country')

    row = pd.DataFrame(data=[ticker_data], index = [ticker])
    row.index.name = 'Symbol'

    all_tickers = get_all_tickers_data()
    all_tickers = pd.concat([all_tickers, row])
    all_tickers.to_csv("{}/all_tickers.csv".format(config.get_utils_path()))
    logger.info("Added new row for ticker '{}' to all tickers".format(ticker))


# Remove selected ticker row from 'all_tickers.csv'
def remove_from_all_tickers(ticker):
    
    data = get_all_tickers_data()
    data = data.drop(index=ticker)
    data.to_csv("{}/all_tickers.csv".format(config.get_utils_path()))
    logger.info("Removed ticker '{}' from all tickers".format(ticker))

# Validate that data file for specified ticker has data up to yesterday
def daily_data_up_to_date(data):
    if data.size == 0:
        logger.debug("Provided data has size 0 - failed to validate if up-to-date")
        return False
    else:
        yesterday = datetime.date.today() - timedelta(days=1)
        while yesterday.weekday() > 5:
            yesterday = yesterday - timedelta(days=1)
        data_dates = [date.date() for date in data.index]
        latest_date = data_dates[-1]
        if yesterday in data_dates:
            logger.debug("Provided data is up-to-date")
            return True
        else:
            logger.info("Provided data is not up-to-date")
            return False

# Validate that selected columns exist within DataFrame 'data'
def validate_columns(data, columns):
    logger.debug("Validating that columns {} exist in data".format(columns))
    for column in columns: 
        if column not in data.columns:
            logger.debug("Column {} does not exist in data".format(column))
            return False
    logger.debug("Columns {} exist in data".format(columns))
    return True

# Return supported exchanges
def get_supported_exchanges():
    return ['NASDAQ', 'NYSE', 'AMEX']

#########
# Tests #
#########

def test():
    pass

if __name__ == "__main__":
    logger.info("stockdata.py initialized")
    test()
    pass