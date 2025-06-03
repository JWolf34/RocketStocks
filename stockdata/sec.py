import logging
from ratelimit import limits, sleep_and_retry
import requests
import pandas as pd
import datetime


# Logging configuration
logger = logging.getLogger(__name__)

class SEC():
    def __init__(self, sd):
        self.headers = {"User-Agent":"johnmwolf34@gmail.com"}
        self.MAX_CALLS = 10
        self.MAX_CALLS = 1
        self.sd = sd # StockData

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
        submissions_json = requests.get(f"https://data.sec.gov/submissions/CIK{self.sd.get_cik(ticker)}.json", headers=self.headers).json()
        return submissions_json
    
    def get_recent_filings(self, ticker, latest=10):
        filings = pd.DataFrame.from_dict(self.get_submissions_data(ticker)['filings']['recent'])[:latest]
        filings['link'] = pd.Series([self.get_link_to_filing(ticker=ticker, filing=filing) for filing in filings.to_dict(orient='records')])
        return filings

    def get_filings_from_today(self, ticker):
        recent_filings = self.get_recent_filings(ticker)
        today_string = datetime.datetime.today().strftime("%Y-%m-%d")
        return recent_filings.loc[recent_filings['filingDate'] == today_string]

    def get_link_to_filing(self, ticker, filing):
        return f"https://sec.gov/Archives/edgar/data/{self.sd.get_cik(ticker).lstrip("0")}/{filing['accessionNumber'].replace("-","")}/{filing['primaryDocument']}"

    @sleep_and_retry
    @limits(calls = 5, period = 1) # 5 calls per second
    def get_accounts_payable(self, ticker):
        logger.debug(f"Fetching accounts payable from SEC for ticker {ticker}")
        json = requests.get(f"https://data.sec.gov/api/xbrl/companyconcept/CIK{self.sd.get_cik(ticker)}/us-gaap/AccountsPayableCurrent.json", headers=self.headers).json()
        return pd.DataFrame.from_dict(json)

    @sleep_and_retry
    @limits(calls = 5, period = 1) # 5 calls per second
    def get_company_facts(self, ticker):
        logger.debug(f"Fetching company facts from SEC for ticker {ticker}")
        json = requests.get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{self.sd.get_cik(ticker)}.json", headers=self.headers).json()
        return pd.DataFrame.from_dict(json)
