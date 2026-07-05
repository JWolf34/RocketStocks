import asyncio
import logging
from ratelimit import limits, sleep_and_retry
import requests
import pandas as pd
import datetime


# Logging configuration
logger = logging.getLogger(__name__)

class SEC():
    def __init__(self, db=None):
        self.headers = {"User-Agent":"johnmwolf34@gmail.com"}
        self.MAX_CALLS = 10
        self.MAX_CALLS = 1
        self._db = db

    async def _get_cik(self, ticker: str) -> str | None:
        if not self._db:
            return None
        row = await self._db.execute(
            "SELECT cik FROM tickers WHERE ticker = %s", [ticker], fetchone=True
        )
        return row[0] if row else None

    @sleep_and_retry
    @limits(calls = 5, period = 1) # 5 calls per 1 second
    def get_cik_from_ticker(self, ticker):
        logger.debug(f"Fetching CIK number for ticker {ticker}")
        tickers_data = requests.get("https://www.sec.gov/files/company_tickers.json", headers=self.headers).json()
        for company in tickers_data.values():
            if company['ticker'] == ticker:
                cik = str(company['cik_str']).zfill(10)
                return cik

    async def get_submissions_data(self, ticker):
        logger.debug(f"Fetching  SEC submissions for ticker {ticker}")
        cik = await self._get_cik(ticker)
        if not cik:
            logger.warning(f"No CIK found for ticker {ticker}, skipping SEC submissions fetch")
            return None
        resp = await asyncio.to_thread(
            requests.get, f"https://data.sec.gov/submissions/CIK{cik}.json", headers=self.headers
        )
        if not resp.text:
            logger.warning(f"Empty response from SEC submissions for ticker {ticker}")
            return None
        return resp.json()

    async def get_recent_filings(self, ticker, latest=10):
        submissions = await self.get_submissions_data(ticker)
        if not submissions:
            return pd.DataFrame()
        filings = pd.DataFrame.from_dict(submissions['filings']['recent'])[:latest]
        links = []
        for filing in filings.to_dict(orient='records'):
            links.append(await self.get_link_to_filing(ticker=ticker, filing=filing))
        filings['link'] = pd.Series(links)
        return filings

    async def get_filings_from_today(self, ticker):
        recent_filings = await self.get_recent_filings(ticker)
        today_string = datetime.datetime.today().strftime("%Y-%m-%d")
        return recent_filings.loc[recent_filings['filingDate'] == today_string]

    async def get_link_to_filing(self, ticker, filing):
        cik = await self._get_cik(ticker)
        if not cik:
            return None
        return f"https://sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{filing['accessionNumber'].replace('-','')}/{filing['primaryDocument']}"

    async def get_accounts_payable(self, ticker):
        logger.debug(f"Fetching accounts payable from SEC for ticker {ticker}")
        try:
            cik = await self._get_cik(ticker)
            resp = await asyncio.to_thread(
                requests.get,
                f"https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/us-gaap/AccountsPayableCurrent.json",
                headers=self.headers,
            )
            return pd.DataFrame.from_dict(resp.json())
        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"Encountered error when fetching accounts payable for ticker '{ticker}':\n{e}")
            return pd.DataFrame()

    async def get_company_facts(self, ticker):
        logger.debug(f"Fetching company facts from SEC for ticker {ticker}")
        try:
            cik = await self._get_cik(ticker)
            resp = await asyncio.to_thread(
                requests.get,
                f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
                headers=self.headers,
            )
            return resp.json()
        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"Encountered error when fetching company facts for ticker '{ticker}':\n{e}")
            return {}

    @sleep_and_retry
    @limits(calls = 5, period = 1) # 5 calls per second
    def get_company_tickers(self):
        logger.debug(f"Fetching all company tickers from SEC")
        response = requests.get("https://www.sec.gov/files/company_tickers.json", headers=self.headers)
        tickers = [ticker for ticker in response.json().values()]
        return pd.DataFrame(tickers)
