"""Repository for the `tickers` table."""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class TickerRepository:
    def __init__(self, db, nasdaq, sec):
        self._db = db
        self._nasdaq = nasdaq
        self._sec = sec

    def update_tickers(self):
        """Update tickers table with the most up-to-date information from the NASDAQ."""
        import time
        logger.info("Updating tickers database table with up-to-date ticker data")
        start_time = time.time()

        column_map = {
            'name': 'name',
            'country': 'country',
            'ipoyear': 'ipoyear',
            'industry': 'industry',
            'sector': 'sector',
            'symbol': 'ticker',
        }
        drop_columns = ['lastsale', 'netchange', 'pctchange', 'volume']

        tickers_data = self._nasdaq.get_all_tickers()
        logger.debug(f"Found {len(tickers_data)} tickers on NASDAQ")
        tickers_data = tickers_data[tickers_data['symbol'].isin(self.get_all_tickers())]
        tickers_data = tickers_data.drop(columns=drop_columns)
        tickers_data = tickers_data.rename(columns=column_map)
        tickers_data = tickers_data[list(column_map.values())]

        for _, row in tickers_data.iterrows():
            ticker = row['ticker']
            set_fields = list(row.items())
            self._db.update(
                table='tickers',
                set_fields=set_fields,
                where_conditions=[('ticker', ticker)],
            )
            logger.info(f"Updated ticker '{ticker}' in database:\n{row}")

        elapsed = time.time() - start_time
        logger.info("Tickers have been updated!")
        logger.debug(f"Updating tickers completed in {elapsed:.2f} seconds")

    async def insert_tickers(self):
        """Identify data on all tickers from the SEC and insert into the database."""
        import time
        logger.info("Updating tickers database table with new tickers from SEC")
        start_time = time.time()

        sec_tickers = self._sec.get_company_tickers()
        sec_column_map = {'ticker': 'ticker', 'cik_str': 'cik'}
        sec_tickers = sec_tickers.filter(list(sec_column_map.keys()))
        sec_tickers = sec_tickers.rename(columns=sec_column_map)
        sec_tickers['cik'] = sec_tickers['cik'].apply(lambda cik: str(cik).zfill(10))

        nasdaq_tickers = self._nasdaq.get_all_tickers()
        nasdaq_column_map = {
            'symbol': 'ticker',
            'name': 'name',
            'country': 'country',
            'ipoyear': 'ipoyear',
            'industry': 'industry',
            'sector': 'sector',
            'url': 'url',
        }
        nasdaq_tickers = nasdaq_tickers.filter(list(nasdaq_column_map.keys()))
        nasdaq_tickers = nasdaq_tickers.rename(columns=nasdaq_column_map)

        all_tickers = pd.merge(sec_tickers, nasdaq_tickers, on='ticker', how='left')
        all_tickers.set_index('ticker')

        values = [tuple(row) for row in all_tickers.values]
        self._db.insert(table='tickers', fields=all_tickers.columns.to_list(), values=values)

        elapsed = time.time() - start_time
        logger.info("Tickers have been updated!")
        logger.debug(f"Insert new tickers completed in {elapsed:.2f} seconds")

    def get_ticker_info(self, ticker: str) -> dict | None:
        """Return ticker row from database as a dict, or None if not found."""
        logger.info(f"Fetching info for ticker '{ticker}' from database")
        fields = self._db.get_table_columns('tickers')
        result = self._db.select(
            table='tickers',
            fields=fields,
            where_conditions=[('ticker', ticker)],
            fetchall=False,
        )
        if result is None:
            logger.warning(f"No info found for ticker '{ticker}'")
            return None
        return dict(zip(fields, result))

    def get_all_ticker_info(self) -> pd.DataFrame:
        """Return information (DataFrame) on all tickers from database."""
        logger.info("Fetching info for all tickers in database")
        columns = self._db.get_table_columns('tickers')
        data = self._db.select(table='tickers', fields=columns, fetchall=True)
        df = pd.DataFrame(data, columns=columns)
        df.set_index('ticker')
        logger.debug(f"Found data for {len(df)} tickers in database")
        return df

    def get_all_tickers(self) -> list:
        """Return list of all ticker symbols in database."""
        logger.info('Fetching all tickers in database')
        results = self._db.select(table='tickers', fields=['ticker'], fetchall=True)
        return [r[0] for r in results]

    def get_all_tickers_by_sector(self, sector: str) -> list | None:
        """Return list of tickers whose sector matches *sector*."""
        logger.info(f"Fetching all tickers in sector {sector} from database")
        results = self._db.select(
            table='tickers',
            fields=['ticker'],
            where_conditions=[('sector', 'LIKE', f"%{sector}%")],
        )
        return [r[0] for r in results] if results else None

    def get_cik(self, ticker: str) -> str | None:
        """Return CIK number of *ticker* from database."""
        result = self._db.select(
            table='tickers',
            fields=['cik'],
            where_conditions=[('ticker', ticker)],
            fetchall=False,
        )
        return result[0] if result else None

    async def validate_ticker(self, ticker: str) -> bool:
        """Return True if *ticker* exists in database."""
        logger.info(f"Verifying that ticker '{ticker}' is valid")
        result = self._db.select(
            table='tickers',
            fields=['ticker'],
            where_conditions=[('ticker', ticker)],
            fetchall=False,
        )
        return result is not None

    async def parse_valid_tickers(self, ticker_string: str) -> tuple[list, list]:
        """Return (valid_tickers, invalid_tickers) from space-separated string."""
        logger.info(f"Parsing valid tickers from string: '{ticker_string}'")
        tickers = ticker_string.upper().split()
        valid, invalid = [], []
        for ticker in tickers:
            if await self.validate_ticker(ticker):
                valid.append(ticker)
            else:
                invalid.append(ticker)
        logger.info(f"Parsed {len(valid)} valid: {valid}, {len(invalid)} invalid: {invalid}")
        return valid, invalid
