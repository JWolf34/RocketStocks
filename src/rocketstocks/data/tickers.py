"""Repository for the `tickers` table."""
import logging
import re
import pandas as pd
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

# Phase 1 — regex patterns (applied in order, most specific first)
_ADR_CLAUSE_RE = re.compile(
    r'\s+American Depositary (?:Shares?|Receipts?)\b.*$', re.IGNORECASE
)
_SHARE_CLASS_DASH_RE = re.compile(
    r'\s+-\s+(?:Class\s+[A-Z]|Ordinary Shares?|Common Stock)\b.*$', re.IGNORECASE
)
_SHARE_CLASS_SUFFIX_RE = re.compile(
    r'\s+Class\s+[A-Z]\s+(?:Common Stock|Ordinary Shares?|Shares?)\b.*$', re.IGNORECASE
)
_COMMON_STOCK_RE = re.compile(
    r'\s+(?:Common Stock|Ordinary Shares?)\b.*$', re.IGNORECASE
)
_SPECIAL_SECURITIES_RE = re.compile(
    r'\s+(?:Warrants?|Units?|Rights?|Preferred Stock)\b.*$', re.IGNORECASE
)
_REGISTERED_SHARES_RE = re.compile(
    r',?\s+Registered Shares?\b.*$', re.IGNORECASE
)

_PHASE1_PATTERNS = [
    _ADR_CLAUSE_RE,
    _SHARE_CLASS_DASH_RE,
    _SHARE_CLASS_SUFFIX_RE,
    _COMMON_STOCK_RE,
    _SPECIAL_SECURITIES_RE,
    _REGISTERED_SHARES_RE,
]

# Phase 2 — legal entity suffixes (conservative, endswith)
_LEGAL_SUFFIXES = [
    ", Inc.", " Inc.", " Inc", ", Corp.", " Corp.", ", Ltd.", " Ltd.",
    " Limited", ", Co.", " Co.", ", LLC", ", PLC", ", SA", ", NV",
    ", SE", " SE", ", L.P.", ", N.A.",
]


def clean_company_name(name: str) -> str:
    """Remove trailing boilerplate and legal suffixes from a company name."""
    if not name:
        return name
    result = name.strip()
    # Phase 1 — regex
    for pattern in _PHASE1_PATTERNS:
        result = pattern.sub('', result)
    # Phase 2 — legal suffixes
    for suffix in _LEGAL_SUFFIXES:
        if result.endswith(suffix):
            result = result[: -len(suffix)]
            break
    return result.rstrip('., ').strip()


class TickerRepository:
    def __init__(self, db, nasdaq, sec, tiingo=None):
        self._db = db
        self._nasdaq = nasdaq
        self._sec = sec
        self._tiingo = tiingo

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
        tickers_data['name'] = tickers_data['name'].apply(clean_company_name)

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

    def insert_tickers(self):
        """Insert all NASDAQ-listed tickers into the database.

        NASDAQ is the source of truth for the active ticker universe.
        All NASDAQ-listed stocks, ETFs, ADRs are inserted regardless of SEC presence.
        SEC CIK is merged in where available.
        """
        import time
        logger.info("Updating tickers database table with new tickers from NASDAQ")
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
        }
        nasdaq_tickers = nasdaq_tickers.filter(list(nasdaq_column_map.keys()))
        nasdaq_tickers = nasdaq_tickers.rename(columns=nasdaq_column_map)
        nasdaq_tickers['name'] = nasdaq_tickers['name'].apply(clean_company_name)

        # NASDAQ is primary; SEC CIK joined in where available
        all_tickers = pd.merge(nasdaq_tickers, sec_tickers, on='ticker', how='left')
        all_tickers['cik'] = all_tickers['cik'].where(all_tickers['cik'].notna(), None)

        values = [tuple(row) for row in all_tickers.values]
        self._db.insert(table='tickers', fields=all_tickers.columns.to_list(), values=values)

        elapsed = time.time() - start_time
        logger.info("Tickers have been updated!")
        logger.debug(f"Insert new tickers completed in {elapsed:.2f} seconds")

    def enrich_ticker(self, ticker: str) -> bool:
        """Enrich a single ticker with Tiingo metadata and SEC SIC code.

        Updates exchange, security_type, name via Tiingo.
        Updates sic_code via SEC submissions if CIK is available.
        Returns True if at least one field was updated.
        """
        if self._tiingo is None:
            logger.warning("Tiingo client not configured; skipping enrichment")
            return False

        updated = False
        metadata = self._tiingo.get_ticker_metadata(ticker)
        if metadata:
            set_fields = [
                ('exchange', metadata.get('exchange')),
                ('security_type', metadata.get('security_type')),
            ]
            tiingo_name = metadata.get('name', '')
            if tiingo_name:
                set_fields.append(('name', clean_company_name(tiingo_name)))
            self._db.update(
                table='tickers',
                set_fields=set_fields,
                where_conditions=[('ticker', ticker)],
            )
            updated = True
            logger.debug(f"Enriched ticker '{ticker}' with Tiingo metadata: {metadata}")

        # SIC code from SEC submissions (requires CIK)
        cik = self.get_cik(ticker)
        if cik:
            try:
                submissions = self._sec.get_submissions_data(ticker)
                sic = submissions.get('sic')
                if sic:
                    self._db.update(
                        table='tickers',
                        set_fields=[('sic_code', str(sic))],
                        where_conditions=[('ticker', ticker)],
                    )
                    updated = True
                    logger.debug(f"Enriched ticker '{ticker}' with SIC code: {sic}")
            except Exception as exc:
                logger.warning(f"Failed to fetch SEC submissions for '{ticker}': {exc}")

        return updated

    def enrich_unenriched_batch(self, limit: int = 240) -> int:
        """Enrich up to *limit* tickers that have no exchange set yet.

        Returns count of tickers successfully enriched.
        """
        with self._db._cursor() as cur:
            cur.execute(
                "SELECT ticker FROM tickers WHERE exchange IS NULL ORDER BY ticker LIMIT %s;",
                (limit,),
            )
            rows = cur.fetchall()

        results = rows
        if not results:
            logger.info("No unenriched tickers found")
            return 0

        tickers = [r[0] for r in results]
        enriched = 0
        for ticker in tickers:
            try:
                if self.enrich_ticker(ticker):
                    enriched += 1
            except Exception as exc:
                logger.warning(f"Failed to enrich ticker '{ticker}': {exc}")

        logger.info(f"Enriched {enriched}/{len(tickers)} tickers in batch")
        return enriched

    def import_delisted_tickers(self) -> int:
        """Import all delisted tickers from Tiingo into the database.

        Runs as a full-refresh (ON CONFLICT DO NOTHING skips existing rows).
        Returns count of newly inserted tickers.
        """
        if self._tiingo is None:
            logger.warning("Tiingo client not configured; skipping delisted import")
            return 0

        all_tickers_df = self._tiingo.list_all_tickers()
        delisted = all_tickers_df[all_tickers_df['delist_date'].notna()].copy()

        if delisted.empty:
            logger.info("No delisted tickers returned from Tiingo")
            return 0

        # Map to tickers table columns (no cik, country, ipoyear, industry, sector, sic_code)
        insert_df = delisted[['ticker', 'name', 'exchange', 'security_type', 'delist_date']].copy()
        # Add required NOT NULL name — already present; add placeholder for missing names
        insert_df = insert_df[insert_df['name'].notna() & (insert_df['name'] != '')]
        insert_df['name'] = insert_df['name'].apply(clean_company_name)
        insert_df = insert_df.drop_duplicates(subset='ticker')

        before_count = len(self._db.select(table='tickers', fields=['ticker'], fetchall=True) or [])
        values = [tuple(row) for row in insert_df.values]
        with self._db._cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO tickers (ticker, name, exchange, security_type, delist_date)
                VALUES %s
                ON CONFLICT (ticker) DO UPDATE SET
                    delist_date = CASE
                        WHEN EXCLUDED.delist_date IS NOT NULL THEN EXCLUDED.delist_date
                        ELSE tickers.delist_date
                    END,
                    name = CASE
                        WHEN tickers.name IS NULL OR tickers.name = '' THEN EXCLUDED.name
                        ELSE tickers.name
                    END
                """,
                values,
            )
        after_count = len(self._db.select(table='tickers', fields=['ticker'], fetchall=True) or [])
        inserted = after_count - before_count
        logger.info(f"Imported {inserted} new delisted tickers from Tiingo")
        return inserted

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
