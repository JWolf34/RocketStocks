"""Repository for the `tickers` table."""
import logging
import re
import pandas as pd

logger = logging.getLogger(__name__)

_TICKERS_COLS = [
    'ticker', 'cik', 'name', 'country', 'ipoyear',
    'industry', 'sector', 'exchange', 'security_type', 'sic_code', 'delist_date',
]

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

    async def update_tickers(self):
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
        tickers_data = tickers_data[tickers_data['symbol'].isin(await self.get_all_tickers())]
        tickers_data = tickers_data.drop(columns=drop_columns)
        tickers_data = tickers_data.rename(columns=column_map)
        tickers_data = tickers_data[list(column_map.values())]
        tickers_data['name'] = tickers_data['name'].apply(clean_company_name)

        for _, row in tickers_data.iterrows():
            ticker = row['ticker']
            await self._db.execute(
                "UPDATE tickers SET name = %s, country = %s, ipoyear = %s, "
                "industry = %s, sector = %s WHERE ticker = %s",
                [row['name'], row['country'], row['ipoyear'],
                 row['industry'], row['sector'], ticker],
            )
            logger.info(f"Updated ticker '{ticker}' in database:\n{row}")

        elapsed = time.time() - start_time
        logger.info("Tickers have been updated!")
        logger.debug(f"Updating tickers completed in {elapsed:.2f} seconds")

    async def insert_tickers(self):
        """Insert all NASDAQ-listed tickers into the database."""
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

        all_tickers = pd.merge(nasdaq_tickers, sec_tickers, on='ticker', how='left')
        all_tickers['cik'] = all_tickers['cik'].where(all_tickers['cik'].notna(), None)

        values = [tuple(row) for row in all_tickers.values]
        cols = all_tickers.columns.tolist()
        placeholders = ', '.join(['%s'] * len(cols))
        col_list = ', '.join(cols)
        await self._db.execute_batch(
            f"INSERT INTO tickers ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
            values,
        )

        elapsed = time.time() - start_time
        logger.info("Tickers have been updated!")
        logger.debug(f"Insert new tickers completed in {elapsed:.2f} seconds")

    async def enrich_ticker(self, ticker: str) -> bool:
        """Enrich a single ticker with Tiingo metadata and SEC SIC code."""
        if self._tiingo is None:
            logger.warning("Tiingo client not configured; skipping enrichment")
            return False

        updated = False
        metadata = self._tiingo.get_ticker_metadata(ticker)
        if metadata:
            tiingo_name = metadata.get('name', '')
            if tiingo_name:
                await self._db.execute(
                    "UPDATE tickers SET exchange = %s, security_type = %s, name = %s WHERE ticker = %s",
                    [metadata.get('exchange'), metadata.get('security_type'),
                     clean_company_name(tiingo_name), ticker],
                )
            else:
                await self._db.execute(
                    "UPDATE tickers SET exchange = %s, security_type = %s WHERE ticker = %s",
                    [metadata.get('exchange'), metadata.get('security_type'), ticker],
                )
            updated = True
            logger.debug(f"Enriched ticker '{ticker}' with Tiingo metadata: {metadata}")

        # SIC code from SEC submissions (requires CIK)
        cik = await self.get_cik(ticker)
        if cik:
            try:
                submissions = await self._sec.get_submissions_data(ticker)
                sic = submissions.get('sic')
                if sic:
                    await self._db.execute(
                        "UPDATE tickers SET sic_code = %s WHERE ticker = %s",
                        [str(sic), ticker],
                    )
                    updated = True
                    logger.debug(f"Enriched ticker '{ticker}' with SIC code: {sic}")
            except Exception as exc:
                logger.warning(f"Failed to fetch SEC submissions for '{ticker}': {exc}")

        return updated

    async def enrich_unenriched_batch(self, limit: int = 240) -> int:
        """Enrich up to *limit* tickers that have no exchange set yet."""
        rows = await self._db.execute(
            "SELECT ticker FROM tickers WHERE exchange IS NULL ORDER BY ticker LIMIT %s",
            [limit],
        )
        if not rows:
            logger.info("No unenriched tickers found")
            return 0

        tickers = [r[0] for r in rows]
        enriched = 0
        for ticker in tickers:
            try:
                if await self.enrich_ticker(ticker):
                    enriched += 1
            except Exception as exc:
                logger.warning(f"Failed to enrich ticker '{ticker}': {exc}")

        logger.info(f"Enriched {enriched}/{len(tickers)} tickers in batch")
        return enriched

    async def import_delisted_tickers(self) -> int:
        """Import all delisted tickers from Tiingo into the database."""
        if self._tiingo is None:
            logger.warning("Tiingo client not configured; skipping delisted import")
            return 0

        all_tickers_df = self._tiingo.list_all_tickers()
        delisted = all_tickers_df[all_tickers_df['delist_date'].notna()].copy()

        if delisted.empty:
            logger.info("No delisted tickers returned from Tiingo")
            return 0

        insert_df = delisted[['ticker', 'name', 'exchange', 'security_type', 'delist_date']].copy()
        insert_df = insert_df[insert_df['name'].notna() & (insert_df['name'] != '')]
        insert_df['name'] = insert_df['name'].apply(clean_company_name)
        insert_df = insert_df.drop_duplicates(subset='ticker')

        before_rows = await self._db.execute("SELECT COUNT(*) FROM tickers", fetchone=True)
        before_count = before_rows[0] if before_rows else 0

        values = [tuple(row) for row in insert_df.values]
        await self._db.execute_batch(
            """
            INSERT INTO tickers (ticker, name, exchange, security_type, delist_date)
            VALUES (%s, %s, %s, %s, %s)
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
        after_rows = await self._db.execute("SELECT COUNT(*) FROM tickers", fetchone=True)
        after_count = after_rows[0] if after_rows else 0
        inserted = after_count - before_count
        logger.info(f"Imported {inserted} new delisted tickers from Tiingo")
        return inserted

    async def get_ticker_info(self, ticker: str) -> dict | None:
        """Return ticker row from database as a dict, or None if not found."""
        logger.info(f"Fetching info for ticker '{ticker}' from database")
        row = await self._db.execute(
            f"SELECT {', '.join(_TICKERS_COLS)} FROM tickers WHERE ticker = %s",
            [ticker],
            fetchone=True,
        )
        if row is None:
            logger.warning(f"No info found for ticker '{ticker}'")
            return None
        return dict(zip(_TICKERS_COLS, row))

    async def get_all_ticker_info(self) -> pd.DataFrame:
        """Return information (DataFrame) on all tickers from database."""
        logger.info("Fetching info for all tickers in database")
        rows = await self._db.execute(
            f"SELECT {', '.join(_TICKERS_COLS)} FROM tickers"
        )
        df = pd.DataFrame(rows or [], columns=_TICKERS_COLS)
        df.set_index('ticker')
        logger.debug(f"Found data for {len(df)} tickers in database")
        return df

    async def get_all_tickers(self) -> list:
        """Return list of all ticker symbols in database."""
        logger.info('Fetching all tickers in database')
        rows = await self._db.execute("SELECT ticker FROM tickers")
        return [r[0] for r in (rows or [])]

    async def get_all_tickers_by_sector(self, sector: str) -> list | None:
        """Return list of tickers whose sector matches *sector*."""
        logger.info(f"Fetching all tickers in sector {sector} from database")
        rows = await self._db.execute(
            "SELECT ticker FROM tickers WHERE sector LIKE %s",
            [f"%{sector}%"],
        )
        return [r[0] for r in rows] if rows else None

    async def get_cik(self, ticker: str) -> str | None:
        """Return CIK number of *ticker* from database."""
        row = await self._db.execute(
            "SELECT cik FROM tickers WHERE ticker = %s",
            [ticker],
            fetchone=True,
        )
        return row[0] if row else None

    async def validate_ticker(self, ticker: str) -> bool:
        """Return True if *ticker* exists in database."""
        logger.info(f"Verifying that ticker '{ticker}' is valid")
        row = await self._db.execute(
            "SELECT ticker FROM tickers WHERE ticker = %s",
            [ticker],
            fetchone=True,
        )
        return row is not None

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
