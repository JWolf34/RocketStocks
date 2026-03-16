"""Tiingo API client — ticker metadata and delisted price history."""
import datetime
import logging
import pandas as pd
from tiingo import TiingoClient

from rocketstocks.core.config.settings import settings

logger = logging.getLogger(__name__)

_DELIST_GRACE_DAYS = 30

_ASSET_TYPE_MAP = {
    'Stock': 'CS',
    'Common Stock': 'CS',
    'ETF': 'ETF',
    'ADR': 'ADR',
}


class Tiingo:
    def __init__(self, api_key=None):
        key = api_key or settings.tiingo_api_key
        self._client = TiingoClient({'api_key': key, 'session': True})

    def list_all_tickers(self) -> pd.DataFrame:
        """Return DataFrame of all Tiingo-supported tickers (active + delisted).

        Columns: ticker, name, exchange, security_type, delist_date
        """
        try:
            raw = self._client.list_tickers()
        except Exception as exc:
            logger.error(f"Tiingo list_tickers() failed: {exc}")
            return pd.DataFrame(columns=['ticker', 'name', 'exchange', 'security_type', 'delist_date'])

        rows = []
        for item in raw:
            end_date = item.get('endDate')
            if end_date:
                try:
                    parsed = pd.to_datetime(end_date).date()
                    days_ago = (datetime.date.today() - parsed).days
                    delist_date = parsed if days_ago > _DELIST_GRACE_DAYS else None
                except Exception:
                    delist_date = None
            else:
                delist_date = None

            asset_type = item.get('assetType', '')
            rows.append({
                'ticker': item.get('ticker', '').upper(),
                'name': item.get('name', ''),
                'exchange': item.get('exchangeCode', ''),
                'security_type': _ASSET_TYPE_MAP.get(asset_type, asset_type),
                'delist_date': delist_date,
            })

        return pd.DataFrame(rows, columns=['ticker', 'name', 'exchange', 'security_type', 'delist_date'])

    def get_ticker_metadata(self, ticker: str) -> dict | None:
        """Return metadata dict for a single ticker. Returns None if not found."""
        try:
            data = self._client.get_ticker_metadata(ticker)
            if not data:
                return None
            asset_type = data.get('assetType', '')
            end_date = data.get('endDate')
            if end_date:
                try:
                    parsed = pd.to_datetime(end_date).date()
                    days_ago = (datetime.date.today() - parsed).days
                    delist_date = parsed if days_ago > _DELIST_GRACE_DAYS else None
                except Exception:
                    delist_date = None
            else:
                delist_date = None
            return {
                'ticker': ticker.upper(),
                'name': data.get('name', ''),
                'exchange': data.get('exchangeCode', ''),
                'security_type': _ASSET_TYPE_MAP.get(asset_type, asset_type),
                'delist_date': delist_date,
            }
        except Exception as exc:
            logger.warning(f"Tiingo get_ticker_metadata({ticker}) failed: {exc}")
            return None

    def get_daily_price_history(self, ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Return daily OHLCV DataFrame for *ticker* (works for delisted tickers).

        start_date / end_date: 'YYYY-MM-DD'. Returns empty DataFrame on error.
        """
        try:
            df = self._client.get_dataframe(ticker, startDate=start_date, endDate=end_date)
            if df is None or df.empty:
                return pd.DataFrame()
            df = df.reset_index()
            # Tiingo returns 'date' or 'Date' index; normalise column names
            df.columns = [c.lower() for c in df.columns]
            col_map = {
                'adjopen': 'open', 'adjhigh': 'high', 'adjlow': 'low',
                'adjclose': 'close', 'adjvolume': 'volume',
            }
            df = df.rename(columns=col_map)
            # Ensure date column is date type
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date']).dt.date
            required = ['open', 'high', 'low', 'close', 'volume', 'date']
            missing = [c for c in required if c not in df.columns]
            if missing:
                logger.warning(f"Tiingo response missing columns {missing} for {ticker}")
                return pd.DataFrame()
            df.insert(0, 'ticker', ticker.upper())
            return df[['ticker', 'open', 'high', 'low', 'close', 'volume', 'date']]
        except Exception as exc:
            logger.warning(f"Tiingo get_daily_price_history({ticker}) failed: {exc}")
            return pd.DataFrame()
