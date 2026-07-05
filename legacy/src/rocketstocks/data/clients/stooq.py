"""Stooq fallback price data source via pandas-datareader."""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class Stooq:
    """Fallback price data source using Stooq via pandas-datareader."""

    def get_daily_price_history(self, ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Return daily OHLCV DataFrame for *ticker* from Stooq.

        Ticker format for US stocks: '{TICKER}.US' (e.g. 'LEH.US').
        Returns empty DataFrame on any error (network, not found, rate limit).
        """
        try:
            import pandas_datareader.data as pdr
            df = pdr.DataReader(f"{ticker}.US", 'stooq', start=start_date, end=end_date)
            if df is None or df.empty:
                return pd.DataFrame()
            df = df.rename(columns=str.lower).reset_index()
            df = df.rename(columns={'Date': 'date'})
            df.columns = [c.lower() for c in df.columns]
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date']).dt.date
            required = ['open', 'high', 'low', 'close', 'volume', 'date']
            missing = [c for c in required if c not in df.columns]
            if missing:
                logger.warning(f"Stooq response missing columns {missing} for {ticker}")
                return pd.DataFrame()
            df.insert(0, 'ticker', ticker.upper())
            return df[['ticker', 'open', 'high', 'low', 'close', 'volume', 'date']]
        except Exception as exc:
            logger.warning(f"Stooq returned no data for {ticker}: {exc}")
            return pd.DataFrame()
