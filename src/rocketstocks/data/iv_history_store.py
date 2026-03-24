"""Repository for the iv_history table — daily IV snapshots per ticker."""
import datetime
import logging

import pandas as pd

logger = logging.getLogger(__name__)

_TABLE = 'iv_history'
_FIELDS = ['ticker', 'date', 'iv', 'atm_iv', 'put_call_ratio']

SECTOR_ETFS = ['XLK', 'XLV', 'XLF', 'XLE', 'XLY', 'XLP', 'XLI', 'XLB', 'XLRE', 'XLU', 'XLC']


class IVHistoryRepository:
    def __init__(self, db):
        self._db = db

    async def insert_iv(
        self,
        ticker: str,
        date: datetime.date,
        iv: float | None,
        atm_iv: float | None,
        put_call_ratio: float | None,
    ) -> None:
        """Upsert a single IV snapshot row."""
        await self._db.execute(
            """
            INSERT INTO iv_history (ticker, date, iv, atm_iv, put_call_ratio)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ticker, date) DO UPDATE SET
                iv             = EXCLUDED.iv,
                atm_iv         = EXCLUDED.atm_iv,
                put_call_ratio = EXCLUDED.put_call_ratio
            """,
            [ticker, date, iv, atm_iv, put_call_ratio],
        )
        logger.debug(f"Upserted iv_history for '{ticker}' on {date}")

    async def get_iv_history(self, ticker: str, days: int = 365) -> pd.DataFrame:
        """Return the last *days* IV rows for *ticker* as a DataFrame.

        Columns: date, iv, atm_iv, put_call_ratio.
        Returns an empty DataFrame if no rows exist.
        """
        rows = await self._db.execute(
            """
            SELECT date, iv, atm_iv, put_call_ratio
            FROM iv_history
            WHERE ticker = %s
            ORDER BY date DESC
            LIMIT %s
            """,
            [ticker, days],
        )
        if not rows:
            return pd.DataFrame(columns=['date', 'iv', 'atm_iv', 'put_call_ratio'])
        df = pd.DataFrame(rows, columns=['date', 'iv', 'atm_iv', 'put_call_ratio'])
        df = df.sort_values('date').reset_index(drop=True)
        return df

    async def get_latest_iv(self, ticker: str) -> float | None:
        """Return the most recent root-level IV for *ticker*, or None."""
        row = await self._db.execute(
            "SELECT iv FROM iv_history WHERE ticker = %s ORDER BY date DESC LIMIT 1",
            [ticker],
            fetchone=True,
        )
        if row is None:
            return None
        return row[0]

    async def collect_daily_snapshots(self, schwab, watchlists, popularity) -> None:
        """Fetch and store IV snapshots for the watchlist + popular + sector-ETF pool.

        Args:
            schwab: Schwab client (get_options_chain, raises SchwabRateLimitError)
            watchlists: Watchlists repository (get_all_watchlist_tickers)
            popularity: PopularityRepository (get_popular_stocks)
        """
        from rocketstocks.data.clients.schwab import SchwabRateLimitError

        today = datetime.date.today()
        logger.info("Starting IV history collection")

        try:
            watchlist_tickers = await watchlists.get_all_watchlist_tickers()
        except Exception as exc:
            logger.warning(f"[collect_iv_history] Failed to fetch watchlist tickers: {exc}")
            watchlist_tickers = []

        try:
            popular_df = popularity.get_popular_stocks(num_stocks=100)
            popular_tickers = popular_df['ticker'].tolist() if not popular_df.empty else []
        except Exception as exc:
            logger.warning(f"[collect_iv_history] Failed to fetch popular tickers: {exc}")
            popular_tickers = []

        pool = sorted(set(watchlist_tickers + popular_tickers + SECTOR_ETFS))
        logger.info(f"[collect_iv_history] Collecting IV for {len(pool)} tickers")

        success = 0
        skipped = 0
        for ticker in pool:
            try:
                chain = await schwab.get_options_chain(ticker=ticker)
            except SchwabRateLimitError as exc:
                logger.warning(f"[collect_iv_history] Rate limited on '{ticker}': {exc}")
                break
            except Exception as exc:
                logger.debug(f"[collect_iv_history] No options chain for '{ticker}': {exc}")
                skipped += 1
                continue

            if not chain:
                skipped += 1
                continue

            iv = chain.get('volatility') or None

            atm_iv = None
            underlying_price = chain.get('underlyingPrice') or 0.0
            call_map = chain.get('callExpDateMap', {})
            if call_map and underlying_price:
                nearest_exp_key = min(call_map.keys(), key=lambda k: float(k.split(':')[1]))
                strikes = call_map[nearest_exp_key]
                if strikes:
                    atm_strike = min(strikes.keys(), key=lambda s: abs(float(s) - underlying_price))
                    contracts = strikes[atm_strike]
                    if contracts:
                        raw_vol = contracts[0].get('volatility')
                        if raw_vol is not None and raw_vol > 0:
                            atm_iv = float(raw_vol)

            put_call_ratio = chain.get('putCallRatio') or None

            try:
                await self.insert_iv(
                    ticker=ticker,
                    date=today,
                    iv=float(iv) if iv is not None else None,
                    atm_iv=atm_iv,
                    put_call_ratio=float(put_call_ratio) if put_call_ratio is not None else None,
                )
                success += 1
            except Exception as exc:
                logger.warning(f"[collect_iv_history] Failed to store IV for '{ticker}': {exc}")

        logger.info(f"[collect_iv_history] Done — {success} stored, {skipped} skipped (no options)")
