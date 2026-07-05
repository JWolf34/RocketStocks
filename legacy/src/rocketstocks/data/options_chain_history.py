"""Repository for the options_chain_history table — per-contract daily snapshots.

Single source of truth for options data. Replaces the old iv_history table:
IV time-series (``get_iv_history`` / ``get_latest_iv``) are derived on demand
from the per-contract ``iv`` column (real implied vol), not a denormalized cache.
"""
import datetime
import logging
import math

import pandas as pd

logger = logging.getLogger(__name__)

_FIELDS = [
    'ticker', 'snapshot_date', 'expiration_date', 'days_to_exp', 'strike',
    'option_type', 'contract_symbol', 'underlying_price', 'bid', 'ask', 'mark', 'last',
    'volume', 'open_interest', 'delta', 'gamma', 'theta', 'vega', 'rho',
    'iv', 'in_the_money',
]

_INSERT_SQL = f"""
INSERT INTO options_chain_history ({', '.join(_FIELDS)})
VALUES ({', '.join(['%s'] * len(_FIELDS))})
ON CONFLICT (ticker, snapshot_date, expiration_date, strike, option_type) DO UPDATE SET
    underlying_price = EXCLUDED.underlying_price,
    bid = EXCLUDED.bid, ask = EXCLUDED.ask, mark = EXCLUDED.mark, last = EXCLUDED.last,
    volume = EXCLUDED.volume, open_interest = EXCLUDED.open_interest,
    delta = EXCLUDED.delta, gamma = EXCLUDED.gamma, theta = EXCLUDED.theta,
    vega = EXCLUDED.vega, rho = EXCLUDED.rho, iv = EXCLUDED.iv,
    in_the_money = EXCLUDED.in_the_money
"""


def _num(value) -> float | None:
    """Coerce a Schwab numeric field to float, mapping 'NaN'/inf/garbage to None."""
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def _bigint(value) -> int | None:
    """Coerce a Schwab count field (volume/OI) to int, mapping garbage to None."""
    num = _num(value)
    return int(num) if num is not None else None


def _to_date(value) -> datetime.date | None:
    """Parse a Schwab ISO timestamp (e.g. '2026-05-15T20:00:00.000+00:00') to a date."""
    if not value:
        return None
    return datetime.datetime.fromisoformat(value).date()


def _contract_tuple(contract: dict, ticker: str, snapshot_date: datetime.date,
                    option_type: str, underlying_price: float | None) -> tuple:
    """Map one Schwab contract dict to a tuple in _FIELDS order."""
    return (
        ticker,
        snapshot_date,
        _to_date(contract.get('expirationDate')),
        contract.get('daysToExpiration'),
        _num(contract.get('strikePrice')),
        option_type,
        contract.get('symbol'),
        underlying_price,
        _num(contract.get('bid')),
        _num(contract.get('ask')),
        _num(contract.get('mark')),
        _num(contract.get('last')),
        _bigint(contract.get('totalVolume')),
        _bigint(contract.get('openInterest')),
        _num(contract.get('delta')),
        _num(contract.get('gamma')),
        _num(contract.get('theta')),
        _num(contract.get('vega')),
        _num(contract.get('rho')),
        _num(contract.get('volatility')),  # per-contract real IV, NOT theoreticalVolatility
        contract.get('inTheMoney'),
    )


def parse_chain(chain: dict, snapshot_date: datetime.date) -> list[tuple]:
    """Flatten a Schwab options chain into per-contract tuples in _FIELDS order.

    Pure function. Iterates both the call and put expiration maps; missing
    fields become None.
    """
    ticker = chain.get('symbol')
    underlying_price = _num(chain.get('underlyingPrice'))
    contracts: list[tuple] = []
    for map_key, option_type in (('callExpDateMap', 'call'), ('putExpDateMap', 'put')):
        exp_map = chain.get(map_key) or {}
        for strikes in exp_map.values():
            for contract_list in strikes.values():
                for contract in contract_list:
                    contracts.append(
                        _contract_tuple(contract, ticker, snapshot_date,
                                        option_type, underlying_price)
                    )
    return contracts


class OptionsChainHistoryRepository:
    def __init__(self, db):
        self._db = db

    async def insert_contracts(self, contracts: list[tuple]) -> None:
        """Bulk upsert contract rows. Re-running same day refreshes OI/volume/greeks."""
        if not contracts:
            return
        await self._db.execute_batch(_INSERT_SQL, contracts)

    async def get_chain_snapshot(
        self, ticker: str, snapshot_date: datetime.date
    ) -> pd.DataFrame:
        """Return all contract rows for *ticker* on *snapshot_date*."""
        rows = await self._db.execute(
            f"SELECT {', '.join(_FIELDS)} FROM options_chain_history "
            "WHERE ticker = %s AND snapshot_date = %s "
            "ORDER BY expiration_date, strike, option_type",
            [ticker, snapshot_date],
        )
        if not rows:
            return pd.DataFrame(columns=_FIELDS)
        return pd.DataFrame(rows, columns=_FIELDS)

    async def get_oi_history(
        self,
        ticker: str,
        expiration: datetime.date,
        strike: float,
        option_type: str,
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
    ) -> pd.DataFrame:
        """Return the daily vol/OI/IV/greeks time-series for one specific contract."""
        cols = ['snapshot_date', 'volume', 'open_interest', 'iv',
                'delta', 'gamma', 'theta', 'vega', 'rho']
        query = (
            f"SELECT {', '.join(cols)} FROM options_chain_history "
            "WHERE ticker = %s AND expiration_date = %s AND strike = %s AND option_type = %s"
        )
        params: list = [ticker, expiration, strike, option_type]
        if start_date is not None:
            query += " AND snapshot_date >= %s"
            params.append(start_date)
        if end_date is not None:
            query += " AND snapshot_date <= %s"
            params.append(end_date)
        query += " ORDER BY snapshot_date"

        rows = await self._db.execute(query, params)
        if not rows:
            return pd.DataFrame(columns=cols)
        return pd.DataFrame(rows, columns=cols)

    async def get_iv_history(self, ticker: str, days: int = 365) -> pd.DataFrame:
        """Return derived daily IV history for *ticker* (newest *days* rows).

        Columns: date, iv, atm_iv, put_call_ratio. Both ``iv`` and ``atm_iv`` carry
        the nearest-expiration ATM call's real IV (kept as two columns for caller
        compatibility); the ATM call is the one whose strike is closest to that day's
        ``underlying_price``. ``put_call_ratio`` is SUM(put volume)/SUM(call volume).
        """
        cols = ['date', 'iv', 'atm_iv', 'put_call_ratio']
        rows = await self._db.execute(
            """
            WITH nearest_exp AS (
                SELECT snapshot_date, MIN(expiration_date) AS exp
                FROM options_chain_history
                WHERE ticker = %s AND option_type = 'call'
                  AND expiration_date >= snapshot_date
                GROUP BY snapshot_date
            ),
            atm AS (
                SELECT DISTINCT ON (o.snapshot_date) o.snapshot_date, o.iv
                FROM options_chain_history o
                JOIN nearest_exp n
                  ON o.snapshot_date = n.snapshot_date AND o.expiration_date = n.exp
                WHERE o.ticker = %s AND o.option_type = 'call'
                  AND o.underlying_price IS NOT NULL
                ORDER BY o.snapshot_date, ABS(o.strike - o.underlying_price)
            ),
            pcr AS (
                SELECT snapshot_date,
                       SUM(volume) FILTER (WHERE option_type = 'put')::float
                         / NULLIF(SUM(volume) FILTER (WHERE option_type = 'call'), 0)
                         AS put_call_ratio
                FROM options_chain_history
                WHERE ticker = %s
                GROUP BY snapshot_date
            )
            SELECT a.snapshot_date, a.iv, a.iv, p.put_call_ratio
            FROM atm a
            LEFT JOIN pcr p ON a.snapshot_date = p.snapshot_date
            ORDER BY a.snapshot_date DESC
            LIMIT %s
            """,
            [ticker, ticker, ticker, days],
        )
        if not rows:
            return pd.DataFrame(columns=cols)
        df = pd.DataFrame(rows, columns=cols)
        df = df.sort_values('date').reset_index(drop=True)
        return df

    async def get_latest_iv(self, ticker: str) -> float | None:
        """Return the most recent day's ATM IV for *ticker*, or None."""
        df = await self.get_iv_history(ticker, days=1)
        if df.empty:
            return None
        value = df['atm_iv'].iloc[-1]
        if value is None or pd.isna(value):
            return None
        return float(value)

    async def collect_daily_snapshots(self, schwab, tickers: list[str]) -> dict:
        """Fetch, parse, and bulk-upsert today's chain for every ticker in *tickers*.

        Single write path. Returns counts: {'success', 'skipped', 'errored'}.
        """
        from rocketstocks.data.clients.schwab import SchwabRateLimitError, SchwabTokenError

        today = datetime.date.today()
        total = len(tickers)
        logger.info(f"Starting options chain history collection for {total} tickers")
        success = skipped = errored = 0

        for i, ticker in enumerate(tickers, 1):
            if i % 100 == 0:
                logger.info(f"options snapshot progress: {i}/{total}")
            try:
                chain = await schwab.get_options_chain(ticker=ticker)
            except SchwabRateLimitError as exc:
                logger.warning(f"Rate-limited on '{ticker}'; aborting run: {exc}")
                break
            except SchwabTokenError as exc:
                logger.error(f"Token error on '{ticker}'; aborting run: {exc}")
                break
            except Exception as exc:
                logger.debug(f"No chain for '{ticker}': {exc}")
                errored += 1
                continue

            if not chain or chain.get('status') != 'SUCCESS':
                skipped += 1
                continue

            contracts = parse_chain(chain, today)
            if not contracts:
                skipped += 1
                continue

            try:
                await self.insert_contracts(contracts)
                success += 1
            except Exception as exc:
                logger.warning(f"Failed to store chain for '{ticker}': {exc}")
                errored += 1

        logger.info(
            f"Options chain history collection done — "
            f"{success} stored, {skipped} skipped, {errored} errored"
        )
        return {'success': success, 'skipped': skipped, 'errored': errored}
