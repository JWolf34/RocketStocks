"""Repository for the market_signals table."""
import datetime
import json
import logging

logger = logging.getLogger(__name__)

_TABLE = 'market_signals'
_FIELDS = [
    'ticker', 'detected_at', 'composite_score', 'price_z', 'vol_z',
    'pct_change', 'dominant_signal', 'rvol', 'status', 'confirmed_at',
    'alert_message_id', 'signal_data',
]
_ACTIVE_CUTOFF_HOURS = 8  # Signals older than 8 hours are eligible to expire


class MarketSignalRepository:
    """Synchronous repository for tracking market signal events.

    All methods are synchronous. The cog wraps calls with asyncio.to_thread().
    """

    def __init__(self, db=None):
        self._db = db

    def insert_signal(
        self,
        ticker: str,
        detected_at: datetime.datetime,
        composite_score: float,
        price_z: float | None,
        vol_z: float | None,
        pct_change: float | None,
        dominant_signal: str | None,
        rvol: float | None,
        signal_data: list | None = None,
    ) -> None:
        """Insert a new market signal record (ignores duplicates)."""
        signal_data_json = json.dumps(signal_data or [])
        sql = (
            f"INSERT INTO {_TABLE} "
            f"(ticker, detected_at, composite_score, price_z, vol_z, pct_change, "
            f"dominant_signal, rvol, signal_data) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            f"ON CONFLICT (ticker, detected_at) DO NOTHING;"
        )
        with self._db._cursor() as cur:
            cur.execute(sql, [
                ticker, detected_at, composite_score, price_z, vol_z,
                pct_change, dominant_signal, rvol, signal_data_json,
            ])
        logger.debug(f"Inserted market signal for '{ticker}' at {detected_at}")

    def get_active_signals(self) -> list[dict]:
        """Return pending, unexpired, today's signals."""
        today = datetime.datetime.utcnow().date()
        sql = (
            f"SELECT {', '.join(_FIELDS)} FROM {_TABLE} "
            f"WHERE status = 'pending' AND DATE(detected_at) = %s;"
        )
        with self._db._cursor() as cur:
            cur.execute(sql, [today])
            rows = cur.fetchall()
        results = []
        for row in rows:
            d = dict(zip(_FIELDS, row))
            if isinstance(d.get('signal_data'), str):
                try:
                    d['signal_data'] = json.loads(d['signal_data'])
                except (ValueError, TypeError):
                    d['signal_data'] = []
            results.append(d)
        return results

    def get_signal_history(self, ticker: str) -> list[dict]:
        """Return observation snapshots (signal_data JSON array) for a ticker."""
        today = datetime.datetime.utcnow().date()
        sql = (
            f"SELECT signal_data FROM {_TABLE} "
            f"WHERE ticker = %s AND status = 'pending' AND DATE(detected_at) = %s "
            f"ORDER BY detected_at ASC;"
        )
        with self._db._cursor() as cur:
            cur.execute(sql, [ticker, today])
            rows = cur.fetchall()
        observations = []
        for (signal_data_raw,) in rows:
            if isinstance(signal_data_raw, str):
                try:
                    data = json.loads(signal_data_raw)
                except (ValueError, TypeError):
                    data = []
            else:
                data = signal_data_raw or []
            if isinstance(data, list):
                observations.extend(data)
        return observations

    def mark_confirmed(self, ticker: str, detected_at: datetime.datetime) -> None:
        """Mark a signal as confirmed."""
        sql = (
            f"UPDATE {_TABLE} SET status = 'confirmed', confirmed_at = CURRENT_TIMESTAMP "
            f"WHERE ticker = %s AND detected_at = %s;"
        )
        with self._db._cursor() as cur:
            cur.execute(sql, [ticker, detected_at])
        logger.debug(f"Marked market signal confirmed for '{ticker}' at {detected_at}")

    def expire_old_signals(self) -> None:
        """Mark pending signals older than the cutoff window as expired."""
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=_ACTIVE_CUTOFF_HOURS)
        sql = (
            f"UPDATE {_TABLE} SET status = 'expired' "
            f"WHERE status = 'pending' AND detected_at < %s;"
        )
        with self._db._cursor() as cur:
            cur.execute(sql, [cutoff])
        logger.debug(f"Expired old market signals before {cutoff}")

    def is_already_signaled(self, ticker: str) -> bool:
        """Return True if ticker has an active pending signal today."""
        today = datetime.datetime.utcnow().date()
        sql = (
            f"SELECT COUNT(*) FROM {_TABLE} "
            f"WHERE ticker = %s AND status = 'pending' AND DATE(detected_at) = %s;"
        )
        with self._db._cursor() as cur:
            cur.execute(sql, [ticker, today])
            count = cur.fetchone()[0]
        return count > 0

    def update_observation(
        self,
        ticker: str,
        detected_at: datetime.datetime,
        pct_change: float,
        composite_score: float,
        vol_z: float | None,
        price_z: float | None,
    ) -> None:
        """Append a new observation snapshot to signal_data JSON."""
        # Fetch current signal_data
        sql_select = (
            f"SELECT signal_data FROM {_TABLE} "
            f"WHERE ticker = %s AND detected_at = %s;"
        )
        with self._db._cursor() as cur:
            cur.execute(sql_select, [ticker, detected_at])
            row = cur.fetchone()
        if row is None:
            return
        existing_raw = row[0]
        if isinstance(existing_raw, str):
            try:
                existing = json.loads(existing_raw)
            except (ValueError, TypeError):
                existing = []
        else:
            existing = existing_raw or []

        observation = {
            'ts': datetime.datetime.utcnow().isoformat(),
            'pct_change': pct_change,
            'vol_z': vol_z,
            'price_z': price_z,
            'composite': composite_score,
        }
        existing.append(observation)
        updated_json = json.dumps(existing)

        sql_update = (
            f"UPDATE {_TABLE} SET signal_data = %s, composite_score = %s, "
            f"pct_change = %s, vol_z = %s, price_z = %s "
            f"WHERE ticker = %s AND detected_at = %s;"
        )
        with self._db._cursor() as cur:
            cur.execute(sql_update, [
                updated_json, composite_score, pct_change,
                vol_z, price_z, ticker, detected_at,
            ])
        logger.debug(f"Updated observation for '{ticker}' at {detected_at}")

    def get_latest_signal(self, ticker: str) -> dict | None:
        """Return the most recent pending signal for a ticker today."""
        today = datetime.datetime.utcnow().date()
        sql = (
            f"SELECT {', '.join(_FIELDS)} FROM {_TABLE} "
            f"WHERE ticker = %s AND status = 'pending' AND DATE(detected_at) = %s "
            f"ORDER BY detected_at DESC LIMIT 1;"
        )
        with self._db._cursor() as cur:
            cur.execute(sql, [ticker, today])
            row = cur.fetchone()
        if row is None:
            return None
        d = dict(zip(_FIELDS, row))
        if isinstance(d.get('signal_data'), str):
            try:
                d['signal_data'] = json.loads(d['signal_data'])
            except (ValueError, TypeError):
                d['signal_data'] = []
        return d
