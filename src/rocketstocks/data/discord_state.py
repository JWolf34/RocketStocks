import datetime
import logging

from psycopg.types.json import Json

logger = logging.getLogger(__name__)


class DiscordState:
    """Database-backed state tracker for Discord message IDs (screeners and alerts)."""

    def __init__(self, db=None):
        self.db = db

    # Screener message IDs #

    async def get_screener_message_id(self, screener_type: str):
        row = await self.db.execute(
            "SELECT messageid FROM reports WHERE type = %s",
            [f'{screener_type}_REPORT'],
            fetchone=True,
        )
        return row[0] if row else None

    async def update_screener_message_id(self, message_id: str, screener_type: str):
        await self.db.execute(
            "UPDATE reports SET messageid = %s WHERE type = %s",
            [message_id, f'{screener_type}_REPORT'],
        )

    async def insert_screener_message_id(self, message_id: str, screener_type: str):
        await self.db.execute(
            "INSERT INTO reports (type, messageid) VALUES (%s, %s) ON CONFLICT (type) DO NOTHING",
            [f'{screener_type}_REPORT', message_id],
        )

    async def update_volume_message_id(self, message_id):
        await self.db.execute(
            "UPDATE reports SET messageid = %s WHERE type = %s",
            [message_id, 'UNUSUAL_VOLUME_REPORT'],
        )

    async def get_volume_message_id(self):
        row = await self.db.execute(
            "SELECT messageid FROM reports WHERE type = %s",
            ['UNUSUAL_VOLUME_REPORT'],
            fetchone=True,
        )
        return row[0] if row else None

    # Alert message IDs #

    async def update_alert_message_data(self, date, ticker, alert_type, messageid, alert_data):
        await self.db.execute(
            "UPDATE alerts SET messageid = %s, alert_data = %s "
            "WHERE date = %s AND ticker = %s AND alert_type = %s",
            [messageid, Json(alert_data), date, ticker, alert_type],
        )

    async def get_alert_message_id(self, date, ticker, alert_type):
        row = await self.db.execute(
            "SELECT messageid FROM alerts WHERE date = %s AND ticker = %s AND alert_type = %s",
            [date, ticker, alert_type],
            fetchone=True,
        )
        return row[0] if row else None

    async def get_alert_message_data(self, date, ticker, alert_type):
        row = await self.db.execute(
            "SELECT alert_data FROM alerts WHERE date = %s AND ticker = %s AND alert_type = %s",
            [date, ticker, alert_type],
            fetchone=True,
        )
        return row[0] if row else None

    async def insert_alert_message_id(self, date, ticker, alert_type, message_id, alert_data):
        await self.db.execute(
            "INSERT INTO alerts (date, ticker, alert_type, messageid, alert_data) "
            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (date, ticker, alert_type) DO NOTHING",
            [date, ticker, alert_type, message_id, Json(alert_data)],
        )

    async def get_alerts_since(self, since_dt: datetime.datetime) -> list[dict]:
        """Return alerts with date >= since_dt.date(). If since_dt has a non-midnight time,
        also filter by created_at (rows with NULL created_at are always included)."""
        rows = await self.db.execute(
            "SELECT date, ticker, alert_type, messageid, alert_data, created_at "
            "FROM alerts WHERE date >= %s ORDER BY date ASC",
            [since_dt.date()],
        ) or []
        since_has_time = since_dt.time() != datetime.time.min

        result = []
        for row in rows:
            created_at = row[5]
            if since_has_time and created_at is not None:
                naive_utc = (
                    created_at.astimezone(datetime.timezone.utc).replace(tzinfo=None)
                    if created_at.tzinfo else created_at
                )
                if naive_utc < since_dt:
                    continue
            alert_data = row[4]
            # alert_data is JSONB — psycopg3 returns a dict directly; handle string fallback
            if isinstance(alert_data, str):
                import json
                try:
                    alert_data = json.loads(alert_data)
                except (ValueError, TypeError):
                    alert_data = {}
            result.append({
                'date': row[0],
                'ticker': row[1],
                'alert_type': row[2],
                'messageid': row[3],
                'alert_data': alert_data or {},
            })
        return result

    async def get_recent_alerts_for_ticker(self, ticker: str) -> list[tuple]:
        """Return [(date, alert_type, messageid)] for today for a ticker."""
        today = datetime.date.today()
        rows = await self.db.execute(
            "SELECT date, alert_type, messageid FROM alerts "
            "WHERE ticker = %s AND date = %s ORDER BY alert_type ASC",
            [ticker, today],
        )
        return rows or []

    async def get_alerts_by_type_today(self, alert_type: str) -> list[str]:
        """Return list of tickers that have the given alert_type posted today."""
        today = datetime.date.today()
        rows = await self.db.execute(
            "SELECT DISTINCT ticker FROM alerts WHERE alert_type = %s AND date = %s",
            [alert_type, today],
        ) or []
        return [row[0] for row in rows]
