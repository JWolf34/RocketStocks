import asyncio
import json
import logging
from rocketstocks.data.db import Postgres

logger = logging.getLogger(__name__)


class DiscordState:
    """Database-backed state tracker for Discord message IDs (screeners and alerts)."""

    def __init__(self, db=None):
        self.db = db or Postgres()

    # Screener message IDs #

    def get_screener_message_id(self, screener_type: str):
        where_conditions = [('type', f'{screener_type}_REPORT')]
        result = self.db.select(table='reports',
                                fields=['messageid'],
                                where_conditions=where_conditions,
                                fetchall=False)
        if not result:
            return result
        else:
            return result[0]

    def update_screener_message_id(self, message_id: str, screener_type: str):
        where_conditions = [('type', f'{screener_type}_REPORT')]
        self.db.update(table='reports',
                       set_fields=[('messageid', message_id)],
                       where_conditions=where_conditions)

    def insert_screener_message_id(self, message_id: str, screener_type: str):
        values = [(f'{screener_type}_REPORT', message_id)]
        self.db.insert(table='reports',
                       fields=self.db.get_table_columns(table='reports'),
                       values=values)

    def update_volume_message_id(self, message_id):
        self.db.update(table='reports',
                       set_fields=[('messageid', message_id)],
                       where_conditions=[('type', 'UNUSUAL_VOLUME_REPORT')])

    def get_volume_message_id(self):
        result = self.db.select(table='reports',
                                fields=['messageid'],
                                where_conditions=[('type', 'UNUSUAL_VOLUME_REPORT')],
                                fetchall=False)
        if result is None:
            return result
        else:
            return result[0]

    # Alert message IDs #

    async def update_alert_message_data(self, date, ticker, alert_type, messageid, alert_data):
        await asyncio.to_thread(
            self.db.update,
            table='alerts',
            set_fields=[('messageid', messageid), ('alert_data', json.dumps(alert_data))],
            where_conditions=[('date', date), ('ticker', ticker), ('alert_type', alert_type)],
        )

    async def get_alert_message_id(self, date, ticker, alert_type):
        result = await asyncio.to_thread(
            self.db.select,
            table='alerts',
            fields=['messageid'],
            where_conditions=[('date', date), ('ticker', ticker), ('alert_type', alert_type)],
            fetchall=False,
        )
        return result[0] if result else None

    async def get_alert_message_data(self, date, ticker, alert_type):
        result = await asyncio.to_thread(
            self.db.select,
            table='alerts',
            fields=['alert_data'],
            where_conditions=[('date', date), ('ticker', ticker), ('alert_type', alert_type)],
            fetchall=False,
        )
        return result[0] if result else None

    async def insert_alert_message_id(self, date, ticker, alert_type, message_id, alert_data):
        fields = await asyncio.to_thread(self.db.get_table_columns, 'alerts')
        values = [(date, ticker, alert_type, message_id, json.dumps(alert_data))]
        await asyncio.to_thread(self.db.insert, table='alerts', fields=fields, values=values)
