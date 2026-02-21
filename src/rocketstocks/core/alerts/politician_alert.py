import datetime
import logging
import pandas as pd
from rocketstocks.core.alerts.base import Alert
from rocketstocks.core.utils.dates import date_utils

logger = logging.getLogger(__name__)


class PoliticianTradeAlert(Alert):
    def __init__(self, politician: dict, trades: pd.DataFrame):
        self.politician = politician
        alert_type = f"POLITICIAN_TRADE_{politician['name'].upper().replace(' ', '_')}"

        super().__init__(
            ticker='N/A',
            alert_type=alert_type,
        )

        # Populate alert data with trades (serialized for DB storage)
        self.alert_data['trades'] = trades.to_json()

    def build_alert_header(self):
        return f"## :rotating_light: Politician Trade Alert: {self.politician['name']}\n\n\n"

    def build_todays_change(self):
        logger.debug("Building today's change...")
        return (
            f"**{self.politician['name']}** has published "
            f"**{len(self.alert_data['trades'])}** trades today, "
            f"{date_utils.format_date_mdy(datetime.date.today())} \n"
        )

    def build_alert(self):
        logger.debug("Building Politician Trade Alert...")
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        alert += self.build_table(df=self.alert_data['trades'])
        return alert

    def override_and_edit(self, prev_alert_data: dict) -> bool:
        if len(self.alert_data['trades']) < len(prev_alert_data['trades']):
            return True
        return False
