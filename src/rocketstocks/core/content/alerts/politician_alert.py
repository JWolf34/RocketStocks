import datetime
import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.formatting import build_df_table
from rocketstocks.core.content.models import PoliticianTradeAlertData
from rocketstocks.core.content import sections
from rocketstocks.core.utils.dates import date_utils

logger = logging.getLogger(__name__)


class PoliticianTradeAlert(Alert):

    def __init__(self, data: PoliticianTradeAlertData):
        super().__init__()
        self.data = data
        self.ticker = 'N/A'
        self.alert_type = f"POLITICIAN_TRADE_{data.politician['name'].upper().replace(' ', '_')}"
        # Store count for override comparison (trades DataFrame not JSON-serializable)
        self.alert_data['num_trades'] = len(data.trades)

    def build_alert(self) -> str:
        logger.debug("Building Politician Trade Alert...")
        todays_change = (
            f"**{self.data.politician['name']}** has published "
            f"**{len(self.data.trades)}** trades today, "
            f"{date_utils.format_date_mdy(datetime.date.today())} \n"
        )
        return (
            sections.alert_header(f"Politician Trade Alert: {self.data.politician['name']}")
            + todays_change
            + build_df_table(df=self.data.trades)
        )

    def override_and_edit(self, prev_alert_data: dict) -> bool:
        return self.alert_data['num_trades'] > prev_alert_data.get('num_trades', 0)
