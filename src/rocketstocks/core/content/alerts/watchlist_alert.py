import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.models import WatchlistMoverData
from rocketstocks.core.content import sections

logger = logging.getLogger(__name__)


class WatchlistMoverAlert(Alert):
    alert_type = "WATCHLIST_MOVER"

    def __init__(self, data: WatchlistMoverData):
        super().__init__()
        self.data = data
        self.ticker = data.ticker
        self.alert_data['pct_change'] = data.quote['quote']['netPercentChange']

    def build_alert(self) -> str:
        logger.debug("Building Watchlist Mover Alert...")
        pct_change = self.alert_data['pct_change']
        todays_change = (
            sections.todays_change(self.data.ticker, pct_change)
            + f" and is on your *{self.data.watchlist}* watchlist\n"
        )
        return (
            sections.alert_header(f"Watchlist Mover: {self.data.ticker}")
            + todays_change
        )
