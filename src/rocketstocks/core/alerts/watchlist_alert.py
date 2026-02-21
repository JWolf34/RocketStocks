import logging
from rocketstocks.core.alerts.base import Alert

logger = logging.getLogger(__name__)


class WatchlistMoverAlert(Alert):
    def __init__(self, ticker: str, quote: dict, watchlist: str):
        super().__init__(
            alert_type="WATCHLIST_MOVER",
            ticker=ticker,
            quote=quote,
            watchlist=watchlist,
        )

    def build_alert_header(self):
        logger.debug("Building alert header...")
        return f"## :rotating_light: Watchlist Mover: {self.ticker}\n"

    def build_todays_change(self):
        logger.debug("Building today's change...")
        message = super().build_todays_change()
        message += f" and is on your *{self.watchlist}* watchlist\n"
        return message

    def build_alert(self):
        logger.debug("Building Watchlist Mover Alert...")
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        return alert
