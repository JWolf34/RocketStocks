import logging
import pandas as pd
from rocketstocks.core.reports.base import Report

logger = logging.getLogger(__name__)


class Screener(Report):
    """Report that is routinely updated with content from a source.

    Sending/edit-in-place logic is handled by bot/senders/report_sender.py.
    """

    def __init__(self, screener_type: str, data: pd.DataFrame, column_map: dict):
        super().__init__()
        self.screener_type = screener_type
        self.column_map = column_map

        self.data = data
        self.format_columns()
        self.update_watchlist()

    def get_tickers(self):
        """Return all tickers from self.data"""
        return self.data['Ticker'].to_list()

    def update_watchlist(self):
        """Update system-generated watchlist for this screener with top 20 tickers."""
        from rocketstocks.data.db import Postgres
        from rocketstocks.data.watchlists import Watchlists

        watchlists = Watchlists(db=Postgres())
        watchlist_id = self.screener_type
        watchlist_tickers = self.get_tickers()[:20]

        if not watchlists.validate_watchlist(watchlist_id):
            watchlists.create_watchlist(watchlist_id=watchlist_id, tickers=watchlist_tickers, systemGenerated=True)
            logger.info(f"Created new watchlist from '{self.screener_type}' screener")
            logger.debug(f"Watchlist created with {len(watchlist_tickers)} tickers: {watchlist_tickers}")
        else:
            watchlists.update_watchlist(watchlist_id=watchlist_id, tickers=watchlist_tickers)
            logger.info(f"Updated watchlist '{self.screener_type}'")
            logger.debug(f"Watchlist updated with {len(watchlist_tickers)} tickers: {watchlist_tickers}")

    def format_columns(self):
        """Format all columns in self.data per column map - rename and drop columns accordingly"""
        self.data = self.data.filter(list(self.column_map.keys()))
        self.data = self.data.rename(columns=self.column_map)
