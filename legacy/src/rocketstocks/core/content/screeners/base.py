"""Base Screener — standalone, no Report inheritance."""
import logging

import pandas as pd

from rocketstocks.core.content.models import EmbedSpec

logger = logging.getLogger(__name__)


class Screener:
    """Standalone base screener.

    Concrete subclasses declare screener_type, column_map, and override
    build(). The update_watchlist() DB side-effect has been moved to
    bot/cogs/reports.py so this class remains free of data-layer imports.
    """

    def __init__(self, screener_type: str, data: pd.DataFrame, column_map: dict):
        self.screener_type = screener_type
        self.column_map = column_map
        self.data = data
        self._format_columns()

    def get_tickers(self) -> list[str]:
        """Return all tickers from self.data."""
        return self.data['Ticker'].to_list()

    def _format_columns(self) -> None:
        """Filter and rename columns per column_map."""
        self.data = self.data.filter(list(self.column_map.keys()))
        self.data = self.data.rename(columns=self.column_map)

    def build(self) -> EmbedSpec:
        raise NotImplementedError
