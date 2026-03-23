"""TickersSummary — tracked ticker universe overview embed for /data tickers."""
import logging

from rocketstocks.core.content.models import COLOR_INDIGO, EmbedSpec, TickersSummaryData
from rocketstocks.core.content.sections_card import tickers_summary_card

logger = logging.getLogger(__name__)


class TickersSummary:
    """Builds a summary embed for all tracked tickers."""

    def __init__(self, data: TickersSummaryData):
        self.data = data

    def build(self) -> EmbedSpec:
        description = tickers_summary_card(self.data.tickers_df)
        if len(description) > 4096:
            description = description[:4093] + '...'
        return EmbedSpec(
            title="Tracked Tickers Summary",
            description=description,
            color=COLOR_INDIGO,
        )
