"""EarningsCard — historical EPS embed for /data earnings."""
import logging

from rocketstocks.core.content.models import COLOR_AMBER, EmbedSpec, EarningsTableData
from rocketstocks.core.content.sections_card import recent_earnings_card, upcoming_earnings_card

logger = logging.getLogger(__name__)


class EarningsCard:
    """Builds an earnings history embed from historical EPS data."""

    def __init__(self, data: EarningsTableData):
        self.data = data

    def build(self) -> EmbedSpec:
        description = recent_earnings_card(self.data.historical_earnings)
        if self.data.next_earnings_info:
            description += upcoming_earnings_card(self.data.next_earnings_info)
        if len(description) > 4096:
            description = description[:4093] + '...'
        return EmbedSpec(
            title=f"Earnings History: {self.data.ticker}",
            description=description,
            color=COLOR_AMBER,
        )
