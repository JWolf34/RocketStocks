"""ShortInterestCard — short interest embed for /data short-interest."""
import logging

from rocketstocks.core.content.models import COLOR_RED, EmbedSpec, ShortInterestData
from rocketstocks.core.content.sections_card import short_interest_card

logger = logging.getLogger(__name__)


class ShortInterestCard:
    """Builds a short interest embed from Schwab fundamentals short data."""

    def __init__(self, data: ShortInterestData):
        self.data = data

    def build(self) -> EmbedSpec:
        description = short_interest_card(
            self.data.short_interest_ratio,
            self.data.short_interest_shares,
            self.data.short_percent_of_float,
            self.data.shares_outstanding,
        )
        if len(description) > 4096:
            description = description[:4093] + '...'
        return EmbedSpec(
            title=f"Short Interest: {self.data.ticker}",
            description=description,
            color=COLOR_RED,
        )
