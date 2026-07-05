"""OptionsSummary — options chain overview embed for /data options."""
import logging

from rocketstocks.core.content.models import COLOR_GOLD, EmbedSpec, OptionsSummaryData
from rocketstocks.core.content.sections_card import options_summary_card

logger = logging.getLogger(__name__)


class OptionsSummary:
    """Builds an options summary embed from Schwab options chain data."""

    def __init__(self, data: OptionsSummaryData):
        self.data = data

    def build(self) -> EmbedSpec:
        description = options_summary_card(self.data.options_chain, self.data.current_price)
        if len(description) > 4096:
            description = description[:4093] + '...'
        return EmbedSpec(
            title=f"Options Summary: {self.data.ticker}",
            description=description,
            color=COLOR_GOLD,
        )
