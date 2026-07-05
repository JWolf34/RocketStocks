"""InsiderCard — insider transaction activity embed for /data insider."""
import logging

from rocketstocks.core.content.models import COLOR_AMBER, EmbedSpec, InsiderData
from rocketstocks.core.content.sections_card import insider_activity_card

logger = logging.getLogger(__name__)


class InsiderCard:
    """Builds an insider activity embed from transaction and purchases data."""

    def __init__(self, data: InsiderData):
        self.data = data

    def build(self) -> EmbedSpec:
        description = insider_activity_card(
            self.data.insider_transactions,
            self.data.insider_purchases,
        )
        if len(description) > 4096:
            description = description[:4093] + '...'
        return EmbedSpec(
            title=f"Insider Activity: {self.data.ticker}",
            description=description,
            color=COLOR_AMBER,
        )
