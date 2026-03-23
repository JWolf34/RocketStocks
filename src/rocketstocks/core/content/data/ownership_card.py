"""OwnershipCard — institutional and insider ownership embed for /data ownership."""
import logging

from rocketstocks.core.content.models import COLOR_TEAL, EmbedSpec, OwnershipData
from rocketstocks.core.content.sections_card import ownership_card

logger = logging.getLogger(__name__)


class OwnershipCard:
    """Builds an ownership breakdown embed from institutional and major holder data."""

    def __init__(self, data: OwnershipData):
        self.data = data

    def build(self) -> EmbedSpec:
        description = ownership_card(
            self.data.institutional_holders,
            self.data.major_holders,
        )
        if len(description) > 4096:
            description = description[:4093] + '...'
        return EmbedSpec(
            title=f"Ownership: {self.data.ticker}",
            description=description,
            color=COLOR_TEAL,
        )
