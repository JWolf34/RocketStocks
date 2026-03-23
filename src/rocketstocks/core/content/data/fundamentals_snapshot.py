"""FundamentalsSnapshot — extended fundamentals embed for /data fundamentals."""
import logging

from rocketstocks.core.content.models import COLOR_ORANGE, EmbedSpec, FundamentalsSnapshotData
from rocketstocks.core.content.sections_card import fundamentals_snapshot_card

logger = logging.getLogger(__name__)


class FundamentalsSnapshot:
    """Builds a fundamentals snapshot embed from Schwab fundamental data."""

    def __init__(self, data: FundamentalsSnapshotData):
        self.data = data

    def build(self) -> EmbedSpec:
        description = fundamentals_snapshot_card(self.data.fundamentals)
        if len(description) > 4096:
            description = description[:4093] + '...'
        return EmbedSpec(
            title=f"Fundamentals: {self.data.ticker}",
            description=description,
            color=COLOR_ORANGE,
        )
