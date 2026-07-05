"""PopularitySnapshot — social-media mention trend embed for /data popularity."""
import logging

from rocketstocks.core.content.models import COLOR_PINK, EmbedSpec, PopularitySnapshotData
from rocketstocks.core.content.sections_card import popularity_card

logger = logging.getLogger(__name__)


class PopularitySnapshot:
    """Builds a popularity snapshot embed from stored popularity history."""

    def __init__(self, data: PopularitySnapshotData):
        self.data = data

    def build(self) -> EmbedSpec:
        description = popularity_card(self.data.popularity)
        if len(description) > 4096:
            description = description[:4093] + '...'
        return EmbedSpec(
            title=f"Popularity: {self.data.ticker}",
            description=description,
            color=COLOR_PINK,
        )
