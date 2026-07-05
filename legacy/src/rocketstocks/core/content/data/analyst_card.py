"""AnalystCard — analyst consensus embed for /data analyst."""
import logging

from rocketstocks.core.content.models import COLOR_BLUE, EmbedSpec, AnalystData
from rocketstocks.core.content.sections_card import analyst_card

logger = logging.getLogger(__name__)


class AnalystCard:
    """Builds an analyst consensus embed from price targets, ratings, and upgrades/downgrades."""

    def __init__(self, data: AnalystData):
        self.data = data

    def build(self) -> EmbedSpec:
        description = analyst_card(
            self.data.price_targets,
            self.data.recommendations,
            self.data.upgrades_downgrades,
        )
        if len(description) > 4096:
            description = description[:4093] + '...'
        return EmbedSpec(
            title=f"Analyst Coverage: {self.data.ticker}",
            description=description,
            color=COLOR_BLUE,
        )
