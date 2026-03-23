"""FinancialHighlights — key financial metrics embed for /data financials."""
import logging

from rocketstocks.core.content.models import COLOR_TEAL, EmbedSpec, FinancialHighlightsData
from rocketstocks.core.content.sections_card import financial_highlights_card

logger = logging.getLogger(__name__)


class FinancialHighlights:
    """Builds a financial highlights embed from yfinance statement data."""

    def __init__(self, data: FinancialHighlightsData):
        self.data = data

    def build(self) -> EmbedSpec:
        description = financial_highlights_card(self.data.financials)
        if len(description) > 4096:
            description = description[:4093] + '...'
        return EmbedSpec(
            title=f"Financial Highlights: {self.data.ticker}",
            description=description,
            color=COLOR_TEAL,
        )
