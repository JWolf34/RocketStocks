"""ForecastCard — NASDAQ EPS forecast embed for /data forecast."""
import logging

from rocketstocks.core.content.models import COLOR_CYAN, EmbedSpec, EarningsForecastData
from rocketstocks.core.content.sections_card import earnings_forecast_card

logger = logging.getLogger(__name__)


class ForecastCard:
    """Builds an earnings forecast embed from NASDAQ quarterly and yearly EPS data."""

    def __init__(self, data: EarningsForecastData):
        self.data = data

    def build(self) -> EmbedSpec:
        description = earnings_forecast_card(
            self.data.quarterly_forecast,
            self.data.yearly_forecast,
        )
        if len(description) > 4096:
            description = description[:4093] + '...'
        return EmbedSpec(
            title=f"Earnings Forecast: {self.data.ticker}",
            description=description,
            color=COLOR_CYAN,
        )
