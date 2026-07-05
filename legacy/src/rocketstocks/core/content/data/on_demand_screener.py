"""OnDemandScreener — on-demand TradingView screener embed for /data screener."""
import logging

from rocketstocks.core.content.models import EmbedSpec, OnDemandScreenerData
from rocketstocks.core.content.screeners.gainer_screener import GainerScreener
from rocketstocks.core.content.screeners.volume_screener import VolumeScreener
from rocketstocks.core.content.models import GainerScreenerData, VolumeScreenerData

logger = logging.getLogger(__name__)


class OnDemandScreener:
    """Delegates to the existing GainerScreener or VolumeScreener based on screener_type."""

    def __init__(self, data: OnDemandScreenerData):
        self.data = data

    def build(self) -> EmbedSpec:
        screener_type = self.data.screener_type
        if screener_type == 'unusual-volume':
            return VolumeScreener(
                data=VolumeScreenerData(unusual_volume=self.data.data)
            ).build()
        else:
            # premarket or intraday
            return GainerScreener(
                data=GainerScreenerData(market_period=screener_type, gainers=self.data.data)
            ).build()
