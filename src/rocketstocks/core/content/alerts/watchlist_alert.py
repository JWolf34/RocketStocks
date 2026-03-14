import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.alerts.earnings_alert import _stat_fields_from_trigger
from rocketstocks.core.content.models import (
    COLOR_GREEN, COLOR_RED,
    WatchlistMoverData, EmbedField, EmbedSpec,
)

logger = logging.getLogger(__name__)


class WatchlistMoverAlert(Alert):
    alert_type = "WATCHLIST_MOVER"
    role_key = "watchlist_mover"

    def __init__(self, data: WatchlistMoverData):
        super().__init__()
        self.data = data
        self.ticker = data.ticker
        self.alert_data['pct_change'] = data.quote['quote']['netPercentChange']

        tr = data.trigger_result
        if tr is not None:
            self.alert_data['zscore'] = tr.zscore
            self.alert_data['percentile'] = tr.percentile
            self.alert_data['classification'] = getattr(tr.classification, 'value', str(tr.classification))
            self.alert_data['signal_type'] = tr.signal_type
            self.alert_data['bb_position'] = tr.bb_position
            self.alert_data['confluence_count'] = tr.confluence_count
            self.alert_data['volume_zscore'] = tr.volume_zscore

    def build(self) -> EmbedSpec:
        logger.debug("Building Watchlist Mover embed...")
        pct_change = self.alert_data['pct_change']
        price = self.data.quote['regular']['regularMarketLastPrice']
        company_name = (self.data.ticker_info or {}).get('name', self.data.ticker)
        sign = "+" if pct_change > 0 else ""

        description = (
            f"**{company_name}** · `{self.data.ticker}` is "
            f"{'🟢' if pct_change > 0 else '🔻'} **{sign}{pct_change:.2f}%** — **${price:.2f}** "
            f"and is on your *{self.data.watchlist}* watchlist"
        )

        fields = [
            EmbedField(name="Price", value=f"${price:.2f}", inline=True),
            EmbedField(name="Change", value=f"{sign}{pct_change:.2f}%", inline=True),
            EmbedField(name="Watchlist", value=self.data.watchlist, inline=True),
        ]

        fields += _stat_fields_from_trigger(self.data.trigger_result)

        return EmbedSpec(
            title=f"🚨 Watchlist Mover: {self.data.ticker}",
            description=description,
            color=COLOR_GREEN if pct_change > 0 else COLOR_RED,
            fields=fields,
            footer="RocketStocks · watchlist-mover",
            timestamp=True,
            url=f"https://finviz.com/quote.ashx?t={self.data.ticker}",
        )
