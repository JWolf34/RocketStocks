import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.alerts.earnings_alert import _stat_fields_from_trigger
from rocketstocks.core.content.models import (
    COLOR_GREEN, COLOR_RED,
    WatchlistMoverData, EmbedField, EmbedSpec,
)
from rocketstocks.core.utils.formatting import (
    change_emoji, finviz_url, format_signed_pct, get_company_name,
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

        self.populate_trigger_data(self.alert_data, data.trigger_result)

    def build(self) -> EmbedSpec:
        logger.debug("Building Watchlist Mover embed...")
        pct_change = self.alert_data['pct_change']
        price = self.data.quote['regular']['regularMarketLastPrice']
        company_name = get_company_name(self.data.ticker_info, self.data.ticker)

        description = (
            f"**{company_name}** · `{self.data.ticker}` is "
            f"{change_emoji(pct_change)} **{format_signed_pct(pct_change)}** — **${price:.2f}** "
            f"and is on your *{self.data.watchlist}* watchlist"
        )

        fields = self.price_change_fields(price, pct_change) + [
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
            url=finviz_url(self.data.ticker),
        )
