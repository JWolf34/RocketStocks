import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.models import (
    COLOR_GREEN, COLOR_RED,
    WatchlistMoverData, EmbedField, EmbedSpec,
)
from rocketstocks.core.content import sections

logger = logging.getLogger(__name__)


class WatchlistMoverAlert(Alert):
    alert_type = "WATCHLIST_MOVER"

    def __init__(self, data: WatchlistMoverData):
        super().__init__()
        self.data = data
        self.ticker = data.ticker
        self.alert_data['pct_change'] = data.quote['quote']['netPercentChange']

    def build_alert(self) -> str:
        logger.debug("Building Watchlist Mover Alert...")
        pct_change = self.alert_data['pct_change']
        todays_change = (
            sections.todays_change(self.data.ticker, pct_change)
            + f" and is on your *{self.data.watchlist}* watchlist\n"
        )
        return (
            sections.alert_header(f"Watchlist Mover: {self.data.ticker}")
            + todays_change
        )

    def build_embed_spec(self) -> EmbedSpec:
        logger.debug("Building Watchlist Mover EmbedSpec...")
        pct_change = self.alert_data['pct_change']
        price = self.data.quote['regular']['regularMarketLastPrice']
        company_name = self.data.ticker_info.get('name', self.data.ticker)
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

        return EmbedSpec(
            title=f"🚨 Watchlist Mover: {self.data.ticker}",
            description=description,
            color=COLOR_GREEN if pct_change > 0 else COLOR_RED,
            fields=fields,
            footer="RocketStocks · watchlist-mover",
            timestamp=True,
            url=f"https://finviz.com/quote.ashx?t={self.data.ticker}",
        )
