import datetime
import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.models import (
    COLOR_GREEN, COLOR_RED,
    SECFilingData, EmbedField, EmbedSpec,
)

logger = logging.getLogger(__name__)


class SECFilingMoverAlert(Alert):
    alert_type = "SEC_FILING_MOVER"

    def __init__(self, data: SECFilingData):
        super().__init__()
        self.data = data
        self.ticker = data.ticker
        self.alert_data['pct_change'] = data.quote['quote']['netPercentChange']

    def build(self) -> EmbedSpec:
        logger.debug("Building SEC Filing Mover embed...")
        pct_change = self.alert_data['pct_change']
        price = self.data.quote['regular']['regularMarketLastPrice']
        company_name = (self.data.ticker_info or {}).get('name', self.data.ticker)
        sign = "+" if pct_change > 0 else ""

        today_string = datetime.datetime.today().strftime("%Y-%m-%d")
        if not self.data.recent_sec_filings.empty and 'filingDate' in self.data.recent_sec_filings.columns:
            todays_filings = self.data.recent_sec_filings[
                self.data.recent_sec_filings['filingDate'] == today_string
            ]
            form_types = ", ".join(todays_filings['form'].tolist()) if not todays_filings.empty else "N/A"
        else:
            form_types = "N/A"

        description = (
            f"**{company_name}** · `{self.data.ticker}` is "
            f"{'🟢' if pct_change > 0 else '🔻'} **{sign}{pct_change:.2f}%** — **${price:.2f}** "
            f"and filed **{form_types}** with the SEC today"
        )

        fields = [
            EmbedField(name="Price", value=f"${price:.2f}", inline=True),
            EmbedField(name="Change", value=f"{sign}{pct_change:.2f}%", inline=True),
            EmbedField(name="Forms Filed Today", value=form_types, inline=True),
        ]

        return EmbedSpec(
            title=f"🚨 SEC Filing Mover: {self.data.ticker}",
            description=description,
            color=COLOR_GREEN if pct_change > 0 else COLOR_RED,
            fields=fields,
            footer="RocketStocks · sec-filing-mover",
            timestamp=True,
            url=f"https://finviz.com/quote.ashx?t={self.data.ticker}",
        )
