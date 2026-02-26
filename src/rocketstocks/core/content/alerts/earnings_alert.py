import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.models import (
    COLOR_GREEN, COLOR_RED,
    EarningsMoverData, EmbedField, EmbedSpec,
)
from rocketstocks.core.content import sections

logger = logging.getLogger(__name__)


class EarningsMoverAlert(Alert):
    alert_type = "EARNINGS_MOVER"

    def __init__(self, data: EarningsMoverData):
        super().__init__()
        self.data = data
        self.ticker = data.ticker
        self.alert_data['pct_change'] = data.quote['quote']['netPercentChange']

    def build_alert(self) -> str:
        logger.debug("Building Earnings Mover Alert...")
        pct_change = self.alert_data['pct_change']
        price = self.data.quote['regular']['regularMarketLastPrice']
        company_name = (self.data.ticker_info or {}).get('name', '')
        todays_change = (
            sections.todays_change(self.data.ticker, pct_change, price=price, company_name=company_name)
            + " and reports earnings today\n"
        )
        return (
            sections.alert_header(f"Earnings Mover: {self.data.ticker}")
            + todays_change
            + sections.earnings_date_section(self.data.ticker, self.data.next_earnings_info)
            + sections.recent_earnings_section(self.data.historical_earnings)
        )

    def build_embed_spec(self) -> EmbedSpec:
        logger.debug("Building Earnings Mover EmbedSpec...")
        pct_change = self.alert_data['pct_change']
        price = self.data.quote['regular']['regularMarketLastPrice']
        company_name = (self.data.ticker_info or {}).get('name', self.data.ticker)
        sign = "+" if pct_change > 0 else ""

        next_info = self.data.next_earnings_info or {}
        eps_forecast = next_info.get('eps_forecast', 'N/A') or 'N/A'
        time_raw = next_info.get('time', '')
        if isinstance(time_raw, list):
            time_raw = time_raw[0] if time_raw else ''
        if 'pre-market' in time_raw:
            time_label = 'Pre-market'
        elif 'after-hours' in time_raw:
            time_label = 'After Hours'
        else:
            time_label = 'N/A'

        description = (
            f"**{company_name}** · `{self.data.ticker}` is reporting earnings today and is "
            f"{'🟢' if pct_change > 0 else '🔻'} **{sign}{pct_change:.2f}%** — **${price:.2f}**"
        )

        fields = [
            EmbedField(name="Price", value=f"${price:.2f}", inline=True),
            EmbedField(name="Change", value=f"{sign}{pct_change:.2f}%", inline=True),
            EmbedField(name="EPS Forecast", value=str(eps_forecast), inline=True),
            EmbedField(name="Time", value=time_label, inline=True),
        ]

        if not self.data.historical_earnings.empty:
            fields.append(EmbedField(
                name="Recent Earnings",
                value=sections.recent_earnings_section(self.data.historical_earnings),
                inline=False,
            ))

        return EmbedSpec(
            title=f"🚨 Earnings Mover: {self.data.ticker}",
            description=description,
            color=COLOR_GREEN if pct_change > 0 else COLOR_RED,
            fields=fields,
            footer="RocketStocks · earnings-mover",
            timestamp=True,
            url=f"https://finviz.com/quote.ashx?t={self.data.ticker}",
        )
