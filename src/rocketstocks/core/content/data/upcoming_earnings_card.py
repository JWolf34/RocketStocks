"""UpcomingEarningsCard — upcoming earnings embed for /data upcoming-earnings."""
import logging

from rocketstocks.core.content.models import COLOR_GREEN, EmbedField, EmbedSpec, UpcomingEarningsData
from rocketstocks.core.utils.formatting import ticker_string

logger = logging.getLogger(__name__)


class UpcomingEarningsCard:
    """Builds an upcoming earnings embed for one or more tickers."""

    def __init__(self, data: UpcomingEarningsData):
        self.data = data

    def build(self) -> EmbedSpec:
        fields = []
        for ticker in self.data.tickers:
            info = self.data.earnings_info.get(ticker)
            if info is None:
                fields.append(EmbedField(name=ticker, value="No upcoming earnings found.", inline=False))
            else:
                timing = info.get('time', 'N/A')
                timing_label = {"pre": "Before Market", "after": "After Market"}.get(timing, timing)
                value = (
                    f"**Date:** {info.get('date', 'N/A')}\n"
                    f"**When:** {timing_label}\n"
                    f"**EPS Forecast:** {info.get('eps_forecast', 'N/A')}\n"
                    f"**Estimates:** {info.get('no_of_ests', 'N/A')}\n"
                    f"**Last Year EPS:** {info.get('last_year_eps', 'N/A')}"
                )
                fields.append(EmbedField(name=ticker, value=value, inline=False))

        footer = None
        if self.data.invalid_tickers:
            footer = f"Invalid tickers: {ticker_string(self.data.invalid_tickers)}"

        return EmbedSpec(
            title="Upcoming Earnings",
            description="",
            color=COLOR_GREEN,
            fields=fields,
            footer=footer,
        )
