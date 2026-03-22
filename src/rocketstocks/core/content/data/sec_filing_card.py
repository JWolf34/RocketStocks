"""SecFilingCard — SEC filing links embed for /data sec-filing."""
import logging

from rocketstocks.core.content.models import COLOR_CYAN, EmbedField, EmbedSpec, SecFilingData
from rocketstocks.core.utils.dates import format_date_mdy
from rocketstocks.core.utils.formatting import ticker_string

logger = logging.getLogger(__name__)


class SecFilingCard:
    """Builds a SEC filing embed with links for one or more tickers."""

    def __init__(self, data: SecFilingData):
        self.data = data

    def build(self) -> EmbedSpec:
        fields = []
        for ticker in self.data.tickers:
            filing = self.data.filings.get(ticker)
            if filing is None:
                fields.append(EmbedField(
                    name=ticker,
                    value=f"No Form {self.data.form} found.",
                    inline=False,
                ))
            else:
                filing_date = format_date_mdy(filing.get('filingDate', ''))
                link = filing.get('link', '')
                fields.append(EmbedField(
                    name=ticker,
                    value=f"[Form {self.data.form} — Filed {filing_date}]({link})",
                    inline=False,
                ))

        return EmbedSpec(
            title=f"SEC Filing: Form {self.data.form}",
            description="",
            color=COLOR_CYAN,
            fields=fields,
        )
