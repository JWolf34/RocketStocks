"""SecFilingCard — SEC filing links embed for /data sec-filing."""
import logging

from rocketstocks.core.content.models import COLOR_CYAN, EmbedField, EmbedSpec, SecFilingData
from rocketstocks.core.utils.dates import format_date_mdy

logger = logging.getLogger(__name__)


class SecFilingCard:
    """Builds a SEC filing embed with links for one or more tickers."""

    def __init__(self, data: SecFilingData):
        self.data = data

    def build(self) -> EmbedSpec:
        fields = []
        for ticker in self.data.tickers:
            ticker_filings = self.data.filings.get(ticker, [])
            if not ticker_filings:
                no_found = f"No Form {self.data.form} found." if self.data.form else "No filings found."
                fields.append(EmbedField(name=ticker, value=no_found, inline=False))
            else:
                filing_lines = []
                for filing in ticker_filings:
                    form = filing.get('form', self.data.form or 'Filing')
                    filing_date = format_date_mdy(filing.get('filingDate', ''))
                    link = filing.get('link', '')
                    if link:
                        filing_lines.append(f"[Form {form} — Filed {filing_date}]({link})")
                    else:
                        filing_lines.append(f"Form {form} — Filed {filing_date}")
                fields.append(EmbedField(name=ticker, value="\n".join(filing_lines), inline=False))

        title = f"SEC Filings: Form {self.data.form}" if self.data.form else "SEC Filings: Recent"
        return EmbedSpec(
            title=title,
            description="",
            color=COLOR_CYAN,
            fields=fields,
        )
