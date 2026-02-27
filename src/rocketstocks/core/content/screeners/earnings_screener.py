import datetime
import logging

import pandas as pd

from rocketstocks.core.config.paths import datapaths
from rocketstocks.core.content.formatting import build_df_table, write_df_to_file
from rocketstocks.core.content.models import COLOR_BLUE, EmbedSpec, WeeklyEarningsData
from rocketstocks.core.content.screeners.base import Screener
from rocketstocks.core.utils.dates import date_utils

logger = logging.getLogger(__name__)

_COLUMN_MAP = {
    'date': 'Date',
    'ticker': 'Ticker',
    'time': 'Time',
    'fiscal_quarter_ending': 'Fiscal Quarter Ending',
    'eps_forecast': 'EPS Forecast',
    'no_of_ests': '# of Ests',
    'last_year_eps': 'Last Year EPS',
    'last_year_rpt_dt': 'Last Year Report Date',
}


class WeeklyEarningsScreener(Screener):
    """Screener for earnings reports releasing this week."""

    def __init__(self, data: WeeklyEarningsData):
        self.today = datetime.datetime.now(tz=date_utils.timezone()).date()
        self.watchlist_tickers = data.watchlist_tickers

        upcoming = data.upcoming_earnings[
            data.upcoming_earnings['date'].between(self.today, self.today + datetime.timedelta(days=7))
        ]

        super().__init__(
            screener_type='weekly-earnings',
            data=upcoming,
            column_map=_COLUMN_MAP,
        )

        self.filepath = f"{datapaths.attachments_path}/upcoming_earnings.csv"
        write_df_to_file(df=self.data, filepath=self.filepath)

    def _build_upcoming_earnings(self) -> str:
        """Table of watchlist tickers reporting earnings, grouped by day."""
        logger.debug("Identifying upcoming earnings for tickers on user watchlists")
        watchlist_earnings = {}

        for i in range(0, 5):
            date = self.today + datetime.timedelta(days=i)
            tickers = self.data[self.data['Date'] == date]['Ticker'].values
            if tickers.any():
                watchlist_earnings[date.strftime('%A')] = [
                    ticker for ticker in tickers if ticker in self.watchlist_tickers
                ]

        watchlist_earnings_df = pd.DataFrame(
            {date: pd.Series(tickers) for date, tickers in watchlist_earnings.items()}
        ).fillna(' ')
        return build_df_table(df=watchlist_earnings_df, style='borderless')

    def build_report(self) -> str:
        logger.debug(f"Building '{self.screener_type}' screener...")
        header = f"# Earnings Releasing the Week of {date_utils.format_date_mdy(self.today)}\n\n"
        return header + self._build_upcoming_earnings()

    def build_embed_spec(self) -> EmbedSpec:
        logger.debug(f"Building '{self.screener_type}' screener EmbedSpec...")
        title = f"📅 Earnings Releasing the Week of {date_utils.format_date_mdy(self.today)}"
        description = self._build_upcoming_earnings()
        return EmbedSpec(
            title=title,
            description=description,
            color=COLOR_BLUE,
            footer="RocketStocks · weekly-earnings",
            timestamp=True,
        )
