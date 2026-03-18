import datetime
import logging

from rocketstocks.core.config.paths import datapaths
from rocketstocks.core.content.formatting import write_df_to_file
from rocketstocks.core.content.models import COLOR_AMBER, EmbedSpec, WeeklyEarningsData
from rocketstocks.core.content.screeners.base import Screener
from rocketstocks.core.content.sections_card import weekly_earnings_cards
from rocketstocks.core.utils.dates import timezone, format_date_mdy

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
        self.today = datetime.datetime.now(tz=timezone()).date()
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

    def build(self) -> EmbedSpec:
        logger.debug(f"Building '{self.screener_type}' screener embed...")
        title = f"📅 Earnings Releasing the Week of {format_date_mdy(self.today)}"
        description = weekly_earnings_cards(self.data, self.watchlist_tickers)
        return EmbedSpec(
            title=title,
            description=description,
            color=COLOR_AMBER,
            footer="RocketStocks · weekly-earnings",
            timestamp=True,
        )
