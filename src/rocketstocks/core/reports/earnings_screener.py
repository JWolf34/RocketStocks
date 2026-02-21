import datetime
import logging
import pandas as pd
from rocketstocks.core.reports.screener import Screener
from rocketstocks.core.utils.dates import date_utils
from rocketstocks.core.config.paths import datapaths

logger = logging.getLogger(__name__)


class WeeklyEarningsScreener(Screener):
    """Screener subclass for posting upcoming week's earnings reports"""

    def __init__(self, upcoming_earnings: pd.DataFrame, watchlist_tickers: list):
        self.today = datetime.datetime.now(tz=date_utils.timezone()).date()
        self.watchlist_tickers = watchlist_tickers
        self.upcoming_earnings = upcoming_earnings[
            upcoming_earnings['date'].between(self.today, self.today + datetime.timedelta(days=7))
        ]
        column_map = {
            'date': 'Date',
            'ticker': 'Ticker',
            'time': 'Time',
            'fiscal_quarter_ending': 'Fiscal Quarter Ending',
            'eps_forecast': 'EPS Forecast',
            'no_of_ests': '# of Ests',
            'last_year_eps': 'Last Year EPS',
            'last_year_rpt_dt': 'Last Year Report Date',
        }
        super().__init__(
            screener_type='weekly-earnings',
            data=self.upcoming_earnings,
            column_map=column_map,
        )

        self.filepath = f"{datapaths.attachments_path}/upcoming_earnings.csv"
        self.write_df_to_file(df=self.upcoming_earnings, filepath=self.filepath)

    def build_report_header(self):
        """Overrides the parent function to generate custom header"""
        logger.debug(f"Building '{self.screener_type}' screener header...")
        return f"# Earnings Releasing the Week of {date_utils.format_date_mdy(self.today)}\n\n"

    def build_upcoming_earnings(self):
        """Return message content with table of upcoming earnings reports divided by day of the week"""
        logger.debug("Identifying upcoming earnings for tickers that exist on user watchlists")
        watchlist_earnings = {}

        for i in range(0, 5):
            date = self.today + datetime.timedelta(days=i)
            tickers = self.data[self.data['Date'] == date]['Ticker'].values
            if tickers.any():
                watchlist_earnings[date.strftime('%A')] = [ticker for ticker in tickers if ticker in self.watchlist_tickers]

        watchlist_earnings_df = pd.DataFrame(
            dict([(date, pd.Series(tickers)) for date, tickers in watchlist_earnings.items()])
        ).fillna(' ')
        message = self.build_df_table(df=watchlist_earnings_df, style='borderless')
        return message

    def build_report(self):
        """Build complete weekly earnings screener content string"""
        logger.debug(f"Building '{self.screener_type}' screener...")
        report = ""
        report += self.build_report_header()
        report += self.build_upcoming_earnings()
        return report
