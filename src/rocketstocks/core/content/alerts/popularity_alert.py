import datetime
import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.models import PopularityAlertData
from rocketstocks.core.content import sections
from rocketstocks.core.utils.dates import date_utils

logger = logging.getLogger(__name__)


class PopularityAlert(Alert):
    alert_type = "POPULARITY_MOVER"

    def __init__(self, data: PopularityAlertData):
        super().__init__()
        self.data = data
        self.ticker = data.ticker
        self.alert_data['pct_change'] = data.quote['quote']['netPercentChange']
        self._compute_popularity_alert_data()

    def _compute_popularity_alert_data(self) -> None:
        """Compute 5-day popularity rank range and store in alert_data."""
        today = date_utils.round_down_nearest_minute(30)
        five_day_popularity = self.data.popularity[
            self.data.popularity['datetime'].between(today - datetime.timedelta(days=5), today)
        ].copy()
        five_day_popularity['date'] = five_day_popularity['datetime'].dt.date

        lowest_ranks_by_day = (
            five_day_popularity
            .groupby('date')['rank']
            .min()
            .reset_index()
        )

        high_rank_row = lowest_ranks_by_day.iloc[lowest_ranks_by_day['rank'].idxmax()].to_dict()
        self.alert_data['high_rank'] = high_rank_row['rank']
        self.alert_data['high_rank_date'] = date_utils.format_date_ymd(high_rank_row['date'])

        low_rank_row = lowest_ranks_by_day.iloc[lowest_ranks_by_day['rank'].idxmin()].to_dict()
        self.alert_data['low_rank'] = low_rank_row['rank']
        self.alert_data['low_rank_date'] = date_utils.format_date_ymd(low_rank_row['date'])

    def build_alert(self) -> str:
        logger.debug("Building Popularity Alert...")
        todays_change = " ".join([
            f"`{self.data.ticker}` has moved **{self.alert_data['high_rank'] - self.alert_data['low_rank']}** spots",
            f"between {date_utils.format_date_mdy(self.alert_data['high_rank_date'])} **({self.alert_data['high_rank']})** "
            f"and {date_utils.format_date_mdy(self.alert_data['low_rank_date'])} **({self.alert_data['low_rank']})** \n",
        ])
        return (
            sections.alert_header(f"Popularity Mover: {self.data.ticker}")
            + todays_change
            + sections.popularity_stats_section(self.data.popularity)
        )

    def override_and_edit(self, prev_alert_data: dict) -> bool:
        return self.alert_data['high_rank'] < (0.5 * float(prev_alert_data.get('high_rank', float('inf'))))
