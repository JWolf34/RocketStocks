import datetime
import logging
import pandas as pd
from rocketstocks.core.alerts.base import Alert
from rocketstocks.core.utils.dates import date_utils

logger = logging.getLogger(__name__)


class PopularityAlert(Alert):
    def __init__(self, ticker: str, quote: dict, popularity: pd.DataFrame):
        super().__init__(
            alert_type="POPULARITY_MOVER",
            ticker=ticker,
            quote=quote,
            popularity=popularity,
        )

    def build_alert_data(self):
        """Extends parent to include 5-day popularity rank range in alert data."""
        super().build_alert_data()

        today = date_utils.round_down_nearest_minute(30)
        five_day_popularity = self.popularity[
            self.popularity['datetime'].between(today - datetime.timedelta(days=5), today)
        ]
        five_day_popularity = five_day_popularity.copy()
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

    def build_popularity_stats(self):
        """Return message content with popularity overview over select intervals."""
        logger.debug("Building popularity stats...")
        message = "## Popularity\n"

        if not self.popularity.empty:
            table_header = {}
            now = date_utils.round_down_nearest_minute(30)
            popularity_today = self.popularity[(self.popularity['datetime'] == now)]
            current_rank = popularity_today['rank'].iloc[0] if not popularity_today.empty else 'N/A'
            table_header['Current'] = current_rank

            table_body = {}
            interval_map = {
                "High Today": 0,
                "High 1D Ago": 1,
                "High 2D Ago": 2,
                "High 3D Ago": 3,
                "High 4D Ago": 4,
                "High 5D Ago": 5,
            }

            for label, interval in interval_map.items():
                interval_date = now.date() - datetime.timedelta(days=interval)
                interval_popularity = self.popularity[self.popularity['datetime'].dt.date == interval_date]
                if not interval_popularity.empty:
                    max_rank = interval_popularity['rank'].min()
                else:
                    max_rank = 'N/A'

                symbol = None
                if max_rank != "N/A" and current_rank != 'N/A':
                    if max_rank < current_rank:
                        symbol = "🔻"
                    elif max_rank > current_rank:
                        symbol = "🟢"
                    else:
                        symbol = '━'

                table_body[label] = f"{max_rank:<3} {f'{symbol} {max_rank - current_rank} spots' if symbol and current_rank != 'N/A' else 'No change'}"

            message += self.build_stats_table(header=table_header, body=table_body, adjust='right')
        else:
            message += "No popularity data found for this stock\n"

        return message

    def build_alert_header(self):
        return f"## :rotating_light: Popularity Mover: {self.ticker}\n\n\n"

    def build_todays_change(self):
        logger.debug("Building today's change...")
        return " ".join([
            f"`{self.ticker}` has moved **{self.alert_data['high_rank'] - self.alert_data['low_rank']}** spots",
            f"between {date_utils.format_date_mdy(self.alert_data['high_rank_date'])} **({self.alert_data['high_rank']})** "
            f"and {date_utils.format_date_mdy(self.alert_data['low_rank_date'])} **({self.alert_data['low_rank']})** \n",
        ])

    def build_alert(self):
        logger.debug("Building Popularity Alert...")
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        alert += self.build_popularity_stats()
        return alert

    def override_and_edit(self, prev_alert_data: dict) -> bool:
        if self.alert_data['high_rank'] < (0.5 * float(prev_alert_data['high_rank'])):
            return True
        return False
