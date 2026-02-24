import datetime
import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.models import (
    COLOR_GREEN, COLOR_RED,
    PopularityAlertData, EmbedField, EmbedSpec,
)
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

    def build_embed_spec(self) -> EmbedSpec:
        logger.debug("Building Popularity Alert EmbedSpec...")
        pct_change = self.alert_data['pct_change']
        company_name = self.data.ticker_info.get('name', self.data.ticker)
        high_rank = self.alert_data['high_rank']
        low_rank = self.alert_data['low_rank']
        spot_diff = high_rank - low_rank
        sign = "+" if pct_change > 0 else ""

        description = (
            f"**{company_name}** · `{self.data.ticker}` has moved **{spot_diff}** spots "
            f"between **#{high_rank}** ({date_utils.format_date_mdy(self.alert_data['high_rank_date'])}) "
            f"and **#{low_rank}** ({date_utils.format_date_mdy(self.alert_data['low_rank_date'])})"
        )

        # Current rank
        now = date_utils.round_down_nearest_minute(30)
        current_rank_series = self.data.popularity[self.data.popularity['datetime'] == now]['rank']
        current_rank = current_rank_series.iloc[0] if not current_rank_series.empty else 'N/A'

        # 5-day high (lowest rank number = most popular)
        five_day_high = self.data.popularity['rank'].min()

        fields = [
            EmbedField(name="Current Rank", value=f"#{current_rank}", inline=True),
            EmbedField(name="5D Best Rank", value=f"#{five_day_high}", inline=True),
            EmbedField(name="5D Change", value=f"{spot_diff} spots", inline=True),
            EmbedField(name="Price Change", value=f"{sign}{pct_change:.2f}%", inline=True),
        ]

        return EmbedSpec(
            title=f"🚨 Popularity Mover: {self.data.ticker}",
            description=description,
            color=COLOR_GREEN if pct_change > 0 else COLOR_RED,
            fields=fields,
            footer="RocketStocks · popularity-mover",
            timestamp=True,
            url=f"https://finviz.com/quote.ashx?t={self.data.ticker}",
        )

    def override_and_edit(self, prev_alert_data: dict) -> bool:
        return self.alert_data['high_rank'] < (0.5 * float(prev_alert_data.get('high_rank', float('inf'))))
