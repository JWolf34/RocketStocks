import datetime
import logging
import pandas as pd
from rocketstocks.core.reports.base import Report
from rocketstocks.core.utils.dates import date_utils

logger = logging.getLogger(__name__)


class Alert(Report):
    """Base alert content class. Inherits report-building helpers from Report.

    Content-only: no Discord channel, no send_alert(), no Buttons.
    Sending is handled by bot/senders/alert_sender.py.
    """

    def __init__(self, alert_type: str, **kwargs):
        super().__init__(**kwargs)

        self.alert_type = alert_type

        # Parse alert-specific kwargs
        self.market_period = kwargs.pop('market_period', None)
        self.pct_change = self.quote['quote']['netPercentChange'] if self.quote else None
        self.rvol = kwargs.pop('rvol', None)
        self.rvol_at_time = kwargs.pop('rvol_at_time', None)
        self.avg_vol_at_time = kwargs.pop('avg_vol_at_time', None)
        self.time = kwargs.pop('time', None)
        self.watchlist = kwargs.pop('watchlist', None)

        # Init and populate alert data (used for override decisions)
        self.alert_data = {}
        self.build_alert_data()

    def build_alert_data(self):
        """Populate alert_data with information used to compare against future alerts."""
        self.alert_data['pct_change'] = self.pct_change

    def build_alert_header(self):
        logger.debug("Building alert header...")
        return f"# :rotatinglight: {self.ticker} ALERT :rotatinglight:\n\n"

    def build_alert(self):
        alert = ''
        alert += self.build_alert_header()
        return alert

    def build_todays_change(self):
        logger.debug("Building today's change...")
        symbol = "🟢" if self.pct_change > 0 else "🔻"
        return f"`{self.ticker}` is {symbol} **{'{:.2f}'.format(self.pct_change)}%**"

    def build_volume_stats(self):
        """Return message content with volume statistics for the alert's ticker."""
        logger.debug("Building volume stats...")
        message = '## Volume Stats\n'
        volume_stats = {}

        if self.quote:
            volume_stats['Volume Today'] = self.format_large_num(self.quote['quote']['totalVolume'])

        if self.rvol:
            volume_stats['Relative Volume (10 Day)'] = "{:.2f}x".format(self.rvol)

        if self.rvol_at_time and self.avg_vol_at_time:
            volume_stats[f'Relative Volume at Time ({self.time})'] = "{:.2f}x".format(self.rvol_at_time)
            volume_stats[f'Current Volume at Time ({self.time})'] = self.format_large_num(self.rvol_at_time * self.avg_vol_at_time)
            volume_stats[f'Average Volume at Time ({self.time})'] = self.format_large_num(self.avg_vol_at_time)

        if self.daily_price_history is not None and not self.daily_price_history.empty:
            volume_stats['Average Volume (10 Day)'] = self.format_large_num(self.daily_price_history['volume'].tail(10).mean())
            volume_stats['Average Volume (30 Day)'] = self.format_large_num(self.daily_price_history['volume'].tail(30).mean())
            volume_stats['Average Volume (90 Day)'] = self.format_large_num(self.daily_price_history['volume'].tail(90).mean())

        if volume_stats:
            message += self.build_stats_table(header={}, body=volume_stats, adjust='right')
        else:
            message += "No volume stats available"

        return message

    def build_todays_sec_filings(self):
        """Return message content with SEC filings for this ticker released today."""
        logger.debug("Building today's SEC filings...")
        message = "## Today's SEC Filings\n\n"
        today_string = datetime.datetime.today().strftime("%Y-%m-%d")
        todays_filings = self.recent_sec_filings[self.recent_sec_filings['filingDate'] == today_string]
        for index, filing in todays_filings.iterrows():
            message += f"[Form {filing['form']} - {filing['filingDate']}]({filing['link']})\n"
        return message

    def override_and_edit(self, prev_alert_data: dict) -> bool:
        """Return True if the alert should be re-posted based on significant new movement."""
        pct_diff = (
            (self.alert_data['pct_change'] - prev_alert_data['pct_change'])
            / abs(prev_alert_data['pct_change'])
        ) * 100.0
        return pct_diff > 100.0
