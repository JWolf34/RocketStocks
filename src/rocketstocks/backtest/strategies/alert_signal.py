"""AlertSignalStrategy — buy when evaluate_price_alert fires, hold N bars."""
import logging

import pandas as pd
from backtesting import Strategy

from rocketstocks.backtest.data_prep import prep_for_signals
from rocketstocks.backtest.registry import register
from rocketstocks.core.analysis.alert_strategy import evaluate_price_alert

logger = logging.getLogger(__name__)


@register('alert_signal')
class AlertSignalStrategy(Strategy):
    """Test whether evaluate_price_alert() is predictive of forward returns.

    Buys on every bar where ``evaluate_price_alert`` fires and holds for
    ``hold_bars`` bars before closing the position.

    Optimizable parameters::

        bt.optimize(hold_bars=range(1, 21), classification=['standard', 'volatile'])

    Attributes:
        hold_bars: Number of bars to hold after entry.
        classification: StockClass value string passed to evaluate_price_alert.
    """

    hold_bars: int = 5
    classification: str = 'standard'

    def init(self):
        pass  # point-in-time signals are computed per bar in next()

    def next(self):
        # Exit: close after hold_bars
        if self.position:
            if len(self.data) - self.trades[-1].entry_bar >= self.hold_bars:
                self.position.close()
            return

        # Need at least 61 bars for the 60-period lookback used by alert_strategy
        if len(self.data) < 61:
            return

        pct_change = (self.data.Close[-1] / self.data.Close[-2] - 1) * 100

        # Convert current window to lowercase-column DataFrame for signal functions
        lookback = self.data.df.iloc[max(0, len(self.data) - 60):len(self.data)].copy()
        lookback_lower = prep_for_signals(lookback)

        trigger = evaluate_price_alert(
            classification=self.classification,
            pct_change=pct_change,
            daily_prices=lookback_lower,
            current_volume=float(self.data.Volume[-1]),
        )

        if trigger.should_alert:
            self.buy()
