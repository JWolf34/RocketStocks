"""CompositeSignalStrategy — buy when composite_score exceeds threshold."""
import logging

from backtesting import Strategy

from rocketstocks.backtest.data_prep import prep_for_signals
from rocketstocks.backtest.registry import register
from rocketstocks.core.analysis.alert_strategy import evaluate_price_alert
from rocketstocks.core.analysis.composite_score import compute_composite_score

logger = logging.getLogger(__name__)


@register('composite_signal')
class CompositeSignalStrategy(Strategy):
    """Test whether compute_composite_score() is predictive of forward returns.

    Chains evaluate_price_alert → compute_composite_score per bar. Buys
    when the composite score's dual-gate passes and the score exceeds
    ``composite_threshold``.

    Optimizable parameters::

        bt.optimize(
            hold_bars=range(1, 21),
            composite_threshold=[2.0, 2.5, 3.0, 3.5],
        )

    Attributes:
        hold_bars: Number of bars to hold after entry.
        classification: StockClass value string.
        composite_threshold: Minimum composite score required to enter.
    """

    hold_bars: int = 5
    classification: str = 'standard'
    composite_threshold: float = 2.5

    def init(self):
        pass

    def next(self):
        if self.position:
            if len(self.data) - self.trades[-1].entry_bar >= self.hold_bars:
                self.position.close()
            return

        if len(self.data) < 61:
            return

        pct_change = (self.data.Close[-1] / self.data.Close[-2] - 1) * 100

        lookback = self.data.df.iloc[max(0, len(self.data) - 60):len(self.data)].copy()
        lookback_lower = prep_for_signals(lookback)

        trigger = evaluate_price_alert(
            classification=self.classification,
            pct_change=pct_change,
            daily_prices=lookback_lower,
            current_volume=float(self.data.Volume[-1]),
        )

        composite = compute_composite_score(trigger, threshold=self.composite_threshold)

        if composite.should_alert:
            self.buy()
