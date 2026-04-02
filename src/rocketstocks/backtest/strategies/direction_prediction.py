"""DirectionPredictionStrategy — tests the direction model in isolation.

Unlike the other strategies, which detect a volume/popularity signal and then
optionally apply the direction filter, this strategy uses the direction model
AS the entry signal. No external alert is required — entry fires whenever the
model predicts an upward move with sufficient probability and confidence.

This is the cleanest way to measure the model's standalone predictive power:
run it against historical data and compare the trade outcomes.

Usage::

    backtest run direction_prediction --tickers AAPL --start 2024-01-01
    backtest walk-forward direction_prediction --folds 5

Parameters (overridable via --params):

    probability_threshold   Min P(up) required for entry (default 0.60)
    forward_bars            Bars ahead used to define the training label (default 5)
    train_fraction          Fraction of data used to train the model (default 0.50)
    min_confidence          Min confidence = abs(P-0.5)*2 required (default 0.20)

    All exit parameters from LeadingIndicatorStrategy are also available.
"""
from __future__ import annotations

import logging

from rocketstocks.backtest.registry import register
from rocketstocks.backtest.strategies.base import LeadingIndicatorStrategy

logger = logging.getLogger(__name__)


@register('direction_prediction')
class DirectionPredictionStrategy(LeadingIndicatorStrategy):
    """Entry when the direction model predicts upward price movement.

    The strategy trains a logistic regression model on the first
    ``train_fraction`` of the dataset, then enters on every bar where
    P(forward_return > 0) >= probability_threshold AND confidence >=
    min_confidence.

    Exit behaviour is inherited from LeadingIndicatorStrategy and controlled
    by exit_mode / exit_zscore / stop_zscore / trail_zscore / hold_bars.
    """

    # Direction model is the sole entry signal — always enable the filter
    use_direction_filter: bool = True

    # Strategy-specific parameter aliases that map to direction filter params.
    # These are exposed at the top level so they appear cleanly in CLI --params.
    probability_threshold: float = 0.60
    forward_bars: int = 5
    train_fraction: float = 0.50
    min_confidence: float = 0.20

    # Requires daily OHLCV for feature computation
    requires_daily: bool = True

    def init(self):
        # Propagate strategy-level aliases to the base class filter params
        # before super().init() runs _train_direction_model().
        self.direction_threshold = self.probability_threshold
        self.direction_forward_bars = self.forward_bars
        self.direction_train_fraction = self.train_fraction
        self.direction_min_confidence = self.min_confidence
        super().init()

    def _detect_signal(self) -> bool:
        """Return True for all bars after the training window.

        The direction filter in next() is the actual gate. _detect_signal()
        just opens the door — the model decides whether to enter.
        """
        if self._direction_model is None or not self._direction_model.is_fitted:
            return False
        current_bar_idx = len(self.data) - 1
        return current_bar_idx >= self._direction_train_cutoff
