"""BuyHoldStrategy — simple buy-and-hold baseline.

Buys on the first bar and holds for the entire period. Run against SPY
to establish an S&P 500 benchmark for comparison with other strategies.
"""
import logging

from backtesting import Strategy

from rocketstocks.backtest.registry import register

logger = logging.getLogger(__name__)


@register('buy_hold')
class BuyHoldStrategy(Strategy):
    """Buy on the first bar and hold indefinitely.

    Intended for use as a passive benchmark::

        python -m rocketstocks.backtest run buy_hold --tickers SPY

    The stored return_pct for this run can then be compared against any
    active strategy using the compare command or the --benchmark flag.
    """

    def init(self):
        pass

    def next(self):
        if not self.position:
            self.buy()
