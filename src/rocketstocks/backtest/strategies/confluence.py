"""ConfluenceStrategy — buy when multiple vectorized signals agree."""
import logging

import pandas as pd
from backtesting import Strategy

from rocketstocks.backtest.registry import register
from rocketstocks.core.analysis.signals import signals

logger = logging.getLogger(__name__)


@register('confluence')
class ConfluenceStrategy(Strategy):
    """Test whether agreement between vectorized signals predicts returns.

    Pre-computes RSI, MACD, ADX, and OBV bullish-signal Series in init()
    using backtesting.py's self.I() wrapper (so they appear on the Bokeh
    chart). Enters when at least ``min_signals`` indicators agree on a
    bullish reading.

    Optimizable parameters::

        bt.optimize(
            hold_bars=range(1, 21),
            min_signals=[1, 2, 3, 4],
        )

    Attributes:
        hold_bars: Number of bars to hold after entry.
        min_signals: Minimum number of bullish signals required (1–4).
    """

    hold_bars: int = 5
    min_signals: int = 2

    def init(self):
        close = pd.Series(self.data.Close, index=self.data.index)
        high = pd.Series(self.data.High, index=self.data.index)
        low = pd.Series(self.data.Low, index=self.data.index)
        volume = pd.Series(self.data.Volume, index=self.data.index)

        self.rsi_bull = self.I(signals.rsi, close, name='RSI_oversold')
        self.macd_bull = self.I(signals.macd, close, name='MACD_bullish')
        self.adx_bull = self.I(signals.adx, close, high, low, name='ADX_trend')
        self.obv_bull = self.I(signals.obv, close, volume, name='OBV_accum')

    def next(self):
        if self.position:
            if len(self.data) - self.trades[-1].entry_bar >= self.hold_bars:
                self.position.close()
            return

        def _is_true(val) -> bool:
            try:
                return bool(val) and not pd.isna(val)
            except Exception:
                return False

        count = sum([
            _is_true(self.rsi_bull[-1]),
            _is_true(self.macd_bull[-1]),
            _is_true(self.adx_bull[-1]),
            _is_true(self.obv_bull[-1]),
        ])

        if count >= self.min_signals:
            self.buy()
