import logging
import numpy as np
import pandas as pd
import mplfinance as mpf
import pandas_ta as ta

logger = logging.getLogger(__name__)


def all_values_are_nan(values):
    if np.isnan(values).all():
        return True
    else:
        return False


def recent_crossover(indicator, signal):
    """Determine if there was a crossover between indicator and signal over the last data points."""
    for i in range(1, len(indicator)):
        curr_indicator = indicator[-i]
        prev_indicator = indicator[-i - 1]
        curr_signal = signal[-i]
        prev_signal = signal[-i - 1]

        if prev_indicator < prev_signal and curr_indicator > curr_signal:
            return 'UP'
        elif prev_indicator > prev_signal and curr_indicator < curr_signal:
            return 'DOWN'

    return None


def format_millions(x, pos):
    """Format value as millions for axis tick labels."""
    return "%1.1fM" % (x * 1e-6)


def recent_bars(df, tf: str = "1y"):
    """Return the number of bars corresponding to the given timeframe string."""
    yearly_divisor = {"all": 0, "10y": 0.1, "5y": 0.2, "4y": 0.25, "3y": 1./3, "2y": 0.5, "1y": 1, "6mo": 2, "3mo": 4}
    yd = yearly_divisor[tf] if tf in yearly_divisor.keys() else 0
    return int(ta.RATE["TRADING_DAYS_PER_YEAR"] / yd) if yd > 0 else df.shape[0]


def get_plot_timeframes():
    return {"all": 0, "10y": 0.1, "5y": 0.2, "4y": 0.25, "3y": 1./3, "2y": 0.5, "1y": 1, "6mo": 2, "3mo": 4}


def ta_ylim(series: pd.Series, percent: float = 0.1):
    smin, smax = series.min(), series.max()
    if isinstance(percent, float) and 0 <= float(percent) <= 1:
        y_min = (1 + percent) * smin if smin < 0 else (1 - percent) * smin
        y_max = (1 - percent) * smax if smax < 0 else (1 + percent) * smax
        return (y_min, y_max)
    return (smin, smax)


def hline(size, value):
    hline = np.empty(size)
    hline.fill(value)
    return hline


def get_plot_types():
    return ['line', 'candle', 'ohlc', 'renko', 'pnf']


def get_plot_styles():
    return mpf.available_styles()
