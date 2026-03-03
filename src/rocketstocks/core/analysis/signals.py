import logging
import numpy as np
import pandas as pd
import pandas_ta as ta

logger = logging.getLogger(__name__)


class signals():

    @staticmethod
    def rsi(close, UPPER_BOUND=70, LOWER_BOUND=30):
        logger.debug("Calculating RSI signal...")
        return ta.rsi(close) < LOWER_BOUND

    def macd(close):
        logger.debug("Calculating MACD signal...")
        macds = ta.macd(close)
        macd = macds[macds.columns[0]]
        macd_sig = macds[macds.columns[1]]
        return macd > macd_sig

    def sma(close, short, long):
        logger.debug("Calculating SMA signal...")
        return ta.sma(close, short) > ta.sma(close, long)

    def adx(close, highs, lows, TREND_UPPER=25, TREND_LOWER=20):
        logger.debug("Calculating ADX signal...")
        adxs = ta.adx(close=close, high=highs, low=lows)
        adx = adxs[adxs.columns[0]]
        dip = adxs[adxs.columns[1]]
        din = adxs[adxs.columns[2]]
        return (adx > TREND_UPPER) & (dip > din)

    def obv(close, volume):
        logger.debug("Calculating OBV signal...")
        obv = ta.obv(close=close, volume=volume)
        return ta.increasing(ta.sma(obv, 10))

    def ad(high, low, close, open, volume):
        logger.debug("Calculating AD signal...")
        ad = ta.ad(high=high, low=low, close=close, volume=volume, open=open)
        return ta.increasing(ta.sma(ad, 10))

    def zscore(close, BUY_THRESHOLD, SELL_THRESHOLD):
        zscore = ta.zscore(close)
        signals = []
        for i in range(0, zscore.shape[0]):
            zscore_i = zscore.iloc[i]
            if i == 0:
                signals.append(0)
            elif zscore.iloc[i] < BUY_THRESHOLD:
                signals.append(1)
            elif zscore.iloc[i] > SELL_THRESHOLD:
                signals.append(0)
            else:
                signals.append(signals[i - 1])
        return pd.Series(signals).set_axis(close.index)

    def roc(close, length=10):
        return ta.roc(close=close, length=length) > 0

    # ------------------------------------------------------------------
    # New statistical signal methods
    # ------------------------------------------------------------------

    @staticmethod
    def bollinger_bands(close: pd.Series, length: int = 20, std: float = 2.0) -> pd.DataFrame:
        """Return a DataFrame with Bollinger Band columns (BBL, BBM, BBU, BBB, BBP).

        Args:
            close: Closing price series.
            length: Lookback period (default 20).
            std: Number of standard deviations for the bands (default 2.0).

        Returns:
            DataFrame from ``ta.bbands()``.  Returns an empty DataFrame if
            there is insufficient data.
        """
        logger.debug(f"Calculating Bollinger Bands (length={length}, std={std})")
        if len(close) < length:
            return pd.DataFrame()
        result = ta.bbands(close, length=length, std=std)
        return result if result is not None else pd.DataFrame()

    @staticmethod
    def price_zscore(close: pd.Series, period: int = 20) -> float:
        """Return the z-score of the latest daily return vs the stock's own history.

        A large positive value means today's return is unusually high; a large
        negative value means it is unusually low.

        Args:
            close: Closing price series (at least *period* + 1 entries).
            period: Lookback window for computing mean and std (default 20).

        Returns:
            Float z-score, or NaN if there is insufficient data.
        """
        logger.debug(f"Calculating price z-score (period={period})")
        if len(close) < period + 1:
            return float('nan')
        returns = close.pct_change().dropna() * 100.0
        if len(returns) < period:
            return float('nan')
        hist = returns.iloc[-(period + 1):-1]
        latest = returns.iloc[-1]
        mean_r = hist.mean()
        std_r = hist.std()
        if std_r == 0 or np.isnan(std_r):
            return float('nan')
        return float((latest - mean_r) / std_r)

    @staticmethod
    def return_percentile(close: pd.Series, period: int = 60) -> float:
        """Return the percentile rank of today's return in its *period*-day distribution.

        Args:
            close: Closing price series (at least *period* + 1 entries).
            period: Lookback window (default 60).

        Returns:
            Percentile in [0, 100], or NaN if there is insufficient data.
        """
        logger.debug(f"Calculating return percentile (period={period})")
        if len(close) < period + 1:
            return float('nan')
        returns = close.pct_change().dropna() * 100.0
        if len(returns) < period:
            return float('nan')
        hist = returns.iloc[-(period + 1):-1]
        latest = returns.iloc[-1]
        n_below = (hist < latest).sum()
        return float(n_below / len(hist) * 100.0)

    @staticmethod
    def volume_zscore(volume_series: pd.Series, curr_volume: float, period: int = 20) -> float:
        """Return the z-score of *curr_volume* vs the historical *volume_series*.

        Args:
            volume_series: Historical daily volume series.
            curr_volume: Today's current volume.
            period: Number of most-recent days used for the baseline (default 20).

        Returns:
            Float z-score, or NaN if there is insufficient data.
        """
        logger.debug(f"Calculating volume z-score (period={period}, curr_volume={curr_volume})")
        hist = volume_series.tail(period)
        if len(hist) < 2:
            return float('nan')
        mean_v = hist.mean()
        std_v = hist.std()
        if std_v == 0 or np.isnan(std_v):
            return float('nan')
        return float((curr_volume - mean_v) / std_v)

    @staticmethod
    def technical_confluence(
        close: pd.Series,
        high: pd.Series,
        low: pd.Series,
        volume: pd.Series,
    ) -> tuple[int, int, dict]:
        """Count how many of RSI / MACD / ADX / OBV agree on a bullish signal.

        Args:
            close: Closing prices.
            high: Daily highs.
            low: Daily lows.
            volume: Daily volumes.

        Returns:
            ``(count, total, details_dict)`` where *count* is the number of
            bullish signals firing, *total* is the number of indicators that
            could be evaluated (may be less than 4 if data is insufficient),
            and *details_dict* maps indicator name → True/False/None.
        """
        logger.debug("Calculating technical confluence")
        details: dict[str, bool | None] = {}

        # RSI — bullish when oversold (< 30)
        try:
            rsi_val = ta.rsi(close)
            if rsi_val is not None and not rsi_val.empty:
                details['rsi'] = bool(rsi_val.iloc[-1] < 30)
            else:
                details['rsi'] = None
        except Exception:
            details['rsi'] = None

        # MACD — bullish when MACD line > signal line
        try:
            macds = ta.macd(close)
            if macds is not None and not macds.empty:
                macd_line = macds.iloc[-1, 0]
                macd_sig = macds.iloc[-1, 1]
                details['macd'] = bool(macd_line > macd_sig)
            else:
                details['macd'] = None
        except Exception:
            details['macd'] = None

        # ADX — bullish when ADX > 25 and +DI > -DI
        try:
            adxs = ta.adx(close=close, high=high, low=low)
            if adxs is not None and not adxs.empty:
                adx_val = adxs.iloc[-1, 0]
                dip = adxs.iloc[-1, 1]
                din = adxs.iloc[-1, 2]
                details['adx'] = bool(adx_val > 25 and dip > din)
            else:
                details['adx'] = None
        except Exception:
            details['adx'] = None

        # OBV — bullish when OBV SMA is increasing
        try:
            obv_series = ta.obv(close=close, volume=volume)
            if obv_series is not None and not obv_series.empty:
                obv_sma = ta.sma(obv_series, 10)
                if obv_sma is not None and len(obv_sma.dropna()) >= 2:
                    increasing = ta.increasing(obv_sma)
                    details['obv'] = bool(increasing.iloc[-1]) if increasing is not None else None
                else:
                    details['obv'] = None
            else:
                details['obv'] = None
        except Exception:
            details['obv'] = None

        evaluated = {k: v for k, v in details.items() if v is not None}
        count = sum(1 for v in evaluated.values() if v)
        total = len(evaluated)
        return count, total, details
