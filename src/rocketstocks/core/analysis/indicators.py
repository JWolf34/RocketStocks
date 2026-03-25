import datetime
import logging
import numpy as np
import pandas as pd
from rocketstocks.core.utils.dates import dt_round_down

logger = logging.getLogger(__name__)


class indicators:

    class volume:

        def avg_vol_at_time(data: pd.DataFrame, periods: int = 10, dt: datetime.datetime = None):
            logger.debug(f"Calculating avg_vol_at_time {dt} over last {periods} periods")
            if dt is None:
                dt = dt_round_down(datetime.datetime.now() - datetime.timedelta(minutes=5))
            else:
                dt = dt_round_down(dt)
            time = datetime.time(hour=dt.hour, minute=dt.minute)

            filtered_data = data[data['datetime'].apply(lambda x: x.time()) == time]
            return filtered_data['volume'].tail(periods).mean(), time

        def rvol(data: pd.DataFrame, periods: int = 10, curr_volume: float = None):
            avg_volume = data['volume'].tail(periods).mean()
            logger.debug(f"Calculating rvol over last {periods} periods")
            rvol = curr_volume / avg_volume
            logger.debug(f"avg_volume ({avg_volume}) / curr_volume ({curr_volume}) = rvol ({rvol})")
            return rvol

        def rvol_at_time(data: pd.DataFrame, today_data: pd.DataFrame, periods: int = 10, dt: datetime.datetime = None):
            avg_vol_at_time, time = indicators.volume.avg_vol_at_time(data=data, periods=periods, dt=dt)

            logger.debug(f"Calculating rvol_at_time {dt} over last {periods} periods")
            try:
                curr_vol_at_time = today_data[today_data['datetime'].apply(lambda x: x.time()) == time]['volume'].iloc[0]
                rvol = curr_vol_at_time / avg_vol_at_time
                logger.debug(f"avg_vol_at_time ({avg_vol_at_time}) / curr_vol_at_time ({curr_vol_at_time}) = rvol ({rvol})")
                return rvol
            except IndexError:
                logger.debug(f"Could not process rvol_at_time - no volume data exists at time {time}. Latest row:\n{today_data.iloc[0]}")
                return np.nan

    class price:
        """Price-based statistical indicators using daily price history."""

        @staticmethod
        def intraday_zscore(
            daily_prices_df: pd.DataFrame,
            current_pct_change: float,
            period: int = 20,
        ) -> float:
            """Return the z-score of *current_pct_change* vs the stock's own 20-day history.

            Args:
                daily_prices_df: Daily OHLCV DataFrame with a 'close' column.
                current_pct_change: Today's current percentage change (as a number, e.g. 5.0 = 5%).
                period: Lookback window for baseline stats (default 20).

            Returns:
                Z-score float, or NaN if there is insufficient data.
            """
            logger.debug(f"Calculating intraday z-score (period={period})")
            if daily_prices_df.empty or 'close' not in daily_prices_df.columns:
                return float('nan')
            close = daily_prices_df['close'].tail(period + 1)
            if len(close) < 2:
                return float('nan')
            hist_returns = close.pct_change().dropna() * 100.0
            if len(hist_returns) < 2:
                return float('nan')
            mean_r = hist_returns.mean()
            std_r = hist_returns.std()
            if std_r == 0 or np.isnan(std_r):
                return float('nan')
            return float((current_pct_change - mean_r) / std_r)

        @staticmethod
        def return_percentile(
            daily_prices_df: pd.DataFrame,
            current_pct_change: float,
            period: int = 60,
        ) -> float:
            """Return the percentile of *current_pct_change* in the 60-day return distribution.

            Args:
                daily_prices_df: Daily OHLCV DataFrame with a 'close' column.
                current_pct_change: Today's current percentage change.
                period: Lookback window (default 60).

            Returns:
                Percentile in [0, 100], or NaN if there is insufficient data.
            """
            logger.debug(f"Calculating return percentile (period={period})")
            if daily_prices_df.empty or 'close' not in daily_prices_df.columns:
                return float('nan')
            close = daily_prices_df['close'].tail(period + 1)
            if len(close) < 2:
                return float('nan')
            hist_returns = close.pct_change().dropna() * 100.0
            if len(hist_returns) < 1:
                return float('nan')
            n_below = (hist_returns < current_pct_change).sum()
            return float(n_below / len(hist_returns) * 100.0)

    class popularity:
        """Popularity-rank statistical indicators."""

        @staticmethod
        def rank_velocity(popularity_df: pd.DataFrame, periods: int = 5) -> float:
            """Return the average rank change per day over the last *periods* observations.

            A negative velocity means the stock is gaining popularity (rank decreasing).
            A positive velocity means the stock is losing popularity (rank increasing).

            Args:
                popularity_df: DataFrame with 'rank' and 'datetime' columns, sorted by datetime.
                periods: Number of most-recent periods to use (default 5).

            Returns:
                Average rank change per period, or NaN if insufficient data.
            """
            logger.debug(f"Calculating rank velocity (periods={periods})")
            if popularity_df.empty or 'rank' not in popularity_df.columns:
                return float('nan')
            recent = popularity_df.sort_values('datetime').tail(periods + 1)['rank']
            if len(recent) < 2:
                return float('nan')
            diffs = recent.diff().dropna()
            return float(diffs.mean())

        @staticmethod
        def mention_acceleration(popularity_df: pd.DataFrame, periods: int = 3) -> float:
            """Return the acceleration of mention counts (second difference).

            Computes the first difference of mentions (velocity), then the first
            difference of velocity (acceleration).  Positive = accelerating
            (growth phase).  Negative = decelerating (peak/decline phase).

            Args:
                popularity_df: DataFrame with 'mentions' and 'datetime' columns.
                periods: Number of most-recent periods to use (default 3). Minimum 3
                    observations are required to compute a single acceleration value.

            Returns:
                Acceleration float, or NaN if there are fewer than 3 data points or
                the required columns are missing.
            """
            logger.debug(f"Calculating mention acceleration (periods={periods})")
            if popularity_df.empty or 'mentions' not in popularity_df.columns:
                return float('nan')
            sorted_df = popularity_df.sort_values('datetime')
            mentions = sorted_df['mentions'].tail(periods + 2)
            if len(mentions) < 3:
                return float('nan')
            velocity = mentions.diff().dropna()
            if len(velocity) < 2:
                return float('nan')
            acceleration = velocity.diff().dropna()
            if acceleration.empty:
                return float('nan')
            return float(acceleration.iloc[-1])

        @staticmethod
        def rank_velocity_zscore(
            popularity_df: pd.DataFrame,
            lookback: int = 30,
            velocity_window: int = 5,
        ) -> float:
            """Return the z-score of the current rank velocity vs its historical distribution.

            Args:
                popularity_df: DataFrame with 'rank' and 'datetime' columns.
                lookback: Number of observations to use for the historical baseline (default 30).
                velocity_window: Window for computing each rolling velocity (default 5).

            Returns:
                Z-score float, or NaN if there is insufficient data.
            """
            logger.debug(f"Calculating rank velocity z-score (lookback={lookback}, window={velocity_window})")
            if popularity_df.empty or 'rank' not in popularity_df.columns:
                return float('nan')
            sorted_df = popularity_df.sort_values('datetime')
            ranks = sorted_df['rank']
            if len(ranks) < velocity_window + lookback:
                return float('nan')

            # Compute rolling velocities over the lookback window
            velocities = []
            for i in range(lookback):
                window_end = len(ranks) - i - 1
                window_start = window_end - velocity_window
                if window_start < 0:
                    break
                window = ranks.iloc[window_start:window_end + 1]
                diffs = window.diff().dropna()
                if not diffs.empty:
                    velocities.append(float(diffs.mean()))

            if len(velocities) < 2:
                return float('nan')

            current_velocity = velocities[0]  # most-recent
            hist_velocities = velocities[1:]   # older observations
            mean_v = np.mean(hist_velocities)
            std_v = np.std(hist_velocities, ddof=1)
            if std_v == 0 or np.isnan(std_v):
                return float('nan')
            return float((current_velocity - mean_v) / std_v)
