"""Volume event detector — detects unusual volume spikes.

Uses rolling z-score of volume relative to historical baseline.
Source-agnostic: works with either daily or 5-minute price data.

Memory model: streams price data one ticker at a time so peak RSS stays
bounded regardless of universe size.
"""
import gc
import logging

import numpy as np
import pandas as pd

from rocketstocks.eda.streaming import fetch_distinct_tickers, stream_tickers

logger = logging.getLogger(__name__)

_DEFAULT_ZSCORE_THRESHOLD = 2.0
_DEFAULT_LOOKBACK = 20
_DEFAULT_MIN_VOLUME = 10_000


class VolumeDetector:
    """Detects unusual volume events using rolling z-score.

    Args:
        zscore_threshold: Minimum volume z-score to emit an event.
        lookback: Rolling window (in bars) for computing baseline mean/std.
        min_volume: Minimum absolute volume to avoid noise from low-liquidity bars.
    """

    def __init__(
        self,
        zscore_threshold: float = _DEFAULT_ZSCORE_THRESHOLD,
        lookback: int = _DEFAULT_LOOKBACK,
        min_volume: int = _DEFAULT_MIN_VOLUME,
    ):
        self.zscore_threshold = zscore_threshold
        self.lookback = lookback
        self.min_volume = min_volume

    async def detect(
        self,
        stock_data,
        timeframe: str = 'daily',
        start_date=None,
        end_date=None,
        tickers: list[str] | None = None,
    ) -> pd.DataFrame:
        """Return events DataFrame in standard format.

        Streams price data one ticker at a time — no full panel is built.

        'signal_value' holds the volume z-score at the time of the event.
        'source_detail' is 'volume_zscore>={threshold}'.
        """
        time_col = 'date' if timeframe == 'daily' else 'datetime'

        if tickers is None:
            tickers = await fetch_distinct_tickers(stock_data, start_date, end_date)

        if not tickers:
            return _empty_events()

        events_list: list[pd.DataFrame] = []

        async for ticker, price_df, _pop_df in stream_tickers(
            stock_data, tickers, timeframe, start_date, end_date
        ):
            gc.collect()

            if price_df.empty or time_col not in price_df.columns:
                continue
            if 'volume' not in price_df.columns or price_df['volume'].isna().all():
                continue

            grp = price_df.sort_values(time_col).copy()
            vol = grp['volume'].astype(float)

            # Rolling z-score (shift by 1 to avoid lookahead) — logic unchanged
            min_win = max(3, self.lookback // 2)
            roll_mean = vol.shift(1).rolling(self.lookback, min_periods=min_win).mean()
            roll_std = vol.shift(1).rolling(self.lookback, min_periods=min_win).std()
            zscore = (vol - roll_mean) / roll_std.replace(0, np.nan)

            grp['_zscore'] = zscore

            mask = (grp['_zscore'] >= self.zscore_threshold) & (vol >= self.min_volume)
            subset = grp[mask].copy()
            if subset.empty:
                continue

            subset['signal_value'] = subset['_zscore']
            subset['source'] = 'volume'
            subset['source_detail'] = f'volume_zscore>={self.zscore_threshold}'
            subset['datetime'] = pd.to_datetime(subset[time_col])
            subset['ticker'] = ticker

            events_list.append(
                subset[['ticker', 'datetime', 'signal_value', 'source', 'source_detail', 'volume']]
            )

        if not events_list:
            logger.info("VolumeDetector: no events found")
            return _empty_events()

        events = pd.concat(events_list, ignore_index=True)
        events = events.sort_values(['ticker', 'datetime']).reset_index(drop=True)
        logger.info(
            f"VolumeDetector: {len(events)} events detected "
            f"({events['ticker'].nunique()} tickers)"
        )
        return events


def _empty_events() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        'ticker', 'datetime', 'signal_value', 'source', 'source_detail', 'volume',
    ])
