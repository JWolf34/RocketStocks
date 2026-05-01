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

from rocketstocks.eda.events.base import _to_naive_utc
from rocketstocks.eda.streaming import (
    fetch_distinct_tickers_from_prices,
    stream_tickers,
)

logger = logging.getLogger(__name__)

_DEFAULT_ZSCORE_THRESHOLD = 2.0
_DEFAULT_LOOKBACK = 20
_DEFAULT_MIN_VOLUME_DAILY = 10_000
_DEFAULT_MIN_VOLUME_5M = 500


class VolumeDetector:
    """Detects unusual volume events using rolling z-score.

    Args:
        zscore_threshold: Minimum volume z-score to emit an event.
        lookback: Rolling window (in bars) for computing baseline mean/std.
        min_volume: Minimum absolute volume to avoid noise from low-liquidity
            bars.  Defaults to 10_000 for daily and 500 for 5m timeframe.
            Pass an explicit value to override the timeframe-scaled default.

    Attributes:
        skipped_tickers: Dict mapping ticker → skip reason for the most
            recent detect() call.  Populated after each call.
    """

    def __init__(
        self,
        zscore_threshold: float = _DEFAULT_ZSCORE_THRESHOLD,
        lookback: int = _DEFAULT_LOOKBACK,
        min_volume: int | None = None,
    ):
        self.zscore_threshold = zscore_threshold
        self.lookback = lookback
        self._min_volume_override = min_volume
        self.skipped_tickers: dict[str, str] = {}

    def _effective_min_volume(self, timeframe: str) -> int:
        if self._min_volume_override is not None:
            return self._min_volume_override
        return _DEFAULT_MIN_VOLUME_DAILY if timeframe == 'daily' else _DEFAULT_MIN_VOLUME_5M

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
        self.skipped_tickers = {}
        time_col = 'date' if timeframe == 'daily' else 'datetime'
        min_volume = self._effective_min_volume(timeframe)

        # When no explicit tickers provided, discover from the price table
        # so volume analysis covers the full price universe, not just
        # tickers that have appeared in the popularity table.
        if tickers is None:
            tickers = await fetch_distinct_tickers_from_prices(
                stock_data, timeframe, start_date, end_date
            )
        else:
            tickers = [t.upper() for t in tickers]

        if not tickers:
            return _empty_events()

        events_list: list[pd.DataFrame] = []
        min_win = max(3, self.lookback // 2)

        async for ticker, price_df, _pop_df in stream_tickers(
            stock_data, tickers, timeframe, start_date, end_date
        ):
            gc.collect()

            if price_df.empty or time_col not in price_df.columns:
                self.skipped_tickers[ticker] = 'no price data'
                continue
            if 'volume' not in price_df.columns or price_df['volume'].isna().all():
                self.skipped_tickers[ticker] = 'no volume data'
                continue

            grp = price_df.sort_values(time_col).copy()
            vol = grp['volume'].astype(float)

            # Rolling z-score (shift by 1 to avoid lookahead)
            roll_mean = vol.shift(1).rolling(self.lookback, min_periods=min_win).mean()
            roll_std = vol.shift(1).rolling(self.lookback, min_periods=min_win).std()
            zscore = (vol - roll_mean) / roll_std.replace(0, np.nan)

            grp['_zscore'] = zscore

            mask = (grp['_zscore'] >= self.zscore_threshold) & (vol >= min_volume)
            subset = grp[mask].copy()
            if subset.empty:
                n_valid_z = zscore.notna().sum()
                if n_valid_z == 0:
                    self.skipped_tickers[ticker] = 'short history (all-NaN z-score)'
                continue

            subset['signal_value'] = subset['_zscore']
            subset['source'] = 'volume'
            subset['source_detail'] = f'volume_zscore>={self.zscore_threshold}'
            subset['datetime'] = _to_naive_utc(pd.to_datetime(subset[time_col].astype(str)))
            subset['ticker'] = ticker.upper()

            events_list.append(
                subset[['ticker', 'datetime', 'signal_value', 'source', 'source_detail', 'volume']]
            )

        if self.skipped_tickers:
            by_reason: dict[str, int] = {}
            for reason in self.skipped_tickers.values():
                by_reason[reason] = by_reason.get(reason, 0) + 1
            parts = ', '.join(f'{r} ({n})' for r, n in sorted(by_reason.items()))
            logger.info(f"VolumeDetector: skipped {len(self.skipped_tickers)} tickers — {parts}")

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
