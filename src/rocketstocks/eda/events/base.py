"""EventDetector protocol and shared event DataFrame utilities.

Every event detector produces a DataFrame with the standard event columns.
All analysis engines consume that format — they are source-agnostic.
"""
import logging
from typing import Protocol, runtime_checkable

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Required columns for every events DataFrame
EVENT_COLS = ('ticker', 'datetime', 'signal_value', 'source')


@runtime_checkable
class EventDetector(Protocol):
    """Protocol for pluggable event sources.

    Implementations detect when a signal condition is met and return events
    in the standard format.  The statistical engines are indifferent to
    which detector produced the events.
    """

    async def detect(
        self,
        stock_data,
        timeframe: str,
        start_date=None,
        end_date=None,
        tickers: list[str] | None = None,
    ) -> pd.DataFrame:
        """Return a DataFrame of events in the standard format.

        Args:
            stock_data: StockData singleton with DB access.
            timeframe: 'daily' or '5m'.
            start_date: Optional earliest date to consider.
            end_date: Optional latest date to consider.
            tickers: Optional explicit ticker list; if None use all available.

        Returns:
            DataFrame with columns: ticker, datetime, signal_value, source,
            plus any source-specific metadata columns.  Never empty — returns
            a zero-row DataFrame with at least the standard columns if nothing
            is detected.
        """
        ...


def empty_events(source: str = '') -> pd.DataFrame:
    """Return a zero-row events DataFrame with the standard columns."""
    return pd.DataFrame(columns=list(EVENT_COLS) + (['source_detail'] if source else []))


def validate_events(df: pd.DataFrame) -> pd.DataFrame:
    """Raise ValueError if required columns are missing; return df unchanged."""
    missing = [c for c in EVENT_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Events DataFrame missing required columns: {missing}")
    return df


def deduplicate_events(
    events: pd.DataFrame,
    window_days: int = 3,
) -> pd.DataFrame:
    """Keep only the first event per ticker within a rolling window.

    Prevents clustering bias when the same ticker fires multiple events
    close together.  For each ticker, events are kept only if they are
    at least *window_days* calendar days after the most recently kept event.

    Args:
        events: Events DataFrame in standard format.
        window_days: Minimum calendar days between retained events per ticker.

    Returns:
        Filtered events DataFrame, same columns, sorted by (ticker, datetime).
    """
    if events.empty:
        return events

    events = events.copy()
    events['datetime'] = pd.to_datetime(events['datetime'])
    events = events.sort_values(['ticker', 'datetime']).reset_index(drop=True)

    window = pd.Timedelta(days=window_days)
    keep_mask = pd.Series(True, index=events.index)
    last_kept: dict[str, pd.Timestamp] = {}

    for idx, row in events.iterrows():
        ticker = row['ticker']
        dt = row['datetime']
        if ticker in last_kept and dt - last_kept[ticker] < window:
            keep_mask[idx] = False
        else:
            last_kept[ticker] = dt

    filtered = events[keep_mask].reset_index(drop=True)
    logger.debug(f"deduplicate_events: {len(events)} → {len(filtered)} events (window={window_days}d)")
    return filtered


def build_control_group(
    events: pd.DataFrame,
    close_dict: dict[str, pd.Series],
    n_samples: int = 500,
    seed: int = 42,
) -> pd.DataFrame:
    """Sample random ticker-date pairs that are NOT in the event set.

    Provides a baseline for comparing event forward returns against
    unconditional returns from the same universe.

    Args:
        events: Events DataFrame (used to exclude event dates).
        close_dict: Per-ticker close Series (used to sample valid dates).
        n_samples: Target number of control observations.
        seed: Random seed for reproducibility.

    Returns:
        DataFrame with columns: ticker, datetime, signal_value=0, source='control'.
    """
    if not close_dict:
        return empty_events('control')

    rng = np.random.default_rng(seed)

    # Build set of (ticker, date_str) pairs to exclude
    event_pairs: set[tuple[str, str]] = set()
    if not events.empty:
        for _, row in events.iterrows():
            dt = pd.Timestamp(row['datetime'])
            event_pairs.add((row['ticker'], dt.date().isoformat()))

    # Collect all valid (ticker, date) candidates
    candidates: list[tuple[str, pd.Timestamp]] = []
    for ticker, series in close_dict.items():
        for ts in series.index:
            key = (ticker, pd.Timestamp(ts).date().isoformat())
            if key not in event_pairs:
                candidates.append((ticker, pd.Timestamp(ts)))

    if not candidates:
        return empty_events('control')

    n = min(n_samples, len(candidates))
    chosen_idx = rng.choice(len(candidates), size=n, replace=False)
    chosen = [candidates[i] for i in chosen_idx]

    rows = [
        {'ticker': t, 'datetime': dt, 'signal_value': 0.0, 'source': 'control'}
        for t, dt in chosen
    ]
    return pd.DataFrame(rows)
