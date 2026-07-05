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
    bar_counts: dict[str, int],
    n_samples: int = 500,
    seed: int = 42,
) -> pd.DataFrame:
    """Sample random (ticker, bar-offset) pairs for use as a control group.

    Memory-efficient replacement for the previous close_dict-based approach.
    Instead of materialising every (ticker, Timestamp) candidate (~millions of
    Python objects), this samples global bar indices and maps them to
    (ticker, local_offset) pairs via a cumulative-sum + searchsorted operation.

    Callers must resolve ``_bar_offset`` → ``datetime`` using the per-ticker
    price data before passing results to analysis engines.  Event-date exclusion
    is also performed at that resolution step (collisions are rare and handled
    there).

    Args:
        events: Events DataFrame (reserved for future use; not currently applied
            at sampling time — exclusion happens during offset resolution).
        bar_counts: {ticker: n_bars} mapping — from ``fetch_bar_counts()`` or
            ``{t: len(s) for t, s in close_dict.items()}``.
        n_samples: Target number of control observations.
        seed: Random seed for reproducibility.

    Returns:
        DataFrame with columns: ticker, _bar_offset, signal_value=0, source='control'.
        ``_bar_offset`` is a zero-based index into the ticker's chronologically
        sorted price series.
    """
    tickers = [t for t, c in bar_counts.items() if c > 0]
    if not tickers:
        return _empty_control()

    rng = np.random.default_rng(seed)

    counts = np.array([bar_counts[t] for t in tickers], dtype=np.int64)
    T = int(counts.sum())
    if T == 0:
        return _empty_control()

    n = min(n_samples, T)
    global_indices = rng.choice(T, size=n, replace=False)

    # Map each global index → (ticker_index, local_offset) via searchsorted on cumsum
    cumsum = np.cumsum(counts)
    ticker_indices = np.searchsorted(cumsum, global_indices, side='right')

    rows = []
    for gi, ti in zip(global_indices, ticker_indices):
        local_offset = int(gi - (cumsum[ti - 1] if ti > 0 else 0))
        rows.append({
            'ticker': tickers[int(ti)],
            '_bar_offset': local_offset,
            'signal_value': 0.0,
            'source': 'control',
        })

    return pd.DataFrame(rows)


def _empty_control() -> pd.DataFrame:
    """Return a zero-row control DataFrame with the expected columns."""
    return pd.DataFrame(columns=['ticker', '_bar_offset', 'signal_value', 'source'])
