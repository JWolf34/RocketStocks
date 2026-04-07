"""Composite event detector — combines multiple detectors with AND/OR logic.

AND: emits an event only when two detectors both fire within a configurable
     time window.  Useful for studying co-occurring signals (e.g. sentiment
     spike AND volume spike on the same ticker and day).

OR:  emits the union of events from all detectors.  Useful for studying
     the combined signal universe.
"""
import logging

import pandas as pd

logger = logging.getLogger(__name__)


class CompositeDetector:
    """Combine N detectors using AND or OR logic.

    Args:
        detectors: List of EventDetector instances to combine.
        mode: 'and' — both must fire; 'or' — either fires.
        window_days: For 'and' mode, the maximum calendar days between
            co-occurring events on the same ticker.
    """

    def __init__(
        self,
        detectors: list,
        mode: str = 'and',
        window_days: int = 1,
    ):
        if not detectors:
            raise ValueError("CompositeDetector requires at least one detector")
        if mode not in ('and', 'or'):
            raise ValueError("mode must be 'and' or 'or'")
        self.detectors = detectors
        self.mode = mode
        self.window_days = window_days

    async def detect(
        self,
        stock_data,
        timeframe: str = 'daily',
        start_date=None,
        end_date=None,
        tickers: list[str] | None = None,
    ) -> pd.DataFrame:
        """Return combined events in standard format.

        The 'source' column is set to 'composite'.
        The 'source_detail' column joins the source_detail values from the
        contributing events (e.g. 'mention_ratio>=3.0 AND volume_zscore>=2.0').
        """
        all_events: list[pd.DataFrame] = []
        for detector in self.detectors:
            events = await detector.detect(
                stock_data, timeframe, start_date, end_date, tickers
            )
            if not events.empty:
                events = events.copy()
                events['datetime'] = pd.to_datetime(events['datetime'])
                all_events.append(events)

        if not all_events:
            return _empty_events()

        if self.mode == 'or':
            combined = pd.concat(all_events, ignore_index=True)
            combined['source'] = 'composite'
            return combined.sort_values(['ticker', 'datetime']).reset_index(drop=True)

        # AND mode: find ticker-days where all detectors fired within the window
        return self._intersect(all_events)

    def _intersect(self, event_sets: list[pd.DataFrame]) -> pd.DataFrame:
        """Return events where all detectors fired within window_days."""
        window = pd.Timedelta(days=self.window_days)
        rows: list[dict] = []

        # Use first detector's events as the anchor
        anchor = event_sets[0].copy()
        others = event_sets[1:]

        for _, anchor_row in anchor.iterrows():
            ticker = anchor_row['ticker']
            dt = pd.Timestamp(anchor_row['datetime'])
            lo = dt - window
            hi = dt + window

            match = True
            matched_details = [str(anchor_row.get('source_detail', anchor_row.get('source', '')))]

            for other in others:
                other_ticker = other[other['ticker'] == ticker]
                other_window = other_ticker[
                    (other_ticker['datetime'] >= lo) & (other_ticker['datetime'] <= hi)
                ]
                if other_window.empty:
                    match = False
                    break
                # Take the closest match
                best = other_window.iloc[
                    (other_window['datetime'] - dt).abs().argsort()[:1]
                ]
                matched_details.append(
                    str(best.iloc[0].get('source_detail', best.iloc[0].get('source', '')))
                )

            if match:
                combined_detail = ' AND '.join(matched_details)
                rows.append({
                    'ticker': ticker,
                    'datetime': dt,
                    'signal_value': float(anchor_row.get('signal_value', float('nan'))),
                    'source': 'composite',
                    'source_detail': combined_detail,
                })

        if not rows:
            logger.info("CompositeDetector (AND): no co-occurring events found")
            return _empty_events()

        result = pd.DataFrame(rows)
        logger.info(
            f"CompositeDetector (AND): {len(result)} co-occurring events "
            f"({result['ticker'].nunique()} tickers)"
        )
        return result.sort_values(['ticker', 'datetime']).reset_index(drop=True)


def _empty_events() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        'ticker', 'datetime', 'signal_value', 'source', 'source_detail',
    ])
