"""Tests for rocketstocks.eda.events.sentiment."""
import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from rocketstocks.eda.events.sentiment import SentimentDetector


def _make_pop_df(rows: list[dict]) -> pd.DataFrame:
    """Create a popularity DataFrame with required columns."""
    defaults = {
        'rank': 5,
        'name': 'Test Corp',
        'upvotes': 10,
        'rank_24h_ago': 5,
        'mentions_24h_ago': 100,
    }
    full_rows = [{**defaults, **row} for row in rows]
    return pd.DataFrame(full_rows)


def _make_stock_data(pop_df: pd.DataFrame) -> MagicMock:
    sd = MagicMock()
    sd.popularity.fetch_popularity = AsyncMock(return_value=pop_df)
    return sd


# ---------------------------------------------------------------------------
# Basic detection
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_detect_mention_spike_emits_event():
    """A 3x mention spike should produce an event."""
    pop = _make_pop_df([{
        'ticker': 'AAPL',
        'datetime': '2024-01-05 10:00:00',
        'mentions': 300,
        'mentions_24h_ago': 100,
    }])
    sd = _make_stock_data(pop)

    detector = SentimentDetector(mention_thresholds=[3.0], mode='mention_ratio', min_mentions=10)
    events = await detector.detect(sd, timeframe='daily')

    assert len(events) >= 1
    assert events.iloc[0]['ticker'] == 'AAPL'
    assert events.iloc[0]['source'] == 'sentiment'


@pytest.mark.asyncio
async def test_detect_below_threshold_no_event():
    """A 1.5x mention ratio should NOT produce an event when threshold is 2.0."""
    pop = _make_pop_df([{
        'ticker': 'AAPL',
        'datetime': '2024-01-05 10:00:00',
        'mentions': 150,
        'mentions_24h_ago': 100,
    }])
    sd = _make_stock_data(pop)

    detector = SentimentDetector(mention_thresholds=[2.0], mode='mention_ratio', min_mentions=10)
    events = await detector.detect(sd, timeframe='daily')

    assert events.empty


@pytest.mark.asyncio
async def test_detect_rank_jump_event():
    """A rank improvement of 60 positions should trigger rank_change event (threshold=50)."""
    pop = _make_pop_df([{
        'ticker': 'TSLA',
        'datetime': '2024-01-05 10:00:00',
        'mentions': 200,
        'rank': 40,
        'rank_24h_ago': 100,
        'mentions_24h_ago': 100,
    }])
    sd = _make_stock_data(pop)

    detector = SentimentDetector(rank_change_thresholds=[50], mode='rank_change', min_mentions=10)
    events = await detector.detect(sd, timeframe='daily')

    assert not events.empty
    assert events.iloc[0]['ticker'] == 'TSLA'


@pytest.mark.asyncio
async def test_detect_min_mentions_filter():
    """Rows with mentions below min_mentions should be excluded."""
    pop = _make_pop_df([{
        'ticker': 'AAPL',
        'datetime': '2024-01-05 10:00:00',
        'mentions': 5,
        'mentions_24h_ago': 1,
    }])
    sd = _make_stock_data(pop)

    detector = SentimentDetector(mention_thresholds=[2.0], mode='mention_ratio', min_mentions=10)
    events = await detector.detect(sd, timeframe='daily')

    assert events.empty


@pytest.mark.asyncio
async def test_detect_ticker_filter():
    """Events for tickers not in the filter should be excluded."""
    pop = _make_pop_df([
        {'ticker': 'AAPL', 'datetime': '2024-01-05 10:00:00', 'mentions': 300, 'mentions_24h_ago': 100},
        {'ticker': 'GOOG', 'datetime': '2024-01-05 10:00:00', 'mentions': 300, 'mentions_24h_ago': 100},
    ])
    sd = _make_stock_data(pop)

    detector = SentimentDetector(mention_thresholds=[2.0], mode='mention_ratio', min_mentions=10)
    events = await detector.detect(sd, timeframe='daily', tickers=['AAPL'])

    assert all(events['ticker'] == 'AAPL')


@pytest.mark.asyncio
async def test_detect_daily_dedup_to_one_per_day():
    """Multiple intraday snapshots on same day should collapse to one event per ticker per day."""
    pop = _make_pop_df([
        {'ticker': 'AAPL', 'datetime': '2024-01-05 09:00:00', 'mentions': 300, 'mentions_24h_ago': 100},
        {'ticker': 'AAPL', 'datetime': '2024-01-05 10:00:00', 'mentions': 500, 'mentions_24h_ago': 100},
        {'ticker': 'AAPL', 'datetime': '2024-01-05 14:00:00', 'mentions': 350, 'mentions_24h_ago': 100},
    ])
    sd = _make_stock_data(pop)

    detector = SentimentDetector(mention_thresholds=[2.0], mode='mention_ratio', min_mentions=10)
    events = await detector.detect(sd, timeframe='daily')

    aapl_daily = events[events['ticker'] == 'AAPL']
    assert len(aapl_daily) == 1  # One per day per source_detail


@pytest.mark.asyncio
async def test_detect_empty_popularity_returns_empty():
    sd = _make_stock_data(pd.DataFrame())
    detector = SentimentDetector()
    events = await detector.detect(sd)
    assert events.empty


@pytest.mark.asyncio
async def test_detect_standard_columns_present():
    """Events DataFrame must always have the four standard columns."""
    pop = _make_pop_df([{
        'ticker': 'AAPL',
        'datetime': '2024-01-05 10:00:00',
        'mentions': 300,
        'mentions_24h_ago': 100,
    }])
    sd = _make_stock_data(pop)

    detector = SentimentDetector(mention_thresholds=[2.0], mode='mention_ratio')
    events = await detector.detect(sd, timeframe='daily')

    for col in ('ticker', 'datetime', 'signal_value', 'source'):
        assert col in events.columns
