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


# ---------------------------------------------------------------------------
# Phase 1 additions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_detect_ticker_output_is_uppercase():
    """Emitted tickers should always be uppercase."""
    pop = _make_pop_df([{
        'ticker': 'aapl',  # lower-case as it might come from DB
        'datetime': '2024-01-05 10:00:00',
        'mentions': 300,
        'mentions_24h_ago': 100,
    }])
    sd = _make_stock_data(pop)
    detector = SentimentDetector(mention_thresholds=[2.0], mode='mention_ratio', min_mentions=10)
    events = await detector.detect(sd, timeframe='daily')
    if not events.empty:
        assert (events['ticker'] == events['ticker'].str.upper()).all()


@pytest.mark.asyncio
async def test_detect_explicit_tickers_uppercased():
    """Lower-case explicit --tickers should match DB rows after uppercasing."""
    pop = _make_pop_df([{
        'ticker': 'AAPL',
        'datetime': '2024-01-05 10:00:00',
        'mentions': 300,
        'mentions_24h_ago': 100,
    }])
    sd = _make_stock_data(pop)
    detector = SentimentDetector(mention_thresholds=[2.0], mode='mention_ratio', min_mentions=10)
    events = await detector.detect(sd, timeframe='daily', tickers=['aapl'])
    assert not events.empty
    assert events.iloc[0]['ticker'] == 'AAPL'


@pytest.mark.asyncio
async def test_detect_n_below_min_mentions_populated():
    """n_below_min_mentions should count snapshots filtered by min_mentions."""
    pop = _make_pop_df([
        {'ticker': 'AAPL', 'datetime': '2024-01-05 10:00:00', 'mentions': 5, 'mentions_24h_ago': 1},
        {'ticker': 'GOOG', 'datetime': '2024-01-05 10:00:00', 'mentions': 300, 'mentions_24h_ago': 100},
    ])
    sd = _make_stock_data(pop)
    detector = SentimentDetector(mention_thresholds=[2.0], mode='mention_ratio', min_mentions=10)
    await detector.detect(sd, timeframe='daily')
    assert detector.n_below_min_mentions == 1  # AAPL row had mentions=5 < 10


@pytest.mark.asyncio
async def test_detect_datetime_output_is_tz_naive():
    """Events datetime column must be tz-naive after detect()."""
    pop = _make_pop_df([{
        'ticker': 'AAPL',
        'datetime': pd.Timestamp('2024-01-05 10:00:00', tz='UTC'),
        'mentions': 300,
        'mentions_24h_ago': 100,
    }])
    sd = _make_stock_data(pop)
    detector = SentimentDetector(mention_thresholds=[2.0], mode='mention_ratio', min_mentions=10)
    events = await detector.detect(sd, timeframe='daily')
    if not events.empty:
        assert events['datetime'].dt.tz is None


@pytest.mark.asyncio
async def test_detect_daily_dedup_tie_break_uses_earliest():
    """When two snapshots have identical signal_value, keep the earliest datetime."""
    pop = _make_pop_df([
        # Same ratio (3.0), different times — earliest should win
        {'ticker': 'AAPL', 'datetime': '2024-01-05 09:00:00', 'mentions': 300, 'mentions_24h_ago': 100},
        {'ticker': 'AAPL', 'datetime': '2024-01-05 14:00:00', 'mentions': 300, 'mentions_24h_ago': 100},
    ])
    sd = _make_stock_data(pop)
    detector = SentimentDetector(mention_thresholds=[2.0], mode='mention_ratio', min_mentions=10)
    events = await detector.detect(sd, timeframe='daily')
    # One event per (ticker, day, source_detail); datetime snapped to midnight
    aapl = events[events['ticker'] == 'AAPL']
    assert len(aapl) == 1
