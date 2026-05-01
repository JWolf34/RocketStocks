"""Tests that date bounds are inclusive (>=/<= ) for all price history fetchers.

This pins the contract fixed in Phase 1: fetch_daily_price_history,
fetch_daily_price_history_batch, and fetch_5m_price_history must all use
inclusive start AND inclusive end so events on the boundary date are not
silently excluded.
"""
import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from rocketstocks.data.price_history import PriceHistoryRepository


def _make_repo(return_value=None):
    db = MagicMock()
    db.execute = AsyncMock(return_value=return_value)
    schwab = MagicMock()
    return PriceHistoryRepository(db, schwab), db


# ---------------------------------------------------------------------------
# fetch_daily_price_history — single-ticker path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_daily_start_is_inclusive():
    repo, db = _make_repo(return_value=None)
    start = datetime.date(2024, 1, 1)
    await repo.fetch_daily_price_history('AAPL', start_date=start)
    sql = db.execute.call_args[0][0]
    assert 'date >= %s' in sql, "start_date must use >= (inclusive)"
    assert 'date > %s' not in sql


@pytest.mark.asyncio
async def test_fetch_daily_end_is_inclusive():
    repo, db = _make_repo(return_value=None)
    end = datetime.date(2024, 6, 1)
    await repo.fetch_daily_price_history('AAPL', end_date=end)
    sql = db.execute.call_args[0][0]
    assert 'date <= %s' in sql, "end_date must use <= (inclusive)"
    assert 'date < %s' not in sql


@pytest.mark.asyncio
async def test_fetch_daily_both_bounds_inclusive():
    repo, db = _make_repo(return_value=None)
    start = datetime.date(2024, 1, 1)
    end = datetime.date(2024, 6, 1)
    await repo.fetch_daily_price_history('AAPL', start_date=start, end_date=end)
    sql = db.execute.call_args[0][0]
    assert 'date >= %s' in sql
    assert 'date <= %s' in sql


# ---------------------------------------------------------------------------
# fetch_daily_price_history_batch — multi-ticker path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_daily_batch_start_is_inclusive():
    repo, db = _make_repo(return_value=None)
    start = datetime.date(2024, 1, 1)
    await repo.fetch_daily_price_history_batch(['AAPL'], start_date=start)
    sql = db.execute.call_args[0][0]
    assert 'date >= %s' in sql, "batch start_date must use >= (inclusive)"
    assert 'date > %s' not in sql


@pytest.mark.asyncio
async def test_fetch_daily_batch_end_is_inclusive():
    repo, db = _make_repo(return_value=None)
    end = datetime.date(2024, 6, 1)
    await repo.fetch_daily_price_history_batch(['AAPL'], end_date=end)
    sql = db.execute.call_args[0][0]
    assert 'date <= %s' in sql, "batch end_date must use <= (inclusive)"


# ---------------------------------------------------------------------------
# fetch_5m_price_history — already correct; confirm it doesn't regress
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_5m_start_is_inclusive():
    repo, db = _make_repo(return_value=None)
    start_dt = datetime.datetime(2024, 1, 1, 9, 30)
    await repo.fetch_5m_price_history('AAPL', start_datetime=start_dt)
    sql = db.execute.call_args[0][0]
    assert 'datetime >= %s' in sql


@pytest.mark.asyncio
async def test_fetch_5m_end_is_inclusive():
    repo, db = _make_repo(return_value=None)
    end_dt = datetime.datetime(2024, 6, 1, 16, 0)
    await repo.fetch_5m_price_history('AAPL', end_datetime=end_dt)
    sql = db.execute.call_args[0][0]
    assert 'datetime <= %s' in sql
