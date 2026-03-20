"""Tests for EarningsResultsRepository."""
import datetime
import pytest
from unittest.mock import AsyncMock, MagicMock


def _make_repo():
    from rocketstocks.data.earnings_results_store import EarningsResultsRepository
    db = MagicMock()
    db.execute = AsyncMock()
    return EarningsResultsRepository(db=db), db


class TestInsertResult:
    @pytest.mark.asyncio
    async def test_insert_calls_db_with_correct_args(self):
        repo, db = _make_repo()
        today = datetime.date(2026, 3, 20)
        await repo.insert_result(
            date=today, ticker='AAPL', eps_actual=1.52, eps_estimate=1.45, surprise_pct=4.83
        )
        db.execute.assert_awaited_once()
        call_args = db.execute.call_args[0]
        params = call_args[1]
        assert params[0] == today
        assert params[1] == 'AAPL'
        assert params[2] == pytest.approx(1.52)
        assert params[3] == pytest.approx(1.45)
        assert params[4] == pytest.approx(4.83)
        assert params[5] == 'yfinance'

    @pytest.mark.asyncio
    async def test_insert_uses_on_conflict_do_nothing(self):
        repo, db = _make_repo()
        await repo.insert_result(
            date=datetime.date.today(), ticker='MSFT', eps_actual=3.0, eps_estimate=2.9, surprise_pct=3.4
        )
        sql = db.execute.call_args[0][0]
        assert 'ON CONFLICT' in sql
        assert 'DO NOTHING' in sql

    @pytest.mark.asyncio
    async def test_insert_accepts_none_fields(self):
        repo, db = _make_repo()
        await repo.insert_result(
            date=datetime.date.today(), ticker='XYZ', eps_actual=None, eps_estimate=None, surprise_pct=None
        )
        db.execute.assert_awaited_once()


class TestGetPostedTickersToday:
    @pytest.mark.asyncio
    async def test_returns_set_of_tickers(self):
        repo, db = _make_repo()
        db.execute = AsyncMock(return_value=[('AAPL',), ('MSFT',)])
        result = await repo.get_posted_tickers_today(datetime.date.today())
        assert result == {'AAPL', 'MSFT'}

    @pytest.mark.asyncio
    async def test_returns_empty_set_when_no_rows(self):
        repo, db = _make_repo()
        db.execute = AsyncMock(return_value=None)
        result = await repo.get_posted_tickers_today(datetime.date.today())
        assert result == set()

    @pytest.mark.asyncio
    async def test_passes_date_to_query(self):
        repo, db = _make_repo()
        db.execute = AsyncMock(return_value=[])
        today = datetime.date(2026, 3, 20)
        await repo.get_posted_tickers_today(today)
        params = db.execute.call_args[0][1]
        assert params == [today]


class TestGetResult:
    @pytest.mark.asyncio
    async def test_returns_dict_when_found(self):
        repo, db = _make_repo()
        db.execute = AsyncMock(return_value=(1.52, 1.45, 4.83))
        result = await repo.get_result(date=datetime.date.today(), ticker='AAPL')
        assert result == {'eps_actual': 1.52, 'eps_estimate': 1.45, 'surprise_pct': 4.83}

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self):
        repo, db = _make_repo()
        db.execute = AsyncMock(return_value=None)
        result = await repo.get_result(date=datetime.date.today(), ticker='AAPL')
        assert result is None

    @pytest.mark.asyncio
    async def test_passes_date_and_ticker_to_query(self):
        repo, db = _make_repo()
        db.execute = AsyncMock(return_value=None)
        today = datetime.date(2026, 3, 20)
        await repo.get_result(date=today, ticker='NVDA')
        params = db.execute.call_args[0][1]
        assert params[0] == today
        assert params[1] == 'NVDA'
