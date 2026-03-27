"""Tests for rocketstocks.data.paper_trading_store.PaperTradingRepository."""
import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from rocketstocks.data.paper_trading_store import PaperTradingRepository


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db():
    db = MagicMock(name='Postgres')
    db.execute = AsyncMock(return_value=None)
    return db


def _make_transaction_ctx(mock_conn):
    """Return an async context manager that yields mock_conn."""
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


@pytest.fixture
def mock_db():
    return _make_db()


@pytest.fixture
def repo(mock_db):
    return PaperTradingRepository(db=mock_db)


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

def test_repo_stores_db_reference():
    db = MagicMock()
    repo = PaperTradingRepository(db=db)
    assert repo._db is db


def test_repo_none_db():
    repo = PaperTradingRepository()
    assert repo._db is None


# ---------------------------------------------------------------------------
# get_portfolio
# ---------------------------------------------------------------------------

async def test_get_portfolio_returns_dict_when_found(repo, mock_db):
    ts = datetime.datetime(2026, 3, 1)
    mock_db.execute.return_value = (1001, 2001, 9500.0, ts)
    result = await repo.get_portfolio(1001, 2001)
    assert result['guild_id'] == 1001
    assert result['user_id'] == 2001
    assert result['cash'] == 9500.0
    assert result['created_at'] == ts


async def test_get_portfolio_returns_none_when_not_found(repo, mock_db):
    mock_db.execute.return_value = None
    result = await repo.get_portfolio(1001, 2001)
    assert result is None


async def test_get_portfolio_queries_correct_keys(repo, mock_db):
    mock_db.execute.return_value = None
    await repo.get_portfolio(1001, 2001)
    sql, params = mock_db.execute.call_args[0]
    assert 'paper_portfolios' in sql
    assert params == [1001, 2001]


# ---------------------------------------------------------------------------
# create_portfolio
# ---------------------------------------------------------------------------

async def test_create_portfolio_inserts_with_starting_cash(repo, mock_db):
    await repo.create_portfolio(1001, 2001)
    sql, params = mock_db.execute.call_args[0]
    assert 'INSERT INTO paper_portfolios' in sql
    assert 'ON CONFLICT' in sql
    assert params == [1001, 2001, 10000.0]


async def test_create_portfolio_custom_starting_cash(repo, mock_db):
    await repo.create_portfolio(1001, 2001, starting_cash=5000.0)
    _, params = mock_db.execute.call_args[0]
    assert params[2] == 5000.0


# ---------------------------------------------------------------------------
# reset_portfolio
# ---------------------------------------------------------------------------

async def test_reset_portfolio_runs_transaction(repo, mock_db):
    mock_conn = MagicMock()
    mock_conn.execute = AsyncMock()
    mock_db.transaction.return_value = _make_transaction_ctx(mock_conn)
    await repo.reset_portfolio(1001, 2001)
    # DELETE positions, transactions, snapshots, pending_orders + UPDATE cash = 5 calls
    assert mock_conn.execute.call_count == 5


async def test_reset_portfolio_deletes_positions(repo, mock_db):
    mock_conn = MagicMock()
    mock_conn.execute = AsyncMock()
    mock_db.transaction.return_value = _make_transaction_ctx(mock_conn)
    await repo.reset_portfolio(1001, 2001)
    calls = [c[0][0] for c in mock_conn.execute.call_args_list]
    assert any('paper_positions' in sql for sql in calls)


async def test_reset_portfolio_restores_cash(repo, mock_db):
    mock_conn = MagicMock()
    mock_conn.execute = AsyncMock()
    mock_db.transaction.return_value = _make_transaction_ctx(mock_conn)
    await repo.reset_portfolio(1001, 2001, starting_cash=7500.0)
    calls = [(c[0][0], c[0][1]) for c in mock_conn.execute.call_args_list]
    update_call = next((sql, p) for sql, p in calls if 'UPDATE paper_portfolios' in sql)
    assert 7500.0 in update_call[1]


# ---------------------------------------------------------------------------
# get_all_portfolios
# ---------------------------------------------------------------------------

async def test_get_all_portfolios_returns_list(repo, mock_db):
    ts = datetime.datetime(2026, 3, 1)
    mock_db.execute.return_value = [
        (1001, 2001, 9500.0, ts),
        (1001, 2002, 10000.0, ts),
    ]
    results = await repo.get_all_portfolios(1001)
    assert len(results) == 2
    assert results[0]['user_id'] == 2001
    assert results[1]['user_id'] == 2002


async def test_get_all_portfolios_empty(repo, mock_db):
    mock_db.execute.return_value = []
    assert await repo.get_all_portfolios(1001) == []


async def test_get_all_portfolios_none_result(repo, mock_db):
    mock_db.execute.return_value = None
    assert await repo.get_all_portfolios(1001) == []


# ---------------------------------------------------------------------------
# get_distinct_guild_ids
# ---------------------------------------------------------------------------

async def test_get_distinct_guild_ids_returns_list(repo, mock_db):
    mock_db.execute.return_value = [(1001,), (1002,)]
    result = await repo.get_distinct_guild_ids()
    assert result == [1001, 1002]


async def test_get_distinct_guild_ids_empty(repo, mock_db):
    mock_db.execute.return_value = None
    assert await repo.get_distinct_guild_ids() == []


# ---------------------------------------------------------------------------
# get_positions / get_position
# ---------------------------------------------------------------------------

async def test_get_positions_returns_list_of_dicts(repo, mock_db):
    mock_db.execute.return_value = [
        (1001, 2001, 'AAPL', 10, 150.0),
        (1001, 2001, 'TSLA', 5, 200.0),
    ]
    result = await repo.get_positions(1001, 2001)
    assert len(result) == 2
    assert result[0]['ticker'] == 'AAPL'
    assert result[0]['shares'] == 10
    assert result[1]['ticker'] == 'TSLA'


async def test_get_positions_empty(repo, mock_db):
    mock_db.execute.return_value = []
    assert await repo.get_positions(1001, 2001) == []


async def test_get_position_returns_dict(repo, mock_db):
    mock_db.execute.return_value = (1001, 2001, 'AAPL', 10, 150.0)
    result = await repo.get_position(1001, 2001, 'AAPL')
    assert result['ticker'] == 'AAPL'
    assert result['shares'] == 10
    assert result['avg_cost_basis'] == 150.0


async def test_get_position_returns_none(repo, mock_db):
    mock_db.execute.return_value = None
    assert await repo.get_position(1001, 2001, 'AAPL') is None


# ---------------------------------------------------------------------------
# execute_buy
# ---------------------------------------------------------------------------

async def test_execute_buy_new_position(repo, mock_db):
    """execute_buy with no existing position creates it at buy price."""
    mock_conn = MagicMock()
    # First conn.execute (SELECT shares) returns None (no position)
    mock_conn.execute = AsyncMock(return_value=None)
    mock_db.transaction.return_value = _make_transaction_ctx(mock_conn)

    await repo.execute_buy(1001, 2001, 'AAPL', 10, 150.0)
    assert mock_conn.execute.call_count == 4  # SELECT, UPDATE portfolio, INSERT position, INSERT tx


async def test_execute_buy_existing_position_weighted_avg(repo, mock_db):
    """execute_buy with existing position computes weighted average."""
    mock_conn = MagicMock()

    # SELECT returns (existing_shares=10, existing_avg=100.0)
    select_result = MagicMock()
    select_result.__iter__ = MagicMock(return_value=iter([10, 100.0]))

    call_count = 0
    async def side_effect(sql, params=None, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # First call is the SELECT — return the row directly
            return (10, 100.0)
        return None

    mock_conn.execute = AsyncMock(side_effect=side_effect)
    mock_db.transaction.return_value = _make_transaction_ctx(mock_conn)

    await repo.execute_buy(1001, 2001, 'AAPL', 10, 150.0)

    # New avg = (10*100 + 10*150) / 20 = 125.0
    upsert_call = mock_conn.execute.call_args_list[2]
    params = upsert_call[0][1]
    # params: [guild_id, user_id, ticker, new_shares, new_avg]
    assert params[3] == 20         # total shares
    assert params[4] == pytest.approx(125.0)


# ---------------------------------------------------------------------------
# execute_sell
# ---------------------------------------------------------------------------

async def test_execute_sell_partial_reduces_shares(repo, mock_db):
    """execute_sell with remaining shares updates position, not deletes."""
    mock_conn = MagicMock()

    call_count = 0
    async def side_effect(sql, params=None, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return (20,)  # existing shares
        return None

    mock_conn.execute = AsyncMock(side_effect=side_effect)
    mock_db.transaction.return_value = _make_transaction_ctx(mock_conn)

    await repo.execute_sell(1001, 2001, 'AAPL', 5, 200.0)

    calls = [(c[0][0], c[0][1]) for c in mock_conn.execute.call_args_list]
    update_pos_call = next(
        (sql, p) for sql, p in calls if 'UPDATE paper_positions' in sql
    )
    assert 15 in update_pos_call[1]  # remaining = 20 - 5


async def test_execute_sell_all_shares_deletes_position(repo, mock_db):
    """execute_sell with 0 remaining shares deletes position."""
    mock_conn = MagicMock()

    call_count = 0
    async def side_effect(sql, params=None, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return (5,)  # existing shares
        return None

    mock_conn.execute = AsyncMock(side_effect=side_effect)
    mock_db.transaction.return_value = _make_transaction_ctx(mock_conn)

    await repo.execute_sell(1001, 2001, 'AAPL', 5, 200.0)

    calls = [c[0][0] for c in mock_conn.execute.call_args_list]
    assert any('DELETE FROM paper_positions' in sql for sql in calls)


# ---------------------------------------------------------------------------
# queue_buy_order / queue_sell_order
# ---------------------------------------------------------------------------

async def test_queue_buy_order_deducts_cash_and_inserts(repo, mock_db):
    mock_conn = MagicMock()
    mock_conn.execute = AsyncMock()
    mock_db.transaction.return_value = _make_transaction_ctx(mock_conn)

    await repo.queue_buy_order(1001, 2001, 'AAPL', 10, 150.0)
    assert mock_conn.execute.call_count == 2
    calls = [(c[0][0], c[0][1]) for c in mock_conn.execute.call_args_list]
    update_call = next(sql for sql, _ in calls if 'UPDATE paper_portfolios' in sql)
    assert 'cash - %s' in update_call


async def test_queue_sell_order_reduces_shares_and_inserts(repo, mock_db):
    mock_conn = MagicMock()

    call_count = 0
    async def side_effect(sql, params=None, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return (20,)  # existing shares
        return None

    mock_conn.execute = AsyncMock(side_effect=side_effect)
    mock_db.transaction.return_value = _make_transaction_ctx(mock_conn)

    await repo.queue_sell_order(1001, 2001, 'AAPL', 5, 150.0)
    assert mock_conn.execute.call_count == 3  # SELECT, UPDATE positions, INSERT order


# ---------------------------------------------------------------------------
# cancel_buy_order / cancel_sell_order
# ---------------------------------------------------------------------------

async def test_cancel_buy_order_returns_false_when_not_found(repo, mock_db):
    mock_db.execute.return_value = None
    result = await repo.cancel_buy_order(99, 1001, 2001)
    assert result is False


async def test_cancel_buy_order_refunds_cash(repo, mock_db):
    mock_db.execute.return_value = (10, 150.0)  # shares, quoted_price
    mock_conn = MagicMock()
    mock_conn.execute = AsyncMock()
    mock_db.transaction.return_value = _make_transaction_ctx(mock_conn)

    result = await repo.cancel_buy_order(1, 1001, 2001)
    assert result is True
    calls = [(c[0][0], c[0][1]) for c in mock_conn.execute.call_args_list]
    update_call = next((sql, p) for sql, p in calls if 'UPDATE paper_portfolios' in sql)
    assert 1500.0 in update_call[1]  # 10 * 150.0


async def test_cancel_sell_order_returns_false_when_not_found(repo, mock_db):
    mock_db.execute.return_value = None
    result = await repo.cancel_sell_order(99, 1001, 2001)
    assert result is False


async def test_cancel_sell_order_restores_shares(repo, mock_db):
    mock_db.execute.return_value = ('AAPL', 10)  # ticker, shares
    mock_conn = MagicMock()
    mock_conn.execute = AsyncMock()
    mock_db.transaction.return_value = _make_transaction_ctx(mock_conn)

    result = await repo.cancel_sell_order(1, 1001, 2001)
    assert result is True
    calls = [c[0][0] for c in mock_conn.execute.call_args_list]
    assert any('INSERT INTO paper_positions' in sql for sql in calls)


# ---------------------------------------------------------------------------
# get_pending_orders / get_all_pending_orders
# ---------------------------------------------------------------------------

async def test_get_pending_orders_returns_dicts(repo, mock_db):
    ts = datetime.datetime(2026, 3, 1, 10)
    mock_db.execute.return_value = [
        (1, 1001, 2001, 'AAPL', 'BUY', 10, 150.0, 'pending', ts, None, None),
    ]
    result = await repo.get_pending_orders(1001, 2001)
    assert len(result) == 1
    assert result[0]['ticker'] == 'AAPL'
    assert result[0]['side'] == 'BUY'
    assert result[0]['status'] == 'pending'


async def test_get_pending_orders_empty(repo, mock_db):
    mock_db.execute.return_value = []
    assert await repo.get_pending_orders(1001, 2001) == []


async def test_get_all_pending_orders_no_guild_filter(repo, mock_db):
    mock_db.execute.return_value = []
    await repo.get_all_pending_orders()
    sql = mock_db.execute.call_args[0][0]
    assert "guild_id" not in sql or "status = 'pending'" in sql


# ---------------------------------------------------------------------------
# mark_order_executed
# ---------------------------------------------------------------------------

async def test_mark_order_executed_updates_status(repo, mock_db):
    await repo.mark_order_executed(42, 175.0)
    sql, params = mock_db.execute.call_args[0]
    assert 'status = \'executed\'' in sql
    assert 175.0 in params
    assert 42 in params


# ---------------------------------------------------------------------------
# get_transactions
# ---------------------------------------------------------------------------

async def test_get_transactions_returns_dicts(repo, mock_db):
    ts = datetime.datetime(2026, 3, 1, 10)
    mock_db.execute.return_value = [
        (1, 1001, 2001, 'AAPL', 'BUY', 10, 150.0, 1500.0, ts),
    ]
    result = await repo.get_transactions(1001, 2001)
    assert len(result) == 1
    assert result[0]['ticker'] == 'AAPL'
    assert result[0]['side'] == 'BUY'
    assert result[0]['total'] == 1500.0


async def test_get_transactions_default_limit_20(repo, mock_db):
    mock_db.execute.return_value = []
    await repo.get_transactions(1001, 2001)
    sql, params = mock_db.execute.call_args[0]
    assert 20 in params


async def test_get_guild_transactions_filters_by_since(repo, mock_db):
    since = datetime.datetime(2026, 3, 1)
    mock_db.execute.return_value = []
    await repo.get_guild_transactions(1001, since)
    sql, params = mock_db.execute.call_args[0]
    assert since in params
    assert 1001 in params


# ---------------------------------------------------------------------------
# insert_snapshot / get_snapshots
# ---------------------------------------------------------------------------

async def test_insert_snapshot_upserts(repo, mock_db):
    date = datetime.date(2026, 3, 1)
    await repo.insert_snapshot(1001, 2001, date, 10500.0, 1000.0, 9500.0)
    sql, params = mock_db.execute.call_args[0]
    assert 'INSERT INTO paper_snapshots' in sql
    assert 'ON CONFLICT' in sql
    assert params == [1001, 2001, date, 10500.0, 1000.0, 9500.0]


async def test_get_snapshots_returns_dicts(repo, mock_db):
    d = datetime.date(2026, 3, 1)
    mock_db.execute.return_value = [
        (1001, 2001, d, 10500.0, 1000.0, 9500.0),
    ]
    result = await repo.get_snapshots(1001, 2001, d, d)
    assert len(result) == 1
    assert result[0]['portfolio_value'] == 10500.0
    assert result[0]['cash'] == 1000.0


async def test_get_all_snapshots_for_date(repo, mock_db):
    d = datetime.date(2026, 3, 1)
    mock_db.execute.return_value = []
    await repo.get_all_snapshots_for_date(1001, d)
    sql, params = mock_db.execute.call_args[0]
    assert 'paper_snapshots' in sql
    assert d in params


async def test_get_all_snapshots_in_range(repo, mock_db):
    start = datetime.date(2026, 3, 1)
    end = datetime.date(2026, 3, 7)
    mock_db.execute.return_value = []
    await repo.get_all_snapshots_in_range(1001, start, end)
    sql, params = mock_db.execute.call_args[0]
    assert start in params
    assert end in params
