"""Tests for rocketstocks.data.iv_history_store.IVHistoryRepository."""
import datetime
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from rocketstocks.data.iv_history_store import IVHistoryRepository


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_db():
    db = MagicMock(name='Postgres')
    db.execute = AsyncMock(return_value=None)
    return db


@pytest.fixture
def repo(mock_db):
    return IVHistoryRepository(db=mock_db)


# ---------------------------------------------------------------------------
# insert_iv
# ---------------------------------------------------------------------------

async def test_insert_iv_executes_upsert(repo, mock_db):
    """insert_iv calls db.execute with correct INSERT … ON CONFLICT SQL."""
    date = datetime.date(2026, 3, 20)
    await repo.insert_iv(ticker='AAPL', date=date, iv=25.5, atm_iv=26.1, put_call_ratio=0.85)
    mock_db.execute.assert_called_once()
    sql, params = mock_db.execute.call_args[0]
    assert 'INSERT INTO iv_history' in sql
    assert 'ON CONFLICT' in sql
    assert params == ['AAPL', date, 25.5, 26.1, 0.85]


async def test_insert_iv_allows_none_values(repo, mock_db):
    """insert_iv accepts None for all optional fields."""
    date = datetime.date(2026, 3, 20)
    await repo.insert_iv(ticker='SPY', date=date, iv=None, atm_iv=None, put_call_ratio=None)
    _, params = mock_db.execute.call_args[0]
    assert params == ['SPY', date, None, None, None]


# ---------------------------------------------------------------------------
# get_iv_history
# ---------------------------------------------------------------------------

async def test_get_iv_history_returns_dataframe(repo, mock_db):
    """get_iv_history returns a DataFrame with the expected columns."""
    mock_db.execute.return_value = [
        (datetime.date(2026, 3, 19), 24.0, 24.5, 0.80),
        (datetime.date(2026, 3, 20), 25.5, 26.1, 0.85),
    ]
    df = await repo.get_iv_history(ticker='AAPL', days=30)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ['date', 'iv', 'atm_iv', 'put_call_ratio']
    assert len(df) == 2


async def test_get_iv_history_sorted_ascending(repo, mock_db):
    """get_iv_history sorts rows by date ascending regardless of DB order."""
    mock_db.execute.return_value = [
        (datetime.date(2026, 3, 20), 25.5, 26.1, 0.85),
        (datetime.date(2026, 3, 19), 24.0, 24.5, 0.80),
    ]
    df = await repo.get_iv_history(ticker='AAPL', days=30)
    assert df['date'].iloc[0] == datetime.date(2026, 3, 19)
    assert df['date'].iloc[1] == datetime.date(2026, 3, 20)


async def test_get_iv_history_empty_returns_empty_df(repo, mock_db):
    """get_iv_history returns an empty DataFrame when no rows exist."""
    mock_db.execute.return_value = None
    df = await repo.get_iv_history(ticker='AAPL', days=365)
    assert isinstance(df, pd.DataFrame)
    assert df.empty
    assert list(df.columns) == ['date', 'iv', 'atm_iv', 'put_call_ratio']


async def test_get_iv_history_passes_days_limit(repo, mock_db):
    """get_iv_history passes the days limit to the SQL query."""
    mock_db.execute.return_value = []
    await repo.get_iv_history(ticker='TSLA', days=90)
    _, params = mock_db.execute.call_args[0]
    assert params == ['TSLA', 90]


# ---------------------------------------------------------------------------
# get_latest_iv
# ---------------------------------------------------------------------------

async def test_get_latest_iv_returns_float(repo, mock_db):
    """get_latest_iv returns the iv value from the most recent row."""
    mock_db.execute.return_value = (28.3,)
    result = await repo.get_latest_iv(ticker='AAPL')
    assert result == pytest.approx(28.3)


async def test_get_latest_iv_returns_none_when_no_rows(repo, mock_db):
    """get_latest_iv returns None when no IV history exists."""
    mock_db.execute.return_value = None
    result = await repo.get_latest_iv(ticker='AAPL')
    assert result is None


async def test_get_latest_iv_queries_with_fetchone(repo, mock_db):
    """get_latest_iv calls db.execute with fetchone=True."""
    mock_db.execute.return_value = (20.0,)
    await repo.get_latest_iv(ticker='TSLA')
    call_kwargs = mock_db.execute.call_args[1]
    assert call_kwargs.get('fetchone') is True


# ---------------------------------------------------------------------------
# collect_daily_snapshots
# ---------------------------------------------------------------------------

def _make_schwab(chain: dict | None = None, rate_limit: bool = False):
    from unittest.mock import AsyncMock
    from rocketstocks.data.clients.schwab import SchwabRateLimitError
    schwab = MagicMock()
    if rate_limit:
        schwab.get_options_chain = AsyncMock(side_effect=SchwabRateLimitError("limit"))
    else:
        schwab.get_options_chain = AsyncMock(return_value=chain or {})
    return schwab


def _make_watchlists(tickers: list[str]):
    wl = MagicMock()
    wl.get_all_watchlist_tickers = AsyncMock(return_value=tickers)
    return wl


def _make_popularity(tickers: list[str]):
    pop = MagicMock()
    df = pd.DataFrame({'ticker': tickers, 'rank': range(1, len(tickers) + 1)})
    pop.get_popular_stocks = MagicMock(return_value=df)
    return pop


def _minimal_chain(ticker: str = 'AAPL', price: float = 190.0) -> dict:
    exp_key = '2026-04-18:25'
    return {
        'volatility': 28.5,
        'putCallRatio': 0.82,
        'underlyingPrice': price,
        'callExpDateMap': {
            exp_key: {
                str(price): [{'volatility': 29.0, 'delta': 0.5}],
            }
        },
        'putExpDateMap': {},
    }


async def test_collect_daily_snapshots_inserts_for_each_ticker(repo, mock_db):
    """collect_daily_snapshots calls insert_iv for every ticker in the pool."""
    schwab = _make_schwab(chain=_minimal_chain())
    watchlists = _make_watchlists(['AAPL', 'MSFT'])
    popularity = _make_popularity([])

    await repo.collect_daily_snapshots(schwab=schwab, watchlists=watchlists, popularity=popularity)

    insert_calls = [
        c for c in mock_db.execute.call_args_list
        if 'INSERT INTO iv_history' in c[0][0]
    ]
    # 2 watchlist tickers + 11 sector ETFs, all receive the same mock chain
    assert len(insert_calls) == 13


async def test_collect_daily_snapshots_skips_empty_chain(repo, mock_db):
    """Tickers with no options chain (empty dict) are skipped without error."""
    schwab = _make_schwab(chain={})
    watchlists = _make_watchlists(['AAPL'])
    popularity = _make_popularity([])

    await repo.collect_daily_snapshots(schwab=schwab, watchlists=watchlists, popularity=popularity)

    insert_calls = [
        c for c in mock_db.execute.call_args_list
        if 'INSERT INTO iv_history' in c[0][0]
    ]
    assert len(insert_calls) == 0


async def test_collect_daily_snapshots_breaks_on_rate_limit(repo, mock_db):
    """SchwabRateLimitError stops the loop without raising."""
    schwab = _make_schwab(rate_limit=True)
    watchlists = _make_watchlists(['AAPL', 'MSFT'])
    popularity = _make_popularity([])

    # Should not raise
    await repo.collect_daily_snapshots(schwab=schwab, watchlists=watchlists, popularity=popularity)

    insert_calls = [
        c for c in mock_db.execute.call_args_list
        if 'INSERT INTO iv_history' in c[0][0]
    ]
    assert len(insert_calls) == 0


async def test_collect_daily_snapshots_deduplicates_pool(repo, mock_db):
    """Tickers appearing in both watchlist and popularity are only processed once."""
    chain = _minimal_chain('AAPL')
    schwab = _make_schwab(chain=chain)
    watchlists = _make_watchlists(['AAPL'])
    popularity = _make_popularity(['AAPL'])  # duplicate

    await repo.collect_daily_snapshots(schwab=schwab, watchlists=watchlists, popularity=popularity)

    insert_calls = [
        c for c in mock_db.execute.call_args_list
        if 'INSERT INTO iv_history' in c[0][0]
    ]
    inserted_tickers = [c[0][1][0] for c in insert_calls]
    # AAPL should appear exactly once despite being in both sources
    assert inserted_tickers.count('AAPL') == 1
