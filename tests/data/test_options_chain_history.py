"""Tests for rocketstocks.data.options_chain_history."""
import datetime
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from rocketstocks.data.options_chain_history import (
    OptionsChainHistoryRepository,
    _FIELDS,
    parse_chain,
)

_FIXTURE = Path(__file__).parent / "fixtures" / "NVDA_options_chain_2026-05-15.json"
_SNAPSHOT = datetime.date(2026, 5, 15)
_IDX = {name: i for i, name in enumerate(_FIELDS)}


@pytest.fixture(scope="module")
def nvda_chain() -> dict:
    with open(_FIXTURE) as fh:
        return json.load(fh)


@pytest.fixture
def mock_db():
    db = MagicMock(name="Postgres")
    db.execute = AsyncMock(return_value=None)
    db.execute_batch = AsyncMock(return_value=None)
    return db


@pytest.fixture
def repo(mock_db):
    return OptionsChainHistoryRepository(db=mock_db)


# ---------------------------------------------------------------------------
# parse_chain — against the real NVDA fixture
# ---------------------------------------------------------------------------

def test_parse_returns_all_contracts(nvda_chain):
    tuples = parse_chain(nvda_chain, _SNAPSHOT)
    assert len(tuples) == nvda_chain["numberOfContracts"] == 4300


def test_parse_splits_calls_and_puts_evenly(nvda_chain):
    tuples = parse_chain(nvda_chain, _SNAPSHOT)
    calls = sum(1 for t in tuples if t[_IDX["option_type"]] == "call")
    puts = sum(1 for t in tuples if t[_IDX["option_type"]] == "put")
    assert calls == 2150
    assert puts == 2150


def test_parse_known_contract_reads_real_iv(nvda_chain):
    """The known ITM call must decode with per-contract IV (~5851.28), NOT the
    chain's 29.0 theoreticalVolatility/HV input."""
    tuples = parse_chain(nvda_chain, _SNAPSHOT)
    match = [t for t in tuples if t[_IDX["contract_symbol"]] == "NVDA  260515C00002500"]
    assert len(match) == 1
    t = match[0]
    assert t[_IDX["strike"]] == 2.5
    assert t[_IDX["option_type"]] == "call"
    assert t[_IDX["in_the_money"]] is True
    assert t[_IDX["iv"]] == pytest.approx(5851.277, rel=1e-4)
    assert t[_IDX["iv"]] != 29.0
    assert t[_IDX["expiration_date"]] == datetime.date(2026, 5, 15)


def test_parse_records_underlying_price_on_every_row(nvda_chain):
    tuples = parse_chain(nvda_chain, _SNAPSHOT)
    prices = {t[_IDX["underlying_price"]] for t in tuples}
    assert prices == {226.58}


def test_parse_sets_snapshot_date(nvda_chain):
    tuples = parse_chain(nvda_chain, _SNAPSHOT)
    assert all(t[_IDX["snapshot_date"]] == _SNAPSHOT for t in tuples)


def test_parse_populates_greeks(nvda_chain):
    tuples = parse_chain(nvda_chain, _SNAPSHOT)
    t = [t for t in tuples if t[_IDX["contract_symbol"]] == "NVDA  260515C00002500"][0]
    for greek in ("delta", "gamma", "theta", "vega", "rho"):
        assert t[_IDX[greek]] is not None


def test_parse_tuple_width_matches_fields(nvda_chain):
    tuples = parse_chain(nvda_chain, _SNAPSHOT)
    assert all(len(t) == len(_FIELDS) for t in tuples)


def test_parse_empty_chain_returns_empty():
    chain = {"symbol": "ZZZZ", "callExpDateMap": {}, "putExpDateMap": {}}
    assert parse_chain(chain, _SNAPSHOT) == []


def test_parse_sanitizes_nan_numerics():
    """Schwab can return the string 'NaN' on illiquid contracts; it must become None."""
    chain = {
        "symbol": "AAA",
        "underlyingPrice": "NaN",
        "callExpDateMap": {
            "2026-06-19:30": {
                "100.0": [{
                    "symbol": "AAA  260619C00100000",
                    "strikePrice": 100.0,
                    "expirationDate": "2026-06-19T20:00:00.000+00:00",
                    "daysToExpiration": 30,
                    "volatility": "NaN",
                    "delta": "NaN",
                    "totalVolume": 5,
                    "openInterest": 10,
                    "inTheMoney": False,
                }]
            }
        },
        "putExpDateMap": {},
    }
    t = parse_chain(chain, _SNAPSHOT)[0]
    assert t[_IDX["underlying_price"]] is None
    assert t[_IDX["iv"]] is None
    assert t[_IDX["delta"]] is None
    assert t[_IDX["volume"]] == 5
    assert t[_IDX["open_interest"]] == 10


# ---------------------------------------------------------------------------
# insert_contracts
# ---------------------------------------------------------------------------

async def test_insert_contracts_bulk_upsert(repo, mock_db):
    contracts = [tuple(range(len(_FIELDS)))]
    await repo.insert_contracts(contracts)
    mock_db.execute_batch.assert_awaited_once()
    sql, values = mock_db.execute_batch.call_args[0]
    assert "INSERT INTO options_chain_history" in sql
    assert "ON CONFLICT (ticker, snapshot_date, expiration_date, strike, option_type) DO UPDATE" in sql
    assert values == contracts


async def test_insert_contracts_empty_is_noop(repo, mock_db):
    await repo.insert_contracts([])
    mock_db.execute_batch.assert_not_called()


# ---------------------------------------------------------------------------
# get_chain_snapshot
# ---------------------------------------------------------------------------

async def test_get_chain_snapshot_returns_dataframe(repo, mock_db):
    mock_db.execute.return_value = [tuple(range(len(_FIELDS)))]
    df = await repo.get_chain_snapshot("NVDA", _SNAPSHOT)
    assert list(df.columns) == _FIELDS
    assert len(df) == 1


async def test_get_chain_snapshot_empty(repo, mock_db):
    mock_db.execute.return_value = None
    df = await repo.get_chain_snapshot("NVDA", _SNAPSHOT)
    assert df.empty
    assert list(df.columns) == _FIELDS


# ---------------------------------------------------------------------------
# get_oi_history
# ---------------------------------------------------------------------------

async def test_get_oi_history_returns_timeseries(repo, mock_db):
    mock_db.execute.return_value = [
        (datetime.date(2026, 5, 13), 100, 500, 45.0, 0.5, 0.01, -0.02, 0.1, 0.05),
        (datetime.date(2026, 5, 14), 120, 510, 46.0, 0.51, 0.01, -0.02, 0.1, 0.05),
    ]
    df = await repo.get_oi_history("NVDA", datetime.date(2026, 6, 19), 230.0, "call")
    assert list(df.columns) == [
        "snapshot_date", "volume", "open_interest", "iv",
        "delta", "gamma", "theta", "vega", "rho",
    ]
    assert len(df) == 2


async def test_get_oi_history_applies_date_filters(repo, mock_db):
    mock_db.execute.return_value = []
    await repo.get_oi_history(
        "NVDA", datetime.date(2026, 6, 19), 230.0, "call",
        start_date=datetime.date(2026, 5, 1), end_date=datetime.date(2026, 5, 14),
    )
    sql, params = mock_db.execute.call_args[0]
    assert "snapshot_date >= %s" in sql
    assert "snapshot_date <= %s" in sql
    assert params == [
        "NVDA", datetime.date(2026, 6, 19), 230.0, "call",
        datetime.date(2026, 5, 1), datetime.date(2026, 5, 14),
    ]


async def test_get_oi_history_empty(repo, mock_db):
    mock_db.execute.return_value = None
    df = await repo.get_oi_history("NVDA", datetime.date(2026, 6, 19), 230.0, "call")
    assert df.empty


# ---------------------------------------------------------------------------
# get_iv_history
# ---------------------------------------------------------------------------

async def test_get_iv_history_returns_expected_columns(repo, mock_db):
    mock_db.execute.return_value = [
        (datetime.date(2026, 5, 14), 45.0, 45.0, 0.9),
        (datetime.date(2026, 5, 13), 44.0, 44.0, 0.8),
    ]
    df = await repo.get_iv_history("NVDA", days=30)
    assert list(df.columns) == ["date", "iv", "atm_iv", "put_call_ratio"]
    assert len(df) == 2


async def test_get_iv_history_sorted_ascending(repo, mock_db):
    mock_db.execute.return_value = [
        (datetime.date(2026, 5, 14), 45.0, 45.0, 0.9),
        (datetime.date(2026, 5, 13), 44.0, 44.0, 0.8),
    ]
    df = await repo.get_iv_history("NVDA", days=30)
    assert df["date"].iloc[0] == datetime.date(2026, 5, 13)
    assert df["date"].iloc[1] == datetime.date(2026, 5, 14)


async def test_get_iv_history_passes_ticker_and_days(repo, mock_db):
    mock_db.execute.return_value = []
    await repo.get_iv_history("TSLA", days=90)
    _, params = mock_db.execute.call_args[0]
    assert params == ["TSLA", "TSLA", "TSLA", 90]


async def test_get_iv_history_empty(repo, mock_db):
    mock_db.execute.return_value = None
    df = await repo.get_iv_history("NVDA")
    assert df.empty
    assert list(df.columns) == ["date", "iv", "atm_iv", "put_call_ratio"]


# ---------------------------------------------------------------------------
# get_latest_iv
# ---------------------------------------------------------------------------

async def test_get_latest_iv_returns_atm_iv(repo, mock_db):
    mock_db.execute.return_value = [(datetime.date(2026, 5, 14), 47.2, 47.2, 0.9)]
    result = await repo.get_latest_iv("NVDA")
    assert result == pytest.approx(47.2)


async def test_get_latest_iv_none_when_empty(repo, mock_db):
    mock_db.execute.return_value = None
    result = await repo.get_latest_iv("NVDA")
    assert result is None


# ---------------------------------------------------------------------------
# collect_daily_snapshots
# ---------------------------------------------------------------------------

def _success_chain(ticker: str = "AAPL") -> dict:
    return {
        "symbol": ticker,
        "status": "SUCCESS",
        "underlyingPrice": 100.0,
        "callExpDateMap": {
            "2026-06-19:30": {
                "100.0": [{
                    "symbol": f"{ticker}  260619C00100000",
                    "strikePrice": 100.0,
                    "expirationDate": "2026-06-19T20:00:00.000+00:00",
                    "daysToExpiration": 30,
                    "volatility": 30.0,
                    "delta": 0.5,
                    "totalVolume": 10,
                    "openInterest": 100,
                    "inTheMoney": False,
                }]
            }
        },
        "putExpDateMap": {},
    }


def _make_schwab(chain=None, *, side_effect=None):
    schwab = MagicMock()
    if side_effect is not None:
        schwab.get_options_chain = AsyncMock(side_effect=side_effect)
    else:
        schwab.get_options_chain = AsyncMock(return_value=chain)
    return schwab


async def test_collect_counts_successes(repo, mock_db):
    schwab = _make_schwab(chain=_success_chain())
    result = await repo.collect_daily_snapshots(schwab, ["AAPL", "MSFT"])
    assert result == {"success": 2, "skipped": 0, "errored": 0}
    assert mock_db.execute_batch.await_count == 2


async def test_collect_skips_non_success_chain(repo, mock_db):
    schwab = _make_schwab(chain={"symbol": "AAPL", "status": "FAILED"})
    result = await repo.collect_daily_snapshots(schwab, ["AAPL"])
    assert result == {"success": 0, "skipped": 1, "errored": 0}
    mock_db.execute_batch.assert_not_called()


async def test_collect_skips_empty_chain(repo, mock_db):
    schwab = _make_schwab(chain=None)
    result = await repo.collect_daily_snapshots(schwab, ["AAPL"])
    assert result == {"success": 0, "skipped": 1, "errored": 0}


async def test_collect_counts_errored_on_exception(repo, mock_db):
    schwab = _make_schwab(side_effect=ValueError("boom"))
    result = await repo.collect_daily_snapshots(schwab, ["AAPL"])
    assert result == {"success": 0, "skipped": 0, "errored": 1}


async def test_collect_aborts_on_rate_limit(repo, mock_db):
    from rocketstocks.data.clients.schwab import SchwabRateLimitError
    schwab = _make_schwab(side_effect=SchwabRateLimitError("limit"))
    result = await repo.collect_daily_snapshots(schwab, ["AAPL", "MSFT"])
    assert result == {"success": 0, "skipped": 0, "errored": 0}
    assert schwab.get_options_chain.await_count == 1  # loop broke after first


async def test_collect_aborts_on_token_error(repo, mock_db):
    from rocketstocks.data.clients.schwab import SchwabTokenError
    schwab = _make_schwab(side_effect=SchwabTokenError("bad token"))
    result = await repo.collect_daily_snapshots(schwab, ["AAPL", "MSFT"])
    assert result == {"success": 0, "skipped": 0, "errored": 0}
    assert schwab.get_options_chain.await_count == 1
