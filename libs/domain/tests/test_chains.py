from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from rs_domain import ChainFetchRecord, ChainPass, FetchStatus, OptionContract

FETCHED = datetime(2026, 11, 9, 23, 30, tzinfo=UTC)


def record(status: FetchStatus, count: int, **overrides) -> ChainFetchRecord:
    fields = {
        "session_date": date(2026, 11, 9),
        "chain_pass": ChainPass.EVENING,
        "underlying": "AAA",
        "status": status,
        "attempts": 1,
        "contract_count": count,
        "fetched_at": FETCHED,
    }
    return ChainFetchRecord(**(fields | overrides))


def test_only_failure_statuses_are_tombstones():
    assert {s for s in FetchStatus if s.is_tombstone} == {FetchStatus.REJECTED, FetchStatus.FAILED}


@pytest.mark.parametrize("status", list(FetchStatus))
def test_a_record_counts_contracts_exactly_when_it_is_ok(status):
    count = 5 if status is FetchStatus.OK else 0
    assert record(status, count).contract_count == count
    with pytest.raises(ValidationError, match="cannot have"):
        record(status, 0 if status is FetchStatus.OK else 5)


def test_a_record_needs_an_aware_utc_fetch_time():
    with pytest.raises(ValidationError, match="naive"):
        record(FetchStatus.EMPTY, 0, fetched_at=datetime(2026, 11, 9, 23, 30))


def test_a_contract_rejects_a_non_finite_price_and_an_unknown_side():
    fields = {
        "underlying": "AAA",
        "option_symbol": "AAA   261120C00100000",
        "expiry": date(2026, 11, 20),
        "strike": 100.0,
        "put_call": "C",
        "bid": 1.0,
        "ask": 1.1,
        "last": 1.0,
        "mark": 1.05,
        "bid_size": 1,
        "ask_size": 1,
        "last_size": 1,
        "volume": 0,
        "open_interest": 0,
        "quote_time": None,
        "trade_time": None,
        "iv": None,
        "delta": None,
        "gamma": None,
        "theta": None,
        "vega": None,
        "rho": None,
        "underlying_price": None,
        "multiplier": None,
        "expiration_type": None,
        "non_standard": None,
    }
    assert OptionContract(**fields).iv is None
    with pytest.raises(ValidationError):
        OptionContract(**(fields | {"bid": float("nan")}))
    with pytest.raises(ValidationError):
        OptionContract(**(fields | {"put_call": "X"}))
