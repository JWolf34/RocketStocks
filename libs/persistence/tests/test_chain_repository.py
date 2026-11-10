"""ChainRepository: a chain and its fetch record stored together, write-once, nulls kept as nulls."""

from datetime import UTC, date, datetime

import pytest
from sqlalchemy import func, select, text

from rs_domain import ChainFetchRecord, ChainPass, FetchStatus, OptionContract
from rs_persistence import ChainFetchRow, ChainRepository, OptionContractRow

NIGHT = date(2026, 11, 9)
FETCHED = datetime(2026, 11, 9, 23, 30, tzinfo=UTC)


def _contract(underlying: str = "AAA", strike: float = 100.0, put_call: str = "C", **overrides) -> OptionContract:
    fields = {
        "underlying": underlying,
        "option_symbol": f"{underlying:<6}261120{put_call}{int(strike * 1000):08d}",
        "expiry": date(2026, 11, 20),
        "strike": strike,
        "put_call": put_call,
        "bid": 1.1,
        "ask": 1.2,
        "last": 1.15,
        "mark": 1.15,
        "bid_size": 10,
        "ask_size": 12,
        "last_size": 1,
        "volume": 250,
        "open_interest": 4_000,
        "quote_time": datetime(2026, 11, 9, 21, 0, tzinfo=UTC),
        "trade_time": datetime(2026, 11, 9, 20, 59, tzinfo=UTC),
        "iv": 31.5,
        "delta": 0.5,
        "gamma": 0.04,
        "theta": -0.05,
        "vega": 0.1,
        "rho": 0.01,
        "underlying_price": 99.5,
        "multiplier": 100,
        "expiration_type": "R",
        "non_standard": False,
    }
    return OptionContract(**(fields | overrides))


def _record(underlying: str = "AAA", status: FetchStatus = FetchStatus.OK, count: int = 0, **overrides):
    fields = {
        "session_date": NIGHT,
        "chain_pass": ChainPass.EVENING,
        "underlying": underlying,
        "status": status,
        "attempts": 1,
        "contract_count": count,
        "fetched_at": FETCHED,
    }
    return ChainFetchRecord(**(fields | overrides))


async def _store(session_factory, record, contracts=()) -> bool:
    async with session_factory() as s:
        stored = await ChainRepository(s).store(record, contracts)
        await s.commit()
    return stored


async def test_a_chain_round_trips_with_its_nulls_and_its_record(session_factory):
    chain = [
        _contract(strike=100.0),
        _contract(strike=100.0, put_call="P", iv=None, delta=None, quote_time=None, trade_time=None),
        _contract(strike=105.0, underlying_price=None, multiplier=None, expiration_type=None, non_standard=None),
    ]
    record = _record(count=3)

    assert await _store(session_factory, record, chain) is True

    async with session_factory() as s:
        repo = ChainRepository(s)
        assert await repo.contracts(NIGHT, "AAA") == chain
        assert await repo.record(NIGHT, ChainPass.EVENING, "AAA") == record
        assert await repo.record(NIGHT, ChainPass.MORNING, "AAA") is None


async def test_an_underlying_is_stored_once_per_pass_and_session(session_factory):
    assert await _store(session_factory, _record(count=1), [_contract()]) is True

    again = await _store(session_factory, _record(count=1, attempts=2), [_contract(bid=9.0)])

    assert again is False
    async with session_factory() as s:
        [kept] = await ChainRepository(s).contracts(NIGHT, "AAA")
        assert kept.bid == 1.1
        assert (await ChainRepository(s).record(NIGHT, ChainPass.EVENING, "AAA")).attempts == 1
        assert await s.scalar(select(func.count()).select_from(OptionContractRow)) == 1

    morning = _record(status=FetchStatus.FAILED, chain_pass=ChainPass.MORNING)
    assert await _store(session_factory, morning) is True


async def test_a_tombstone_and_an_empty_chain_are_records_without_contract_rows(session_factory):
    await _store(session_factory, _record("BAD", FetchStatus.FAILED, attempts=3))
    await _store(session_factory, _record("REJ", FetchStatus.REJECTED))
    await _store(session_factory, _record("EMP", FetchStatus.EMPTY))
    await _store(session_factory, _record("AAA", count=1), [_contract()])

    async with session_factory() as s:
        repo = ChainRepository(s)
        assert await s.scalar(select(func.count()).select_from(OptionContractRow)) == 1
        assert (await repo.record(NIGHT, ChainPass.EVENING, "BAD")).status.is_tombstone
        assert (await repo.record(NIGHT, ChainPass.EVENING, "REJ")).status.is_tombstone
        assert not (await repo.record(NIGHT, ChainPass.EVENING, "EMP")).status.is_tombstone
        assert await repo.contracts(NIGHT, "BAD") == []
        assert await repo.recorded_underlyings(NIGHT, ChainPass.EVENING) == {"AAA", "BAD", "REJ", "EMP"}
        assert await repo.recorded_underlyings(NIGHT, ChainPass.MORNING) == set()
        assert await repo.recorded_underlyings(date(2026, 11, 10), ChainPass.EVENING) == set()


async def test_a_record_and_its_contracts_must_agree(session_factory):
    async with session_factory() as s:
        repo = ChainRepository(s)
        with pytest.raises(ValueError, match="counts 2, got 1"):
            await repo.store(_record(count=2), [_contract()])
        with pytest.raises(ValueError, match="another underlying"):
            await repo.store(_record(count=1), [_contract("BBB")])
        assert await s.scalar(select(func.count()).select_from(ChainFetchRow)) == 0


async def test_a_chain_larger_than_one_insert_statement_is_stored_whole(session_factory):
    chain = [_contract(strike=float(strike)) for strike in range(1, 3501)]

    await _store(session_factory, _record(count=len(chain)), chain)

    async with session_factory() as s:
        assert await s.scalar(select(func.count()).select_from(OptionContractRow)) == 3500


async def test_a_chunk_of_stored_contracts_compresses_and_reads_back(session_factory):
    chain = [_contract(strike=100.0), _contract(strike=100.0, put_call="P")]
    await _store(session_factory, _record(count=2), chain)

    async with session_factory() as s:
        chunk = await s.scalar(text("SELECT show_chunks('option_contract')::text LIMIT 1"))
        await s.execute(text(f"SELECT compress_chunk('{chunk}')"))
        assert await ChainRepository(s).contracts(NIGHT, "AAA") == chain
        await s.rollback()


async def test_only_the_evening_pass_stores_contract_rows(session_factory):
    async with session_factory() as s:
        with pytest.raises(ValueError, match="only the evening pass"):
            await ChainRepository(s).store(_record(count=1, chain_pass=ChainPass.MORNING), [_contract()])


async def test_contracts_that_tie_on_expiry_strike_and_side_read_back_in_option_symbol_order(session_factory):
    adjusted = _contract(option_symbol="AAA1  261120C00100000")
    standard = _contract()
    await _store(session_factory, _record(count=2), [adjusted, standard])

    async with session_factory() as s:
        got = await ChainRepository(s).contracts(NIGHT, "AAA")

    assert [c.option_symbol for c in got] == sorted([adjusted.option_symbol, standard.option_symbol])
