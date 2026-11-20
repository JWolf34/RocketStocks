"""chain_fetch, option_contract and option_open_interest: one underlying's chain and the record
of fetching it.

The evening pass stores a chain's contracts and the morning pass only its open interest. Either
is stored together with its fetch record, so a record that says `ok` always has its rows and a
stored row always has a record. Both are write-once: a rerun finds the underlying already
recorded for the pass and stores nothing.
"""

from collections.abc import Sequence
from datetime import UTC, date, datetime

from sqlalchemy import func, literal, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import (
    ChainFetchRecord,
    ChainPass,
    FetchStatus,
    OpenInterest,
    OptionContract,
)
from rs_persistence.models import (
    ChainFetchRow,
    OptionContractRow,
    OptionOpenInterestRow,
    UniverseListingRow,
)

# 27 bound parameters per contract row; stay far below the protocol's 65535 limit.
_INSERT_CHUNK = 1500
# Four per open-interest row.
_OPEN_INTEREST_CHUNK = 5000


def _record_from_row(row: ChainFetchRow) -> ChainFetchRecord:
    return ChainFetchRecord(
        session_date=row.session_date,
        chain_pass=ChainPass(row.chain_pass),
        underlying=row.underlying,
        status=FetchStatus(row.status),
        attempts=row.attempts,
        contract_count=row.contract_count,
        fetched_at=row.fetched_at.astimezone(UTC),
    )


def _contract_from_row(row: OptionContractRow) -> OptionContract:
    return OptionContract.model_validate(
        {
            **{column: getattr(row, column) for column in OptionContract.model_fields},
            "quote_time": None if row.quote_time is None else row.quote_time.astimezone(UTC),
            "trade_time": None if row.trade_time is None else row.trade_time.astimezone(UTC),
        }
    )


class ChainRepository:
    """Caller owns the commit, so a record and its contracts commit together."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def store(self, record: ChainFetchRecord, contracts: Sequence[OptionContract] = ()) -> bool:
        """Store the record and its contracts unless the underlying already has a record for
        that pass and session; returns whether anything was stored.

        `contracts` must be exactly the record's `contract_count` contracts of its underlying,
        and only the evening pass has contract rows to store: the morning pass stores its
        open interest with `store_open_interest`.
        """
        if contracts and record.chain_pass is not ChainPass.EVENING:
            raise ValueError(f"{record.underlying}: only the evening pass stores contract rows")
        if len(contracts) != record.contract_count:
            raise ValueError(f"{record.underlying}: record counts {record.contract_count}, got {len(contracts)}")
        if any(contract.underlying != record.underlying for contract in contracts):
            raise ValueError(f"{record.underlying}: contracts of another underlying")
        if not await self._store_record(record):
            return False
        for start in range(0, len(contracts), _INSERT_CHUNK):
            chunk = contracts[start : start + _INSERT_CHUNK]
            await self._session.execute(
                pg_insert(OptionContractRow).values(
                    [{"session_date": record.session_date, **contract.model_dump()} for contract in chunk]
                )
            )
        return True

    async def store_open_interest(self, record: ChainFetchRecord, interest: Sequence[OpenInterest] = ()) -> bool:
        """Store the morning record and its open interest, named by the record's session, unless
        the underlying already has a morning record for that session; returns whether anything
        was stored.

        `interest` must be exactly the record's `contract_count` rows of its underlying.
        """
        if record.chain_pass is not ChainPass.MORNING:
            raise ValueError(f"{record.underlying}: only the morning pass stores open interest alone")
        if len(interest) != record.contract_count:
            raise ValueError(f"{record.underlying}: record counts {record.contract_count}, got {len(interest)}")
        if any(row.underlying != record.underlying for row in interest):
            raise ValueError(f"{record.underlying}: open interest of another underlying")
        if not await self._store_record(record):
            return False
        for start in range(0, len(interest), _OPEN_INTEREST_CHUNK):
            chunk = interest[start : start + _OPEN_INTEREST_CHUNK]
            await self._session.execute(
                pg_insert(OptionOpenInterestRow).values(
                    [{"session_date": record.session_date, **row.model_dump()} for row in chunk]
                )
            )
        return True

    async def _store_record(self, record: ChainFetchRecord) -> bool:
        stored = await self._session.scalar(
            pg_insert(ChainFetchRow)
            .values(record.model_dump())
            .on_conflict_do_nothing()
            .returning(ChainFetchRow.underlying)
        )
        return stored is not None

    async def recorded_underlyings(self, session_date: date, chain_pass: ChainPass) -> set[str]:
        """Every underlying a pass has a record for in a session, Tombstones included."""
        underlyings = await self._session.scalars(
            select(ChainFetchRow.underlying).where(
                ChainFetchRow.session_date == session_date, ChainFetchRow.chain_pass == chain_pass.value
            )
        )
        return set(underlyings)

    async def first_session(self, chain_pass: ChainPass) -> date | None:
        """The earliest session a pass has any record for; None before its first."""
        return await self._session.scalar(
            select(func.min(ChainFetchRow.session_date)).where(ChainFetchRow.chain_pass == chain_pass.value)
        )

    async def tombstone_unrecorded(
        self, session_date: date, chain_pass: ChainPass, snapshot_date: date, fetched_at: datetime
    ) -> list[str]:
        """Write a Tombstone with no attempts for every optionable listing of the Universe of
        `snapshot_date` that the pass has no record for in `session_date`; returns their
        underlyings. Records already there, whatever their status, are kept."""
        c = ChainFetchRow.__table__.c
        # Typed literals: the enum columns take no untyped text.
        tombstones = select(
            literal(session_date, c.session_date.type),
            literal(chain_pass.value, c.chain_pass.type),
            UniverseListingRow.symbol,
            literal(FetchStatus.FAILED.value, c.status.type),
            literal(0, c.attempts.type),
            literal(0, c.contract_count.type),
            literal(fetched_at, c.fetched_at.type),
        ).where(UniverseListingRow.snapshot_date == snapshot_date, UniverseListingRow.optionable)
        written = await self._session.scalars(
            pg_insert(ChainFetchRow)
            .from_select(
                [c.session_date, c.chain_pass, c.underlying, c.status, c.attempts, c.contract_count, c.fetched_at],
                tombstones,
            )
            .on_conflict_do_nothing()
            .returning(ChainFetchRow.underlying)
        )
        return sorted(written)

    async def record(self, session_date: date, chain_pass: ChainPass, underlying: str) -> ChainFetchRecord | None:
        row = await self._session.get(ChainFetchRow, (session_date, chain_pass.value, underlying))
        return None if row is None else _record_from_row(row)

    async def contracts(self, session_date: date, underlying: str) -> list[OptionContract]:
        """The stored chain of one underlying for a session, ordered by expiry, strike and side."""
        rows = await self._session.scalars(
            select(OptionContractRow)
            .where(OptionContractRow.session_date == session_date, OptionContractRow.underlying == underlying)
            .order_by(
                OptionContractRow.expiry,
                OptionContractRow.strike,
                OptionContractRow.put_call,
                OptionContractRow.option_symbol,
            )
        )
        return [_contract_from_row(row) for row in rows]

    async def open_interest(self, session_date: date, underlying: str) -> list[OpenInterest]:
        """The morning's open interest of one underlying for a session, ordered by option symbol."""
        rows = await self._session.scalars(
            select(OptionOpenInterestRow)
            .where(OptionOpenInterestRow.session_date == session_date, OptionOpenInterestRow.underlying == underlying)
            .order_by(OptionOpenInterestRow.option_symbol)
        )
        return [
            OpenInterest(underlying=row.underlying, option_symbol=row.option_symbol, open_interest=row.open_interest)
            for row in rows
        ]
