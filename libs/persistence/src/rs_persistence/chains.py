"""chain_fetch and option_contract: one underlying's chain and the record of fetching it.

A chain and its fetch record are stored together, so a record that says `ok` always has its
contracts and a stored contract always has a record. Both are write-once: a rerun finds the
underlying already recorded and stores nothing.
"""

from collections.abc import Sequence
from datetime import UTC, date

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import ChainFetchRecord, ChainPass, FetchStatus, OptionContract
from rs_persistence.models import ChainFetchRow, OptionContractRow

# 27 bound parameters per contract row; stay far below the protocol's 65535 limit.
_INSERT_CHUNK = 1500


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
        and only the evening pass has contract rows to store: the morning pass keeps its
        records here and its open interest elsewhere.
        """
        if contracts and record.chain_pass is not ChainPass.EVENING:
            raise ValueError(f"{record.underlying}: only the evening pass stores contract rows")
        if len(contracts) != record.contract_count:
            raise ValueError(f"{record.underlying}: record counts {record.contract_count}, got {len(contracts)}")
        if any(contract.underlying != record.underlying for contract in contracts):
            raise ValueError(f"{record.underlying}: contracts of another underlying")
        stored = await self._session.scalar(
            pg_insert(ChainFetchRow)
            .values(record.model_dump())
            .on_conflict_do_nothing()
            .returning(ChainFetchRow.underlying)
        )
        if stored is None:
            return False
        for start in range(0, len(contracts), _INSERT_CHUNK):
            chunk = contracts[start : start + _INSERT_CHUNK]
            await self._session.execute(
                pg_insert(OptionContractRow).values(
                    [{"session_date": record.session_date, **contract.model_dump()} for contract in chunk]
                )
            )
        return True

    async def recorded_underlyings(self, session_date: date, chain_pass: ChainPass) -> set[str]:
        """Every underlying a pass has a record for in a session, Tombstones included."""
        underlyings = await self._session.scalars(
            select(ChainFetchRow.underlying).where(
                ChainFetchRow.session_date == session_date, ChainFetchRow.chain_pass == chain_pass.value
            )
        )
        return set(underlyings)

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
