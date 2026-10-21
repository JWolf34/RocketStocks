"""candidate: every Trigger hit with the selector's verdict.

A candidate is decided once and never updated; its unique key (trigger, rule
version, stock, hit time) makes a rerun of a boundary a no-op.
"""

from datetime import UTC, date, datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import Candidate, Verdict
from rs_persistence.models import CandidateRow


def candidate_from_row(row: CandidateRow) -> Candidate:
    return Candidate(
        trigger=row.trigger,
        rule_version=row.rule_version,
        symbol=row.symbol,
        hit_at=row.hit_at.astimezone(UTC),
        inputs=row.inputs,
        verdict=Verdict(row.verdict),
        decided_at=row.decided_at.astimezone(UTC),
        alert_id=row.alert_id,
    )


class CandidateRepository:
    """Caller owns the commit, so a candidate and the Alert it produced commit together."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def is_decided(self, candidate: Candidate) -> bool:
        """Whether this Trigger version already decided this stock's hit at this time."""
        return (
            await self._session.scalar(
                select(CandidateRow.id).where(
                    CandidateRow.trigger == candidate.trigger,
                    CandidateRow.rule_version == candidate.rule_version,
                    CandidateRow.symbol == candidate.symbol,
                    CandidateRow.hit_at == candidate.hit_at,
                )
            )
            is not None
        )

    async def insert(self, candidate: Candidate) -> int | None:
        """Store the candidate; returns its id, or None when this Trigger version
        already decided this stock's hit at this time (nothing is written then)."""
        return await self._session.scalar(
            pg_insert(CandidateRow)
            .values(
                trigger=candidate.trigger,
                rule_version=candidate.rule_version,
                symbol=candidate.symbol,
                hit_at=candidate.hit_at,
                inputs=candidate.inputs,
                verdict=candidate.verdict.value,
                decided_at=candidate.decided_at,
                alert_id=candidate.alert_id,
            )
            .on_conflict_do_nothing(
                index_elements=[
                    CandidateRow.trigger,
                    CandidateRow.rule_version,
                    CandidateRow.symbol,
                    CandidateRow.hit_at,
                ]
            )
            .returning(CandidateRow.id)
        )

    async def list_window(self, trigger: str, start: datetime, end: datetime) -> list[Candidate]:
        """One Trigger's candidates (every rule version) with hit_at in [start, end),
        oldest hit first."""
        rows = await self._session.scalars(
            select(CandidateRow)
            .where(
                CandidateRow.trigger == trigger,
                CandidateRow.hit_at >= start,
                CandidateRow.hit_at < end,
            )
            .order_by(CandidateRow.hit_at, CandidateRow.id)
        )
        return [candidate_from_row(row) for row in rows]

    async def first_hits_by_day(self, trigger: str, rule_version: str) -> dict[tuple[str, date], datetime]:
        """The earliest stored hit of one Trigger version for each stock and Eastern day."""
        result = await self._session.execute(
            select(CandidateRow.symbol, CandidateRow.day, func.min(CandidateRow.hit_at))
            .where(CandidateRow.trigger == trigger, CandidateRow.rule_version == rule_version)
            .group_by(CandidateRow.symbol, CandidateRow.day)
        )
        return {(symbol, day): first.astimezone(UTC) for symbol, day, first in result}
