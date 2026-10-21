"""outcome and track_record: the nightly rewrite of one Trigger version's Outcomes
and Track record."""

from collections.abc import Sequence
from datetime import UTC

from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import Outcome, TrackRecord
from rs_persistence.models import OutcomeRow, TrackRecordRow

# 16 bound parameters per row; stay far below the protocol's 65535 limit.
_INSERT_CHUNK = 2000


def track_record_from_row(row: TrackRecordRow) -> TrackRecord:
    return TrackRecord(
        trigger=row.trigger,
        rule_version=row.rule_version,
        horizon=row.horizon,
        computed_at=row.computed_at.astimezone(UTC),
        cases=row.cases,
        live_cases=row.live_cases,
        outcomes=row.outcomes,
        unpriced=row.unpriced,
        split_excluded=row.split_excluded,
        no_move_size=row.no_move_size,
        share_at_2x=row.share_at_2x,
        ordinary_share=row.ordinary_share,
        median_multiple=row.median_multiple,
        median_excess=row.median_excess,
    )


class TrackRecordRepository:
    """Caller owns the commit, so a version's Outcomes and its Track record commit
    together. Reads go through rs_query."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def replace_outcomes(self, trigger: str, rule_version: str, outcomes: Sequence[Outcome]) -> None:
        """Make `outcomes` the whole set stored for this Trigger version."""
        await self._session.execute(
            delete(OutcomeRow).where(OutcomeRow.trigger == trigger, OutcomeRow.rule_version == rule_version)
        )
        for start in range(0, len(outcomes), _INSERT_CHUNK):
            chunk = outcomes[start : start + _INSERT_CHUNK]
            await self._session.execute(
                pg_insert(OutcomeRow).values([o.model_dump(mode="python") for o in chunk])
            )

    async def upsert(self, records: Sequence[TrackRecord]) -> None:
        """Write each horizon's row, replacing the one already stored for it."""
        for record in records:
            values = record.model_dump(mode="python")
            stmt = pg_insert(TrackRecordRow).values(values)
            keys = ("trigger", "rule_version", "horizon")
            await self._session.execute(
                stmt.on_conflict_do_update(
                    index_elements=[getattr(TrackRecordRow, key) for key in keys],
                    set_={name: stmt.excluded[name] for name in values if name not in keys},
                )
            )
