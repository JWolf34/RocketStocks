"""base_rate reads."""

from datetime import UTC

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import BaseRate, BaseRateInput
from rs_persistence import BaseRateRow

# The logged rate replaces the reconstructed one once this many real alerts are measured.
BASE_RATE_LOGGED_MIN_N = 30


def _to_base_rate(row: BaseRateRow) -> BaseRate:
    return BaseRate(
        alert_type=row.alert_type,
        rule_version=row.rule_version,
        computed_at=row.computed_at.astimezone(UTC),
        input=BaseRateInput(row.input),
        n=row.n,
        n_unpriced=row.n_unpriced,
        hit_rate_1d=row.hit_rate_1d,
        hit_rate_5d=row.hit_rate_5d,
        hit_rate_10d=row.hit_rate_10d,
        median_excess_1d=row.median_excess_1d,
        median_excess_5d=row.median_excess_5d,
        median_excess_10d=row.median_excess_10d,
        survivorship_caveat=row.survivorship_caveat,
    )


async def _latest(
    session: AsyncSession, alert_type: str, rule_version: str, kind: BaseRateInput
) -> BaseRate | None:
    row = await session.scalar(
        select(BaseRateRow)
        .where(
            BaseRateRow.alert_type == alert_type,
            BaseRateRow.rule_version == rule_version,
            BaseRateRow.input == kind.value,
        )
        .order_by(BaseRateRow.computed_at.desc(), BaseRateRow.id.desc())
        .limit(1)
    )
    return None if row is None else _to_base_rate(row)


async def current_base_rate(
    session: AsyncSession, alert_type: str, rule_version: str
) -> BaseRate | None:
    """The base rate the bot should show for a rule version: the latest `logged`
    row once its N reaches BASE_RATE_LOGGED_MIN_N, otherwise the latest
    `reconstructed` row. None when there is nothing to show (a thin logged row is
    never shown on its own)."""
    logged = await _latest(session, alert_type, rule_version, BaseRateInput.LOGGED)
    if logged is not None and logged.n >= BASE_RATE_LOGGED_MIN_N:
        return logged
    return await _latest(session, alert_type, rule_version, BaseRateInput.RECONSTRUCTED)
