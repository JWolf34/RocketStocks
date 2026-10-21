"""trigger_role: the server role an Alert pings for each Trigger."""

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from rs_persistence.models import TriggerRoleRow


class TriggerRoleRepository:
    """Caller owns the commit."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def all(self) -> dict[str, int]:
        """Trigger -> Discord role id, for every Trigger that has one."""
        rows = await self._session.execute(select(TriggerRoleRow.trigger, TriggerRoleRow.role_id))
        return {trigger: role_id for trigger, role_id in rows}

    async def get(self, trigger: str) -> int | None:
        return await self._session.scalar(
            select(TriggerRoleRow.role_id).where(TriggerRoleRow.trigger == trigger)
        )

    async def set(self, trigger: str, role_id: int) -> None:
        """Map `trigger` to `role_id`, replacing an earlier mapping."""
        statement = insert(TriggerRoleRow).values(trigger=trigger, role_id=role_id)
        await self._session.execute(
            statement.on_conflict_do_update(
                index_elements=[TriggerRoleRow.trigger], set_={"role_id": statement.excluded.role_id}
            )
        )

    async def remove(self, trigger: str) -> bool:
        """Drop the mapping; False when there was none."""
        result = await self._session.execute(
            delete(TriggerRoleRow).where(TriggerRoleRow.trigger == trigger)
        )
        return result.rowcount > 0  # type: ignore[attr-defined]
