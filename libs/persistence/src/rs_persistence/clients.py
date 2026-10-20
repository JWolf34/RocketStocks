"""API-consumer rows."""

from typing import Any, cast

from sqlalchemy import func, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from rs_persistence.models import ServiceClientRow


class ServiceClientRepository:
    """API-consumer rows (ADR-0012, rev-003). Speaks key HASHES only - key
    generation and hashing live in rs_serve; this layer never sees a raw key.
    Caller owns the commit (same convention as AlertRepository)."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def provision(self, name: str, api_key_hash: str) -> None:
        self._session.add(ServiceClientRow(name=name, api_key_hash=api_key_hash))
        await self._session.flush()

    async def get_active_by_hash(self, api_key_hash: str) -> ServiceClientRow | None:
        """The auth lookup: unknown and revoked both resolve to None, so the
        middleware's uniform-401 needs no branching (S005: no validity leak)."""
        return await self._session.scalar(
            select(ServiceClientRow).where(
                ServiceClientRow.api_key_hash == api_key_hash,
                ServiceClientRow.revoked_at.is_(None),
            )
        )

    async def revoke(self, name: str) -> bool:
        """Set revoked_at now(); False if no active row matched (unknown name
        or already revoked) so the CLI can report instead of silently no-op."""
        result = await self._session.execute(
            update(ServiceClientRow)
            .where(ServiceClientRow.name == name, ServiceClientRow.revoked_at.is_(None))
            .values(revoked_at=func.now())
        )
        # execute() is typed as the generic Result; an UPDATE yields a CursorResult.
        return cast(CursorResult[Any], result).rowcount == 1
