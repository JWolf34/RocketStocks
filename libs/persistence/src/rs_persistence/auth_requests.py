"""schwab_auth_request: the `/setup schwab` handshake between bot and ingest.

State machine (each transition is guarded by the expected current state, so a
stale or repeated call is a no-op returning False):

    requested --set_auth_url--> url_ready --set_callback_url--> callback_received
    callback_received --complete--> done
    requested | url_ready | callback_received --fail--> failed
    requested | url_ready | callback_received --expire_older_than--> expired

The callback URL carries a single-use OAuth code: it is cleared when the request
reaches a terminal state, and callers must never log it.
"""

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import AuthRequestState, SchwabAuthRequest
from rs_persistence.models import SchwabAuthRequestRow

_OPEN = (
    AuthRequestState.REQUESTED,
    AuthRequestState.URL_READY,
    AuthRequestState.CALLBACK_RECEIVED,
)


def _to_request(row: SchwabAuthRequestRow) -> SchwabAuthRequest:
    return SchwabAuthRequest(
        id=row.id,
        created_at=row.created_at.astimezone(UTC),
        requested_by=row.requested_by,
        state=AuthRequestState(row.state),
        auth_url=row.auth_url,
        callback_url=row.callback_url,
        error=row.error,
        completed_at=None if row.completed_at is None else row.completed_at.astimezone(UTC),
    )


class AuthRequestRepository:
    """Caller owns the commit."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, requested_by: int) -> SchwabAuthRequest:
        """Open a request for a Discord user id; created_at is the DB clock."""
        row = SchwabAuthRequestRow(requested_by=requested_by)
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
        return _to_request(row)

    async def get(self, request_id: int) -> SchwabAuthRequest | None:
        row = await self._session.get(SchwabAuthRequestRow, request_id)
        return None if row is None else _to_request(row)

    async def list_in_state(self, state: AuthRequestState) -> list[SchwabAuthRequest]:
        """Requests in one state, oldest first - how ingest finds work to do."""
        rows = await self._session.scalars(
            select(SchwabAuthRequestRow)
            .where(SchwabAuthRequestRow.state == state.value)
            .order_by(SchwabAuthRequestRow.id)
        )
        return [_to_request(row) for row in rows]

    async def set_auth_url(self, request_id: int, auth_url: str) -> bool:
        return await self._move(
            request_id,
            (AuthRequestState.REQUESTED,),
            AuthRequestState.URL_READY,
            auth_url=auth_url,
        )

    async def set_callback_url(self, request_id: int, callback_url: str) -> bool:
        return await self._move(
            request_id,
            (AuthRequestState.URL_READY,),
            AuthRequestState.CALLBACK_RECEIVED,
            callback_url=callback_url,
        )

    async def complete(self, request_id: int) -> bool:
        return await self._move(
            request_id,
            (AuthRequestState.CALLBACK_RECEIVED,),
            AuthRequestState.DONE,
            callback_url=None,
            completed_at=func.now(),
        )

    async def fail(self, request_id: int, error: str) -> bool:
        """Record why the exchange failed; `error` is shown to the user, so it must
        not contain the callback URL or any token."""
        return await self._move(
            request_id,
            _OPEN,
            AuthRequestState.FAILED,
            error=error,
            callback_url=None,
            completed_at=func.now(),
        )

    async def expire_older_than(self, cutoff: datetime) -> int:
        """Expire open requests created before `cutoff` (the caller's clock minus
        the request window); returns how many expired."""
        expired = await self._session.scalars(
            update(SchwabAuthRequestRow)
            .where(
                SchwabAuthRequestRow.state.in_([s.value for s in _OPEN]),
                SchwabAuthRequestRow.created_at < cutoff,
            )
            .values(
                state=AuthRequestState.EXPIRED.value,
                callback_url=None,
                completed_at=func.now(),
            )
            .returning(SchwabAuthRequestRow.id)
        )
        return len(expired.all())

    async def _move(
        self,
        request_id: int,
        from_states: tuple[AuthRequestState, ...],
        to_state: AuthRequestState,
        **values: Any,
    ) -> bool:
        moved = await self._session.scalar(
            update(SchwabAuthRequestRow)
            .where(
                SchwabAuthRequestRow.id == request_id,
                SchwabAuthRequestRow.state.in_([s.value for s in from_states]),
            )
            .values(state=to_state.value, **values)
            .returning(SchwabAuthRequestRow.id)
        )
        return moved is not None
