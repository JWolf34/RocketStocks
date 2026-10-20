"""API-key auth (ADR-0012, S005 enforcement depth).

X-API-Key → SHA-256 → active service_clients row. Absent, unknown, and
revoked keys all produce the same 401 body — a caller can never learn whether
a key exists. Scope/rate-limit enforcement is Phase 3; every route (\
/metrics included — ADR-0015) sits behind this middleware.

SHA-256 rather than a slow hash is deliberate (S005 spec): keys are 256-bit
random values (secrets.token_urlsafe), so there is nothing to brute-force;
the hash only keeps a DB leak from disclosing usable keys.
"""

import hashlib

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from rs_persistence import ServiceClientRepository

_UNAUTHORIZED = {"detail": "unauthorized"}


def hash_api_key(key: str) -> str:
    """The single hash convention both the middleware and the provisioning
    CLI use; Phase 3's scope enforcement must keep matching it."""
    return hashlib.sha256(key.encode()).hexdigest()


class ApiKeyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        key = request.headers.get("X-API-Key")
        if key is None:
            return JSONResponse(_UNAUTHORIZED, status_code=401)
        async with request.app.state.session_factory() as session:
            client = await ServiceClientRepository(session).get_active_by_hash(hash_api_key(key))
        if client is None:
            return JSONResponse(_UNAUTHORIZED, status_code=401)
        return await call_next(request)
