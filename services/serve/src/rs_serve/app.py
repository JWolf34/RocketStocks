"""FastAPI app factory. The engine/session factory live on app.state so the
middleware and routes share one pool; the lifespan only disposes it (creation
is eager, so test clients that skip lifespan startup still get a working app).
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from rs_persistence import build_engine, build_session_factory
from rs_serve.auth import ApiKeyMiddleware
from rs_serve.bars import router as bars_router
from rs_serve.settings import ServeSettings


def create_app(settings: ServeSettings | None = None) -> FastAPI:
    settings = settings or ServeSettings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        await app.state.engine.dispose()

    app = FastAPI(title="rs-serve", lifespan=lifespan)
    app.state.settings = settings
    app.state.engine = build_engine(settings.rs_db_url)
    app.state.session_factory = build_session_factory(app.state.engine)
    app.add_middleware(ApiKeyMiddleware)
    app.include_router(bars_router)
    return app
