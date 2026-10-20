"""rev-002 + SchwabTokenStore: single-row semantics, round-trip, and the
sync schedule_save callback schwab-py fires from inside the event loop."""

import logging

import pytest
from sqlalchemy import create_engine as create_sync_engine
from sqlalchemy import inspect, text

from rs_persistence import SchwabTokenStore, build_engine, build_session_factory

TOKEN_A = {"creation_timestamp": 1, "token": {"access_token": "a", "refresh_token": "r1"}}
TOKEN_B = {"creation_timestamp": 2, "token": {"access_token": "b", "refresh_token": "r2"}}


@pytest.fixture
async def store(migrated_db_url):
    engine = build_engine(migrated_db_url)
    yield SchwabTokenStore(build_session_factory(engine))
    await engine.dispose()


def test_rev002_schema(migrated_db_url):
    engine = create_sync_engine(migrated_db_url)
    try:
        columns = {c["name"] for c in inspect(engine).get_columns("schwab_tokens")}
        assert columns == {"id", "token_data", "updated_at"}
        with engine.connect() as conn:
            with pytest.raises(Exception, match="schwab_tokens_single_row"):
                conn.execute(text("INSERT INTO schwab_tokens (id, token_data) VALUES (2, '{}')"))
    finally:
        engine.dispose()


async def test_load_returns_none_before_first_save(store):
    assert await store.load() is None


async def test_updated_at_tracks_saves(store):
    assert await store.updated_at() is None

    await store.save(TOKEN_A)
    first = await store.updated_at()
    await store.save(TOKEN_B)
    second = await store.updated_at()

    assert first is not None and second is not None
    assert second > first


async def test_save_load_roundtrip_and_upsert(store):
    await store.save(TOKEN_A)
    assert await store.load() == TOKEN_A

    await store.save(TOKEN_B)
    assert await store.load() == TOKEN_B  # same row, replaced


async def test_upsert_bumps_updated_at(store, migrated_db_url):
    await store.save(TOKEN_A)
    engine = build_engine(migrated_db_url)
    try:
        async with engine.connect() as conn:
            first = (await conn.execute(text("SELECT updated_at FROM schwab_tokens"))).scalar_one()
            await conn.execute(text("UPDATE schwab_tokens SET updated_at = updated_at - interval '1 hour'"))
            await conn.commit()
        await store.save(TOKEN_B)
        async with engine.connect() as conn:
            second = (await conn.execute(text("SELECT updated_at FROM schwab_tokens"))).scalar_one()
        assert second >= first
    finally:
        await engine.dispose()


async def test_schedule_save_persists_from_sync_callback(store):
    # schwab-py calls token_write_func synchronously from within the loop,
    # passing extra kwargs on auto-refresh; both must be tolerated.
    store.schedule_save(TOKEN_A, refresh_token="r1")
    await store.drain()
    assert await store.load() == TOKEN_A


async def test_schedule_save_preserves_order(store):
    # Back-to-back refreshes: the later token must win, never be clobbered
    # by an earlier save landing late.
    store.schedule_save(TOKEN_A)
    store.schedule_save(TOKEN_B)
    await store.drain()
    assert await store.load() == TOKEN_B


async def test_schedule_save_failure_is_logged(caplog):
    store = SchwabTokenStore(session_factory=None)

    async def boom(token):
        raise RuntimeError("db down")

    store.save = boom
    with caplog.at_level(logging.ERROR, logger="rs_persistence.token_store"):
        store.schedule_save(TOKEN_A)
        await store.drain()
    assert "token save failed" in caplog.text
    assert not store._pending
