"""ServiceClientRepository: the auth storage seam (S005, ADR-0012).

The repository speaks hashes only — key generation and hashing are rs_serve's
concern. rev-003 is proven by the conftest's upgrade-to-head plus these
round-trips against the real table.
"""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from rs_persistence import ServiceClientRepository, build_engine, build_session_factory

HASH_A = "a" * 64  # SHA-256 hex digests are 64 chars; opaque to this layer
HASH_B = "b" * 64


@pytest.fixture
async def session_factory(migrated_db_url):
    engine = build_engine(migrated_db_url)
    factory = build_session_factory(engine)
    async with factory() as s:
        await s.execute(text("TRUNCATE service_clients RESTART IDENTITY"))
        await s.commit()
    yield factory
    await engine.dispose()


async def test_provision_then_lookup_by_hash(session_factory):
    async with session_factory() as s:
        repo = ServiceClientRepository(s)
        await repo.provision(name="bot", api_key_hash=HASH_A)
        await s.commit()

    async with session_factory() as s:
        client = await ServiceClientRepository(s).get_active_by_hash(HASH_A)
    assert client is not None
    assert client.name == "bot"
    assert client.revoked_at is None


async def test_unknown_hash_returns_none(session_factory):
    async with session_factory() as s:
        assert await ServiceClientRepository(s).get_active_by_hash(HASH_A) is None


async def test_revoked_client_no_longer_resolves(session_factory):
    async with session_factory() as s:
        repo = ServiceClientRepository(s)
        await repo.provision(name="bot", api_key_hash=HASH_A)
        await s.commit()

    async with session_factory() as s:
        repo = ServiceClientRepository(s)
        assert await repo.revoke("bot") is True
        await s.commit()

    async with session_factory() as s:
        assert await ServiceClientRepository(s).get_active_by_hash(HASH_A) is None


async def test_revoke_unknown_name_returns_false(session_factory):
    async with session_factory() as s:
        assert await ServiceClientRepository(s).revoke("ghost") is False


async def test_duplicate_name_rejected(session_factory):
    # name is UNIQUE (rev-003): re-provisioning an existing consumer must fail
    # loudly, not silently mint a second key row.
    async with session_factory() as s:
        repo = ServiceClientRepository(s)
        await repo.provision(name="bot", api_key_hash=HASH_A)
        await s.commit()

    async with session_factory() as s:
        with pytest.raises(IntegrityError):
            await ServiceClientRepository(s).provision(name="bot", api_key_hash=HASH_B)
            await s.commit()
