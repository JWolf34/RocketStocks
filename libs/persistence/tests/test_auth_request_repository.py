"""AuthRequestRepository: the /setup schwab state machine."""

from datetime import timedelta

from sqlalchemy import func, select, text

from rs_domain import AuthRequestState
from rs_persistence import AuthRequestRepository

ADMIN = 1234567890123456789  # a Discord snowflake exceeds 32 bits


async def test_create_returns_a_requested_row(session_factory):
    async with session_factory() as s:
        request = await AuthRequestRepository(s).create(ADMIN)
        await s.commit()

    assert request.state is AuthRequestState.REQUESTED
    assert request.requested_by == ADMIN
    assert (request.auth_url, request.callback_url, request.error, request.completed_at) == (None,) * 4


async def test_happy_path(session_factory):
    async with session_factory() as s:
        repo = AuthRequestRepository(s)
        request = await repo.create(ADMIN)
        assert await repo.set_auth_url(request.id, "https://schwab.example/auth") is True
        assert await repo.set_callback_url(request.id, "https://127.0.0.1/?code=abc") is True
        mid = await repo.get(request.id)
        assert mid is not None
        assert mid.state is AuthRequestState.CALLBACK_RECEIVED
        assert (mid.auth_url, mid.callback_url) == (
            "https://schwab.example/auth",
            "https://127.0.0.1/?code=abc",
        )
        assert await repo.complete(request.id) is True
        await s.commit()

    async with session_factory() as s:
        done = await AuthRequestRepository(s).get(request.id)
    assert done is not None
    assert done.state is AuthRequestState.DONE
    assert done.completed_at is not None
    assert done.callback_url is None  # the single-use OAuth code is not kept


async def test_transitions_only_run_from_the_expected_state(session_factory):
    async with session_factory() as s:
        repo = AuthRequestRepository(s)
        request = await repo.create(ADMIN)
        # Out-of-order calls are no-ops.
        assert await repo.set_callback_url(request.id, "cb") is False
        assert await repo.complete(request.id) is False
        assert await repo.set_auth_url(request.id, "url") is True
        assert await repo.set_auth_url(request.id, "again") is False
        current = await repo.get(request.id)
        assert current is not None and current.auth_url == "url"
        # Unknown ids are no-ops too.
        assert await repo.set_auth_url(request.id + 999, "url") is False


async def test_fail_records_the_error_from_any_open_state_and_is_terminal(session_factory):
    async with session_factory() as s:
        repo = AuthRequestRepository(s)
        request = await repo.create(ADMIN)
        await repo.set_auth_url(request.id, "url")
        await repo.set_callback_url(request.id, "cb")
        assert await repo.fail(request.id, "bad callback URL") is True
        failed = await repo.get(request.id)
        assert failed is not None
        assert (failed.state, failed.error, failed.callback_url) == (
            AuthRequestState.FAILED,
            "bad callback URL",
            None,
        )
        assert failed.completed_at is not None
        assert await repo.fail(request.id, "again") is False
        assert await repo.complete(request.id) is False

        early = await repo.create(ADMIN)
        assert await repo.fail(early.id, "no auth url") is True


async def test_expire_older_than_only_touches_open_requests_before_the_cutoff(session_factory):
    async with session_factory() as s:
        repo = AuthRequestRepository(s)
        old_open = await repo.create(ADMIN)
        old_done = await repo.create(ADMIN)
        await repo.set_auth_url(old_done.id, "url")
        await repo.set_callback_url(old_done.id, "cb")
        await repo.complete(old_done.id)
        fresh = await repo.create(ADMIN)
        await s.execute(
            text("UPDATE schwab_auth_request SET created_at = now() - interval '1 hour' WHERE id <> :id"),
            {"id": fresh.id},
        )
        await s.commit()
        cutoff = await s.scalar(select(func.now() - timedelta(minutes=30)))

    async with session_factory() as s:
        repo = AuthRequestRepository(s)
        assert await repo.expire_older_than(cutoff - timedelta(hours=1)) == 0
        assert await repo.expire_older_than(cutoff) == 1
        assert await repo.expire_older_than(cutoff) == 0
        await s.commit()
        expired = await repo.get(old_open.id)
        assert expired is not None and expired.state is AuthRequestState.EXPIRED
        assert expired.completed_at is not None
        assert (await repo.get(old_done.id)).state is AuthRequestState.DONE
        assert (await repo.get(fresh.id)).state is AuthRequestState.REQUESTED


async def test_list_in_state_is_oldest_first(session_factory):
    async with session_factory() as s:
        repo = AuthRequestRepository(s)
        a = await repo.create(ADMIN)
        b = await repo.create(ADMIN)
        c = await repo.create(ADMIN)
        await repo.set_auth_url(b.id, "url")
        assert [r.id for r in await repo.list_in_state(AuthRequestState.REQUESTED)] == [a.id, c.id]
        assert [r.id for r in await repo.list_in_state(AuthRequestState.URL_READY)] == [b.id]
        assert await repo.list_in_state(AuthRequestState.DONE) == []


async def test_create_and_callback_notify_the_ingest_channel(migrated_db_url):
    import psycopg

    from rs_persistence import AUTH_NOTIFY_CHANNEL, build_engine, build_session_factory

    conninfo = migrated_db_url.replace("+psycopg", "", 1)
    engine = build_engine(migrated_db_url)
    try:
        async with await psycopg.AsyncConnection.connect(conninfo, autocommit=True) as listener:
            await listener.execute(f'LISTEN "{AUTH_NOTIFY_CHANNEL}"')
            async with build_session_factory(engine)() as s:
                repo = AuthRequestRepository(s)
                request = await repo.create(ADMIN)
                await s.commit()  # one transaction would collapse the identical notifies
                await repo.set_auth_url(request.id, "https://schwab.example/auth")
                await repo.set_callback_url(request.id, "https://127.0.0.1/?code=abc")
                await s.commit()
            payloads = []
            async for note in listener.notifies(timeout=2, stop_after=2):
                payloads.append(note.payload)
    finally:
        await engine.dispose()

    assert payloads == [str(request.id), str(request.id)]
