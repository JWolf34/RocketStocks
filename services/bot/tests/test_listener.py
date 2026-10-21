"""ListenLoop: the Finding-3 witness. A dedicated autocommit connection holds
LISTEN; a NOTIFY only sets the sweep's wake event (never posts). Every
(re)connect sets the wake too, so a severed connection self-heals into a
recovery sweep rather than a silent dead LISTEN — the structural cover for
the acceptance-2026-07-09 Finding-3 failure class.

Real ephemeral Postgres; NOTIFYs are staged through the real AlertRepository
transactional path."""

import asyncio
import contextlib
from datetime import UTC, datetime

import pytest
from sqlalchemy import text

from rs_bot.counters import new_counters
from rs_bot.listener import ListenLoop
from rs_domain import Alert
from rs_persistence import AlertRepository

ALERT = Alert(
    symbol="SPY",
    alert_type="volume_threshold",
    triggered_at=datetime(2026, 7, 6, 14, 30, tzinfo=UTC),
    payload={"volume": 750_000, "threshold": 500_000},
)


async def _wait_for(event: asyncio.Event, timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        await event.wait()


async def _commit_alert(session_factory) -> int:
    async with session_factory() as s:
        alert_id = await AlertRepository(s).insert(ALERT)
        await s.commit()
    return alert_id


@pytest.fixture
async def running_loop(migrated_db_url, session_factory):
    wake = asyncio.Event()
    counters = new_counters()
    loop = ListenLoop(
        migrated_db_url.replace("+psycopg", ""),
        wake,
        counters=counters,
        initial_backoff=0.1,
        max_backoff=0.5,
    )
    task = asyncio.create_task(loop.run())
    # Connect is signalled through the wake event itself (sweep-on-connect).
    await _wait_for(wake)
    wake.clear()
    yield wake, counters, session_factory
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def test_commit_wakes_the_sweep(running_loop):
    wake, counters, session_factory = running_loop
    await _commit_alert(session_factory)
    await _wait_for(wake)
    assert counters["notify_wakes_total"] == 1
    assert counters["listen_reconnects_total"] == 0


async def test_severed_listen_self_heals(running_loop):
    # The Finding-3 drill: kill the listener's backend out from under it.
    # pg_stat_activity is server-wide: scope to this test's database or the kill
    # also severs LISTEN connections of concurrent pytest processes on the server.
    wake, counters, session_factory = running_loop
    async with session_factory() as s:
        await s.execute(
            text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE query ILIKE 'LISTEN%' AND datname = current_database()"
                " AND pid <> pg_backend_pid()"
            )
        )
        await s.commit()

    # Reconnect is visible in the counter AND sets the wake (recovery sweep).
    await _wait_for(wake)
    assert counters["listen_reconnects_total"] >= 1

    # The healed connection still delivers: a fresh commit wakes again.
    wake.clear()
    await _commit_alert(session_factory)
    await _wait_for(wake)
    assert counters["notify_wakes_total"] == 1
