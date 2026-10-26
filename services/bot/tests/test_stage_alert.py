"""stage_alert commits through the real AlertRepository path:
the staged row lands unposted (so the sweep will deliver it) and is clearly
marked as a drill, and the transactional NOTIFY fires - witnessed by a raw
LISTEN connection, same as the acceptance drills rely on."""

import psycopg

from rs_bot.tools.stage_alert import stage
from rs_persistence import AlertRepository


async def test_stage_commits_an_unposted_marked_alert_with_notify(migrated_db_url, session_factory):
    raw_dsn = migrated_db_url.replace("+psycopg", "")
    async with await psycopg.AsyncConnection.connect(raw_dsn, autocommit=True) as listener:
        await listener.execute("LISTEN alerts")
        alert_id = await stage(session_factory)
        got = [n async for n in listener.notifies(timeout=5, stop_after=1)]
    assert [n.payload for n in got] == [str(alert_id)]

    async with session_factory() as s:
        pending = await AlertRepository(s).list_undelivered()
    assert [p.id for p in pending] == [alert_id]
    assert pending[0].alert.symbol.startswith("TEST-")
    assert pending[0].alert.trigger == "staged_test"
    assert pending[0].alert.triggered_at.tzinfo is not None
