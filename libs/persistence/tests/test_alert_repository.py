"""AlertRepository: the alerts row, its outbox ledger row, and the Bus-2
NOTIFY are one atomic unit (grilling #4/#6). A raw LISTEN connection witnesses
that the notify arrives only at commit, carrying the alert id."""

from datetime import UTC, datetime

import psycopg
from sqlalchemy import func, select, text

from rs_domain import Alert
from rs_persistence import (
    AlertRepository,
    AlertRow,
    OutboxRow,
)

ALERT = Alert(
    symbol="SPY",
    alert_type="volume_threshold",
    triggered_at=datetime(2026, 7, 6, 14, 30, tzinfo=UTC),
    payload={"volume": 750_000, "threshold": 500_000},
)


def _alert(symbol: str) -> Alert:
    return ALERT.model_copy(update={"symbol": symbol})


async def _count(session_factory, model) -> int:
    async with session_factory() as s:
        return await s.scalar(select(func.count()).select_from(model))


async def test_insert_writes_alert_and_outbox_atomically(session_factory):
    async with session_factory() as s:
        alert_id = await AlertRepository(s).insert(ALERT)
        await s.commit()

    assert await _count(session_factory, AlertRow) == 1
    assert await _count(session_factory, OutboxRow) == 1
    async with session_factory() as s:
        alert = await s.get(AlertRow, alert_id)
        outbox = await s.scalar(select(OutboxRow))
    assert alert.symbol == "SPY"
    assert alert.payload == {"volume": 750_000, "threshold": 500_000}
    assert alert.posted_at is None  # the bot stamps this on delivery (S004)
    # Ledger row stands alone: id + the full alert content, no join needed. The
    # shape is pinned with literals (not re-derived from ALERT.model_dump) so a
    # serialization change — e.g. triggered_at drifting off ISO-8601 — fails
    # here instead of silently breaking the S004 bot that parses this ledger.
    assert outbox.event_type == "alert_created"
    assert outbox.payload == {
        "alert_id": alert_id,
        "symbol": "SPY",
        "alert_type": "volume_threshold",
        "triggered_at": "2026-07-06T14:30:00Z",
        "payload": {"volume": 750_000, "threshold": 500_000},
    }


async def test_rollback_persists_neither_row(session_factory):
    # A failure before commit must drop the alert AND its outbox row together.
    async with session_factory() as s:
        await AlertRepository(s).insert(ALERT)
        await s.rollback()
    assert await _count(session_factory, AlertRow) == 0
    assert await _count(session_factory, OutboxRow) == 0


async def test_listen_receives_notify_only_at_commit(session_factory, migrated_db_url):
    raw_dsn = migrated_db_url.replace("+psycopg", "")
    async with await psycopg.AsyncConnection.connect(raw_dsn, autocommit=True) as listener:
        await listener.execute("LISTEN alerts")

        async with session_factory() as s:
            alert_id = await AlertRepository(s).insert(ALERT)
            # NOTIFY is transactional — nothing may arrive before the commit.
            assert [n async for n in listener.notifies(timeout=0.3, stop_after=1)] == []
            await s.commit()

        got = [n async for n in listener.notifies(timeout=5, stop_after=1)]

    assert len(got) == 1
    assert got[0].channel == "alerts"
    assert got[0].payload == str(alert_id)  # id only; the row is the truth


async def test_custom_channel_routes_the_notify(session_factory, migrated_db_url):
    # The notify_channel seam must actually move the NOTIFY — guards against a
    # regression that hard-codes 'alerts' into the pg_notify call.
    raw_dsn = migrated_db_url.replace("+psycopg", "")
    async with await psycopg.AsyncConnection.connect(raw_dsn, autocommit=True) as listener:
        await listener.execute("LISTEN custom_alerts")
        async with session_factory() as s:
            alert_id = await AlertRepository(s, notify_channel="custom_alerts").insert(ALERT)
            await s.commit()
        got = [n async for n in listener.notifies(timeout=5, stop_after=1)]
    assert [(n.channel, n.payload) for n in got] == [("custom_alerts", str(alert_id))]


async def test_list_unposted_returns_pending_in_id_order(session_factory):
    # The bot's sweep cursor (S004, ADR-0005): every row with posted_at IS NULL,
    # id order, full drain — a posted row leaves the pending set.
    async with session_factory() as s:
        repo = AlertRepository(s)
        id_a = await repo.insert(_alert("AAA"))
        id_b = await repo.insert(_alert("BBB"))
        id_c = await repo.insert(_alert("CCC"))
        await s.commit()
    async with session_factory() as s:
        await s.execute(text("UPDATE alerts SET posted_at = now() WHERE id = :id"), {"id": id_b})
        await s.commit()

    async with session_factory() as s:
        pending = await AlertRepository(s).list_unposted()

    assert [(p.id, p.alert.symbol) for p in pending] == [(id_a, "AAA"), (id_c, "CCC")]
    # The envelope carries the domain Alert reconstructed at the boundary
    # (UTC-normalized, validated) — not a raw row.
    assert pending[0].alert == _alert("AAA")


async def test_mark_posted_stamps_dbside_and_clears_pending(session_factory):
    async with session_factory() as s:
        alert_id = await AlertRepository(s).insert(ALERT)
        await s.commit()

    async with session_factory() as s:
        stamp = await AlertRepository(s).mark_posted(alert_id)
        await s.commit()

    # DB-side clock (S004 pin #9): the returned stamp is exactly what the row
    # now holds, tz-aware, and the row has left the pending set.
    assert stamp is not None and stamp.tzinfo is not None
    async with session_factory() as s:
        row = await s.get(AlertRow, alert_id)
        assert row.posted_at == stamp
        assert await AlertRepository(s).list_unposted() == []


async def test_mark_posted_is_idempotent(session_factory):
    # Already-marked and never-existed both return None (caller warns); the
    # first stamp is never overwritten — the crash-dupe audit trail survives.
    async with session_factory() as s:
        alert_id = await AlertRepository(s).insert(ALERT)
        await s.commit()

    async with session_factory() as s:
        repo = AlertRepository(s)
        first = await repo.mark_posted(alert_id)
        await s.commit()
        assert await repo.mark_posted(alert_id) is None
        assert await repo.mark_posted(alert_id + 999) is None
        await s.commit()

    async with session_factory() as s:
        row = await s.get(AlertRow, alert_id)
    assert row.posted_at == first


async def test_multiple_alerts_share_one_transaction(session_factory, migrated_db_url):
    # The caller-commits design allows batching a session's alerts; each gets a
    # distinct id, its own outbox row, and its own NOTIFY at the shared commit.
    raw_dsn = migrated_db_url.replace("+psycopg", "")
    async with await psycopg.AsyncConnection.connect(raw_dsn, autocommit=True) as listener:
        await listener.execute("LISTEN alerts")
        async with session_factory() as s:
            repo = AlertRepository(s)
            id_a = await repo.insert(ALERT)
            id_b = await repo.insert(ALERT)
            await s.commit()
        got = [n async for n in listener.notifies(timeout=5, stop_after=2)]

    assert id_a != id_b
    assert await _count(session_factory, AlertRow) == 2
    assert await _count(session_factory, OutboxRow) == 2
    assert sorted(n.payload for n in got) == sorted([str(id_a), str(id_b)])
