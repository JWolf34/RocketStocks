"""AlertRepository: the alerts row and its NOTIFY are one atomic unit.
A raw LISTEN connection witnesses that the notify arrives only
at commit, carrying the alert id."""

from datetime import UTC, date, datetime

import psycopg
import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

from rs_domain import Alert
from rs_persistence import AlertRepository, AlertRow
from rs_persistence.alerts import alert_from_row

ALERT = Alert(
    symbol="SPY",
    trigger="volume_threshold",
    triggered_at=datetime(2026, 7, 6, 14, 30, tzinfo=UTC),
    payload={"volume": 750_000, "threshold": 500_000},
)


def _alert(symbol: str) -> Alert:
    return ALERT.model_copy(update={"symbol": symbol})


async def _count(session_factory, model) -> int:
    async with session_factory() as s:
        return await s.scalar(select(func.count()).select_from(model))


async def test_insert_writes_the_alert_row(session_factory):
    async with session_factory() as s:
        alert_id = await AlertRepository(s).insert(ALERT)
        await s.commit()

    assert await _count(session_factory, AlertRow) == 1
    async with session_factory() as s:
        row = await s.get(AlertRow, alert_id)
    assert row.symbol == "SPY"
    assert row.payload == {"volume": 750_000, "threshold": 500_000}
    assert row.posted_at is None  # the bot stamps this on delivery
    assert row.post_attempts == 0
    assert (row.rule_version, row.alert_price, row.quote_time, row.reply_to) == (None,) * 4


async def test_insert_stores_the_rule_fields(session_factory):
    quote_time = datetime(2026, 7, 6, 14, 29, 58, tzinfo=UTC)
    versioned = ALERT.model_copy(
        update={
            "rule_version": "1",
            "alert_price": 123.45,
            "quote_time": quote_time,
        }
    )
    async with session_factory() as s:
        await AlertRepository(s).insert(versioned)
        await s.commit()

    async with session_factory() as s:
        assert alert_from_row((await s.scalars(select(AlertRow))).one()) == versioned


async def _insert_committed(session_factory, alert: Alert) -> int:
    async with session_factory() as s:
        alert_id = await AlertRepository(s).insert(alert)
        await s.commit()
    return alert_id


@pytest.mark.parametrize(
    ("triggered_at", "day"),
    [
        (datetime(2026, 7, 7, 3, 59, tzinfo=UTC), date(2026, 7, 6)),  # 23:59 EDT
        (datetime(2026, 7, 7, 4, 0, tzinfo=UTC), date(2026, 7, 7)),  # midnight EDT
        (datetime(2026, 12, 8, 4, 59, tzinfo=UTC), date(2026, 12, 7)),  # 23:59 EST
        (datetime(2026, 12, 8, 5, 0, tzinfo=UTC), date(2026, 12, 8)),  # midnight EST
    ],
)
async def test_the_day_is_the_us_eastern_day_of_the_trigger_time(session_factory, triggered_at, day):
    alert_id = await _insert_committed(session_factory, ALERT.model_copy(update={"triggered_at": triggered_at}))

    async with session_factory() as s:
        assert (await s.get(AlertRow, alert_id)).day == day


async def test_a_second_alert_for_a_stock_on_one_eastern_day_is_refused_and_notifies_nothing(
    session_factory, migrated_db_url
):
    first_alert = ALERT.model_copy(update={"triggered_at": datetime(2026, 7, 7, 4, 30, tzinfo=UTC)})
    # 22:30 EDT the same Eastern day, though the next UTC day.
    later_alert = first_alert.model_copy(update={"triggered_at": datetime(2026, 7, 8, 2, 30, tzinfo=UTC)})
    raw_dsn = migrated_db_url.replace("+psycopg", "")
    async with await psycopg.AsyncConnection.connect(raw_dsn, autocommit=True) as listener:
        await listener.execute("LISTEN alerts")
        first = await _insert_committed(session_factory, first_alert)
        with pytest.raises(IntegrityError):
            await _insert_committed(session_factory, later_alert)
        # NOTIFYs reach a listener in commit order, so once the sentinel is
        # here, any notify the refused insert had wrongly sent would be too.
        async with session_factory() as s:
            await s.execute(select(func.pg_notify("alerts", "sentinel")))
            await s.commit()
        got = [n async for n in listener.notifies(timeout=30, stop_after=2)]

    assert await _count(session_factory, AlertRow) == 1
    assert [n.payload for n in got] == [str(first), "sentinel"]


async def test_other_stocks_other_days_and_replies_do_not_conflict(session_factory):
    base = ALERT.model_copy(update={"triggered_at": datetime(2026, 7, 6, 14, 30, tzinfo=UTC)})
    parent = await _insert_committed(session_factory, base)

    ids = [
        await _insert_committed(session_factory, base.model_copy(update={"symbol": "QQQ"})),
        await _insert_committed(
            session_factory, base.model_copy(update={"triggered_at": datetime(2026, 7, 7, 14, 30, tzinfo=UTC)})
        ),
        await _insert_committed(session_factory, base.model_copy(update={"reply_to": parent})),
    ]

    assert len({parent, *ids}) == 4


async def test_a_reply_is_stored_pointing_at_its_alert(session_factory):
    parent = await _insert_committed(session_factory, ALERT)
    reply = await _insert_committed(session_factory, ALERT.model_copy(update={"reply_to": parent}))

    async with session_factory() as s:
        pending = await AlertRepository(s).list_unposted()
    assert [(p.id, p.alert.reply_to) for p in pending] == [(parent, None), (reply, parent)]


async def test_alerted_symbols_names_stocks_with_an_alert_on_that_eastern_day(session_factory):
    day = datetime(2026, 7, 6, 14, 30, tzinfo=UTC)
    async with session_factory() as s:
        repo = AlertRepository(s)
        await repo.insert(ALERT.model_copy(update={"symbol": "AAA", "triggered_at": day}))
        await repo.insert(ALERT.model_copy(update={"symbol": "BBB", "triggered_at": datetime(2026, 7, 7, 14, 0, tzinfo=UTC)}))
        await s.commit()

    async with session_factory() as s:
        repo = AlertRepository(s)
        assert await repo.alerted_symbols(date(2026, 7, 6), {"AAA", "BBB", "CCC"}) == {"AAA"}
        assert await repo.alerted_symbols(date(2026, 7, 7), {"AAA", "BBB"}) == {"BBB"}
        assert await repo.alerted_symbols(date(2026, 7, 8), {"AAA"}) == set()


async def test_rollback_persists_nothing(session_factory):
    async with session_factory() as s:
        await AlertRepository(s).insert(ALERT)
        await s.rollback()
    assert await _count(session_factory, AlertRow) == 0


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
    # The bot's sweep cursor: every row with posted_at IS NULL,
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

    # DB-side clock: the returned stamp is exactly what the row
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
    # distinct id and its own NOTIFY at the shared commit.
    raw_dsn = migrated_db_url.replace("+psycopg", "")
    async with await psycopg.AsyncConnection.connect(raw_dsn, autocommit=True) as listener:
        await listener.execute("LISTEN alerts")
        async with session_factory() as s:
            repo = AlertRepository(s)
            id_a = await repo.insert(_alert("AAA"))
            id_b = await repo.insert(_alert("BBB"))
            await s.commit()
        got = [n async for n in listener.notifies(timeout=5, stop_after=2)]

    assert id_a != id_b
    assert await _count(session_factory, AlertRow) == 2
    assert sorted(n.payload for n in got) == sorted([str(id_a), str(id_b)])


async def test_increment_post_attempts_counts_and_caps_the_sweep(session_factory):
    async with session_factory() as s:
        repo = AlertRepository(s)
        id_a = await repo.insert(_alert("AAA"))
        id_b = await repo.insert(_alert("BBB"))
        await s.commit()

    async with session_factory() as s:
        repo = AlertRepository(s)
        assert await repo.increment_post_attempts(id_a) == 1
        assert await repo.increment_post_attempts(id_a) == 2
        await s.commit()

    async with session_factory() as s:
        repo = AlertRepository(s)
        pending = await repo.list_unposted()
        assert [(p.id, p.post_attempts) for p in pending] == [(id_a, 2), (id_b, 0)]
        # At the cap the alert leaves the sweep but stays unposted.
        assert [p.id for p in await repo.list_unposted(max_attempts=2)] == [id_b]
        assert [p.id for p in await repo.list_unposted(max_attempts=3)] == [id_a, id_b]


async def test_increment_post_attempts_ignores_posted_and_unknown(session_factory):
    async with session_factory() as s:
        repo = AlertRepository(s)
        alert_id = await repo.insert(ALERT)
        await repo.mark_posted(alert_id)
        await s.commit()

    async with session_factory() as s:
        repo = AlertRepository(s)
        assert await repo.increment_post_attempts(alert_id) is None
        assert await repo.increment_post_attempts(alert_id + 999) is None
