"""SweepWorker: the single posting path (spec pins #3/#4). The sweep drains
list_unposted in id order; per row render → send → mark_posted → commit. Send
failures skip-and-continue with uncapped retry each sweep (pin #5b). The wake
event triggers an off-interval sweep; NOTIFY itself never posts.

Sender is faked at the send-protocol seam — these tests never touch Discord;
the cursor semantics run against the real ephemeral Postgres (ADR-0023 tier)."""

import asyncio
from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from rs_domain import Alert
from rs_persistence import AlertRepository, build_engine, build_session_factory

from rs_bot.counters import new_counters
from rs_bot.sweep import SweepWorker


def _alert(symbol: str) -> Alert:
    return Alert(
        symbol=symbol,
        alert_type="volume_threshold",
        triggered_at=datetime(2026, 7, 6, 14, 30, tzinfo=timezone.utc),
        payload={"volume": 750_000, "threshold": 500_000},
    )


class FakeSender:
    """Send-protocol fake: records embeds, mints message ids, optionally fails
    on a planted predicate (the poison-skip drills)."""

    def __init__(self, fail_when=None):
        self.sent = []
        self.fail_when = fail_when
        self._next_id = 1000

    async def send(self, embed):
        if self.fail_when is not None and self.fail_when(embed):
            raise RuntimeError("planted send failure")
        self.sent.append(embed)
        self._next_id += 1
        return self._next_id


@pytest.fixture
async def session_factory(migrated_db_url):
    engine = build_engine(migrated_db_url)
    factory = build_session_factory(engine)
    async with factory() as s:
        await s.execute(text("TRUNCATE alerts, outbox RESTART IDENTITY"))
        await s.commit()
    yield factory
    await engine.dispose()


async def _insert(session_factory, *alerts: Alert) -> list[int]:
    async with session_factory() as s:
        repo = AlertRepository(s)
        ids = [await repo.insert(a) for a in alerts]
        await s.commit()
    return ids


async def _unposted_ids(session_factory) -> list[int]:
    async with session_factory() as s:
        return [p.id for p in await AlertRepository(s).list_unposted()]


async def test_sweep_drains_backlog_in_id_order_and_stamps(session_factory):
    await _insert(session_factory, _alert("AAA"), _alert("BBB"), _alert("CCC"))
    sender = FakeSender()
    worker = SweepWorker(session_factory, sender, sweep_seconds=60, counters=new_counters())

    await worker.sweep()

    assert [e.title for e in sender.sent] == [
        "AAA · volume_threshold",
        "BBB · volume_threshold",
        "CCC · volume_threshold",
    ]
    assert await _unposted_ids(session_factory) == []
    assert worker.counters["discord_posts_total"] == 3
    assert worker.counters["sweeps_total"] == 1
    assert worker.counters["post_failures_total"] == 0

    # An immediate second sweep posts nothing — the cursor holds.
    await worker.sweep()
    assert len(sender.sent) == 3
    assert worker.counters["discord_posts_total"] == 3


async def test_poison_send_skips_row_and_retries_next_sweep(session_factory):
    ids = await _insert(session_factory, _alert("AAA"), _alert("BAD"), _alert("CCC"))
    sender = FakeSender(fail_when=lambda e: "BAD" in e.title)
    worker = SweepWorker(session_factory, sender, sweep_seconds=60, counters=new_counters())

    await worker.sweep()

    # The failing row is skipped, healthy rows still post (pin #5b)...
    assert [e.title for e in sender.sent] == [
        "AAA · volume_threshold",
        "CCC · volume_threshold",
    ]
    assert worker.counters["post_failures_total"] == 1
    assert worker.counters["discord_posts_total"] == 2
    # ...and stays on the cursor for the next sweep (uncapped retry).
    assert await _unposted_ids(session_factory) == [ids[1]]

    sender.fail_when = None
    await worker.sweep()
    assert sender.sent[-1].title == "BAD · volume_threshold"
    assert await _unposted_ids(session_factory) == []


async def test_run_sweeps_on_wake_without_waiting_interval(session_factory):
    sender = FakeSender()
    # Interval far beyond the test timeout: only the wake can trigger the post.
    worker = SweepWorker(session_factory, sender, sweep_seconds=300, counters=new_counters())
    task = asyncio.create_task(worker.run())
    try:
        # run() opens with the unconditional initial sweep.
        async with asyncio.timeout(5):
            while worker.counters["sweeps_total"] < 1:
                await asyncio.sleep(0.01)

        await _insert(session_factory, _alert("AAA"))
        worker.wake.set()
        async with asyncio.timeout(5):
            while not sender.sent:
                await asyncio.sleep(0.01)
        assert sender.sent[0].title == "AAA · volume_threshold"
    finally:
        worker.request_stop()
        async with asyncio.timeout(5):
            await task
    assert await _unposted_ids(session_factory) == []
