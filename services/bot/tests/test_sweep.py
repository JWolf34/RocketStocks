"""SweepWorker: the single posting path (spec pins #3/#4). The sweep drains
list_unposted in id order; per row render → send → mark_posted → commit. Send
failures skip-and-continue with uncapped retry each sweep (pin #5b). The wake
event triggers an off-interval sweep; NOTIFY itself never posts.

Sender is faked at the send-protocol seam — these tests never touch Discord;
the cursor semantics run against the real ephemeral Postgres (ADR-0023 tier)."""

import asyncio
import logging
from datetime import datetime, timezone

import pytest

from rs_domain import Alert
from rs_persistence import AlertRepository

from rs_bot.counters import new_counters
from rs_bot.sweep import SweepWorker
from rs_observability.testing import captured_json_stream, json_records


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


# -- S006 unit of work: one posting attempt = one correlation_id ---------------


@pytest.fixture
def json_stream():
    with captured_json_stream("bot") as buffer:
        yield buffer


class LoggingSender(FakeSender):
    """Send-protocol fake that logs like discord.py's internals do — the
    'retry internals group under the alert' claim needs a third-party-style
    record emitted inside the attempt."""

    async def send(self, embed):
        logging.getLogger("discord.http").warning("rate limited, retrying")
        return await super().send(embed)


async def test_posting_attempt_records_share_one_correlation_id(session_factory, json_stream):
    (alert_id,) = await _insert(session_factory, _alert("SPY"))
    worker = SweepWorker(session_factory, LoggingSender(), sweep_seconds=60, counters=new_counters())

    await worker.sweep()

    records = json_records(json_stream)
    (discord_record,) = [r for r in records if r["logger"] == "discord.http"]
    (posted_record,) = [r for r in records if r["event"].startswith("posted alert")]
    assert posted_record["alert_id"] == alert_id  # the cross-service join key
    assert posted_record["correlation_id"]
    # discord.py's record groups under the same attempt: same id, same alert.
    assert discord_record["correlation_id"] == posted_record["correlation_id"]
    assert discord_record["alert_id"] == alert_id
    assert posted_record["service"] == "bot"


async def test_each_posting_attempt_is_its_own_unit_of_work(session_factory, json_stream):
    await _insert(session_factory, _alert("AAA"), _alert("BBB"))
    worker = SweepWorker(session_factory, FakeSender(), sweep_seconds=60, counters=new_counters())

    await worker.sweep()

    first, second = [r for r in json_records(json_stream) if r["event"].startswith("posted alert")]
    assert first["correlation_id"] != second["correlation_id"]
    assert first["alert_id"] != second["alert_id"]


async def test_send_failure_record_is_correlated_too(session_factory, json_stream):
    (alert_id,) = await _insert(session_factory, _alert("SPY"))
    sender = FakeSender(fail_when=lambda embed: True)
    worker = SweepWorker(session_factory, sender, sweep_seconds=60, counters=new_counters())

    await worker.sweep()

    (record,) = [r for r in json_records(json_stream) if r["event"].startswith("send failed")]
    assert record["level"] == "error"
    assert record["alert_id"] == alert_id
    assert record["correlation_id"]
    assert "Traceback" in record["exception"]
