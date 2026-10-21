"""SweepWorker: the single posting path. The sweep drains
list_unposted in id order; per row render → send → mark_posted → commit. Send
failures skip-and-continue and are retried each sweep up to a cap, then the
alert is abandoned with one ops notice. The wake
event triggers an off-interval sweep; NOTIFY itself never posts.

Sender is faked at the send-protocol seam — these tests never touch Discord;
the cursor semantics run against the real ephemeral Postgres."""

import asyncio
import json
import logging
from datetime import UTC, datetime
from types import SimpleNamespace

import aiohttp
import discord
import httpx
import pytest

from rs_bot.counters import new_counters
from rs_bot.sweep import SweepWorker
from rs_domain import (
    Alert,
    FactUnavailable,
    PopularitySurgePayload,
    SurgeFacts,
    TrackRecord,
    TrackRecordInput,
)
from rs_observability import HealthTracker, OpsSettings, configure_ops
from rs_observability.testing import captured_json_stream, json_records
from rs_persistence import AlertRepository, TrackRecordRepository


def _alert(symbol: str) -> Alert:
    return Alert(
        symbol=symbol,
        alert_type="volume_threshold",
        triggered_at=datetime(2026, 7, 6, 14, 30, tzinfo=UTC),
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
    worker = SweepWorker(session_factory, sender, sweep_seconds=60, health=HealthTracker("bot"), counters=new_counters())

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
    worker = SweepWorker(
        session_factory, sender, sweep_seconds=60, health=HealthTracker("bot"), retry_gap_seconds=0, counters=new_counters()
    )

    await worker.sweep()

    # The failing row is skipped, healthy rows still post...
    assert [e.title for e in sender.sent] == [
        "AAA · volume_threshold",
        "CCC · volume_threshold",
    ]
    assert worker.counters["post_failures_total"] == 1
    assert worker.counters["discord_posts_total"] == 2
    # ...and stays on the cursor for the next sweep (retried while under the cap).
    assert await _unposted_ids(session_factory) == [ids[1]]

    sender.fail_when = None
    await worker.sweep()
    assert sender.sent[-1].title == "BAD · volume_threshold"
    assert await _unposted_ids(session_factory) == []


@pytest.fixture
def ops_messages():
    messages: list[str] = []

    def webhook(request: httpx.Request) -> httpx.Response:
        messages.append(json.loads(request.content)["content"])
        return httpx.Response(204)

    settings = OpsSettings(_env_file=None, rs_ops_webhook_url="https://discord.test/api/webhooks/1/t")
    configure_ops(settings, transport=httpx.MockTransport(webhook))
    yield messages
    configure_ops(OpsSettings(_env_file=None))


async def test_deterministic_failure_stops_at_the_cap_with_one_ops_notice(
    session_factory, ops_messages
):
    ids = await _insert(session_factory, _alert("BAD"), _alert("CCC"))
    sender = FakeSender(fail_when=lambda e: "BAD" in e.title)
    worker = SweepWorker(
        session_factory,
        sender,
        sweep_seconds=60,
        health=HealthTracker("bot"),
        max_attempts=3,
        retry_gap_seconds=0,
        counters=new_counters(),
    )

    for _ in range(6):
        await worker.sweep()

    assert worker.counters["post_failures_total"] == 3  # not retried past the cap
    assert worker.counters["posts_abandoned_total"] == 1
    assert [e.title for e in sender.sent] == ["CCC · volume_threshold"]  # healthy row behind posts
    assert len(ops_messages) == 1
    assert f"#{ids[0]}" in ops_messages[0] and "BAD" in ops_messages[0]
    # Out of the sweep, but never marked posted.
    assert await _unposted_ids(session_factory) == [ids[0]]
    async with session_factory() as s:
        assert await AlertRepository(s).list_unposted(max_attempts=3) == []


async def test_a_failed_row_is_not_retried_inside_the_retry_gap(session_factory):
    (alert_id,) = await _insert(session_factory, _alert("BAD"))
    sender = FakeSender(fail_when=lambda e: True)
    worker = SweepWorker(session_factory, sender, sweep_seconds=60, health=HealthTracker("bot"), counters=new_counters())

    for _ in range(4):  # a NOTIFY burst: sweeps back to back
        await worker.sweep()

    assert worker.counters["post_failures_total"] == 1
    assert await _unposted_ids(session_factory) == [alert_id]


async def test_last_attempt_posts_the_plain_embed(session_factory):
    await _insert(session_factory, _surge_alert())
    shapes = []

    class Rejecting(FakeSender):
        async def send(self, embed):
            shapes.append(bool(embed.fields))
            if embed.fields:
                raise RuntimeError("typed embed rejected")
            return await super().send(embed)

    worker = SweepWorker(
        session_factory,
        Rejecting(),
        sweep_seconds=60,
        health=HealthTracker("bot"),
        max_attempts=2,
        retry_gap_seconds=0,
        counters=new_counters(),
    )
    await worker.sweep()
    await worker.sweep()

    assert shapes == [True, False]
    assert await _unposted_ids(session_factory) == []


def _surge_alert() -> Alert:
    unavailable = FactUnavailable(reason="test")
    payload = PopularitySurgePayload(
        rule_version="1",
        polled_at=datetime(2026, 7, 6, 14, 30, tzinfo=UTC),
        fired=("rank_jump",),
        rank=5,
        mentions=100,
        rank_24h_ago=80,
        mentions_24h_ago=None,
        rank_gain=75,
        mention_ratio=None,
        mention_multiplier=5.0,
        mention_min_prior=15,
        facts=SurgeFacts(
            price_move=unavailable,
            volume=unavailable,
            next_earnings=unavailable,
            benchmark=unavailable,
        ),
    )
    return Alert(
        symbol="GME",
        alert_type="popularity_surge",
        triggered_at=datetime(2026, 7, 6, 14, 30, tzinfo=UTC),
        payload=payload.model_dump(mode="json"),
        rule_version="1",
    )


async def test_surge_alert_is_posted_with_the_current_track_record(session_factory):
    await _insert(session_factory, _surge_alert())
    sender = FakeSender()
    worker = SweepWorker(session_factory, sender, sweep_seconds=60, health=HealthTracker("bot"), counters=new_counters())

    await worker.sweep()
    (before,) = sender.sent
    assert "No track record has been measured" in {f.name: f.value for f in before.fields}["Track record"]

    async with session_factory() as s:
        await TrackRecordRepository(s).insert(
            TrackRecord(
                alert_type="popularity_surge",
                rule_version="1",
                computed_at=datetime(2026, 7, 5, tzinfo=UTC),
                input=TrackRecordInput.RECONSTRUCTED,
                n=77,
                survivorship_caveat="caveat",
            )
        )
        await s.commit()
    await _insert(session_factory, _surge_alert().model_copy(update={"dedup_key": "again"}))
    await worker.sweep()

    after = {f.name: f.value for f in sender.sent[-1].fields}
    assert "N=77" in after["Track record"]


async def test_run_sweeps_on_wake_without_waiting_interval(session_factory):
    sender = FakeSender()
    # Interval far beyond the test timeout: only the wake can trigger the post.
    worker = SweepWorker(session_factory, sender, sweep_seconds=300, health=HealthTracker("bot"), counters=new_counters())
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


# -- unit of work: one posting attempt = one correlation_id ---------------


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
    worker = SweepWorker(session_factory, LoggingSender(), sweep_seconds=60, health=HealthTracker("bot"), counters=new_counters())

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
    worker = SweepWorker(session_factory, FakeSender(), sweep_seconds=60, health=HealthTracker("bot"), counters=new_counters())

    await worker.sweep()

    first, second = [r for r in json_records(json_stream) if r["event"].startswith("posted alert")]
    assert first["correlation_id"] != second["correlation_id"]
    assert first["alert_id"] != second["alert_id"]


async def test_send_failure_record_is_correlated_too(session_factory, json_stream):
    (alert_id,) = await _insert(session_factory, _alert("SPY"))
    sender = FakeSender(fail_when=lambda embed: True)
    worker = SweepWorker(session_factory, sender, sweep_seconds=60, health=HealthTracker("bot"), counters=new_counters())

    await worker.sweep()

    (record,) = [r for r in json_records(json_stream) if r["event"].startswith("send failed")]
    assert record["level"] == "error"
    assert record["alert_id"] == alert_id
    assert record["correlation_id"]
    assert "Traceback" in record["exception"]


def _http_error(status: int) -> discord.HTTPException:
    return discord.HTTPException(SimpleNamespace(status=status, reason="test"), "test")


class ScriptedSender(FakeSender):
    """Fails each send with the next planted error (None = succeed); the last entry repeats."""

    def __init__(self, script):
        super().__init__()
        self._script = script
        self.calls = 0

    async def send(self, embed):
        outcome = self._script[min(self.calls, len(self._script) - 1)]
        self.calls += 1
        if outcome is not None:
            raise outcome
        return await super().send(embed)


def _worker(session_factory, sender) -> SweepWorker:
    return SweepWorker(
        session_factory,
        sender,
        sweep_seconds=60,
        health=HealthTracker("bot"),
        retry_gap_seconds=0,
        counters=new_counters(),
    )


async def test_discord_pings_once_when_sends_start_failing_and_once_when_they_recover(session_factory, notices):
    await _insert(session_factory, _alert("AAA"), _alert("BBB"))
    worker = _worker(session_factory, ScriptedSender([ConnectionError("down")] * 4 + [None]))

    for _ in range(2):  # two sends fail per sweep, four in all
        await worker.sweep()
    assert len(notices) == 1
    assert notices[0].startswith("Discord is failing (ConnectionError)")

    await worker.sweep()

    assert await _unposted_ids(session_factory) == []
    assert len(notices) == 2
    assert notices[1] == "Discord recovered."


async def test_an_embed_discord_rejects_is_one_alert_not_a_discord_failure(session_factory, notices):
    await _insert(session_factory, _alert("AAA"), _alert("BBB"))
    worker = _worker(session_factory, ScriptedSender([_http_error(400), None]))

    await worker.sweep()

    assert worker.counters["post_failures_total"] == 1
    assert notices == []


async def test_a_bug_in_one_alerts_send_says_nothing_about_discord(session_factory, notices):
    await _insert(session_factory, _alert("AAA"), _alert("BBB"))
    worker = _worker(session_factory, ScriptedSender([RuntimeError("bad embed"), None]))

    await worker.sweep()

    assert worker.counters["post_failures_total"] == 1
    assert notices == []


async def test_a_dropped_connection_is_a_discord_failure(session_factory, notices):
    await _insert(session_factory, _alert("AAA"))
    worker = _worker(session_factory, ScriptedSender([aiohttp.ServerDisconnectedError()]))

    await worker.sweep()

    assert len(notices) == 1
    assert notices[0].startswith("Discord is failing (ServerDisconnectedError)")


@pytest.mark.parametrize("status", [403, 429, 503])
async def test_a_refused_or_overloaded_discord_is_a_failure(session_factory, notices, status):
    await _insert(session_factory, _alert("AAA"))
    worker = _worker(session_factory, ScriptedSender([_http_error(status)]))

    await worker.sweep()

    assert len(notices) == 1
    assert notices[0].startswith("Discord is failing (HTTPException)")


async def test_a_rejected_embed_proves_discord_answers_so_it_ends_a_failure(session_factory, notices):
    await _insert(session_factory, _alert("AAA"), _alert("BBB"))
    worker = _worker(session_factory, ScriptedSender([ConnectionError("down"), _http_error(400), None]))

    await worker.sweep()  # AAA: down; BBB: Discord answers, if only to say no to the embed
    await worker.sweep()

    assert [n.split(" (")[0] for n in notices] == ["Discord is failing", "Discord recovered."]
