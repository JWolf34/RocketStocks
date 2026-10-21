"""Delivery never posts twice: the sending mark, the nonce, reconciliation of a row found
still sending (by history, and by the nonce when history misses it), threads, replies and
the stale rule. A crash is planted by leaving the row marked sending and the message in
the fake Discord, which is what a process killed between a send and its record leaves."""

from datetime import UTC, datetime, timedelta

from sqlalchemy import text

from rs_bot.counters import new_counters
from rs_bot.render import render
from rs_bot.sweep import SweepWorker
from rs_bot.testing import HIT, FakeDiscord, FixedClock
from rs_bot.testing import alert as _alert
from rs_bot.testing import surge_alert as _surge_alert
from rs_domain import (
    Alert,
    Delivery,
    FactUnavailable,
    NextEarningsFact,
    PriceMoveFact,
    SurgeFacts,
)
from rs_observability import HealthTracker
from rs_persistence import AlertRepository, AlertRow, PendingAlert

MARKETS_OPEN = datetime(2026, 7, 6, 14, 35, tzinfo=UTC)


def _worker(session_factory, fake: FakeDiscord, *, now: datetime = MARKETS_OPEN, **overrides) -> SweepWorker:
    options = {
        "sweep_seconds": 60,
        "health": HealthTracker("bot"),
        "counters": new_counters(),
        "retry_gap_seconds": 0,
    } | overrides
    return SweepWorker(session_factory, fake, clock=FixedClock(now), **options)


def _reply(symbol: str, alert_id: int, minutes_after_hit: int = 20) -> Alert:
    return Alert(
        symbol=symbol,
        trigger="volume_threshold",
        triggered_at=HIT + timedelta(minutes=minutes_after_hit),
        payload={"volume": 900_000},
        reply_to=alert_id,
    )


async def _insert(session_factory, *alerts: Alert) -> list[int]:
    async with session_factory() as s:
        repo = AlertRepository(s)
        ids = [await repo.insert(a) for a in alerts]
        await s.commit()
    return ids


async def _row(session_factory, row_id: int) -> AlertRow:
    async with session_factory() as s:
        return await s.get_one(AlertRow, row_id)


async def _mark_sending(session_factory, row_id: int) -> None:
    async with session_factory() as s:
        assert await AlertRepository(s).mark_sending(row_id)
        await s.commit()


def _as_posted_before_the_crash(fake: FakeDiscord, row_id: int, alert: Alert, *, where: int = 0):
    """The message the killed process sent for `row_id`: the embed the bot renders, with the
    row id as its nonce."""
    return fake.plant(render(PendingAlert(id=row_id, alert=alert)), nonce=row_id, where=where)


# -- the sending mark and the nonce ------------------------------------------------------


async def test_the_sending_mark_is_committed_before_the_send_and_the_nonce_is_the_row_id(session_factory):
    (row_id,) = await _insert(session_factory, _alert("AAA"))
    fake = FakeDiscord()
    seen = []

    async def read_the_row_from_another_connection():
        row = await _row(session_factory, row_id)
        seen.append((row.delivery, row.sending_at is not None, row.message_id))

    fake.before_send = read_the_row_from_another_connection

    await _worker(session_factory, fake).sweep()

    assert seen == [(Delivery.SENDING, True, None)]
    (message,) = fake.messages
    assert message.nonce == row_id
    row = await _row(session_factory, row_id)
    assert (row.delivery, row.message_id) == (Delivery.POSTED, message.id)
    assert row.posted_at is not None


async def test_an_alert_posts_once_and_its_thread_is_started_from_its_message(session_factory):
    (row_id,) = await _insert(session_factory, _alert("AAA"))
    fake = FakeDiscord()
    worker = _worker(session_factory, fake)

    await worker.sweep()
    await worker.sweep()

    (message,) = fake.messages
    row = await _row(session_factory, row_id)
    assert row.thread_id in fake.threads
    assert fake.threads == {message.id: f"AAA alert #{row_id}"}
    assert row.thread_id == message.id


# -- reconciliation ------------------------------------------------------------------------


async def test_a_crash_after_the_send_is_recorded_from_history_not_sent_again(session_factory, notices):
    alert = _alert("AAA")
    (row_id,) = await _insert(session_factory, alert)
    await _mark_sending(session_factory, row_id)
    fake = FakeDiscord()
    posted = _as_posted_before_the_crash(fake, row_id, alert)
    worker = _worker(session_factory, fake)

    await worker.sweep()

    assert fake.messages == [posted]  # nothing new was sent
    row = await _row(session_factory, row_id)
    assert (row.delivery, row.message_id) == (Delivery.POSTED, posted.id)
    assert row.thread_id == posted.id
    assert worker.counters["posts_reconciled_total"] == 1
    assert notices == []


async def test_history_only_matches_the_bots_own_message_for_that_row(session_factory):
    alert = _alert("AAA")
    (row_id,) = await _insert(session_factory, alert)
    await _mark_sending(session_factory, row_id)
    fake = FakeDiscord()
    someone_else = _as_posted_before_the_crash(fake, row_id, alert)
    someone_else.from_bot = False
    _as_posted_before_the_crash(fake, row_id + 1, alert)  # another row's message

    await _worker(session_factory, fake).sweep()

    assert len(fake.messages) == 3  # neither stood in for this row: it was sent
    assert (await _row(session_factory, row_id)).message_id == fake.messages[-1].id


async def test_a_crash_after_the_send_is_caught_by_the_nonce_when_history_misses_it(session_factory):
    alert = _alert("AAA")
    (row_id,) = await _insert(session_factory, alert)
    await _mark_sending(session_factory, row_id)
    fake = FakeDiscord()
    posted = _as_posted_before_the_crash(fake, row_id, alert)
    fake.hide_from_history = True

    await _worker(session_factory, fake).sweep()

    assert fake.messages == [posted]  # Discord answered the resend with the first message
    row = await _row(session_factory, row_id)
    assert (row.delivery, row.message_id) == (Delivery.POSTED, posted.id)


async def test_a_crash_before_the_send_sends_it(session_factory):
    (row_id,) = await _insert(session_factory, _alert("AAA"))
    await _mark_sending(session_factory, row_id)
    fake = FakeDiscord()

    await _worker(session_factory, fake).sweep()

    assert [m.nonce for m in fake.messages] == [row_id]
    assert (await _row(session_factory, row_id)).delivery == Delivery.POSTED


async def test_history_is_read_from_the_first_send(session_factory):
    (row_id,) = await _insert(session_factory, _alert("AAA"))
    await _mark_sending(session_factory, row_id)
    sending_at = (await _row(session_factory, row_id)).sending_at
    fake = FakeDiscord()

    await _worker(session_factory, fake).sweep()

    assert fake.history_reads == [(0, sending_at)]


async def test_a_send_that_posted_and_then_failed_is_not_sent_again_on_retry(session_factory):
    (row_id,) = await _insert(session_factory, _alert("AAA"))
    fake = FakeDiscord()
    fake.fail_after_posting = [TimeoutError()]
    worker = _worker(session_factory, fake)

    await worker.sweep()  # posted, but the answer never came back
    assert (await _row(session_factory, row_id)).delivery == Delivery.SENDING
    await worker.sweep()

    assert len(fake.messages) == 1
    row = await _row(session_factory, row_id)
    assert (row.delivery, row.message_id) == (Delivery.POSTED, fake.messages[0].id)


async def test_an_unreadable_history_posts_nothing_and_pings_once(session_factory, notices):
    alert = _alert("AAA")
    (row_id,) = await _insert(session_factory, alert)
    await _mark_sending(session_factory, row_id)
    fake = FakeDiscord()
    posted = _as_posted_before_the_crash(fake, row_id, alert)
    fake.history_failure = ConnectionError("down")
    worker = _worker(session_factory, fake)

    for _ in range(3):
        await worker.sweep()

    assert fake.messages == [posted]  # no second post while it could not be checked
    assert (await _row(session_factory, row_id)).delivery == Delivery.SENDING
    assert len(notices) == 1  # the Discord source ping covers it: one cause, one ping
    assert notices[0].startswith("Discord is failing (ConnectionError)")

    fake.history_failure = None
    await worker.sweep()

    assert fake.messages == [posted]
    assert (await _row(session_factory, row_id)).message_id == posted.id


async def test_an_unreadable_history_that_is_not_a_discord_failure_still_pings_once(session_factory, notices):
    (row_id,) = await _insert(session_factory, _alert("AAA"))
    await _mark_sending(session_factory, row_id)
    fake = FakeDiscord()
    fake.history_failure = RuntimeError("a bug in reading")
    worker = _worker(session_factory, fake)

    for _ in range(3):
        await worker.sweep()

    assert fake.messages == []
    (ping,) = notices
    assert f"#{row_id}" in ping and "AAA" in ping and "could not be read" in ping


async def test_a_row_found_posted_is_not_stale_however_old_it_is(session_factory):
    alert = _alert("AAA")
    (row_id,) = await _insert(session_factory, alert)
    await _mark_sending(session_factory, row_id)
    fake = FakeDiscord()
    posted = _as_posted_before_the_crash(fake, row_id, alert)

    await _worker(session_factory, fake, now=HIT + timedelta(days=1)).sweep()

    assert (await _row(session_factory, row_id)).delivery == Delivery.POSTED
    assert (await _row(session_factory, row_id)).message_id == posted.id


# -- replies -------------------------------------------------------------------------------


async def test_a_reply_posts_into_its_alerts_thread_without_mentions(session_factory):
    (alert_id,) = await _insert(session_factory, _alert("AAA"))
    fake = FakeDiscord()
    worker = _worker(session_factory, fake)
    await worker.sweep()
    thread_id = (await _row(session_factory, alert_id)).thread_id
    assert thread_id is not None

    (reply_id,) = await _insert(session_factory, _reply("AAA", alert_id))
    await worker.sweep()

    (reply,) = fake.in_thread(thread_id)
    assert reply.nonce == reply_id
    assert reply.mentions_allowed is False
    row = await _row(session_factory, reply_id)
    assert (row.delivery, row.message_id, row.thread_id) == (Delivery.POSTED, reply.id, None)
    assert len(fake.threads) == 1  # a reply starts no thread of its own


async def test_a_reply_creates_the_thread_first_when_its_alert_posted_without_one(session_factory):
    (alert_id,) = await _insert(session_factory, _alert("AAA"))
    fake = FakeDiscord()
    fake.open_thread_failures = [RuntimeError("no permission")] * 2
    worker = _worker(session_factory, fake)
    await worker.sweep()
    alert = await _row(session_factory, alert_id)
    assert (alert.delivery, alert.thread_id) == (Delivery.POSTED, None)

    (reply_id,) = await _insert(session_factory, _reply("AAA", alert_id))
    fake.open_thread_failures.clear()
    await worker.sweep()

    alert = await _row(session_factory, alert_id)
    assert alert.thread_id == alert.message_id
    (reply,) = fake.in_thread(alert.thread_id)
    assert reply.nonce == reply_id


async def test_a_thread_that_could_not_be_started_is_retried(session_factory):
    (alert_id,) = await _insert(session_factory, _alert("AAA"))
    fake = FakeDiscord()
    fake.open_thread_failures = [RuntimeError("no permission")]

    await _worker(session_factory, fake).sweep()  # the retry gap is zero: tried again in the same sweep

    row = await _row(session_factory, alert_id)
    assert row.thread_id == row.message_id
    assert fake.open_thread_failures == []
    assert len(fake.messages) == 1


async def test_a_failed_thread_is_not_retried_inside_the_retry_gap(session_factory):
    (alert_id,) = await _insert(session_factory, _alert("AAA"))
    fake = FakeDiscord()
    fake.open_thread_failures = [RuntimeError("no permission")] * 3
    worker = _worker(session_factory, fake, retry_gap_seconds=60)

    await worker.sweep()
    await worker.sweep()

    assert (await _row(session_factory, alert_id)).thread_id is None
    assert len(fake.open_thread_failures) == 2  # tried once, then held back


async def test_a_reply_crash_after_the_send_is_recorded_from_the_threads_history(session_factory):
    (alert_id,) = await _insert(session_factory, _alert("AAA"))
    fake = FakeDiscord()
    worker = _worker(session_factory, fake)
    await worker.sweep()
    thread_id = (await _row(session_factory, alert_id)).thread_id
    assert thread_id is not None
    reply = _reply("AAA", alert_id)
    (reply_id,) = await _insert(session_factory, reply)
    await _mark_sending(session_factory, reply_id)
    posted = _as_posted_before_the_crash(fake, reply_id, reply, where=thread_id)

    await worker.sweep()

    assert fake.in_thread(thread_id) == [posted]
    assert (await _row(session_factory, reply_id)).message_id == posted.id


async def test_a_reply_waits_for_an_alert_that_has_not_posted_yet(session_factory):
    (alert_id,) = await _insert(session_factory, _alert("AAA"))
    (reply_id,) = await _insert(session_factory, _reply("AAA", alert_id))
    fake = FakeDiscord()
    fake.send_failures = [RuntimeError("bad embed")]
    worker = _worker(session_factory, fake)

    await worker.sweep()
    assert fake.messages == []
    assert (await _row(session_factory, reply_id)).delivery == Delivery.PENDING

    await worker.sweep()  # the Alert posts, then its reply follows in the same sweep
    assert (await _row(session_factory, reply_id)).delivery == Delivery.POSTED
    assert [m.nonce for m in fake.messages] == [alert_id, reply_id]


# -- the stale rule ------------------------------------------------------------------------


async def test_an_alert_older_than_an_hour_is_recorded_stale_and_not_posted(session_factory, notices):
    (row_id,) = await _insert(session_factory, _alert("AAA"))
    fake = FakeDiscord()
    worker = _worker(session_factory, fake, now=HIT + timedelta(hours=1, seconds=1))

    await worker.sweep()

    assert fake.messages == []
    assert (await _row(session_factory, row_id)).delivery == Delivery.STALE
    assert worker.counters["posts_stale_total"] == 1
    async with session_factory() as s:
        assert await AlertRepository(s).list_undelivered() == []
    assert notices == []


async def test_an_alert_just_inside_the_hour_still_posts(session_factory):
    (row_id,) = await _insert(session_factory, _alert("AAA"))
    fake = FakeDiscord()

    await _worker(session_factory, fake, now=HIT + timedelta(hours=1)).sweep()

    assert (await _row(session_factory, row_id)).delivery == Delivery.POSTED


async def test_a_reply_is_stale_by_its_own_hit_time(session_factory):
    (alert_id,) = await _insert(session_factory, _alert("AAA"))
    fake = FakeDiscord()
    await _worker(session_factory, fake).sweep()
    (reply_id,) = await _insert(session_factory, _reply("AAA", alert_id, minutes_after_hit=20))

    await _worker(session_factory, fake, now=HIT + timedelta(minutes=20, hours=1, seconds=1)).sweep()

    assert (await _row(session_factory, reply_id)).delivery == Delivery.STALE
    assert len(fake.messages) == 1


async def test_a_reply_to_an_alert_that_never_posted_is_recorded_stale(session_factory):
    (alert_id,) = await _insert(session_factory, _alert("AAA"))
    async with session_factory() as s:
        await AlertRepository(s).mark_stale(alert_id)
        await s.commit()
    (reply_id,) = await _insert(session_factory, _reply("AAA", alert_id))
    fake = FakeDiscord()

    await _worker(session_factory, fake).sweep()

    assert fake.messages == []
    assert (await _row(session_factory, reply_id)).delivery == Delivery.STALE


async def test_a_reply_to_an_alert_posted_before_messages_were_stored_is_recorded_stale(session_factory):
    (alert_id,) = await _insert(session_factory, _alert("AAA"))
    async with session_factory() as s:
        await s.execute(
            text("UPDATE alerts SET delivery = 'posted', posted_at = now() WHERE id = :id"),
            {"id": alert_id},
        )
        await s.commit()
    (reply_id,) = await _insert(session_factory, _reply("AAA", alert_id))
    fake = FakeDiscord()

    await _worker(session_factory, fake).sweep()

    assert fake.messages == []
    assert (await _row(session_factory, reply_id)).delivery == Delivery.STALE


# -- facts that were unavailable -----------------------------------------------------------


def _facts(**unavailable: str) -> SurgeFacts:
    fine = {
        "price_move": PriceMoveFact(last_price=10.0, previous_close=9.0, change_pct=11.1, quote_time=HIT),
        "next_earnings": NextEarningsFact(date=HIT.date(), estimated=False),
    }
    return SurgeFacts(**(fine | {name: FactUnavailable(reason=why) for name, why in unavailable.items()}))


async def test_an_alert_posted_with_missing_facts_pings_once_naming_the_stock_and_facts(
    session_factory, notices
):
    (row_id,) = await _insert(
        session_factory,
        _surge_alert(_facts(price_move="Schwab has no quote for this symbol", next_earnings="no upcoming earnings date found")),
    )
    worker = _worker(session_factory, FakeDiscord())

    await worker.sweep()
    await worker.sweep()

    (ping,) = notices
    assert f"#{row_id}" in ping and "GME" in ping
    assert "price move (Schwab has no quote for this symbol)" in ping
    assert "next earnings (no upcoming earnings date found)" in ping


async def test_an_alert_with_every_fact_pings_no_one(session_factory, notices):
    await _insert(session_factory, _surge_alert(_facts()))

    await _worker(session_factory, FakeDiscord()).sweep()

    assert notices == []


async def test_an_alert_that_did_not_post_does_not_ping_about_facts(session_factory, notices):
    await _insert(session_factory, _surge_alert(_facts(price_move="timed out after 5s")))

    await _worker(session_factory, FakeDiscord(), now=HIT + timedelta(hours=2)).sweep()

    assert notices == []


async def test_a_reply_does_not_ping_about_facts(session_factory, notices):
    (alert_id,) = await _insert(session_factory, _surge_alert(_facts()))
    worker = _worker(session_factory, FakeDiscord())
    await worker.sweep()
    reply = _surge_alert(_facts(price_move="timed out after 5s")).model_copy(
        update={"reply_to": alert_id, "triggered_at": HIT + timedelta(minutes=30)}
    )
    await _insert(session_factory, reply)

    await worker.sweep()

    assert notices == []

