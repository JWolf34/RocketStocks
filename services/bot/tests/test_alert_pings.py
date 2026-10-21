"""An Alert pings the role mapped to its Trigger; a reply never does. A mapped role that was
deleted is left out and the operator gets one Ops ping. Delivery is the sweep over the real
database, with Discord and the server's roles faked."""

import json
from datetime import timedelta

import httpx

from rs_bot.counters import new_counters
from rs_bot.render import POPULARITY_SURGE
from rs_bot.sweep import SweepWorker
from rs_bot.testing import FakeDiscord, FakeRoles, FixedClock
from rs_bot.testing import alert as _alert
from rs_bot.testing import surge_alert as _surge_alert
from rs_domain import Alert
from rs_observability import HealthTracker, OpsSettings, configure_ops
from rs_persistence import AlertRepository, TriggerRoleRepository

ROLE = 501


def _worker(session_factory, fake: FakeDiscord, roles: FakeRoles | None, **overrides) -> SweepWorker:
    options = {
        "clock": FixedClock(),
        "sweep_seconds": 60,
        "health": HealthTracker("bot"),
        "counters": new_counters(),
        "retry_gap_seconds": 0,
    } | overrides
    return SweepWorker(session_factory, fake, roles=roles, **options)


async def _insert(session_factory, *alerts: Alert) -> list[int]:
    async with session_factory() as s:
        repo = AlertRepository(s)
        ids = [await repo.insert(a) for a in alerts]
        await s.commit()
    return ids


async def _map(session_factory, trigger: str, role_id: int = ROLE) -> None:
    async with session_factory() as s:
        await TriggerRoleRepository(s).set(trigger, role_id)
        await s.commit()


def _about_roles(notices: list[str]) -> list[str]:
    """The Ops pings about a role (the test Alert also pings for its unavailable facts)."""
    return [n for n in notices if "role ping" in n]


def _roles(*role_ids: int) -> FakeRoles:
    roles = FakeRoles()
    for role_id in role_ids:
        roles.add(role_id)
    return roles


async def test_an_alert_pings_the_role_mapped_to_its_trigger(session_factory):
    await _map(session_factory, POPULARITY_SURGE)
    await _insert(session_factory, _surge_alert())
    fake = FakeDiscord()

    await _worker(session_factory, fake, _roles(ROLE)).sweep()

    (message,) = fake.messages
    assert message.role_ids == (ROLE,)


async def test_a_reply_pings_nobody_even_when_its_trigger_has_a_role(session_factory):
    await _map(session_factory, POPULARITY_SURGE)
    (alert_id,) = await _insert(session_factory, _surge_alert())
    fake = FakeDiscord()
    worker = _worker(session_factory, fake, _roles(ROLE))
    await worker.sweep()
    thread_id = next(iter(fake.threads))

    reply = _surge_alert().model_copy(
        update={"reply_to": alert_id, "triggered_at": _surge_alert().triggered_at + timedelta(minutes=20)}
    )
    await _insert(session_factory, reply)
    await worker.sweep()

    (posted,) = [m for m in fake.messages if m.where == 0]
    (in_thread,) = fake.in_thread(thread_id)
    assert posted.role_ids == (ROLE,)
    assert in_thread.role_ids == ()
    assert in_thread.mentions_allowed is False


async def test_an_alert_whose_trigger_has_no_role_pings_nobody(session_factory):
    await _map(session_factory, "another_trigger")
    await _insert(session_factory, _surge_alert(), _alert("AAA"))
    fake = FakeDiscord()

    await _worker(session_factory, fake, _roles(ROLE)).sweep()

    assert [m.role_ids for m in fake.messages] == [(), ()]


async def test_a_deleted_role_is_left_out_and_the_operator_is_told_once(session_factory, notices):
    await _map(session_factory, POPULARITY_SURGE)
    (alert_id,) = await _insert(session_factory, _surge_alert())
    fake = FakeDiscord()

    await _worker(session_factory, fake, _roles()).sweep()  # the server has no such role

    (message,) = fake.messages
    assert message.role_ids == ()  # it still posted
    (ping,) = _about_roles(notices)
    assert f"Alert #{alert_id}" in ping
    assert str(ROLE) in ping
    assert "/setup roles add" in ping


async def test_without_a_role_lookup_nothing_is_pinged(session_factory, notices):
    await _map(session_factory, POPULARITY_SURGE)
    await _insert(session_factory, _surge_alert())
    fake = FakeDiscord()

    await _worker(session_factory, fake, None).sweep()

    assert [m.role_ids for m in fake.messages] == [()]
    assert _about_roles(notices) == []


async def test_an_alert_waits_when_the_server_is_not_known_and_is_not_called_a_deleted_role(
    session_factory, notices
):
    await _map(session_factory, POPULARITY_SURGE)
    await _insert(session_factory, _surge_alert())
    fake = FakeDiscord()
    roles = _roles(ROLE)
    roles.guild_known = False
    worker = _worker(session_factory, fake, roles)

    await worker.sweep()
    assert fake.messages == []
    assert _about_roles(notices) == []

    roles.guild_known = True
    await worker.sweep()
    assert [m.role_ids for m in fake.messages] == [(ROLE,)]


async def test_a_deleted_role_pings_the_operator_once_however_many_alerts_follow(session_factory):
    sent: list[str] = []

    def webhook(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content)["content"])
        return httpx.Response(204)

    # The real de-dup is on here, unlike the `notices` fixture, which shows every send.
    configure_ops(
        OpsSettings(_env_file=None, rs_ops_webhook_url="https://discord.test/api/webhooks/1/t"),
        transport=httpx.MockTransport(webhook),
    )
    try:
        await _map(session_factory, POPULARITY_SURGE)
        first = _surge_alert()
        await _insert(session_factory, first, first.model_copy(update={"symbol": "AMC"}))
        fake = FakeDiscord()

        await _worker(session_factory, fake, _roles()).sweep()
    finally:
        configure_ops(OpsSettings(_env_file=None))

    assert len(fake.messages) == 2  # both still posted
    assert len(_about_roles(sent)) == 1
