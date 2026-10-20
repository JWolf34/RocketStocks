"""notify_ops: webhook delivery that never raises, with keyed de-duplication."""

import asyncio
import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from rs_observability import OpsSettings, configure_ops, notify_ops
from rs_observability import ops as ops_module

WEBHOOK = "https://discord.test/api/webhooks/123/secret-token"


class FakeClock:
    def __init__(self) -> None:
        self.current = datetime(2026, 1, 5, 15, 0, tzinfo=UTC)

    def now(self) -> datetime:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)


class Webhook:
    """Records requests and answers with a canned handler."""

    def __init__(self, handler: Callable[[httpx.Request], httpx.Response] | None = None) -> None:
        self.requests: list[httpx.Request] = []
        self._handler = handler or (lambda request: httpx.Response(204))

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self._handler(request)

    @property
    def contents(self) -> list[str]:
        return [json.loads(request.content)["content"] for request in self.requests]


@pytest.fixture(autouse=True)
def isolated_notifier(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RS_OPS_WEBHOOK_URL", raising=False)
    monkeypatch.setattr(ops_module, "_notifier", None)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


def configure(webhook: Webhook, clock: FakeClock, **overrides: float) -> None:
    settings = OpsSettings(_env_file=None, rs_ops_webhook_url=WEBHOOK, **overrides)
    configure_ops(settings, clock=clock, transport=httpx.MockTransport(webhook))


async def test_posts_message_to_webhook(clock: FakeClock) -> None:
    webhook = Webhook()
    configure(webhook, clock)

    await notify_ops("ingest stalled")

    assert [str(request.url) for request in webhook.requests] == [WEBHOOK]
    assert webhook.contents == ["ingest stalled"]


async def test_mentions_are_disabled(clock: FakeClock) -> None:
    webhook = Webhook()
    configure(webhook, clock)

    await notify_ops("@everyone look")

    assert json.loads(webhook.requests[0].content)["allowed_mentions"] == {"parse": []}


async def test_unset_webhook_logs_error_and_does_not_raise(caplog: pytest.LogCaptureFixture) -> None:
    configure_ops(OpsSettings(_env_file=None))

    with caplog.at_level(logging.ERROR):
        await notify_ops("token expired")

    assert [r.levelno for r in caplog.records] == [logging.ERROR]
    assert "token expired" in caplog.records[0].getMessage()


def test_webhook_url_is_read_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RS_OPS_WEBHOOK_URL", WEBHOOK)

    settings = OpsSettings(_env_file=None)

    assert settings.rs_ops_webhook_url is not None
    assert settings.rs_ops_webhook_url.get_secret_value() == WEBHOOK


async def test_http_error_status_logs_error_and_does_not_raise(clock: FakeClock, caplog: pytest.LogCaptureFixture) -> None:
    webhook = Webhook(lambda request: httpx.Response(500))
    configure(webhook, clock)

    with caplog.at_level(logging.ERROR):
        await notify_ops("db down")

    assert len(webhook.requests) == 1
    assert [r.levelno for r in caplog.records] == [logging.ERROR]
    text = caplog.records[0].getMessage()
    assert "db down" in text
    assert "500" in text


async def test_timeout_logs_error_without_leaking_webhook_url(clock: FakeClock, caplog: pytest.LogCaptureFixture) -> None:
    def time_out(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    configure(Webhook(time_out), clock)

    with caplog.at_level(logging.ERROR):
        await notify_ops("slow webhook")

    assert [r.levelno for r in caplog.records] == [logging.ERROR]
    text = caplog.records[0].getMessage()
    assert "slow webhook" in text
    assert "secret-token" not in text


async def test_unexpected_failure_is_logged_not_raised(clock: FakeClock, caplog: pytest.LogCaptureFixture) -> None:
    def explode(request: httpx.Request) -> httpx.Response:
        raise RuntimeError("transport bug")

    configure(Webhook(explode), clock)

    with caplog.at_level(logging.ERROR):
        await notify_ops("still safe")

    assert any("still safe" in r.getMessage() for r in caplog.records)


async def test_same_key_is_suppressed_inside_window(clock: FakeClock) -> None:
    webhook = Webhook()
    configure(webhook, clock, rs_ops_dedup_window_seconds=600)

    await notify_ops("flap", key="schwab-token")
    clock.advance(599)
    await notify_ops("flap", key="schwab-token")

    assert webhook.contents == ["flap"]


async def test_same_key_sends_again_after_window(clock: FakeClock) -> None:
    webhook = Webhook()
    configure(webhook, clock, rs_ops_dedup_window_seconds=600)

    await notify_ops("flap", key="schwab-token")
    clock.advance(600)
    await notify_ops("flap", key="schwab-token")

    assert webhook.contents == ["flap", "flap"]


async def test_different_keys_and_keyless_calls_are_independent(clock: FakeClock) -> None:
    webhook = Webhook()
    configure(webhook, clock)

    await notify_ops("a", key="one")
    await notify_ops("b", key="two")
    await notify_ops("c")
    await notify_ops("c")

    assert webhook.contents == ["a", "b", "c", "c"]


async def test_failed_send_does_not_start_suppression_window(clock: FakeClock) -> None:
    responses = iter([httpx.Response(500), httpx.Response(204)])
    webhook = Webhook(lambda request: next(responses))
    configure(webhook, clock)

    await notify_ops("retry me", key="k")
    await notify_ops("retry me", key="k")

    assert len(webhook.requests) == 2


async def test_long_message_is_truncated_with_marker(clock: FakeClock) -> None:
    webhook = Webhook()
    configure(webhook, clock)

    await notify_ops("x" * 5000)

    (content,) = webhook.contents
    assert len(content) == 2000
    assert content.endswith(ops_module.TRUNCATION_MARKER)
    assert content.startswith("x" * 100)


async def test_message_at_limit_is_not_truncated(clock: FakeClock) -> None:
    webhook = Webhook()
    configure(webhook, clock)

    await notify_ops("y" * 2000)

    assert webhook.contents == ["y" * 2000]


async def test_concurrent_same_key_sends_once(clock: FakeClock) -> None:
    webhook = Webhook()
    configure(webhook, clock)

    await asyncio.gather(*(notify_ops("flap", key="k") for _ in range(5)))

    assert webhook.contents == ["flap"]


async def test_invalid_webhook_url_is_logged_not_raised(
    clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    settings = OpsSettings(_env_file=None, rs_ops_webhook_url="not-a-url-secret")
    configure_ops(settings, clock=clock)

    with caplog.at_level(logging.ERROR):
        await notify_ops("bad config")

    assert any("bad config" in r.getMessage() for r in caplog.records)
    assert all("not-a-url-secret" not in r.getMessage() for r in caplog.records)
