"""TokenAgeNotifier: one DM per threshold per token, from the token's age alone."""

from datetime import UTC, datetime, timedelta

import pytest

from rs_bot.token_notices import TokenAgeNotifier
from rs_persistence import SchwabTokenStore

ISSUED = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)
EXPIRES = ISSUED + timedelta(days=7)


class FakeClock:
    def __init__(self, now: datetime) -> None:
        self.current = now

    def now(self) -> datetime:
        return self.current


class FakeMessenger:
    def __init__(self) -> None:
        self.sent: list[str] = []
        self.fail = False

    async def send_dm(self, text: str) -> None:
        if self.fail:
            raise RuntimeError("DMs closed")
        self.sent.append(text)


@pytest.fixture
def messenger() -> FakeMessenger:
    return FakeMessenger()


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(ISSUED + timedelta(days=1))


@pytest.fixture
async def notifier(session_factory, messenger, clock) -> TokenAgeNotifier:
    await _issue(session_factory, ISSUED)
    return TokenAgeNotifier(session_factory, messenger, clock)


async def _issue(session_factory, issued: datetime) -> None:
    await SchwabTokenStore(session_factory).save(
        {"creation_timestamp": int(issued.timestamp()), "token": {"refresh_token": "SECRET"}}
    )


async def test_no_notice_while_the_token_is_young(notifier, messenger):
    assert await notifier.check() is None
    assert messenger.sent == []


async def test_each_threshold_fires_once(notifier, messenger, clock):
    clock.current = EXPIRES - timedelta(hours=23)
    assert await notifier.check() == 24
    assert await notifier.check() is None  # polled again: still the same notice
    assert len(messenger.sent) == 1

    clock.current = EXPIRES - timedelta(hours=5)
    assert await notifier.check() == 6
    assert await notifier.check() is None
    assert len(messenger.sent) == 2

    clock.current = EXPIRES - timedelta(hours=1)
    assert await notifier.check() is None
    assert len(messenger.sent) == 2


async def test_the_notice_says_when_the_token_expires(notifier, messenger, clock):
    clock.current = EXPIRES - timedelta(hours=23)
    await notifier.check()
    assert f"<t:{int(EXPIRES.timestamp())}:R>" in messenger.sent[0]
    assert "/setup schwab" in messenger.sent[0]


async def test_a_restart_does_not_repeat_a_notice(session_factory, messenger, clock, notifier):
    clock.current = EXPIRES - timedelta(hours=23)
    await notifier.check()

    restarted = TokenAgeNotifier(session_factory, messenger, clock)
    assert await restarted.check() is None
    assert len(messenger.sent) == 1


async def test_a_bot_that_was_down_sends_only_the_tighter_notice(notifier, messenger, clock):
    clock.current = EXPIRES - timedelta(hours=5)
    assert await notifier.check() == 6
    assert len(messenger.sent) == 1

    clock.current = EXPIRES - timedelta(hours=4)
    assert await notifier.check() is None  # the 24h notice was skipped, not owed


async def test_a_new_login_starts_the_thresholds_over(session_factory, notifier, messenger, clock):
    clock.current = EXPIRES - timedelta(hours=5)
    await notifier.check()

    relogin = clock.current
    await _issue(session_factory, relogin)
    clock.current = relogin + timedelta(days=7) - timedelta(hours=23)

    assert await notifier.check() == 24
    assert len(messenger.sent) == 2


async def test_an_expired_token_gets_no_countdown(notifier, messenger, clock):
    clock.current = EXPIRES + timedelta(minutes=1)
    assert await notifier.check() is None
    assert messenger.sent == []


async def test_a_failed_dm_is_retried_not_marked_sent(notifier, messenger, clock):
    clock.current = EXPIRES - timedelta(hours=23)
    messenger.fail = True
    with pytest.raises(RuntimeError):
        await notifier.check()

    messenger.fail = False
    assert await notifier.check() == 24
    assert len(messenger.sent) == 1


async def test_no_token_means_no_notice(session_factory, messenger, clock):
    notifier = TokenAgeNotifier(session_factory, messenger, clock)
    assert await notifier.check() is None
