"""Out-of-band ops notices: any service posts to a private Discord channel
through a webhook, without the bot.

`notify_ops` never raises - it is called from failure paths, where a second
failure must not mask the first. An unset or failing webhook is logged at
ERROR with the full message, so the JSON stream stays the fallback record.
"""

import logging
from datetime import datetime, timedelta

import httpx
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from rs_domain.clock import SystemClock
from rs_domain.ports import Clock

logger = logging.getLogger(__name__)

DISCORD_CONTENT_LIMIT = 2000
TRUNCATION_MARKER = "... [truncated]"


class OpsSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    rs_ops_webhook_url: SecretStr | None = None
    rs_ops_dedup_window_seconds: float = 900.0
    rs_ops_timeout_seconds: float = 5.0


class _OpsNotifier:
    def __init__(
        self,
        settings: OpsSettings,
        clock: Clock,
        transport: httpx.AsyncBaseTransport | None,
    ) -> None:
        url = settings.rs_ops_webhook_url
        self._webhook_url = url.get_secret_value() if url is not None else ""
        self._window = timedelta(seconds=settings.rs_ops_dedup_window_seconds)
        self._timeout = settings.rs_ops_timeout_seconds
        self._clock = clock
        self._transport = transport
        self._last_sent: dict[str, datetime] = {}

    async def notify(self, message: str, key: str | None) -> None:
        now = self._clock.now()
        if key is not None and self._suppressed(key, now):
            return
        if not self._webhook_url:
            logger.error("ops notice not delivered (RS_OPS_WEBHOOK_URL unset): %s", message)
            return
        # Reserve the key before the await so concurrent callers with the same
        # key send once; a failed send releases it so the next call retries.
        if key is not None:
            self._last_sent[key] = now
        try:
            delivered = await self._post(message)
        except BaseException:
            self._release(key)
            raise
        if not delivered:
            self._release(key)

    def _suppressed(self, key: str, now: datetime) -> bool:
        self._last_sent = {k: t for k, t in self._last_sent.items() if now - t < self._window}
        return key in self._last_sent

    def _release(self, key: str | None) -> None:
        if key is not None:
            self._last_sent.pop(key, None)

    async def _post(self, message: str) -> bool:
        payload = {"content": _truncate(message), "allowed_mentions": {"parse": []}}
        try:
            async with httpx.AsyncClient(timeout=self._timeout, transport=self._transport) as client:
                response = await client.post(self._webhook_url, json=payload)
        except httpx.HTTPError as exc:
            # The exception text can embed the webhook URL (a credential), so log the type only.
            logger.error("ops notice not delivered (%s): %s", type(exc).__name__, message)
            return False
        if not response.is_success:
            logger.error("ops notice not delivered (HTTP %d): %s", response.status_code, message)
            return False
        return True


def _truncate(message: str) -> str:
    if len(message) <= DISCORD_CONTENT_LIMIT:
        return message
    return message[: DISCORD_CONTENT_LIMIT - len(TRUNCATION_MARKER)] + TRUNCATION_MARKER


_notifier: _OpsNotifier | None = None


def configure_ops(
    settings: OpsSettings | None = None,
    *,
    clock: Clock | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> None:
    """Replace the process-wide notifier (and reset its de-dup memory).

    Optional in production: the first `notify_ops` call builds one from the
    environment. Tests inject settings, a fake clock, and a mock transport.
    """
    global _notifier
    _notifier = _OpsNotifier(settings or OpsSettings(), clock or SystemClock(), transport)


async def notify_ops(message: str, *, key: str | None = None) -> None:
    """Post `message` to the ops channel; never raises.

    A `key` suppresses repeats of the same key inside the configured window, so
    a flapping condition sends one notice, not hundreds. Content beyond
    Discord's 2000-character limit is truncated with a marker.
    """
    try:
        notifier = _notifier
        if notifier is None:
            configure_ops()
            notifier = _notifier
        assert notifier is not None
        await notifier.notify(message, key)
    except Exception:
        logger.exception("ops notice failed unexpectedly: %s", message)
