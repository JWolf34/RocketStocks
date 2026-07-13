"""Entry point: `python -m rs_bot`.

Lifecycle contract (spec): continuous daemon — connect Discord → resolve the
dev channel (unresolvable/no-permission → loud exit 1) → start LISTEN →
unconditional initial sweep → run until SIGTERM (graceful: finish the
in-flight post, exit 0).

Failure modes: DB down at startup = loud exit (the initial sweep is
crash-loud); LISTEN drop = backoff reconnect + recovery sweep (ListenLoop);
Discord outage = sends fail, rows stay unposted, the periodic sweep
self-heals after discord.py resumes. A dead sweep worker or a fatally dead
gateway exits 1 loudly — this daemon never limps on silently.
"""

import asyncio
import contextlib
import logging
import signal
import sys

import discord
from pydantic import ValidationError

from rs_observability import (
    DictCounterCollector,
    configure_logging,
    install_excepthooks,
    start_metrics_server,
)
from rs_persistence import NOTIFY_CHANNEL, build_engine, build_session_factory

from rs_bot.counters import new_counters
from rs_bot.listener import ListenLoop
from rs_bot.settings import BotSettings
from rs_bot.sweep import SweepWorker

logger = logging.getLogger(__name__)


def psycopg_conninfo(sqlalchemy_url: str) -> str:
    """RS_DB_URL is the shared SQLAlchemy DSN (postgresql+psycopg://); the
    LISTEN connection is raw psycopg and wants the driverless form."""
    return sqlalchemy_url.replace("+psycopg", "", 1)


class DiscordChannelSender:
    """EmbedSender backed by the resolved dev channel (full gateway client —
    spec pin #2). Returns the message id for the per-post G5 audit log."""

    def __init__(self, channel: discord.abc.Messageable) -> None:
        self._channel = channel

    async def send(self, embed: discord.Embed) -> int:
        message = await self._channel.send(embed=embed)
        return message.id


async def run_bot(settings: BotSettings) -> int:
    install_excepthooks()  # the asyncio half — needs asyncio.run()'s loop
    engine = build_engine(settings.rs_db_url)
    counters = new_counters()
    # Default intents only — no privileged intents (spec pin #7).
    client = discord.Client(intents=discord.Intents.default())
    listen_task = sweep_task = gateway_task = metrics_server = None
    try:
        # :9091 side-port (ADR-0015), up before Discord so a wedged login is
        # scrapeable. namespace="" — the ADR names the vocabulary bare.
        metrics_server, _ = start_metrics_server(
            settings.metrics_port, [DictCounterCollector(counters, namespace="")]
        )
        try:
            await client.login(settings.discord_token.get_secret_value())
        except discord.LoginFailure as exc:
            logger.error("Discord login failed — check RS_BOT_DISCORD_TOKEN: %s", exc)
            return 1
        gateway_task = asyncio.create_task(client.connect(), name="gateway")
        try:
            channel = await client.fetch_channel(settings.dev_channel_id)
        except discord.DiscordException as exc:
            logger.error(
                "cannot resolve dev channel %d — check RS_DEV_CHANNEL_ID and the bot's "
                "invite/permissions (operator checklist §4): %s",
                settings.dev_channel_id,
                exc,
            )
            return 1
        if not isinstance(channel, discord.abc.Messageable):
            logger.error(
                "dev channel %d is not a messageable channel (%s)",
                settings.dev_channel_id,
                type(channel).__name__,
            )
            return 1
        logger.info("resolved dev channel %d (%s)", settings.dev_channel_id, channel)

        worker = SweepWorker(
            build_session_factory(engine),
            DiscordChannelSender(channel),
            sweep_seconds=settings.sweep_seconds,
            counters=counters,
        )
        listener = ListenLoop(psycopg_conninfo(settings.rs_db_url), worker.wake, counters=counters)
        listen_task = asyncio.create_task(listener.run(), name="listen")

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, worker.request_stop)

        # run() opens with the unconditional initial sweep — the backlog
        # reconcile; a DB down at startup crashes it, and we exit 1 below.
        sweep_task = asyncio.create_task(worker.run(), name="sweep")
        done, _ = await asyncio.wait({sweep_task, gateway_task}, return_when=asyncio.FIRST_COMPLETED)

        if sweep_task in done:
            if (exc := sweep_task.exception()) is not None:
                logger.error("sweep worker crashed", exc_info=exc)
                return 1
            logger.info("shutdown requested — in-flight post finished")
            return 0
        # client.connect() returns only on close or a fatal gateway error;
        # transient Discord outages are reconnected inside discord.py.
        logger.error("Discord gateway ended unexpectedly", exc_info=gateway_task.exception())
        return 1
    finally:
        for task in (listen_task, sweep_task, gateway_task):
            if task is None:
                continue
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        logger.info("counters at shutdown: %s", counters)
        if metrics_server is not None:
            metrics_server.shutdown()
        await client.close()
        await engine.dispose()


def main() -> int:
    configure_logging("bot")
    install_excepthooks()  # sys/threading; the asyncio half joins in run_bot
    try:
        settings = BotSettings()
    except ValidationError as exc:
        logger.error("invalid configuration: %s", exc)
        return 1
    logger.info(
        "rs_bot starting: dev_channel_id=%d sweep=%.0fs listen_channel=%s",
        settings.dev_channel_id,
        settings.sweep_seconds,
        NOTIFY_CHANNEL,
    )
    return asyncio.run(run_bot(settings))


if __name__ == "__main__":
    sys.exit(main())
