"""The daemon as an operator runs it: a real `python -m rs_ingest` process
against the test database, probed through its metrics port and healthcheck
command, stopped with SIGTERM."""

import asyncio
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from rs_persistence import SchwabTokenStore, build_engine, build_session_factory

# Shape schwab-py expects of a stored token; nothing here ever reaches Schwab.
STORED_TOKEN = {
    "creation_timestamp": 1_760_000_000,
    "token": {
        "access_token": "a",
        "refresh_token": "r",
        "expires_in": 1800,
        "token_type": "Bearer",
        "scope": "api",
        "expires_at": 1_760_001_800,
    },
}
WAIT_SECONDS = 30.0


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class DaemonProcess:
    def __init__(self, db_url: str, workdir: Path) -> None:
        self.port = free_port()
        self.env = {key: value for key, value in os.environ.items() if not key.startswith("RS_")}
        self.env |= {
            "RS_DB_URL": db_url,
            "SCHWAB_API_KEY": "key",
            "SCHWAB_API_SECRET": "secret",
            "RS_EDGAR_USER_AGENT": "RocketStocks test@example.com",
            # The startup poll must never reach the real vendors: a refused proxy fails it fast.
            "HTTPS_PROXY": "http://127.0.0.1:9",
            "HTTP_PROXY": "http://127.0.0.1:9",
            "NO_PROXY": "127.0.0.1,localhost",  # the healthcheck probe still reaches the metrics port
            "RS_INGEST_METRICS_PORT": str(self.port),
            "RS_INGEST_TOKEN_POLL_SECONDS": "0.2",
            "RS_LOG_FORMAT": "json",
        }
        self.process = subprocess.Popen(
            [sys.executable, "-m", "rs_ingest"],
            env=self.env,
            cwd=workdir,  # no .env here, so only the environment above applies
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

    def metrics(self) -> str:
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/metrics", timeout=2) as response:
            return str(response.read().decode())

    def healthcheck(self) -> int:
        return subprocess.run(
            [sys.executable, "-m", "rs_ingest.healthcheck"],
            env=self.env,
            capture_output=True,
            timeout=15,
        ).returncode

    def wait_for(self, condition: Callable[[], bool]) -> None:
        deadline = time.monotonic() + WAIT_SECONDS
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                pytest.fail(f"daemon exited early ({self.process.returncode}): {self.output()}")
            try:
                if condition():
                    return
            except OSError:
                pass
            time.sleep(0.1)
        pytest.fail(f"condition not met in {WAIT_SECONDS}s; output: {self.output()}")

    def output(self) -> str:
        if self.process.poll() is None:
            return "(still running)"
        return str(self.process.stdout.read()) if self.process.stdout else ""

    def terminate(self) -> int:
        self.process.send_signal(signal.SIGTERM)
        return self.process.wait(timeout=WAIT_SECONDS)


def clear_token(db_url: str) -> None:
    engine = create_engine(db_url)
    try:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM schwab_tokens"))
    finally:
        engine.dispose()


@pytest.fixture
def daemon(migrated_db_url: str, tmp_path: Path) -> Iterator[DaemonProcess]:
    clear_token(migrated_db_url)  # the DB is shared across the session; start token-less
    running = DaemonProcess(migrated_db_url, tmp_path)
    yield running
    if running.process.poll() is None:
        running.process.kill()
        running.process.wait()


def store_token(db_url: str) -> None:
    async def save() -> None:
        engine = build_engine(db_url)
        try:
            await SchwabTokenStore(build_session_factory(engine)).save(STORED_TOKEN)
        finally:
            await engine.dispose()

    asyncio.run(save())


def test_daemon_without_a_token_stays_up_gated_and_exits_zero_on_sigterm(daemon):
    daemon.wait_for(lambda: daemon.healthcheck() == 0)
    daemon.wait_for(lambda: "schwab_token_dead 1.0" in daemon.metrics())

    assert daemon.terminate() == 0
    output = daemon.process.stdout.read()
    assert "no Schwab token stored" in output
    assert "rs_ingest daemon stopped" in output


def test_a_token_stored_after_startup_resumes_the_gate(daemon, migrated_db_url):
    daemon.wait_for(lambda: "schwab_token_dead 1.0" in daemon.metrics())

    store_token(migrated_db_url)

    daemon.wait_for(lambda: "schwab_token_dead 0.0" in daemon.metrics())
    assert daemon.terminate() == 0


def test_healthcheck_fails_when_the_daemon_is_not_running(daemon):
    daemon.wait_for(lambda: daemon.healthcheck() == 0)
    daemon.terminate()

    assert daemon.healthcheck() == 1
