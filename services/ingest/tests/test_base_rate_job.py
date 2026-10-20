"""The weekly base-rate job: schedule, the subprocess boundary and failure reporting."""

import asyncio
import json
import os
import sys
from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo

import httpx
import pytest

from rs_ingest.base_rate import JOB_NAME, research_environment, run_event_study
from rs_ingest.cron import CronTable, Weekly
from rs_ingest.jobs import Dependencies, register_jobs
from rs_observability import OpsSettings, configure_ops
from rs_observability import ops as ops_module


@pytest.fixture
def notices(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    sent: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content)["content"])
        return httpx.Response(204)

    monkeypatch.setattr(ops_module, "_notifier", None)
    configure_ops(
        OpsSettings(
            _env_file=None,
            rs_ops_webhook_url="https://discord.test/api/webhooks/1/secret",
            rs_ops_dedup_window_seconds=0.0,
        ),
        transport=httpx.MockTransport(handler),
    )
    return sent


def python(code: str) -> list[str]:
    return [sys.executable, "-c", code]


def test_registered_for_sunday_evening_chicago_without_schwab():
    cron = CronTable()
    register_jobs(cron, Dependencies(session_factory=None, clock=None, schwab=None))  # type: ignore[arg-type]

    [job] = [j for j in cron if j.name == JOB_NAME]

    assert job.needs_schwab is False
    assert job.time_limit == timedelta(hours=2)
    assert job.schedule == Weekly(6, time(20, 0), ZoneInfo("America/Chicago"))
    # Sunday 2026-10-25 20:00 CDT
    assert job.schedule.next_after(datetime(2026, 10, 20, tzinfo=UTC)) == datetime(
        2026, 10, 26, 1, 0, tzinfo=UTC
    )


def test_the_subprocess_environment_is_an_allowlist():
    source = {
        "PATH": "/bin",
        "RS_RESEARCH_DB_URL": "postgresql+psycopg://rs_research@db/x",
        "RS_DB_URL": "postgresql+psycopg://rs_ingest@db/x",
        "SCHWAB_API_SECRET": "s3cret",
    }
    assert research_environment(source) == {
        "PATH": "/bin",
        "RS_RESEARCH_DB_URL": "postgresql+psycopg://rs_research@db/x",
    }


async def test_a_zero_exit_is_a_quiet_success(notices):
    await run_event_study(python("raise SystemExit(0)"), {})
    assert notices == []


async def test_a_non_zero_exit_sends_an_ops_notice_and_raises(notices):
    with pytest.raises(RuntimeError, match="status 3"):
        await run_event_study(python("raise SystemExit(3)"), {})

    assert len(notices) == 1
    assert "status 3" in notices[0]


async def test_the_subprocess_sees_only_the_allowlisted_environment(tmp_path):
    out = tmp_path / "env.json"
    code = f"import json, os; json.dump(sorted(os.environ), open({str(out)!r}, 'w'))"
    environ = {**os.environ, "RS_RESEARCH_DB_URL": "x", "RS_DB_URL": "y", "SCHWAB_API_KEY": "z"}

    await run_event_study(python(code), environ)

    seen = json.loads(out.read_text())
    assert "RS_RESEARCH_DB_URL" in seen
    assert "RS_DB_URL" not in seen
    assert "SCHWAB_API_KEY" not in seen


async def test_a_cancelled_run_kills_the_subprocess(tmp_path):
    pid_file = tmp_path / "pid"
    code = f"import os, time; open({str(pid_file)!r}, 'w').write(str(os.getpid())); time.sleep(60)"
    task = asyncio.create_task(run_event_study(python(code), {}))
    for _ in range(200):
        if pid_file.exists() and pid_file.read_text():
            break
        await asyncio.sleep(0.05)
    pid = int(pid_file.read_text())

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
