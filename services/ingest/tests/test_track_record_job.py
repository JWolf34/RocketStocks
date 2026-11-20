"""The Track record run: its place in the evening sweep, the subprocess boundary and failure reporting."""

import asyncio
import json
import os
import sys

import pytest
from sqlalchemy import select

from rs_ingest.cron import CronTable
from rs_ingest.jobs import Dependencies, register_jobs
from rs_ingest.outcomes import main
from rs_ingest.track_record import (
    COMMAND,
    run_outcomes,
    subprocess_environment,
)
from rs_persistence import TrackRecordRow

DB_URL = "postgresql+psycopg://rs_ingest:pw@db/rocketstocks"


def python(code: str) -> list[str]:
    return [sys.executable, "-c", code]


def test_it_is_the_last_stage_of_the_evening_sweep_not_a_job_of_its_own():
    cron = CronTable()
    # Registration only builds clients; none of these collaborators is touched.
    deps = Dependencies(
        session_factory=None,  # type: ignore[arg-type]
        db_url=DB_URL,
        clock=None,  # type: ignore[arg-type]
        schwab=None,  # type: ignore[arg-type]
        schwab_limiter=None,  # type: ignore[arg-type]
        calendar=None,  # type: ignore[arg-type]
        http=None,  # type: ignore[arg-type]
        edgar_user_agent="RocketStocks test@example.com",
        trip_token_gate=None,  # type: ignore[arg-type]
        health=None,  # type: ignore[arg-type]
    )
    register_jobs(cron, deps)

    assert "track_record" not in {job.name for job in cron}
    assert deps.run_outcomes is run_outcomes


def test_the_subprocess_environment_is_an_allowlist_plus_the_database_url():
    source = {
        "PATH": "/bin",
        "RS_LOG_LEVEL": "DEBUG",
        "RS_DB_URL": "postgresql+psycopg://stale@db/x",
        "SCHWAB_API_SECRET": "s3cret",
    }
    assert subprocess_environment(source, DB_URL) == {
        "PATH": "/bin",
        "RS_LOG_LEVEL": "DEBUG",
        "RS_DB_URL": DB_URL,
    }


async def test_a_zero_exit_is_a_quiet_success():
    await run_outcomes(DB_URL, python("raise SystemExit(0)"), {})


async def test_a_non_zero_exit_raises_so_the_daemon_counts_and_pings_it():
    with pytest.raises(RuntimeError, match="status 3"):
        await run_outcomes(DB_URL, python("raise SystemExit(3)"), {})


async def test_the_subprocess_sees_the_database_url_but_no_other_secret(tmp_path):
    out = tmp_path / "env.json"
    code = f"import json, os; json.dump(dict(os.environ), open({str(out)!r}, 'w'))"
    environ = {**os.environ, "SCHWAB_API_KEY": "z", "SCHWAB_API_SECRET": "y", "RS_DB_URL": "stale"}

    await run_outcomes(DB_URL, python(code), environ)

    seen = json.loads(out.read_text())
    assert seen["RS_DB_URL"] == DB_URL
    assert "SCHWAB_API_KEY" not in seen
    assert "SCHWAB_API_SECRET" not in seen


async def test_a_cancelled_run_kills_the_subprocess(tmp_path):
    pid_file = tmp_path / "pid"
    code = f"import os, time; open({str(pid_file)!r}, 'w').write(str(os.getpid())); time.sleep(60)"
    task = asyncio.create_task(run_outcomes(DB_URL, python(code), {}))
    async with asyncio.timeout(10):
        while not (pid_file.exists() and pid_file.read_text()):
            await asyncio.sleep(0.05)
    pid = int(pid_file.read_text())

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


def test_the_entry_point_refuses_to_run_without_a_database_url(monkeypatch, capsys):
    monkeypatch.delenv("RS_DB_URL", raising=False)

    assert main() == 2
    assert "RS_DB_URL is not set" in capsys.readouterr().err


async def test_the_real_subprocess_connects_measures_and_writes_the_track_record(session_factory, migrated_db_url):
    # An empty store has no cases: the run still writes a Track record saying zero Outcomes per horizon.
    await run_outcomes(migrated_db_url, COMMAND)

    async with session_factory() as s:
        rows = (await s.scalars(select(TrackRecordRow).order_by(TrackRecordRow.horizon))).all()
    assert [(r.horizon, r.cases, r.outcomes) for r in rows] == [(1, 0, 0), (5, 0, 0), (20, 0, 0)]


async def test_a_subprocess_that_cannot_reach_the_database_fails_the_job():
    with pytest.raises(RuntimeError, match="status 1"):
        await run_outcomes("postgresql+psycopg://nobody:x@127.0.0.1:1/none", COMMAND)
