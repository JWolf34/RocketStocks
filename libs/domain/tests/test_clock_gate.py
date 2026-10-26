"""The clock-discipline gate script itself: it must flag every ambient-time
spelling and must never go silently dead."""

import os
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "check_clock_discipline.sh"


def _run(root: Path) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "CLOCK_GATE_ROOT": str(root)}
    return subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True)


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    for member in ("libs/a", "services/b", "tools/c"):
        (tmp_path / member / "src" / "pkg").mkdir(parents=True)
    return tmp_path


@pytest.mark.parametrize(
    "line",
    [
        "x = datetime.now(UTC)",
        "x = datetime.utcnow()",
        "x = datetime.datetime.now()",
        "x = datetime.today()",
        "x = date.today()",
        "x = time.time()",
        "x = time.monotonic()",
        "from time import monotonic",
        "from time import perf_counter, time",
        "import datetime as dt",
        "from datetime import UTC, datetime as dt",
        "    datetime as dt,",
        "import time as t",
        "x = loop.time()",
        "x = asyncio.get_running_loop().time()",
        "x = self._loop.time()",
    ],
)
@pytest.mark.parametrize("member", ["libs/a", "services/b", "tools/c"])
def test_ambient_time_is_flagged_in_every_member(tree, member, line):
    (tree / member / "src" / "pkg" / "mod.py").write_text(line + "\n")
    result = _run(tree)
    assert result.returncode == 1, result.stdout + result.stderr
    assert "mod.py" in result.stderr


def test_allowed_spellings_pass(tree):
    (tree / "libs/a/src/pkg/mod.py").write_text(
        "from datetime import UTC, datetime\n"
        "import time\n"
        "started = time.perf_counter()\n"
        "t = datetime(2026, 7, 6, tzinfo=UTC).time()\n"
        '"""The next earnings date as of the given day; time as of now."""\n'
        "clock.now()\n"
    )
    result = _run(tree)
    assert result.returncode == 0, result.stdout + result.stderr


def test_the_clock_implementation_is_allowlisted(tree):
    clock = tree / "libs/a/src/rs_domain/clock.py"
    clock.parent.mkdir(parents=True)
    clock.write_text("x = datetime.now(UTC)\n")
    assert _run(tree).returncode == 0


@pytest.mark.parametrize(
    "path", ["libs/a/src/rs_sources/_ratelimit.py", "services/b/src/rs_ingest/rate_limit.py"]
)
def test_the_rate_limiters_may_read_the_loop_clock(tree, path):
    limiter = tree / path
    limiter.parent.mkdir(parents=True, exist_ok=True)
    limiter.write_text("x = asyncio.get_running_loop().time()\n")
    assert _run(tree).returncode == 0


def test_a_root_without_members_is_an_error_not_a_pass(tmp_path):
    (tmp_path / "libs/a/src").mkdir(parents=True)
    (tmp_path / "services").mkdir()
    result = _run(tmp_path)
    assert result.returncode == 2
    assert "services" in result.stderr

