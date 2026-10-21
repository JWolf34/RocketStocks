"""The plan-citation gate script itself: it must flag every citation spelling and
retired word, honour its small allowlist, and never go silently dead.

The banned spellings are assembled from fragments so this file does not trip
the gate it tests."""

import os
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "check_plan_citations.sh"

MIGRATION = "libs/a/src/rs_persistence/migrations/versions/0099_x.py"
# The real allowlist names the persistence member, so plant files under it.
REAL_MIGRATION = "libs/persistence/src/rs_persistence/migrations/versions/0099_x.py"
REAL_MIGRATION_TEST = "libs/persistence/tests/test_migrations_0006_onward.py"


def _run(root: Path) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PLAN_CITATIONS_GATE_ROOT": str(root)}
    return subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True)


def _plant(root: Path, rel: str, text: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text + "\n")


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    for member in ("libs/a", "services/b", "tools/c"):
        (tmp_path / member / "src" / "pkg").mkdir(parents=True)
        (tmp_path / member / "tests").mkdir()
    return tmp_path


CITATIONS = [
    "# " + "ADR" + "-0015 says so",
    "# (" + "grilling" + " #4)",
    "# " + "slice" + " 1 needs this",
    "# " + "Slice" + " 2",
    "# " + "slice" + "-3 table",
    "# " + "slice" + "_4 table",
    "# " + "slice" + "1 table",
    "# the " + "S0" + "04 cursor",
    "x = '" + "S0" + "06'",
]

RETIRED_WORDS = [
    "# " + "attent" + "ion surge",
    "# " + "Attent" + "ion",
    "# " + "ATTENT" + "ION",
    "x = 'popularity_" + "attent" + "ion'",
    "# " + "base" + "_rate",
    "# " + "BASE" + "_RATE",
    "# the " + "base" + " rate",
    "# the " + "Base" + " Rate",
    "# " + "Base" + "Rate",
]


@pytest.mark.parametrize("line", CITATIONS + RETIRED_WORDS)
@pytest.mark.parametrize("where", ["libs/a/src/pkg/mod.py", "services/b/tests/test_mod.py", "tools/c/src/pkg/mod.py"])
def test_a_planted_citation_fails_in_src_and_tests(tree, where, line):
    _plant(tree, where, line)
    result = _run(tree)
    assert result.returncode == 1, result.stdout + result.stderr
    assert where in result.stderr
    assert line in result.stderr


def test_a_clean_tree_passes(tree):
    _plant(
        tree,
        "libs/a/src/pkg/mod.py",
        '"""A slice of the list; the token row has a single writer.\n'
        "Nothing here cites a plan: " + "ADR" + " numbers, S1, S0042, slices and ADR-12 are not citations,\n"
        'and neither is an adrift boat."""\n'
        "rest = items[1:]\n",
    )
    _plant(tree, "services/b/tests/test_mod.py", "# popularity surge and track record\n")
    result = _run(tree)
    assert result.returncode == 0, result.stdout + result.stderr


def test_a_cache_directory_is_not_scanned(tree):
    _plant(tree, "libs/a/src/pkg/__pycache__/mod.py", "# " + "ADR" + "-0015")
    assert _run(tree).returncode == 0


def test_migration_files_may_keep_the_old_names(tree):
    _plant(tree, REAL_MIGRATION, "# renames " + "attent" + "ion_surge and " + "base" + "_rate")
    _plant(tree, REAL_MIGRATION_TEST, "x = '" + "attent" + "ion_surge'")
    result = _run(tree)
    assert result.returncode == 0, result.stdout + result.stderr


def test_the_old_names_stay_banned_in_a_lookalike_migration_path(tree):
    _plant(tree, MIGRATION, "# " + "attent" + "ion")
    assert _run(tree).returncode == 1


def test_a_different_test_file_is_not_allowlisted(tree):
    _plant(tree, "libs/persistence/tests/test_other.py", "# " + "base" + "_rate")
    assert _run(tree).returncode == 1


@pytest.mark.parametrize("path", [REAL_MIGRATION, REAL_MIGRATION_TEST])
def test_citations_are_banned_even_in_allowlisted_files(tree, path):
    _plant(tree, path, "# " + "ADR" + "-0004")
    result = _run(tree)
    assert result.returncode == 1, result.stdout + result.stderr
    assert path in result.stderr


def test_a_root_without_members_is_an_error_not_a_pass(tmp_path):
    (tmp_path / "libs/a/src").mkdir(parents=True)
    (tmp_path / "services").mkdir()
    result = _run(tmp_path)
    assert result.returncode == 2
    assert "services" in result.stderr
