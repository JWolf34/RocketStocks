# ruff: noqa: F811 — fixture params named after the imported migrated_db_url
# fixture are how pytest injects it, not redefinitions (serve conftest idiom).

import json
import logging

import pytest
import structlog

from rs_migrate.__main__ import main
from rs_persistence.testing import migrated_db_url  # noqa: F401


def test_requires_rs_db_url(monkeypatch, capsys):
    monkeypatch.delenv("RS_DB_URL", raising=False)
    assert main(["current"]) == 2
    assert "RS_DB_URL" in capsys.readouterr().err


def test_requires_an_action():
    with pytest.raises(SystemExit):
        main([])


def test_alembic_records_render_on_the_json_stream(monkeypatch, capsys, migrated_db_url):
    """S006: the migrate container's alembic output joins the one JSON stream
    (alembic is named in the spec's third-party logger list)."""
    monkeypatch.setenv("RS_LOG_FORMAT", "json")
    monkeypatch.setenv("RS_DB_URL", migrated_db_url)
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    try:
        assert main(["current"]) == 0
        json_lines = [
            json.loads(line)
            for line in capsys.readouterr().out.strip().splitlines()
            if line.startswith("{")
        ]
        assert any(
            record["logger"].startswith("alembic") and record["service"] == "migrate"
            for record in json_lines
        )
    finally:
        root.handlers[:] = saved_handlers
        root.setLevel(saved_level)
        structlog.reset_defaults()
