# ruff: noqa: F811 - fixture params named after the imported migrated_db_url
# fixture are how pytest injects it, not redefinitions.

import json
import logging

import pytest
import structlog
from sqlalchemy import create_engine, text

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
    """The migrate container's alembic output joins the one JSON stream."""
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


def test_check_passes_on_a_migrated_database(monkeypatch, migrated_db_url):
    monkeypatch.setenv("RS_DB_URL", migrated_db_url)
    assert main(["check"]) == 0


def test_check_fails_on_schema_drift(monkeypatch, capsys, migrated_db_url):
    monkeypatch.setenv("RS_DB_URL", migrated_db_url)
    engine = create_engine(migrated_db_url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE track_record ADD COLUMN drifted integer"))
    try:
        assert main(["check"]) == 1
        assert "drift" in capsys.readouterr().err
    finally:
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE track_record DROP COLUMN drifted"))
        engine.dispose()
