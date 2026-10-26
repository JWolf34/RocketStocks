"""Daemon seams that don't need Discord: the DSN conversion for the raw-psycopg LISTEN
connection. The full lifecycle (channel resolution, SIGTERM, kill/restart) is covered by
the live acceptance runbook; the Discord sender is in test_discord_sender."""

from rs_bot.__main__ import psycopg_conninfo


def test_psycopg_conninfo_strips_the_sqlalchemy_driver():
    assert (
        psycopg_conninfo("postgresql+psycopg://u:p@localhost:5498/rs")
        == "postgresql://u:p@localhost:5498/rs"
    )
