import pytest

from rs_migrate.__main__ import main


def test_requires_rs_db_url(monkeypatch, capsys):
    monkeypatch.delenv("RS_DB_URL", raising=False)
    assert main(["current"]) == 2
    assert "RS_DB_URL" in capsys.readouterr().err


def test_requires_an_action():
    with pytest.raises(SystemExit):
        main([])
