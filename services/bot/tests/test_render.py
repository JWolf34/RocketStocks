"""render(PendingAlert) -> Embed: built from the alert row alone (ADR-0024),
raw payload numbers shown as-is (ADR-0026 transparency), alert id in the
footer. A render exception yields the generic fallback embed — an alert can
be render-ugly, never render-poisoned (spec pin #5a)."""

from datetime import UTC, datetime

import rs_bot.render
from rs_bot.render import render
from rs_domain import Alert
from rs_persistence import PendingAlert

PENDING = PendingAlert(
    id=42,
    alert=Alert(
        symbol="SPY",
        alert_type="volume_threshold",
        triggered_at=datetime(2026, 7, 6, 14, 30, tzinfo=UTC),
        payload={"volume": 750_000, "threshold": 500_000},
    ),
)


def test_embed_carries_symbol_type_timestamp_payload_and_id():
    embed = render(PENDING)
    assert "SPY" in embed.title and "volume_threshold" in embed.title
    assert embed.timestamp == PENDING.alert.triggered_at
    fields = {f.name: f.value for f in embed.fields}
    assert fields == {"volume": "750000", "threshold": "500000"}
    assert "42" in embed.footer.text


def test_render_failure_falls_back_to_generic_embed(monkeypatch):
    def poisoned(pending):
        raise RuntimeError("planted render fault")

    monkeypatch.setattr(rs_bot.render, "_render_full", poisoned)
    embed = render(PENDING)
    # The generic shape: symbol + type, triggered_at, payload as text, id.
    assert "SPY" in embed.title and "volume_threshold" in embed.title
    assert embed.timestamp == PENDING.alert.triggered_at
    assert "750000" in embed.description and "threshold" in embed.description
    assert "42" in embed.footer.text
