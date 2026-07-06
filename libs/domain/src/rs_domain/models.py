"""Domain models — the validated shapes the rest of v2 consumes (ADR-0004).

Prices are float64: the wire delivers JSON floats, detection math runs in
float64, and DOUBLE PRECISION round-trips them bit-exactly, which the replay
contract (ADR-0023) depends on. NUMERIC/Decimal is reserved for future
accounting-grade tables (auto-trader era), never this path.
"""

from pydantic import BaseModel, ConfigDict, NonNegativeInt

from rs_domain._types import UtcDatetime


class Bar(BaseModel):
    """One closed bar as detection saw it. `bucket` is the UTC start instant."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    bucket: UtcDatetime
    open: float
    high: float
    low: float
    close: float
    volume: NonNegativeInt


class Alert(BaseModel):
    """A detection decision to announce. Minimum shape S003 needs; grows later."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    alert_type: str
    triggered_at: UtcDatetime
    payload: dict = {}
