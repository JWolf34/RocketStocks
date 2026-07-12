"""GET /v1/bars/{ticker}?start=&end= — the S005 range read.

Serves the stream-built detection log (the only Phase-1 bar data) — an
explicit, temporary ADR-0022 deviation recorded in the spec; when research
history lands, the backing read swaps and `meta.source` flips from "stream".
The envelope (ADR-0017) is the durable contract: `meta` is extensible,
consumers must ignore unknown fields and treat absent `truncated` as false.
No row cap by design — cap/pagination belong to the Phase-3 contract review.
"""

from datetime import timedelta
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from rs_domain import Bar, UtcDatetime
from rs_persistence import BarRepository

router = APIRouter()

BAR_INTERVAL = timedelta(minutes=5)


class BarsMeta(BaseModel):
    as_of: UtcDatetime | None  # newest returned bar's bucket CLOSE; null if empty
    interval: str
    source: str


class BarsResponse(BaseModel):
    data: list[Bar]
    meta: BarsMeta


@router.get("/v1/bars/{ticker}")
async def get_bars(
    ticker: str,
    # UtcDatetime enforces the domain convention at the edge: naive or
    # non-UTC-offset input → 422 (S002 binding convention).
    start: Annotated[UtcDatetime, Query()],
    end: Annotated[UtcDatetime, Query()],
    request: Request,
) -> BarsResponse:
    if start >= end:
        raise HTTPException(status_code=422, detail="start must be before end")
    async with request.app.state.session_factory() as session:
        bars = await BarRepository(session).list_range(ticker.upper(), start, end)
    as_of = bars[-1].bucket + BAR_INTERVAL if bars else None
    return BarsResponse(data=bars, meta=BarsMeta(as_of=as_of, interval="5m", source="stream"))
