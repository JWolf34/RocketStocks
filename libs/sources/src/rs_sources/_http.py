"""One JSON GET with every transport and decode failure folded into SourceError."""

from datetime import datetime
from typing import Any

import httpx

from rs_sources.errors import SourceError


async def get_json(
    http: httpx.AsyncClient,
    url: str,
    *,
    headers: dict[str, str],
    params: dict[str, str] | None = None,
) -> Any:
    try:
        response = await http.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as exc:
        raise SourceError(f"GET {url} failed: {exc!r}", status_code=exc.response.status_code) from exc
    except (httpx.HTTPError, ValueError) as exc:
        raise SourceError(f"GET {url} failed: {exc!r}") from exc


def require_aware(moment: datetime) -> None:
    # A naive instant would be read as local time by astimezone and crash comparisons.
    if moment.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
