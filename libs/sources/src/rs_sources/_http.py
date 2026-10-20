"""One JSON GET with every transport and decode failure folded into SourceError."""

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
    except (httpx.HTTPError, ValueError) as exc:
        raise SourceError(f"GET {url} failed: {exc!r}") from exc
