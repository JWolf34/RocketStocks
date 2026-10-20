"""When the Schwab refresh token was issued, read through the bot-safe view."""

from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def schwab_token_issued_at(session: AsyncSession) -> datetime | None:
    """When the stored token was issued, or None before the first login.

    Reads the schwab_token_issue view, so a role that may not read schwab_tokens can use it.
    """
    issued = await session.scalar(text("SELECT issued_at FROM schwab_token_issue"))
    return None if issued is None else issued.astimezone(UTC)
