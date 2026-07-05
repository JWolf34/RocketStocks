"""Repository for the alert_roles table."""
import logging

logger = logging.getLogger(__name__)

_TABLE = 'alert_roles'


class AlertRolesRepository:
    """Async repository for tracking alert subscription roles per guild."""

    def __init__(self, db=None):
        self._db = db

    async def upsert(self, guild_id: int, role_key: str, role_id: int) -> None:
        """Insert or update a role mapping for a guild."""
        await self._db.execute(
            """
            INSERT INTO alert_roles (guild_id, role_key, role_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (guild_id, role_key) DO UPDATE SET role_id = EXCLUDED.role_id
            """,
            [guild_id, role_key, role_id],
        )
        logger.debug(f"Upserted alert role: guild={guild_id} key={role_key} role_id={role_id}")

    async def get_role_id(self, guild_id: int, role_key: str) -> int | None:
        """Return the Discord role ID for (guild_id, role_key), or None."""
        row = await self._db.execute(
            "SELECT role_id FROM alert_roles WHERE guild_id = %s AND role_key = %s",
            [guild_id, role_key],
            fetchone=True,
        )
        return row[0] if row else None

    async def get_role_ids(self, guild_id: int, keys: list[str]) -> list[int]:
        """Return role IDs for all matching (guild_id, role_key) pairs."""
        if not keys:
            return []
        placeholders = ", ".join(["%s"] * len(keys))
        rows = await self._db.execute(
            f"SELECT role_id FROM alert_roles WHERE guild_id = %s AND role_key IN ({placeholders})",
            [guild_id, *keys],
        )
        return [row[0] for row in (rows or [])]

    async def get_all_for_guild(self, guild_id: int) -> dict[str, int]:
        """Return all role mappings for a guild as {role_key: role_id}."""
        rows = await self._db.execute(
            "SELECT role_key, role_id FROM alert_roles WHERE guild_id = %s",
            [guild_id],
        )
        return {row[0]: row[1] for row in (rows or [])}

    async def delete_role(self, guild_id: int, role_key: str) -> None:
        """Remove a single role mapping for a guild."""
        await self._db.execute(
            "DELETE FROM alert_roles WHERE guild_id = %s AND role_key = %s",
            [guild_id, role_key],
        )
        logger.debug(f"Deleted alert role: guild={guild_id} key={role_key}")

    async def delete_guild(self, guild_id: int) -> None:
        """Remove all role mappings for a guild."""
        await self._db.execute(
            "DELETE FROM alert_roles WHERE guild_id = %s",
            [guild_id],
        )
        logger.debug(f"Deleted all alert roles for guild={guild_id}")
