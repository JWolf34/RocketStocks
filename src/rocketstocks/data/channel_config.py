"""ChannelConfigRepository — DB-backed per-guild channel configuration."""
import logging

logger = logging.getLogger(__name__)

# Canonical config_type values
REPORTS = "reports"
ALERTS = "alerts"
SCREENERS = "screeners"
NOTIFICATIONS = "notifications"
TRADE = "trade"
ALL_CONFIG_TYPES = [REPORTS, ALERTS, SCREENERS, NOTIFICATIONS, TRADE]


class ChannelConfigRepository:
    def __init__(self, db=None):
        self._db = db

    async def get_channel_id(self, guild_id: int, config_type: str) -> int | None:
        """Return the configured channel ID for (guild_id, config_type), or None."""
        row = await self._db.execute(
            "SELECT channel_id FROM channel_config WHERE guild_id = %s AND config_type = %s",
            [guild_id, config_type],
            fetchone=True,
        )
        return row[0] if row else None

    async def get_all_for_guild(self, guild_id: int) -> dict[str, int]:
        """Return a dict mapping config_type -> channel_id for all configured types in a guild."""
        rows = await self._db.execute(
            "SELECT config_type, channel_id FROM channel_config WHERE guild_id = %s",
            [guild_id],
        )
        return {row[0]: row[1] for row in rows} if rows else {}

    async def get_all_guilds_for_type(self, config_type: str) -> list[tuple[int, int]]:
        """Return [(guild_id, channel_id)] for every guild that has config_type configured."""
        rows = await self._db.execute(
            "SELECT guild_id, channel_id FROM channel_config WHERE config_type = %s",
            [config_type],
        )
        return [(row[0], row[1]) for row in rows] if rows else []

    async def upsert_channel(self, guild_id: int, config_type: str, channel_id: int) -> None:
        """Insert or update a channel mapping. Uses ON CONFLICT DO UPDATE to overwrite."""
        await self._db.execute(
            """
            INSERT INTO channel_config (guild_id, config_type, channel_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (guild_id, config_type) DO UPDATE SET channel_id = EXCLUDED.channel_id
            """,
            [guild_id, config_type, channel_id],
        )
        logger.debug(f"Upserted channel_config: guild={guild_id} type={config_type} channel={channel_id}")

    async def delete_channel(self, guild_id: int, config_type: str) -> None:
        """Remove the channel mapping for a specific config_type in a guild."""
        await self._db.execute(
            "DELETE FROM channel_config WHERE guild_id = %s AND config_type = %s",
            [guild_id, config_type],
        )

    async def delete_guild(self, guild_id: int) -> None:
        """Remove all channel mappings for a guild."""
        await self._db.execute(
            "DELETE FROM channel_config WHERE guild_id = %s",
            [guild_id],
        )

    async def is_fully_configured(self, guild_id: int) -> bool:
        """Return True if all 5 config types are set for the guild."""
        configured = await self.get_all_for_guild(guild_id)
        return all(ct in configured for ct in ALL_CONFIG_TYPES)

    async def get_unconfigured_guilds(self, guild_ids: list[int]) -> list[int]:
        """Return the subset of guild_ids that are not fully configured."""
        result = []
        for gid in guild_ids:
            if not await self.is_fully_configured(gid):
                result.append(gid)
        return result
