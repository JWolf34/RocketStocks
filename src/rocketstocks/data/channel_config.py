"""ChannelConfigRepository — DB-backed per-guild channel configuration."""
import logging

from rocketstocks.data.db import Postgres

logger = logging.getLogger(__name__)

# Canonical config_type values
REPORTS = "reports"
ALERTS = "alerts"
SCREENERS = "screeners"
CHARTS = "charts"
NOTIFICATIONS = "notifications"
ALL_CONFIG_TYPES = [REPORTS, ALERTS, SCREENERS, CHARTS, NOTIFICATIONS]


class ChannelConfigRepository:
    def __init__(self, db: Postgres = None):
        self._db = db or Postgres()

    def get_channel_id(self, guild_id: int, config_type: str) -> int | None:
        """Return the configured channel ID for (guild_id, config_type), or None."""
        row = self._db.select(
            table="channel_config",
            fields=["channel_id"],
            where_conditions=[("guild_id", "=", guild_id), ("config_type", "=", config_type)],
            fetchall=False,
        )
        return row[0] if row else None

    def get_all_for_guild(self, guild_id: int) -> dict[str, int]:
        """Return a dict mapping config_type -> channel_id for all configured types in a guild."""
        rows = self._db.select(
            table="channel_config",
            fields=["config_type", "channel_id"],
            where_conditions=[("guild_id", "=", guild_id)],
        )
        return {row[0]: row[1] for row in rows} if rows else {}

    def get_all_guilds_for_type(self, config_type: str) -> list[tuple[int, int]]:
        """Return [(guild_id, channel_id)] for every guild that has config_type configured."""
        rows = self._db.select(
            table="channel_config",
            fields=["guild_id", "channel_id"],
            where_conditions=[("config_type", "=", config_type)],
        )
        return [(row[0], row[1]) for row in rows] if rows else []

    def upsert_channel(self, guild_id: int, config_type: str, channel_id: int) -> None:
        """Insert or update a channel mapping.  Uses ON CONFLICT DO UPDATE to overwrite."""
        with self._db._cursor() as cur:
            cur.execute(
                """
                INSERT INTO channel_config (guild_id, config_type, channel_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (guild_id, config_type) DO UPDATE SET channel_id = EXCLUDED.channel_id
                """,
                (guild_id, config_type, channel_id),
            )
        logger.debug(f"Upserted channel_config: guild={guild_id} type={config_type} channel={channel_id}")

    def delete_channel(self, guild_id: int, config_type: str) -> None:
        """Remove the channel mapping for a specific config_type in a guild."""
        self._db.delete(
            table="channel_config",
            where_conditions=[("guild_id", "=", guild_id), ("config_type", "=", config_type)],
        )

    def delete_guild(self, guild_id: int) -> None:
        """Remove all channel mappings for a guild."""
        self._db.delete(
            table="channel_config",
            where_conditions=[("guild_id", "=", guild_id)],
        )

    def is_fully_configured(self, guild_id: int) -> bool:
        """Return True if all 5 config types are set for the guild."""
        configured = self.get_all_for_guild(guild_id)
        return all(ct in configured for ct in ALL_CONFIG_TYPES)

    def get_unconfigured_guilds(self, guild_ids: list[int]) -> list[int]:
        """Return the subset of guild_ids that are not fully configured."""
        return [gid for gid in guild_ids if not self.is_fully_configured(gid)]
