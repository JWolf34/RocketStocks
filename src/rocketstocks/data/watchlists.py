import logging

logger = logging.getLogger(__name__)

# Watchlist type constants
NAMED = 'named'
PERSONAL = 'personal'
SYSTEM = 'system'
CLASSIFICATION = 'classification'

# Default types shown to users in commands / autocomplete
_USER_TYPES = (NAMED, PERSONAL)
# Default types excluded from system-internal consumers (alerts, classification job)
_NON_SYSTEM_TYPES = (NAMED, PERSONAL, CLASSIFICATION)


class Watchlists(object):

    def __init__(self, db):
        self.db = db
        self.db_table = 'watchlists'
        self.db_fields = ['id', 'tickers', 'systemgenerated', 'watchlist_type', 'owner_id', 'display_name']

    # ------------------------------------------------------------------
    # Static helpers
    # ------------------------------------------------------------------

    @staticmethod
    def resolve_personal_id(user_id: int) -> str:
        """Convert a Discord user ID to the personal watchlist storage key."""
        return f"personal:{user_id}"

    @staticmethod
    def _display_name_for(watchlist_id: str, watchlist_type: str, display_name: str | None) -> str:
        """Return the human-readable name for a watchlist row."""
        if watchlist_type == PERSONAL:
            return "personal"
        if display_name:
            return display_name
        return watchlist_id

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    async def get_watchlist_tickers(self, watchlist_id: str) -> list[str]:
        logger.debug(f"Fetching tickers from watchlist with ID '{watchlist_id}'")
        row = await self.db.execute(
            "SELECT tickers FROM watchlists WHERE id = %s",
            [watchlist_id],
            fetchone=True,
        )
        if row is None:
            return []
        return sorted(row[0].split()) if row[0] else []

    async def get_watchlist_counts(self, watchlist_types: list[str] | None = None) -> dict:
        """Return {display_name: ticker_count}.

        ``watchlist_types`` filters which categories are included.  Defaults to
        named + personal (user-facing watchlists only).
        """
        logger.debug("Fetching watchlist counts")
        types = watchlist_types if watchlist_types is not None else list(_USER_TYPES)
        placeholders = ','.join(['%s'] * len(types))
        rows = await self.db.execute(
            f"SELECT id, tickers, watchlist_type, display_name FROM watchlists "
            f"WHERE watchlist_type IN ({placeholders})",
            types,
        )
        result = {}
        for row in (rows or []):
            wl_id, tickers_str, wl_type, disp = row[0], row[1], row[2], row[3]
            name = self._display_name_for(wl_id, wl_type, disp)
            count = len(tickers_str.split()) if tickers_str and tickers_str.strip() else 0
            result[name] = count
        return result

    async def get_all_watchlist_tickers(self, watchlist_types: list[str] | None = None) -> list[str]:
        """Return a sorted, deduplicated list of all tickers across the specified watchlist types.

        Defaults to named watchlists only (historical behaviour for alert pipeline).
        """
        logger.debug("Fetching tickers from all available watchlists")
        types = watchlist_types if watchlist_types is not None else [NAMED]
        placeholders = ','.join(['%s'] * len(types))
        rows = await self.db.execute(
            f"SELECT tickers FROM watchlists WHERE watchlist_type IN ({placeholders})",
            types,
        )
        tickers: list[str] = []
        for row in (rows or []):
            if row[0]:
                tickers += row[0].split()
        return sorted(set(tickers))

    async def get_watchlists(self, watchlist_types: list[str] | None = None) -> list[str]:
        """Return a sorted list of watchlist identifiers.

        For personal watchlists the returned value is the literal string "personal"
        (not the storage key) so it can be fed directly to Discord autocomplete.
        Defaults to named watchlists; pass ``watchlist_types=['named', 'personal']``
        to include personal.
        """
        logger.debug("Fetching all watchlists")
        types = watchlist_types if watchlist_types is not None else [NAMED]
        placeholders = ','.join(['%s'] * len(types))
        rows = await self.db.execute(
            f"SELECT id, watchlist_type, display_name FROM watchlists "
            f"WHERE watchlist_type IN ({placeholders})",
            types,
        )
        names: list[str] = []
        has_personal = False
        for row in (rows or []):
            wl_id, wl_type, disp = row[0], row[1], row[2]
            name = self._display_name_for(wl_id, wl_type, disp)
            if name == "personal":
                has_personal = True  # deduplicate — many users, one "personal" entry
            else:
                names.append(name)
        if has_personal or PERSONAL in types:
            names.append("personal")
        return sorted(names)

    async def get_ticker_to_watchlist_map(self, watchlist_types: list[str] | None = None) -> dict[str, str]:
        """Return {ticker: display_name} for all watchlists of the given types.

        Personal watchlists map to "Personal" (title-cased) so the display name
        can be shown directly in alert embeds.
        Defaults to named + personal.
        """
        logger.debug("Building ticker-to-watchlist map")
        types = watchlist_types if watchlist_types is not None else list(_USER_TYPES)
        placeholders = ','.join(['%s'] * len(types))
        rows = await self.db.execute(
            f"SELECT id, tickers, watchlist_type, display_name FROM watchlists "
            f"WHERE watchlist_type IN ({placeholders})",
            types,
        )
        mapping: dict[str, str] = {}
        for row in (rows or []):
            wl_id, tickers_str, wl_type, disp = row[0], row[1], row[2], row[3]
            label = "Personal" if wl_type == PERSONAL else (disp or wl_id)
            if tickers_str:
                for ticker in tickers_str.split():
                    if ticker:
                        mapping[ticker] = label
        return mapping

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    async def update_watchlist(self, watchlist_id: str, tickers: list[str]) -> None:
        logger.debug(f"Updating watchlist '{watchlist_id}': {tickers}")
        await self.db.execute(
            "UPDATE watchlists SET tickers = %s WHERE id = %s",
            [' '.join(tickers), watchlist_id],
        )

    async def create_watchlist(
        self,
        watchlist_id: str,
        tickers: list[str],
        watchlist_type: str = NAMED,
        owner_id: int | None = None,
        display_name: str | None = None,
        # Legacy parameter kept for backward compatibility; derives systemgenerated
        systemGenerated: bool | None = None,
    ) -> None:
        logger.debug(f"Creating watchlist with ID '{watchlist_id}' and tickers {tickers}")
        if systemGenerated is not None:
            watchlist_type = SYSTEM if systemGenerated else watchlist_type
        system_gen = watchlist_type == SYSTEM
        await self.db.execute(
            "INSERT INTO watchlists (id, tickers, systemgenerated, watchlist_type, owner_id, display_name) "
            "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            [watchlist_id, " ".join(tickers), system_gen, watchlist_type, owner_id, display_name],
        )

    async def delete_watchlist(self, watchlist_id: str) -> None:
        logger.debug(f"Deleting watchlist '{watchlist_id}'...")
        await self.db.execute(
            "DELETE FROM watchlists WHERE id = %s",
            [watchlist_id],
        )

    async def rename_watchlist(self, old_id: str, new_id: str) -> bool:
        """Rename a watchlist atomically. Returns False if old_id doesn't exist or new_id already exists."""
        logger.debug(f"Renaming watchlist '{old_id}' to '{new_id}'")
        if not await self.validate_watchlist(old_id):
            logger.warning(f"Cannot rename: watchlist '{old_id}' does not exist")
            return False
        if await self.validate_watchlist(new_id):
            logger.warning(f"Cannot rename: watchlist '{new_id}' already exists")
            return False
        async with self.db.transaction() as conn:
            cur = await conn.execute(
                "SELECT tickers, systemgenerated, watchlist_type, owner_id, display_name "
                "FROM watchlists WHERE id = %s",
                [old_id],
            )
            row = await cur.fetchone()
            tickers_str, system_gen, wl_type, owner_id, disp = row[0], row[1], row[2], row[3], row[4]
            await conn.execute(
                "INSERT INTO watchlists (id, tickers, systemgenerated, watchlist_type, owner_id, display_name) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                [new_id, tickers_str, system_gen, wl_type, owner_id, new_id],
            )
            await conn.execute(
                "DELETE FROM watchlists WHERE id = %s",
                [old_id],
            )
        return True

    async def validate_watchlist(self, watchlist_id: str) -> bool:
        logger.debug(f"Validating watchlist '{watchlist_id}' exists")
        row = await self.db.execute(
            "SELECT id FROM watchlists WHERE id = %s",
            [watchlist_id],
            fetchone=True,
        )
        if row is None:
            logger.warning(f"Watchlist '{watchlist_id}' does not exist")
            return False
        return True

    async def get_classification_overrides(self) -> dict[str, str]:
        """Return {ticker: classification} from watchlists with watchlist_type='classification'."""
        logger.debug("Fetching classification overrides from watchlists")
        rows = await self.db.execute(
            "SELECT id, tickers, display_name FROM watchlists WHERE watchlist_type = 'classification'"
        )
        overrides: dict[str, str] = {}
        for row in (rows or []):
            watchlist_id, tickers_str, display_name = row[0], row[1], row[2]
            # display_name holds the category (set during migration); fall back to id prefix strip
            category = display_name or watchlist_id[len('class:'):]
            for ticker in (tickers_str or '').split():
                if ticker:
                    overrides[ticker] = category
        logger.debug(f"Found {len(overrides)} classification overrides")
        return overrides
