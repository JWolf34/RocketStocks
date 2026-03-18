import logging

logger = logging.getLogger(__name__)


class Watchlists(object):

    def __init__(self, db):
        self.db = db
        self.db_table = 'watchlists'
        self.db_fields = ['id', 'tickers', 'systemgenerated']

    async def get_watchlist_tickers(self, watchlist_id):
        logger.debug(f"Fetching tickers from watchlist with ID '{watchlist_id}'")
        row = await self.db.execute(
            "SELECT tickers FROM watchlists WHERE id = %s",
            [watchlist_id],
            fetchone=True,
        )
        if row is None:
            return []
        return sorted(row[0].split())

    async def get_watchlist_counts(self, no_personal=True, no_systemGenerated=True) -> dict:
        """Return {watchlist_id: ticker_count} in a single query (no N+1)."""
        logger.debug("Fetching watchlist counts")
        watchlists = await self.db.execute(
            "SELECT id, tickers, systemgenerated FROM watchlists"
        )
        result = {}
        for row in (watchlists or []):
            wl_id, tickers_str, is_system = row[0], row[1], row[2]
            if wl_id.isdigit() and no_personal:
                continue
            if is_system and no_systemGenerated:
                continue
            count = len(tickers_str.split()) if tickers_str and tickers_str.strip() else 0
            result[wl_id] = count
        return result

    async def get_all_watchlist_tickers(self, no_personal=True, no_systemGenerated=True):
        logger.debug("Fetching tickers from all available watchlists (besides personal)")
        watchlists = await self.db.execute(
            "SELECT id, tickers, systemgenerated FROM watchlists"
        )
        tickers = []
        for watchlist in (watchlists or []):
            watchlist_id, watchlist_tickers, is_systemGenerated = watchlist[0], watchlist[1].split(), watchlist[2]
            if watchlist_id.isdigit() and no_personal:
                pass
            elif is_systemGenerated and no_systemGenerated:
                pass
            else:
                tickers += watchlist_tickers
        return sorted(set(tickers))

    async def get_watchlists(self, no_personal=True, no_systemGenerated=True):
        logger.debug("Fetching all watchlists")
        filtered_watchlists = []
        watchlists = await self.db.execute(
            "SELECT id, tickers, systemgenerated FROM watchlists"
        )
        for watchlist in (watchlists or []):
            watchlist_id = watchlist[0]
            is_systemGenerated = watchlist[2]
            if watchlist_id.isdigit() and no_personal:
                pass
            elif is_systemGenerated and no_systemGenerated:
                pass
            else:
                filtered_watchlists.append(watchlist_id)
        if no_personal:
            filtered_watchlists.append("personal")
        return sorted(filtered_watchlists)

    async def update_watchlist(self, watchlist_id, tickers):
        logger.debug(f"Updating watchlist '{watchlist_id}': {tickers}")
        await self.db.execute(
            "UPDATE watchlists SET tickers = %s WHERE id = %s",
            [' '.join(tickers), watchlist_id],
        )

    async def create_watchlist(self, watchlist_id, tickers, systemGenerated):
        logger.debug(f"Creating watchlist with ID '{watchlist_id}' and tickers {tickers}")
        await self.db.execute(
            "INSERT INTO watchlists (id, tickers, systemgenerated) VALUES (%s, %s, %s) "
            "ON CONFLICT DO NOTHING",
            [watchlist_id, " ".join(tickers), systemGenerated],
        )

    async def delete_watchlist(self, watchlist_id):
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
                "SELECT tickers, systemgenerated FROM watchlists WHERE id = %s",
                [old_id],
            )
            row = await cur.fetchone()
            tickers_str, system_generated = row[0], row[1]
            await conn.execute(
                "INSERT INTO watchlists (id, tickers, systemgenerated) VALUES (%s, %s, %s)",
                [new_id, tickers_str, system_generated],
            )
            await conn.execute(
                "DELETE FROM watchlists WHERE id = %s",
                [old_id],
            )
        return True

    async def validate_watchlist(self, watchlist_id):
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
        """Return {ticker: classification} from watchlists named ``class:<category>``."""
        logger.debug("Fetching classification overrides from watchlists")
        watchlists = await self.db.execute("SELECT id, tickers FROM watchlists")
        overrides: dict[str, str] = {}
        for row in (watchlists or []):
            watchlist_id, tickers_str = row[0], row[1]
            if not watchlist_id.startswith('class:'):
                continue
            category = watchlist_id[len('class:'):]
            for ticker in (tickers_str or '').split():
                if ticker:
                    overrides[ticker] = category
        logger.debug(f"Found {len(overrides)} classification overrides")
        return overrides
