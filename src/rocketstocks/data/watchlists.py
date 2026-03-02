import logging

# Logging configuration
logger = logging.getLogger(__name__)


class Watchlists(object):

    def __init__(self, db):
        self.db = db # Postgres
        self.db_table = 'watchlists'
        self.db_fields = ['id', 'tickers', 'systemgenerated']

    # Return tickers from watchlist - global by default, personal if chosen by user
    def get_watchlist_tickers(self, watchlist_id):
        logger.debug(f"Fetching tickers from watchlist with ID '{watchlist_id}'")

        tickers = self.db.select(table='watchlists',
                                        fields=['tickers'],
                                        where_conditions=[('id', watchlist_id)],
                                        fetchall=False)
        if tickers is None:
            return tickers
        else:
            return sorted(tickers[0].split())


    # Return tickers from all available watchlists
    def get_all_watchlist_tickers(self, no_personal=True, no_systemGenerated=True):
        logger.debug("Fetching tickers from all available watchlists (besides personal)")

        watchlists = self.db.select(table='watchlists',
                                       fields=['id', 'tickers', 'systemgenerated'],
                                       fetchall=True)
        tickers = []
        for watchlist in watchlists:
            watchlist_id, watchlist_tickers, is_systemGenerated = watchlist[0], watchlist[1].split(), watchlist[2]
            if watchlist_id.isdigit() and no_personal:
                pass
            elif is_systemGenerated and no_systemGenerated:
                pass
            else:
                tickers += watchlist_tickers

        return sorted(set(tickers))

    # Return list of existing watchlists
    def get_watchlists(self, no_personal=True, no_systemGenerated=True):
        logger.debug("Fetching all watchlists")
        filtered_watchlists = []
        watchlists = self.db.select(table='watchlists',
                                       fields = ['id', 'tickers', 'systemgenerated'],
                                       fetchall=True)
        for i in range(len(watchlists)):
            watchlist = watchlists[i]
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

    # Set content of watchlist to provided tickers
    def update_watchlist(self, watchlist_id, tickers):
        logger.debug(f"Updating watchlist '{watchlist_id}': {tickers}")

        self.db.update(table='watchlists',
                          set_fields=[('tickers', ' '.join(tickers))],
                          where_conditions=[('id', watchlist_id)])

    # Create a new watchlist with id 'watchlist_id'
    def create_watchlist(self, watchlist_id, tickers, systemGenerated):
        logger.debug(f"Creating watchlist with ID '{watchlist_id}' and tickers {tickers}")
        self.db.insert(table=self.db_table, fields=self.db_fields, values=[(watchlist_id, " ".join(tickers), systemGenerated)])

    # Delete watchlist with id 'watchlist_id'
    def delete_watchlist(self, watchlist_id):
        logger.debug(f"Deleting watchlist '{watchlist_id}'...")
        self.db.delete(table=self.db_table, where_conditions=[('id', watchlist_id)])

    # Validate watchlist exists in the database
    def validate_watchlist(self, watchlist_id):
        logger.debug(f"Validating watchlist '{watchlist_id}' exists")

        result = self.db.select(table='watchlists',
                                       fields=['id'],
                                       where_conditions=[('id', watchlist_id)],
                                       fetchall=False)
        if result is None:
            logger.warning(f"Watchlist '{watchlist_id}' does not exist")
            return False
        else:
            return True

    def get_classification_overrides(self) -> dict[str, str]:
        """Return {ticker: classification} from watchlists named ``class:<category>``.

        Watchlists with IDs of the form ``class:volatile``, ``class:meme``,
        ``class:blue_chip``, or ``class:standard`` are treated as explicit
        classification overrides for their member tickers.
        """
        logger.debug("Fetching classification overrides from watchlists")
        watchlists = self.db.select(
            table='watchlists',
            fields=['id', 'tickers'],
            fetchall=True,
        )
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
