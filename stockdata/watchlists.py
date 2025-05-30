import logging

# Logging configuration
logger = logging.getLogger(__name__)


class Watchlists(object):

    def __init__(self, db):
        self.db = db # Postgres
        self.db_table = 'watchlists'
        self.db_fields = ['id', 'tickers', 'systemgenerated']
        
    # Return tickers from watchlist - global by default, personal if chosen by user
    def get_tickers_from_watchlist(self, watchlist_id):
        logger.debug("Fetching tickers from watchlist with ID '{}'".format(watchlist_id))
        
        tickers = self.db.select(table='watchlists',
                                        fields=['tickers'],
                                        where_conditions=[('id', watchlist_id)],
                                        fetchall=False)
        if tickers is None:
            return tickers
        else:
            return sorted(tickers[0].split())
       

    # Return tickers from all available watchlists
    def get_tickers_from_all_watchlists(self, no_personal=True, no_systemGenerated=True):
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
        logger.debug("Updating watchlist '{}': {}".format(watchlist_id, tickers))

        self.db.update(table='watchlists', 
                          set_fields=[('tickers', ' '.join(tickers))], 
                          where_conditions=[('id', watchlist_id)])

    # Create a new watchlist with id 'watchlist_id'
    def create_watchlist(self, watchlist_id, tickers, systemGenerated):
        logger.debug("Creating watchlist with ID '{}' and tickers {}".format(watchlist_id, tickers))
        self.db.insert(table=self.db_table, fields=self.db_fields, values=[(watchlist_id, " ".join(tickers), systemGenerated)])

    # Delete watchlist with id 'watchlist_id'
    def delete_watchlist(self, watchlist_id):
        logger.debug("Deleting watchlist '{}'...".format(watchlist_id))
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

