"""RocketStocks v2 read layer: typed, capped, paginated async queries.

Reads only; writes go through rs_persistence repositories. Every function takes
an AsyncSession (the caller owns it) and returns frozen models, never ORM rows.
"""

from rs_query._page import InvalidCursor, Page
from rs_query.alerts import AlertRecord, recent_alerts
from rs_query.daily_bars import daily_bars
from rs_query.popularity import popularity_between, popularity_history
from rs_query.token_age import schwab_token_issued_at
from rs_query.track_records import TRACK_RECORD_LOGGED_MIN_N, current_track_record

__all__ = [
    "TRACK_RECORD_LOGGED_MIN_N",
    "AlertRecord",
    "InvalidCursor",
    "Page",
    "current_track_record",
    "daily_bars",
    "popularity_between",
    "popularity_history",
    "recent_alerts",
    "schwab_token_issued_at",
]
