"""build_data_api — standalone factory for DataAPI outside the Discord bot."""
import logging

from rocketstocks.data.stockdata import StockData
from rocketstocks.api.client import DataAPI

logger = logging.getLogger(__name__)


async def build_data_api() -> DataAPI:
    """Open a DB pool, boot StockData, and return a ready DataAPI.

    Schwab initialisation is attempted but never fatal — a missing or expired
    token causes SchwabTokenError only when a Schwab-dependent method is called,
    not at construction time.
    """
    sd = StockData()
    await sd.db.open()
    logger.debug("DB pool opened")

    try:
        await sd.init_schwab()
        logger.debug("Schwab client initialised")
    except Exception as exc:
        logger.warning(f"Schwab init failed (non-fatal): {exc}")

    return DataAPI(sd)
