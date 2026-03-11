import logging
from rocketstocks.data.watchlists import Watchlists

logger = logging.getLogger(__name__)

_SEED_KEY = "__boilerplate_seeded__"

_BOILERPLATE_WATCHLISTS = {
    "mag7":            ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA"],
    "semiconductors":  ["NVDA", "AMD", "INTC", "QCOM", "AVGO", "MU", "TSM", "AMAT", "LRCX", "KLAC", "ARM"],
    "big-banks":       ["JPM", "BAC", "WFC", "GS", "MS", "C", "BLK", "AXP"],
    "energy":          ["XOM", "CVX", "COP", "OXY", "SLB", "EOG", "MPC", "PSX"],
    "pharma-biotech":  ["JNJ", "PFE", "MRNA", "LLY", "ABBV", "GILD", "AMGN", "BMY", "REGN"],
    "ev-auto":         ["TSLA", "RIVN", "F", "GM", "LCID", "NIO"],
    "high-growth":     ["SNOW", "DDOG", "CRWD", "NET", "PLTR", "UBER", "ABNB", "SHOP", "COIN", "MSTR"],
    "retail-consumer": ["AMZN", "WMT", "COST", "TGT", "HD", "NKE", "SBUX", "MCD"],
}


async def seed_boilerplate_watchlists(watchlists: Watchlists) -> None:
    if await watchlists.validate_watchlist(_SEED_KEY):
        logger.debug("Boilerplate watchlists already seeded, skipping")
        return
    logger.info("Seeding boilerplate watchlists...")
    for name, tickers in _BOILERPLATE_WATCHLISTS.items():
        await watchlists.create_watchlist(name, tickers, systemGenerated=False)
        logger.info(f"  Created watchlist '{name}' with {len(tickers)} tickers")
    await watchlists.create_watchlist(_SEED_KEY, [], systemGenerated=True)
    logger.info("Boilerplate watchlist seeding complete")
