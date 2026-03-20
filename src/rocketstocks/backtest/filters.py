"""Composable ticker filtering for backtest runs."""
import logging
from dataclasses import asdict, dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class TickerFilter:
    """Composable filter for selecting tickers to backtest.

    All fields are optional. When multiple fields are set they are
    AND-combined (intersection). An empty filter returns all non-delisted
    tickers. Set ``tickers`` to an explicit list to bypass all other filters.

    Attributes:
        tickers: Explicit ticker list. When set, all other fields are ignored.
        classifications: StockClass values to include
            (e.g. ``['volatile', 'meme']``).
        sectors: Sector names to include (e.g. ``['Technology']``).
        industries: Industry names to include (e.g. ``['Biotechnology']``).
        min_market_cap: Minimum market cap in dollars (inclusive).
        max_market_cap: Maximum market cap in dollars (inclusive).
        min_popularity_rank: Most-popular end of rank range (1 = #1).
        max_popularity_rank: Least-popular end of rank range.
        exclude_delisted: Exclude tickers with a delist_date set (default True).
    """

    tickers: list[str] | None = None
    classifications: list[str] | None = None
    sectors: list[str] | None = None
    industries: list[str] | None = None
    min_market_cap: float | None = None
    max_market_cap: float | None = None
    min_popularity_rank: int | None = None
    max_popularity_rank: int | None = None
    exclude_delisted: bool = True

    def to_dict(self) -> dict:
        """Serialize to a plain dict for JSONB storage in backtest_runs."""
        return {k: v for k, v in asdict(self).items() if v is not None}

    async def apply(self, stock_data) -> list[str]:
        """Apply all active filters and return the intersection of matching tickers.

        Args:
            stock_data: StockData singleton with tickers, ticker_stats,
                and popularity repositories.

        Returns:
            Sorted list of ticker symbols matching all active filters.
        """
        if self.tickers is not None:
            return sorted(self.tickers)

        # Start from the full set
        all_tickers: set[str] = set(await stock_data.tickers.get_all_tickers())

        # --- delist filter ---
        if self.exclude_delisted:
            ticker_info = await stock_data.tickers.get_all_ticker_info()
            delisted = set(
                ticker_info[ticker_info['delist_date'].notna()]['ticker'].tolist()
            )
            all_tickers -= delisted

        # --- sector / industry filter (tickers table) ---
        if self.sectors or self.industries:
            ticker_info = await stock_data.tickers.get_all_ticker_info()
            if self.sectors:
                sector_set = set(
                    ticker_info[ticker_info['sector'].isin(self.sectors)]['ticker'].tolist()
                )
                all_tickers &= sector_set
            if self.industries:
                industry_set = set(
                    ticker_info[ticker_info['industry'].isin(self.industries)]['ticker'].tolist()
                )
                all_tickers &= industry_set

        # --- classification / market cap filter (ticker_stats table) ---
        if self.classifications or self.min_market_cap is not None or self.max_market_cap is not None:
            all_stats = await stock_data.ticker_stats.get_all_stats()
            stats_tickers: set[str] = set()
            for stat in all_stats:
                ticker = stat['ticker']
                if self.classifications and stat.get('classification') not in self.classifications:
                    continue
                mc = stat.get('market_cap')
                if self.min_market_cap is not None and (mc is None or mc < self.min_market_cap):
                    continue
                if self.max_market_cap is not None and (mc is None or mc > self.max_market_cap):
                    continue
                stats_tickers.add(ticker)
            all_tickers &= stats_tickers

        # --- popularity rank filter (popularity table) ---
        if self.min_popularity_rank is not None or self.max_popularity_rank is not None:
            pop_df = await stock_data.popularity.fetch_popularity(limit=5000)
            if not pop_df.empty:
                latest = (
                    pop_df.sort_values('datetime')
                    .drop_duplicates('ticker', keep='last')
                )
                if self.max_popularity_rank is not None:
                    latest = latest[latest['rank'] <= self.max_popularity_rank]
                if self.min_popularity_rank is not None:
                    latest = latest[latest['rank'] >= self.min_popularity_rank]
                pop_tickers = set(latest['ticker'].tolist())
                all_tickers &= pop_tickers
            else:
                logger.warning('TickerFilter: popularity data empty; popularity rank filter had no effect')

        result = sorted(all_tickers)
        logger.info(f'TickerFilter resolved {len(result)} tickers')
        return result
