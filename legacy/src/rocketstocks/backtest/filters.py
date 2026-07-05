"""Composable ticker filtering for backtest runs."""
import asyncio
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
            (e.g. ``['volatile', 'meme']``). Tickers without a ticker_stats
            row are treated as ``'standard'``.
        sectors: Sector names to include (e.g. ``['Technology']``).
        industries: Industry names to include (e.g. ``['Biotechnology']``).
        exchanges: Exchange names to include (e.g. ``['NYSE', 'NASDAQ']``).
        watchlists: Named watchlist IDs whose tickers should be included
            (e.g. ``['mag7', 'semiconductors']``). Multiple watchlists are
            OR-combined before intersecting with the working set.
        min_market_cap: Minimum market cap in dollars (inclusive). Falls back
            to TradingView data when ticker_stats coverage is sparse.
        max_market_cap: Maximum market cap in dollars (inclusive). Falls back
            to TradingView data when ticker_stats coverage is sparse.
        min_volatility: Minimum 20-day volatility in percent (inclusive).
            Only tickers with a ticker_stats row and non-NULL volatility_20d
            are considered.
        max_volatility: Maximum 20-day volatility in percent (inclusive).
            Only tickers with a ticker_stats row and non-NULL volatility_20d
            are considered.
        min_popularity_rank: Most-popular end of rank range (1 = #1).
        max_popularity_rank: Least-popular end of rank range.
        exclude_delisted: Exclude tickers with a delist_date set (default True).
    """

    tickers: list[str] | None = None
    classifications: list[str] | None = None
    sectors: list[str] | None = None
    industries: list[str] | None = None
    exchanges: list[str] | None = None
    watchlists: list[str] | None = None
    min_market_cap: float | None = None
    max_market_cap: float | None = None
    min_volatility: float | None = None
    max_volatility: float | None = None
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
                popularity, watchlists, and trading_view repositories/clients.

        Returns:
            Sorted list of ticker symbols matching all active filters.
        """
        if self.tickers is not None:
            return sorted(self.tickers)

        # Start from the full set
        all_tickers: set[str] = set(await stock_data.tickers.get_all_tickers())
        counts: dict[str, int] = {'start': len(all_tickers)}

        # --- delist / sector / industry / exchange filter (tickers table) ---
        needs_ticker_info = (
            self.exclude_delisted
            or self.sectors
            or self.industries
            or self.exchanges
        )
        if needs_ticker_info:
            ticker_info = await stock_data.tickers.get_all_ticker_info()

            if self.exclude_delisted:
                delisted = set(
                    ticker_info[ticker_info['delist_date'].notna()]['ticker'].tolist()
                )
                all_tickers -= delisted
                counts['after_delist'] = len(all_tickers)

            if self.sectors:
                sector_set = set(
                    ticker_info[ticker_info['sector'].isin(self.sectors)]['ticker'].tolist()
                )
                all_tickers &= sector_set
                counts['after_sector'] = len(all_tickers)

            if self.industries:
                industry_set = set(
                    ticker_info[ticker_info['industry'].isin(self.industries)]['ticker'].tolist()
                )
                all_tickers &= industry_set
                counts['after_industry'] = len(all_tickers)

            if self.exchanges:
                exchange_col = ticker_info['exchange'] if 'exchange' in ticker_info.columns else None
                if exchange_col is not None:
                    exchange_set = set(
                        ticker_info[ticker_info['exchange'].isin(self.exchanges)]['ticker'].tolist()
                    )
                else:
                    logger.warning('TickerFilter: exchange column not found in ticker_info; exchange filter skipped')
                    exchange_set = all_tickers.copy()
                all_tickers &= exchange_set
                counts['after_exchange'] = len(all_tickers)

        # --- watchlist filter ---
        if self.watchlists:
            wl_tickers: set[str] = set()
            for wl_name in self.watchlists:
                wl_list = await stock_data.watchlists.get_watchlist_tickers(wl_name)
                wl_tickers.update(wl_list)
            all_tickers &= wl_tickers
            counts['after_watchlist'] = len(all_tickers)

        # --- ticker_stats block (classification / market cap / volatility) ---
        needs_stats = (
            self.classifications
            or self.min_market_cap is not None
            or self.max_market_cap is not None
            or self.min_volatility is not None
            or self.max_volatility is not None
        )
        if needs_stats:
            all_stats = await stock_data.ticker_stats.get_all_stats()
            # Build per-ticker lookups
            classification_map: dict[str, str] = {}
            market_cap_map: dict[str, float | None] = {}
            volatility_map: dict[str, float | None] = {}
            for stat in all_stats:
                t = stat['ticker']
                classification_map[t] = stat.get('classification') or 'standard'
                market_cap_map[t] = stat.get('market_cap')
                volatility_map[t] = stat.get('volatility_20d')

            # -- classification sub-block --
            # Tickers absent from ticker_stats default to 'standard'.
            if self.classifications:
                all_tickers = {
                    t for t in all_tickers
                    if classification_map.get(t, 'standard') in self.classifications
                }
                counts['after_classification'] = len(all_tickers)

            # -- market cap sub-block --
            if self.min_market_cap is not None or self.max_market_cap is not None:
                # Check coverage: how many tickers have a non-NULL market_cap
                covered = sum(
                    1 for t in all_tickers
                    if market_cap_map.get(t) is not None
                )
                coverage_pct = covered / len(all_tickers) if all_tickers else 1.0

                if coverage_pct < 0.5:
                    logger.info(
                        f'TickerFilter: market_cap coverage {covered}/{len(all_tickers)} '
                        f'({coverage_pct:.0%}) — fetching TradingView fallback'
                    )
                    try:
                        tv_df = await asyncio.to_thread(
                            stock_data.trading_view.get_market_caps
                        )
                        tv_map: dict[str, float] = dict(
                            zip(tv_df['ticker'], tv_df['market_cap'])
                        )
                    except Exception as exc:
                        logger.warning(f'TickerFilter: TradingView market cap fetch failed: {exc}')
                        tv_map = {}

                    # Merge: ticker_stats takes precedence
                    merged_cap: dict[str, float | None] = {**tv_map, **{
                        t: v for t, v in market_cap_map.items() if v is not None
                    }}
                    n_stats = sum(1 for t in all_tickers if market_cap_map.get(t) is not None)
                    n_tv = sum(1 for t in all_tickers if t in tv_map and market_cap_map.get(t) is None)
                    n_missing = len(all_tickers) - n_stats - n_tv
                    logger.info(
                        f'TickerFilter market cap sources: {n_stats} from ticker_stats, '
                        f'{n_tv} from TradingView, {n_missing} without data'
                    )
                else:
                    merged_cap = {t: v for t, v in market_cap_map.items() if v is not None}

                cap_filtered: set[str] = set()
                for t in all_tickers:
                    mc = merged_cap.get(t)
                    if mc is None:
                        continue
                    if self.min_market_cap is not None and mc < self.min_market_cap:
                        continue
                    if self.max_market_cap is not None and mc > self.max_market_cap:
                        continue
                    cap_filtered.add(t)
                all_tickers = cap_filtered
                counts['after_market_cap'] = len(all_tickers)

            # -- volatility sub-block --
            if self.min_volatility is not None or self.max_volatility is not None:
                vol_filtered: set[str] = set()
                for t in all_tickers:
                    vol = volatility_map.get(t)
                    if vol is None:
                        continue
                    if self.min_volatility is not None and vol < self.min_volatility:
                        continue
                    if self.max_volatility is not None and vol > self.max_volatility:
                        continue
                    vol_filtered.add(t)
                all_tickers = vol_filtered
                counts['after_volatility'] = len(all_tickers)

        # --- popularity rank filter (popularity table) ---
        if self.min_popularity_rank is not None or self.max_popularity_rank is not None:
            pop_df = await stock_data.popularity.fetch_popularity(limit=5000)
            if pop_df.empty:
                logger.warning(
                    'TickerFilter: popularity data is empty; '
                    'popularity rank filter returned 0 tickers'
                )
                all_tickers = set()
            else:
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
            counts['after_popularity'] = len(all_tickers)

        result = sorted(all_tickers)
        if not result:
            stage_str = ' → '.join(f'{k}={v}' for k, v in counts.items())
            logger.warning(f'TickerFilter resolved 0 tickers. Stages: {stage_str}')
        else:
            logger.info(f'TickerFilter resolved {len(result)} tickers')
        return result
