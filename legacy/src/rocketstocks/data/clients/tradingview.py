import logging

import pandas as pd
from tradingview_screener import Query, Column as col

logger = logging.getLogger(__name__)

# Common filters applied to every TradingView screener query
_COMMON_FILTERS = [
    col('exchange').isin(['AMEX', 'CBOE', 'NASDAQ', 'NYSE']),
    col('is_primary') == True,
    col('typespecs').has('common'),
    col('typespecs').has_none_of('preferred'),
    col('type') == 'stock',
    col('active_symbol') == True,
]

_COMMON_PROPS = {
    'symbols': {'query': {'types': ['stock', 'fund', 'dr', 'structured']}},
}


def _base_query(columns: list, extra_filters: list = None, order_field: str = 'change',
                preset: str = 'gainers', limit: int = 100) -> Query:
    """Build a TradingView screener query with common settings pre-applied."""
    filters = _COMMON_FILTERS + (extra_filters or [])
    q = (
        Query()
        .select(*columns)
        .where(*filters)
        .order_by(order_field, ascending=False, nulls_first=False)
        .limit(limit)
        .set_markets('america')
        .set_property('symbols', _COMMON_PROPS['symbols'])
        .set_property('preset', preset)
    )
    return q


class TradingView:
    def __init__(self):
        pass

    @staticmethod
    def get_premarket_gainers():
        _, gainers = _base_query(
            columns=[
                'logoid', 'name', 'premarket_close', 'premarket_change_abs',
                'premarket_change', 'premarket_volume', 'premarket_gap',
                'close', 'change', 'volume', 'market_cap_basic', 'Perf.1Y.MarketCap',
                'description', 'type', 'typespecs', 'update_mode', 'pricescale',
                'minmov', 'fractional', 'minmove2', 'fundamental_currency_code', 'currency',
            ],
            extra_filters=[
                col('premarket_change') > 0,
                col('premarket_change').not_empty(),
            ],
            order_field='premarket_change',
            preset='pre-market-gainers',
        ).get_scanner_data()
        return gainers

    @staticmethod
    def get_intraday_gainers():
        logger.debug("Fetching intraday gainers from TradingView")
        _, gainers = _base_query(
            columns=[
                'name', 'description', 'logoid', 'update_mode', 'type', 'typespecs',
                'market_cap_basic', 'fundamental_currency_code', 'close', 'pricescale',
                'minmov', 'fractional', 'minmove2', 'currency', 'change', 'volume',
                'price_earnings_ttm', 'earnings_per_share_diluted_ttm',
                'earnings_per_share_diluted_yoy_growth_ttm', 'dividends_yield_current',
                'sector.tr', 'sector', 'market', 'recommendation_mark',
                'relative_volume_10d_calc',
            ],
            extra_filters=[
                col('close').between(2, 10000),
                col('change') > 0,
            ],
            order_field='change',
            preset='gainers',
        ).get_scanner_data()
        return gainers

    @staticmethod
    def get_postmarket_gainers():
        logger.debug("Fetching after hours gainers from TradingView")
        _, gainers = _base_query(
            columns=[
                'logoid', 'name', 'postmarket_close', 'postmarket_change_abs',
                'postmarket_change', 'postmarket_volume', 'close', 'change', 'volume',
                'market_cap_basic', 'Perf.1Y.MarketCap', 'description', 'type', 'typespecs',
                'update_mode', 'pricescale', 'minmov', 'fractional', 'minmove2',
                'fundamental_currency_code', 'currency',
            ],
            extra_filters=[
                col('postmarket_change') > 0,
                col('postmarket_change').not_empty(),
            ],
            order_field='postmarket_change',
            preset='after_hours_gainers',
        ).get_scanner_data()
        return gainers

    @staticmethod
    def get_unusual_volume_movers():
        logger.debug("Fetching stocks with unusual volume from TradingView")
        _, unusual_volume = _base_query(
            columns=[
                'name', 'description', 'logoid', 'update_mode', 'type', 'typespecs',
                'relative_volume_10d_calc', 'average_volume_10d_calc', 'close', 'pricescale',
                'minmov', 'fractional', 'minmove2', 'currency', 'change', 'volume',
                'market_cap_basic', 'fundamental_currency_code', 'price_earnings_ttm',
                'earnings_per_share_diluted_ttm', 'earnings_per_share_diluted_yoy_growth_ttm',
                'dividends_yield_current', 'sector.tr', 'sector', 'market',
                'recommendation_mark',
            ],
            order_field='relative_volume_10d_calc',
            preset='unusual_volume',
        ).get_scanner_data()
        return unusual_volume

    @staticmethod
    def get_market_caps(limit: int = 10000) -> pd.DataFrame:
        """Fetch market cap for all active US stocks via TradingView screener.

        Returns a DataFrame with columns ``ticker`` (str) and ``market_cap``
        (float, USD). Used as a fallback when ``ticker_stats`` has sparse
        market_cap coverage.

        Args:
            limit: Maximum number of tickers to return (default 10 000).
        """
        logger.debug(f"Fetching market caps for up to {limit} tickers from TradingView")
        _, df = (
            Query()
            .select('name', 'market_cap_basic')
            .where(*_COMMON_FILTERS)
            .order_by('market_cap_basic', ascending=False, nulls_first=False)
            .limit(limit)
            .set_markets('america')
            .get_scanner_data()
        )
        if df.empty:
            return pd.DataFrame(columns=['ticker', 'market_cap'])
        df = df[['name', 'market_cap_basic']].rename(
            columns={'name': 'ticker', 'market_cap_basic': 'market_cap'}
        )
        df = df[df['market_cap'].notna()].copy()
        df['market_cap'] = df['market_cap'].astype(float)
        return df.reset_index(drop=True)

    @staticmethod
    def get_unusual_volume_at_time_movers():
        logger.debug("Fetching stocks with unusual volume (intraday) from TradingView")
        _, unusual_volume = _base_query(
            columns=[
                'name', 'description', 'logoid', 'update_mode', 'type', 'typespecs',
                'relative_volume_intraday|5', 'close', 'pricescale', 'minmov', 'fractional',
                'minmove2', 'currency', 'change', 'volume', 'market_cap_basic',
                'fundamental_currency_code', 'price_earnings_ttm',
                'earnings_per_share_diluted_ttm', 'earnings_per_share_diluted_yoy_growth_ttm',
                'dividends_yield_current', 'sector.tr', 'sector', 'market',
                'recommendation_mark',
            ],
            order_field='relative_volume_intraday|5',
            preset='unusual_volume',
        ).get_scanner_data()
        return unusual_volume
