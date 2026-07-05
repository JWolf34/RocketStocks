"""PriceSnapshot — OHLCV + technical indicators embed for /data price."""
import logging

import pandas as pd

from rocketstocks.core.content.models import COLOR_BLUE, EmbedSpec, PriceSnapshotData
from rocketstocks.core.content.sections_card import (
    ohlcv_card,
    performance_card,
    technical_signals_card,
)

logger = logging.getLogger(__name__)


class PriceSnapshot:
    """Builds a price snapshot embed combining OHLCV, performance, and technical signals."""

    def __init__(self, data: PriceSnapshotData):
        self.data = data

    def build(self) -> EmbedSpec:
        ticker = self.data.ticker
        freq = self.data.frequency
        hist = self.data.daily_price_history
        quote = self.data.quote

        sections = []

        if quote is not None:
            try:
                sections.append(ohlcv_card(quote, hist if freq == 'daily' else None))
            except Exception:
                logger.debug(f"[PriceSnapshot] ohlcv_card failed for {ticker}", exc_info=True)

            if freq == 'daily' and hist is not None and not hist.empty:
                try:
                    sections.append(performance_card(hist, quote))
                except Exception:
                    logger.debug(f"[PriceSnapshot] performance_card failed for {ticker}", exc_info=True)
        elif hist is not None and not hist.empty:
            # No quote — show last known price from history
            row = hist.iloc[-1]
            sections.append(
                f"__**Last Session**__\n"
                f"Open **${row['open']:.2f}** · High **${row['high']:.2f}** · "
                f"Low **${row['low']:.2f}** · Close **${row['close']:.2f}**\n"
                f"Vol **{int(row.get('volume', 0)):,}**\n\n"
            )

        # Technical signals work on close series — valid for both daily and 5m
        if hist is not None and not hist.empty:
            # For 5m data, rename datetime col if present so technical_signals_card gets a clean close series
            hist_for_signals = hist.copy() if freq == '5m' else hist
            try:
                sections.append(technical_signals_card(hist_for_signals))
            except Exception:
                logger.debug(f"[PriceSnapshot] technical_signals_card failed for {ticker}", exc_info=True)

        if not sections:
            description = "No price data available."
        else:
            description = "".join(sections)
            if len(description) > 4096:
                description = description[:4093] + '...'

        freq_label = "5-Minute" if freq == '5m' else "Daily"
        return EmbedSpec(
            title=f"{freq_label} Price Snapshot: {ticker}",
            description=description,
            color=COLOR_BLUE,
        )
