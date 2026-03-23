import discord
import logging

from rocketstocks.core.utils.formatting import finviz_url

logger = logging.getLogger(__name__)

_GITHUB_DOCS_BASE = "https://github.com/JWolf34/RocketStocks/blob/main/docs/alerts"
POPULARITY_SURGE_DOC_URL = f"{_GITHUB_DOCS_BASE}/popularity_surge.md"
MOMENTUM_CONFIRMATION_DOC_URL = f"{_GITHUB_DOCS_BASE}/momentum_confirmation.md"
MARKET_ALERT_DOC_URL = f"{_GITHUB_DOCS_BASE}/market_alert.md"
MARKET_MOVER_DOC_URL = f"{_GITHUB_DOCS_BASE}/market_mover.md"
WATCHLIST_MOVER_DOC_URL = f"{_GITHUB_DOCS_BASE}/watchlist_mover.md"
EARNINGS_MOVER_DOC_URL = f"{_GITHUB_DOCS_BASE}/earnings_mover.md"


class WatchlistSelect(discord.ui.View):
    """Ephemeral dropdown for adding a ticker to a watchlist."""

    def __init__(self, ticker: str, watchlists: list[str]):
        super().__init__(timeout=60)
        self.ticker = ticker
        options = [
            discord.SelectOption(label="Personal" if wl == "personal" else wl, value=wl)
            for wl in watchlists
        ]
        select = discord.ui.Select(placeholder="Choose a watchlist...", options=options)
        select.callback = self._select_callback
        self.add_item(select)

    async def _select_callback(self, interaction: discord.Interaction):
        selected = interaction.data["values"][0]
        watchlist_repo = interaction.client.stock_data.watchlists
        watchlist_id = watchlist_repo.resolve_personal_id(interaction.user.id) if selected == "personal" else selected
        try:
            if not await watchlist_repo.validate_watchlist(watchlist_id):
                await watchlist_repo.create_watchlist(watchlist_id=watchlist_id, tickers=[], watchlist_type='personal', owner_id=interaction.user.id)
            current_tickers = await watchlist_repo.get_watchlist_tickers(watchlist_id) or []
            if self.ticker in current_tickers:
                await interaction.response.send_message(
                    f"**{self.ticker}** is already on the *{selected}* watchlist.", ephemeral=True
                )
                return
            merged = sorted(set(current_tickers + [self.ticker]))
            await watchlist_repo.update_watchlist(watchlist_id=watchlist_id, tickers=merged)
            await interaction.response.send_message(
                f"Added **{self.ticker}** to the *{selected}* watchlist! ({len(merged)} tickers total)",
                ephemeral=True,
            )
        except Exception:
            logger.exception("Error adding ticker to watchlist")
            await interaction.response.send_message(
                "An error occurred while updating the watchlist.", ephemeral=True
            )


class AlertButtons(discord.ui.View):
    """Standard URL buttons shown on most alert messages."""

    def __init__(self, ticker: str, doc_url: str | None = None):
        super().__init__(timeout=None)
        self.ticker = ticker
        self.add_item(discord.ui.Button(
            label="StockInvest",
            style=discord.ButtonStyle.url,
            url=f"https://stockinvest.us/stock/{ticker}",
        ))
        self.add_item(discord.ui.Button(
            label="FinViz",
            style=discord.ButtonStyle.url,
            url=finviz_url(ticker),
        ))
        self.add_item(discord.ui.Button(
            label="Yahoo! Finance",
            style=discord.ButtonStyle.url,
            url=f"https://finance.yahoo.com/quote/{ticker}",
        ))
        if doc_url:
            self.add_item(discord.ui.Button(
                label="What does this mean?",
                style=discord.ButtonStyle.url,
                url=doc_url,
            ))

    @discord.ui.button(label="Add to Watchlist", style=discord.ButtonStyle.success)
    async def add_to_watchlist(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            watchlists = await interaction.client.stock_data.watchlists.get_watchlists(watchlist_types=['named', 'personal'])
            view = WatchlistSelect(ticker=self.ticker, watchlists=watchlists)
            await interaction.response.send_message(
                f"Choose a watchlist to add **{self.ticker}** to:", view=view, ephemeral=True
            )
        except Exception:
            logger.exception("Error fetching watchlists")
            await interaction.response.send_message(
                "An error occurred while fetching watchlists.", ephemeral=True
            )

    @discord.ui.button(label="Generate Report", style=discord.ButtonStyle.primary)
    async def generate_report(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        try:
            reports_cog = interaction.client.get_cog("Reports")
            if reports_cog is None:
                await interaction.followup.send("Reports cog is not available.", ephemeral=True)
                return
            from rocketstocks.bot.senders.embed_utils import spec_to_embed
            content = await reports_cog.build_stock_report(self.ticker)
            embed = spec_to_embed(content.build())
            await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception:
            logger.exception("Error generating stock report")
            await interaction.followup.send(
                "An error occurred while generating the report.", ephemeral=True
            )


class PopularitySurgeAlertButtons(AlertButtons):
    """AlertButtons with an added ApeWisdom link for popularity surge alerts."""

    def __init__(self, ticker: str, doc_url: str | None = None):
        super().__init__(ticker, doc_url)
        self.add_item(discord.ui.Button(
            label="ApeWisdom",
            style=discord.ButtonStyle.url,
            url=f"https://apewisdom.io/stocks/{ticker}",
        ))


class PoliticianTradeButtons(discord.ui.View):
    """URL button linking to a politician's Capitol Trades profile."""

    def __init__(self, politician: dict):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(
            label="Capitol Trades",
            style=discord.ButtonStyle.url,
            url=f"https://www.capitoltrades.com/politicians/{politician['politician_id']}",
        ))
