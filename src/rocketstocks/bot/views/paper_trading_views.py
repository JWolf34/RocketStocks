"""Discord UI views for paper trading commands."""
import discord


class TradeConfirmView(discord.ui.View):
    """60-second confirm/cancel view for trade orders."""

    def __init__(self, side: str):
        super().__init__(timeout=60)
        self.confirmed: bool | None = None
        # Colour the confirm button by trade side
        confirm_style = discord.ButtonStyle.success if side == "BUY" else discord.ButtonStyle.danger
        confirm_label = "Confirm Buy" if side == "BUY" else "Confirm Sell"

        confirm_button = discord.ui.Button(
            label=confirm_label,
            style=confirm_style,
        )
        confirm_button.callback = self._on_confirm
        self.add_item(confirm_button)

        cancel_button = discord.ui.Button(
            label="Cancel",
            style=discord.ButtonStyle.secondary,
        )
        cancel_button.callback = self._on_cancel
        self.add_item(cancel_button)

    async def _on_confirm(self, interaction: discord.Interaction):
        self.confirmed = True
        self.stop()
        await interaction.response.defer()

    async def _on_cancel(self, interaction: discord.Interaction):
        self.confirmed = False
        self.stop()
        await interaction.response.defer()

    async def on_timeout(self):
        self.confirmed = None
        self.stop()


class ConfirmResetView(discord.ui.View):
    """Confirmation view for resetting a paper trading portfolio."""

    def __init__(self):
        super().__init__(timeout=30)
        self.confirmed: bool | None = None

    @discord.ui.button(label="Yes, reset my portfolio", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.confirmed = True
        self.stop()
        await interaction.response.defer()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.confirmed = False
        self.stop()
        await interaction.response.defer()

    async def on_timeout(self):
        self.confirmed = None
        self.stop()
