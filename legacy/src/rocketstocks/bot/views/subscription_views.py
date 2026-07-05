"""Views for alert subscription management."""
import logging
import discord

logger = logging.getLogger(__name__)

# Ordered list of (role_key, display_label) for all subscribable alert roles
ALERT_ROLE_DEFS: list[tuple[str, str]] = [
    ("earnings_mover", "🚨 Earnings Mover"),
    ("watchlist_mover", "📋 Watchlist Mover"),
    ("popularity_surge", "🔥 Popularity Surge"),
    ("momentum_confirmed", "⚡ Momentum Confirmed"),
    ("volume_accumulation", "📊 Volume Accumulation"),
    ("breakout", "🚀 Breakout"),
    ("all_alerts", "🔔 All Alerts"),
]


class AlertSubscriptionSelect(discord.ui.Select):
    """Multi-select dropdown for choosing alert subscriptions."""

    def __init__(self, guild_roles: dict[str, int], member_role_ids: set[int]):
        options = [
            discord.SelectOption(
                label=label,
                value=role_key,
                default=(role_key in guild_roles and guild_roles[role_key] in member_role_ids),
            )
            for role_key, label in ALERT_ROLE_DEFS
            if role_key in guild_roles
        ]
        super().__init__(
            placeholder="Choose alerts to subscribe to...",
            min_values=0,
            max_values=max(len(options), 1),
            options=options or [discord.SelectOption(label="No roles configured", value="_none")],
        )
        self._guild_roles = guild_roles
        self._member_role_ids = member_role_ids

    async def callback(self, interaction: discord.Interaction):
        selected_keys = set(self.values) - {"_none"}
        guild = interaction.guild
        member = interaction.user

        previously_selected = {
            role_key
            for role_key, role_id in self._guild_roles.items()
            if role_id in self._member_role_ids
        }

        to_add = selected_keys - previously_selected
        to_remove = previously_selected - selected_keys

        added, removed = [], []
        for role_key in to_add:
            role_id = self._guild_roles.get(role_key)
            if role_id:
                role = guild.get_role(role_id)
                if role:
                    try:
                        await member.add_roles(role)
                        added.append(role_key)
                    except discord.Forbidden:
                        logger.warning(f"Missing permissions to add role {role_key} to {member.id}")

        for role_key in to_remove:
            role_id = self._guild_roles.get(role_key)
            if role_id:
                role = guild.get_role(role_id)
                if role:
                    try:
                        await member.remove_roles(role)
                        removed.append(role_key)
                    except discord.Forbidden:
                        logger.warning(f"Missing permissions to remove role {role_key} from {member.id}")

        label_map = dict(ALERT_ROLE_DEFS)
        lines = []
        for key in added:
            lines.append(f"✅ Added: {label_map.get(key, key)}")
        for key in removed:
            lines.append(f"➖ Removed: {label_map.get(key, key)}")

        if not lines:
            summary = "No changes made."
        else:
            summary = "\n".join(lines)

        await interaction.response.edit_message(
            content=f"**Subscriptions updated!**\n{summary}",
            view=None,
        )


class AlertSubscriptionView(discord.ui.View):
    """Ephemeral view wrapping an AlertSubscriptionSelect."""

    def __init__(self, select: AlertSubscriptionSelect):
        super().__init__(timeout=120)
        self.add_item(select)


class ManageSubscriptionsButton(discord.ui.Button):
    """Persistent button that opens the subscription select dropdown."""

    def __init__(self):
        super().__init__(
            label="Manage My Subscriptions",
            style=discord.ButtonStyle.primary,
            custom_id="manage_subscriptions",
        )

    async def callback(self, interaction: discord.Interaction):
        subscriptions_cog = interaction.client.get_cog("Reports")
        if subscriptions_cog is None:
            await interaction.response.send_message(
                "Subscriptions are not available right now.", ephemeral=True
            )
            return
        await subscriptions_cog._send_subscription_select(interaction)


class SubscriptionEntryView(discord.ui.View):
    """Persistent view with the Manage My Subscriptions button.

    Registered at startup via bot.add_view() so it survives restarts.
    """

    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(ManageSubscriptionsButton())
