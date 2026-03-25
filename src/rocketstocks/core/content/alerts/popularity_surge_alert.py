import logging
import math

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.models import (
    COLOR_PURPLE,
    EmbedField, EmbedSpec,
)
from rocketstocks.core.utils.market import MarketUtils
from rocketstocks.core.utils.formatting import (
    change_emoji, finviz_url, format_signed_pct, get_company_name, is_valid_number,
)

logger = logging.getLogger(__name__)


class PopularitySurgeAlert(Alert):
    alert_type = "POPULARITY_SURGE"
    role_key = "popularity_surge"

    def __init__(self, data):  # data: PopularitySurgeData
        super().__init__()
        self.data = data
        self.ticker = data.ticker

        surge_result = data.surge_result
        self.alert_data['current_rank'] = surge_result.current_rank
        self.alert_data['rank_change'] = surge_result.rank_change
        self.alert_data['mentions'] = surge_result.mentions
        self.alert_data['mention_ratio'] = surge_result.mention_ratio
        self.alert_data['surge_types'] = [st.value for st in surge_result.surge_types]
        self.alert_data['rank_velocity_zscore'] = surge_result.rank_velocity_zscore
        self.alert_data['mention_acceleration'] = surge_result.mention_acceleration

        # pct_change for base class momentum tracking
        pct_change = data.quote['quote'].get('netPercentChange', 0.0)
        self.alert_data['pct_change'] = pct_change

    def build(self) -> EmbedSpec:
        logger.debug("Building Popularity Surge embed...")

        surge_result = self.data.surge_result
        company_name = get_company_name(self.data.ticker_info, self.data.ticker)
        price = MarketUtils().get_current_price(self.data.quote)
        pct_change = self.alert_data['pct_change']

        # Mention acceleration phase label
        accel = surge_result.mention_acceleration
        if accel is not None and not math.isnan(accel):
            if accel > 0:
                phase_label = "📈 Social traction **accelerating**"
            else:
                phase_label = "📉 Social traction decelerating"
        else:
            phase_label = None

        # Build surge reason bullets
        reasons = []
        for st in surge_result.surge_types:
            if st.value == 'mention_surge':
                ratio = surge_result.mention_ratio or 0
                reasons.append(f"• Mentions surged **{ratio:.1f}x** vs 24h ago")
            elif st.value == 'rank_jump':
                reasons.append(f"• Rank jumped **{surge_result.rank_change}** spots in 24h")
            elif st.value == 'new_entrant':
                reasons.append(f"• Newly entered top {new_entrant_cutoff_label(surge_result.current_rank)}")
            elif st.value == 'velocity_spike':
                display_zscore = -(surge_result.rank_velocity_zscore or 0)
                reasons.append(f"• Gaining popularity at **+{display_zscore:.2f}σ** above normal pace")

        reason_text = "\n".join(reasons) if reasons else "Unusual popularity activity detected"
        description_parts = [
            f"**{company_name}** · `{self.data.ticker}` is seeing unusual social traction "
            f"({format_signed_pct(pct_change)} · ${price:.2f})\n\n{reason_text}"
        ]
        if phase_label:
            description_parts.append(f"\n{phase_label}")
        description = "\n".join(description_parts)

        fields = []

        if surge_result.current_rank is not None:
            fields.append(EmbedField(name="Current Rank", value=f"#{surge_result.current_rank}", inline=True))

        if surge_result.rank_24h_ago is not None:
            fields.append(EmbedField(name="Rank 24h Ago", value=f"#{surge_result.rank_24h_ago}", inline=True))

        if surge_result.rank_change is not None:
            direction = "↑" if surge_result.rank_change > 0 else "↓"
            fields.append(EmbedField(
                name="Rank Change",
                value=f"{direction}{abs(surge_result.rank_change)} spots",
                inline=True,
            ))

        if surge_result.mentions is not None:
            fields.append(EmbedField(name="Mentions", value=str(surge_result.mentions), inline=True))

        mention_ratio = surge_result.mention_ratio
        if is_valid_number(mention_ratio):
            fields.append(EmbedField(name="Mention Surge", value=f"{mention_ratio:.1f}x", inline=True))

        fields += self.price_change_fields(price, pct_change)

        # Confidence stat
        confidence_pct = getattr(self.data, 'confidence_pct', None)
        if confidence_pct is not None:
            fields.append(EmbedField(
                name="Signal Confidence (30d)",
                value=f"**{confidence_pct:.1f}%** of past surges confirmed",
                inline=True,
            ))

        history = self.data.popularity_history
        if (not history.empty
                and 'rank' in history.columns
                and 'datetime' in history.columns):
            recent = (
                history
                .sort_values('datetime')
                .tail(8)['rank']
                .tolist()
            )
            if len(recent) >= 2:
                trend_str = " → ".join(str(int(r)) for r in recent)
                fields.append(EmbedField(name=f"Rank Trend (Last {len(recent)} Intervals)", value=trend_str, inline=False))

        return EmbedSpec(
            title=f"🔥 Popularity Surge: {self.data.ticker}",
            description=description,
            color=COLOR_PURPLE,
            fields=fields,
            footer="RocketStocks · popularity-surge",
            timestamp=True,
            url=finviz_url(self.data.ticker),
        )

    def override_and_edit(self, prev_alert_data: dict) -> bool:
        """Re-post when mention_ratio is >= 1.5x the previous value, else use base logic."""
        mention_ratio = self.alert_data.get('mention_ratio') or 0.0
        prev_mention_ratio = prev_alert_data.get('mention_ratio') or 0.0

        if mention_ratio >= 1.5 and mention_ratio > prev_mention_ratio * 1.5:
            return True

        return super().override_and_edit(prev_alert_data)


def new_entrant_cutoff_label(current_rank: int | None) -> str:
    """Return a label describing the top-N rank cutoff for a new entrant."""
    if current_rank is None:
        return "top 500"
    # Round up to the nearest 100 for a natural label
    cutoff = ((current_rank // 100) + 1) * 100
    return f"top {cutoff}"
