"""Base Alert — standalone, no Report inheritance."""
import logging

from rocketstocks.core.content.models import EmbedField, EmbedSpec
from rocketstocks.core.utils.formatting import change_emoji, format_signed_pct

logger = logging.getLogger(__name__)

_MOMENTUM_HISTORY_KEY = 'momentum_history'


class Alert:
    """Standalone base alert.

    Concrete subclasses declare alert_type and implement build().
    The override_and_edit() method uses momentum acceleration z-scores
    to decide whether to post an update.
    """

    alert_type: str = "ALERT"
    role_key: str | None = None

    def __init__(self):
        self.alert_data: dict = {}

    def build(self) -> EmbedSpec:
        """Return an EmbedSpec for rich Discord embed output.

        Subclasses must override this.
        """
        raise NotImplementedError

    @staticmethod
    def populate_trigger_data(alert_data: dict, trigger_result) -> None:
        """Copy AlertTriggerResult fields into alert_data dict.

        Safe to call when trigger_result is None — does nothing in that case.
        """
        if trigger_result is None:
            return
        alert_data['zscore'] = trigger_result.zscore
        alert_data['percentile'] = trigger_result.percentile
        alert_data['classification'] = getattr(
            trigger_result.classification, 'value', str(trigger_result.classification)
        )
        alert_data['signal_type'] = trigger_result.signal_type
        alert_data['bb_position'] = trigger_result.bb_position
        alert_data['confluence_count'] = trigger_result.confluence_count
        alert_data['volume_zscore'] = trigger_result.volume_zscore

    @staticmethod
    def price_change_fields(price: float, pct_change: float) -> list[EmbedField]:
        """Return standard Price and Change embed fields."""
        return [
            EmbedField(name="Price", value=f"${price:.2f}", inline=True),
            EmbedField(name="Change", value=format_signed_pct(pct_change), inline=True),
        ]

    def override_and_edit(self, prev_alert_data: dict) -> bool:
        """Return True if the alert should be re-posted based on significant movement.

        Uses momentum acceleration z-score logic from the momentum module.
        Falls back to the ">100% relative change" heuristic when there is
        insufficient momentum history.
        """
        from rocketstocks.core.analysis.momentum import should_update_alert
        pct_change = self.alert_data.get('pct_change')
        if pct_change is None:
            return False
        return should_update_alert(
            current_pct=pct_change,
            prev_alert_data=prev_alert_data,
        )

    def record_momentum(self, prev_alert_data: dict) -> None:
        """Append a momentum snapshot to ``alert_data['momentum_history']``.

        This should be called by the alert sender *after* override_and_edit()
        and *before* persisting the new alert_data to the database.

        Args:
            prev_alert_data: The previously stored alert_data dict (used to
                compute velocity and acceleration deltas).
        """
        from rocketstocks.core.analysis.momentum import build_momentum_snapshot
        pct_change = self.alert_data.get('pct_change')
        if pct_change is None:
            return

        snapshot = build_momentum_snapshot(
            current_pct=pct_change,
            prev_alert_data=prev_alert_data,
        )

        history: list = self.alert_data.get(_MOMENTUM_HISTORY_KEY, [])
        history.append(snapshot)
        self.alert_data[_MOMENTUM_HISTORY_KEY] = history
        logger.debug(f"Recorded momentum snapshot: {snapshot}")
