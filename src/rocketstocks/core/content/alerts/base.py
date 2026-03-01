"""Base Alert — standalone, no Report inheritance."""
import logging

from rocketstocks.core.content.models import EmbedSpec

logger = logging.getLogger(__name__)


class Alert:
    """Standalone base alert.

    Concrete subclasses declare alert_type and implement build() and
    override_and_edit(). The pct_change threshold check is provided as a helper
    so subclasses can reuse the logic without deep inheritance.
    """

    alert_type: str = "ALERT"

    def __init__(self):
        self.alert_data: dict = {}

    def build(self) -> EmbedSpec:
        """Return an EmbedSpec for rich Discord embed output.

        Subclasses must override this.
        """
        raise NotImplementedError

    def override_and_edit(self, prev_alert_data: dict) -> bool:
        """Return True if the alert should be re-posted based on significant movement.

        Default: triggers when pct_change has moved >100% relative to the previous alert.
        Subclasses may extend or replace this logic.
        """
        pct_change = self.alert_data.get('pct_change')
        prev_pct_change = prev_alert_data.get('pct_change')
        if pct_change is None or prev_pct_change is None:
            return False
        pct_diff = ((pct_change - prev_pct_change) / abs(prev_pct_change)) * 100.0
        return pct_diff > 100.0
