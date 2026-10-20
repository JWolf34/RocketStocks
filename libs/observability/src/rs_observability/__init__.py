"""RocketStocks v2 observability: logging, metrics, and health."""

from rs_observability.hooks import install_excepthooks
from rs_observability.logs import configure_logging, unit_of_work
from rs_observability.metrics import (
    CallableGaugeCollector,
    DictCounterCollector,
    LabeledDictCounterCollector,
    start_metrics_server,
)
from rs_observability.ops import OpsSettings, configure_ops, notify_ops

__all__ = [
    "CallableGaugeCollector",
    "DictCounterCollector",
    "LabeledDictCounterCollector",
    "OpsSettings",
    "configure_logging",
    "configure_ops",
    "install_excepthooks",
    "notify_ops",
    "start_metrics_server",
    "unit_of_work",
]
