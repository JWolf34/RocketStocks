import logging
import logging.handlers
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

_LOG_DIR = Path(__file__).parent.parent.parent.parent.parent / "logs"
_LOG_FILE = _LOG_DIR / "rocketstocks.log"
_FORMAT = "%(asctime)s [%(levelname)-8s] [%(name)s] > %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class ModuleFilter(logging.Filter):
    """Only pass log records from modules within the rocketstocks package."""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.name.startswith("rocketstocks") or record.name == "__main__"


def setup_logging(level: int = logging.DEBUG) -> None:
    """Configure root logger with stdout and file handlers."""
    root = logging.getLogger()
    root.setLevel(level)

    if any(h.name == "stdout" for h in root.handlers):
        return

    formatter = logging.Formatter(_FORMAT, datefmt=_DATE_FORMAT)
    module_filter = ModuleFilter()

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.set_name("stdout")
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(formatter)
    stdout_handler.addFilter(module_filter)
    root.addHandler(stdout_handler)

    try:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            _LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        file_handler.set_name("file")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        file_handler.addFilter(module_filter)
        root.addHandler(file_handler)
    except OSError as e:
        stdout_handler.setLevel(logging.DEBUG)
        logging.warning(f"Could not create log file handler: {e} — logging to stdout only")


def get_file_handler() -> logging.Handler | None:
    """Return the named file handler attached to the root logger, or None."""
    for handler in logging.getLogger().handlers:
        if handler.name == "file":
            return handler
    return None
