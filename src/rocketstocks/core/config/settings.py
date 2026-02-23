import json
import logging
from rocketstocks.core.config.environment import get_env

logger = logging.getLogger(__name__)


class config:
    """Utilities for editing the config file"""
    def __init__(self):
        self.path = get_env("CONFIG_PATH")

    def load_config(self):
        try:
            with open(self.path) as config:
                data = json.load(config)
            return data
        except FileNotFoundError:
            logger.error(f"Config file not found: {self.path}")

    def write_config(self, data):
        with open(self.path, 'w') as config_file:
            json.dump(data, config_file)


