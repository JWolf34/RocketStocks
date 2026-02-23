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


# Channel IDs and guild ID — read from environment
guild_id = int(get_env('DISCORD_GUILD_ID') or '0')
reports_channel_id = int(get_env("REPORTS_CHANNEL_ID") or '0')
alerts_channel_id = int(get_env("ALERTS_CHANNEL_ID") or '0')
screeners_channel_id = int(get_env("SCREENERS_CHANNEL_ID") or '0')
charts_channel_id = int(get_env("CHARTS_CHANNEL_ID") or '0')
notifications_channel_id = int(get_env("NOTIFICATIONS_CHANNEL_ID") or '0')
