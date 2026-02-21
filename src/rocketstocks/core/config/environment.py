import os
import logging
from dotenv import load_dotenv

# Load dotenv
load_dotenv()

logger = logging.getLogger(__name__)


def get_env(var_name: str):
    variable = os.getenv(var_name)
    if variable is not None:
        return variable
    else:
        logger.error(f"Failed to fetch environment variable '{var_name}'")
        return variable
