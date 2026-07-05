import os
import logging

logger = logging.getLogger(__name__)


def validate_path(path):
    """Validate specified path exists and create it if needed"""
    logger.info(f"Validating that path {path} exists")
    if not (os.path.isdir(path)):
        logger.info(f"Path {path} does not exist. Creating path...")
        os.makedirs(path)
        return
    else:
        logger.info(f"Path {path} exists in the filesystem")
        return True


class datapaths:

    attachments_path = "discord/attachments"
