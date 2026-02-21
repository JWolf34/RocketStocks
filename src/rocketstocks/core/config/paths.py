import os
import logging

logger = logging.getLogger(__name__)


def validate_path(path):
    """Validate specified path exists and create it if needed"""
    logger.info("Validating that path {} exists".format(path))
    if not (os.path.isdir(path)):
        logger.info("Path {} does not exist. Creating path...".format(path))
        os.makedirs(path)
        return
    else:
        logger.info("Path {} exists in the filesystem".format(path))
        return True


class datapaths:

    attachments_path = "discord/attachments"
