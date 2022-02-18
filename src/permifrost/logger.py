import logging

import coloredlogs

logger = logging.getLogger(__name__)
logger_style = "%(asctime)s: [%(levelname)-8s] [%(module)s] %(message)s"
coloredlogs.install(level="DEBUG", logger=logger, fmt=logger_style)

GLOBAL_LOGGER = logger
