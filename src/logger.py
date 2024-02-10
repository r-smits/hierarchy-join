import logging
from logging import Logger, StreamHandler

logger: Logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
stream_handler: StreamHandler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)
