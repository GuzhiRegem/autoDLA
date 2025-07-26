import logging
import sys
import os

logger = logging.getLogger("autodla")
logger.setLevel(logging.DEBUG)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

VERBOSE = os.environ.get("AUTODLA_SQL_VERBOSE", "false").lower() in (
    "true", "1", "yes"
)
logger.disabled = not VERBOSE
