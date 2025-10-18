"""
File: logging_setup.py
Purpose: Configure structured JSON logging for production observability.
"""

import logging
from python_json_logger import jsonlogger
from .config import settings

def configure_logging() -> None:
    """Configure root logger for JSON output and level from settings."""
    logger = logging.getLogger()
    logger.setLevel(settings.LOG_LEVEL)
    handler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    handler.setFormatter(formatter)
    logger.handlers = [handler]
