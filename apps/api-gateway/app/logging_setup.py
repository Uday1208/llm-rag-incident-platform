"""
File: logging_setup.py
Purpose: Configure structured JSON logging for production observability.
"""

import logging
from pythonjsonlogger import jsonlogger

def configure_logging() -> None:
    """Configure root logger for JSON output and sane defaults."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    handler.setFormatter(formatter)
    logger.handlers = [handler]
