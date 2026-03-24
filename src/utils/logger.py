"""
TalentScope AI — Logging Module
Structured logging for pipeline observability.
"""

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """Create a configured logger with console output."""
    logger = logging.getLogger(name)

    if not logger.handlers:
        from src.utils.config import LOG_LEVEL

        logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger