import logging
import sys
from typing import Dict, Any


def setup_logging(
    level: int = logging.INFO,
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
) -> None:
    """
    Configure logging for the application

    Args:
        level: The logging level
        format: The log message format
    """
    logging.basicConfig(
        level=level,
        format=format,
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("app.log")],
    )


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the specified name

    Args:
        name: The name for the logger

    Returns:
        logging.Logger: Configured logger instance
    """
    return logging.getLogger(name)
