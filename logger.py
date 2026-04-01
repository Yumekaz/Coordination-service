"""
Logging configuration for the Coordination Service.
Provides structured logging for debugging and monitoring.
"""

import logging
import sys
from config import LOG_LEVEL, LOG_FORMAT


def setup_logging(name: str = "coordination-service") -> logging.Logger:
    """
    Set up and return a configured logger.
    
    Args:
        name: The logger name (typically module name)
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logger.addHandler(handler)
        logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
    
    return logger


# Pre-configured loggers for each component
def get_logger(component: str) -> logging.Logger:
    """Get a logger for a specific component."""
    return setup_logging(f"coordination-service.{component}")
