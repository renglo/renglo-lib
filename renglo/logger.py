"""
Logging utility for renglo-lib.

The library always uses stdlib logging so importing it does not require Flask.
Applications can still route logs through their own handlers/formatters.
"""

import logging
import sys

# Create a default logger for when Flask is not available
_default_logger = None


def _get_default_logger():
    """Get or create the default logger."""
    global _default_logger
    if _default_logger is None:
        _default_logger = logging.getLogger('renglo')
        if not _default_logger.handlers:
            handler = logging.StreamHandler(sys.stderr)
            handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
            _default_logger.addHandler(handler)
            _default_logger.setLevel(logging.INFO)
    return _default_logger


def get_logger():
    """
    Get a logger that works without framework dependencies.
    
    Returns:
        Logger instance that supports debug(), info(), warning(), error() methods.
        Uses Python's standard logging.
    
    Usage:
        from renglo.logger import get_logger
        
        logger = get_logger()
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")
    """
    return _get_default_logger()
