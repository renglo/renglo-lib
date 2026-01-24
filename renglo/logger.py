"""
Logging utility for renglo-lib that works with or without Flask.

This module provides a logger that automatically detects if Flask's current_app
is available and uses it, otherwise falls back to Python's standard logging.
This allows renglo-lib to work in both Flask contexts and standalone (e.g., Lambda handlers).
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
    Get a logger that works with or without Flask.
    
    Returns:
        Logger instance that supports debug(), info(), warning(), error() methods.
        If Flask's current_app is available, uses current_app.logger.
        Otherwise, uses Python's standard logging.
    
    Usage:
        from renglo.logger import get_logger
        
        logger = get_logger()
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")
    """
    try:
        # Try to get Flask's current_app
        from flask import has_request_context, current_app
        if has_request_context() and hasattr(current_app, 'logger'):
            return current_app.logger
    except (ImportError, RuntimeError):
        # Flask not available or not in request context
        pass
    
    # Fall back to standard logging
    return _get_default_logger()
