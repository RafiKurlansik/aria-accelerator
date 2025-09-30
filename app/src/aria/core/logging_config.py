"""Logging configuration for ARIA application.

This module provides centralized logging setup with console and file logging
capabilities, following best practices for structured logging.
"""

import logging
import sys
from typing import Optional
from pathlib import Path


def setup_logging(
    level: str = "INFO",
    log_file: Optional[Path] = None,
) -> logging.Logger:
    """Set up logging configuration for the application.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path for logging output
        
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger("aria")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger


def log_api_call(
    logger: logging.Logger,
    endpoint: str,
    payload: dict,
    status_code: Optional[int] = None,
    response: Optional[dict] = None,
    error: Optional[Exception] = None
) -> None:
    """Log API call information with appropriate log level.
    
    Args:
        logger: Logger instance to use
        endpoint: API endpoint that was called
        payload: Request payload (sensitive data will be sanitized)
        status_code: HTTP status code of the response
        response: Response data (optional)
        error: Exception that occurred (optional)
    """
    # Sanitize payload (remove auth tokens)
    sanitized_payload = payload.copy() if isinstance(payload, dict) else {}
    if 'messages' in sanitized_payload:
        # Keep the messages but sanitize any auth info
        pass
    
    # Extract endpoint name for logging
    endpoint_short = endpoint.split('/')[-1] if isinstance(endpoint, str) else 'unknown'
    
    # Log based on success/failure
    if error:
        logger.error(f"API call failed to {endpoint_short}: {error}")
    elif status_code and status_code >= 400:
        logger.error(f"API call failed to {endpoint_short} with status {status_code}")
    else:
        logger.info(f"API call successful to {endpoint_short} with status {status_code}")


def get_logger(name: str = "aria") -> logging.Logger:
    """Get a logger instance for a specific module.
    
    Args:
        name: Name of the logger (typically module name)
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


# Convenience functions for common logging patterns
def log_info(message: str) -> None:
    """Log an info message.
    
    Args:
        message: Message to log
    """
    logger = get_logger()
    logger.info(message)


def log_warning(message: str) -> None:
    """Log a warning message.
    
    Args:
        message: Message to log
    """
    logger = get_logger()
    logger.warning(message)


def log_error(message: str) -> None:
    """Log an error message.
    
    Args:
        message: Message to log
    """
    logger = get_logger()
    logger.error(message)


def log_success(message: str) -> None:
    """Log a success message.
    
    Args:
        message: Message to log
    """
    logger = get_logger()
    logger.info(f"SUCCESS: {message}")