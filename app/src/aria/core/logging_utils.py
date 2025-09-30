"""Logging utilities for ARIA application.

This module provides standardized logging utilities and patterns for
consistent logging across all services and modules.
"""

import time
from typing import Dict, Any, Optional, Callable
from functools import wraps
from aria.core.logging_config import get_logger


def log_operation(operation_name: str, logger_name: Optional[str] = None):
    """Decorator to log function execution with timing and error handling.
    
    Args:
        operation_name: Human-readable name for the operation
        logger_name: Optional logger name, defaults to function's module
        
    Example:
        @log_operation("Question Extraction")
        def extract_questions(self, content):
            # function implementation
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get logger - use provided name or function's module
            if logger_name:
                logger = get_logger(logger_name)
            else:
                logger = get_logger(func.__module__)
            
            start_time = time.time()
            logger.info(f"Starting {operation_name}")
            
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                logger.info(f"✅ {operation_name} completed successfully in {execution_time:.2f}s")
                return result
                
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"❌ {operation_name} failed after {execution_time:.2f}s: {e}")
                raise
                
        return wrapper
    return decorator


def log_api_request(logger, method: str, endpoint: str, status_code: int, response_time: float, 
                   error: Optional[str] = None) -> None:
    """Log API request with standardized format.
    
    Args:
        logger: Logger instance
        method: HTTP method (GET, POST, etc.)
        endpoint: API endpoint path
        status_code: HTTP status code
        response_time: Response time in seconds
        error: Optional error message
    """
    endpoint_short = endpoint.split('/')[-1] if endpoint else 'unknown'
    
    if error:
        logger.error(f"🌐 {method} {endpoint_short} → {status_code} | {response_time:.2f}s | ERROR: {error}")
    elif status_code >= 400:
        logger.error(f"🌐 {method} {endpoint_short} → {status_code} | {response_time:.2f}s")
    elif status_code >= 300:
        logger.warning(f"🌐 {method} {endpoint_short} → {status_code} | {response_time:.2f}s")
    else:
        logger.info(f"🌐 {method} {endpoint_short} → {status_code} | {response_time:.2f}s")


def log_service_status(logger, service_name: str, status: str, details: Optional[Dict[str, Any]] = None) -> None:
    """Log service status with standardized format.
    
    Args:
        logger: Logger instance
        service_name: Name of the service
        status: Status (starting, ready, error, stopped)
        details: Optional additional details
    """
    status_emoji = {
        'starting': '🔄',
        'ready': '✅',
        'error': '❌',
        'stopped': '⏹️',
        'warning': '⚠️'
    }.get(status.lower(), '📊')
    
    message = f"{status_emoji} {service_name}: {status.upper()}"
    
    if details:
        detail_str = ", ".join(f"{k}={v}" for k, v in details.items())
        message += f" | {detail_str}"
    
    if status.lower() in ['error']:
        logger.error(message)
    elif status.lower() in ['warning']:
        logger.warning(message)
    else:
        logger.info(message)


def log_user_action(logger, action: str, user_id: Optional[str] = None, 
                   session_id: Optional[str] = None, details: Optional[Dict[str, Any]] = None) -> None:
    """Log user action with standardized format.
    
    Args:
        logger: Logger instance
        action: Description of the action
        user_id: Optional user identifier
        session_id: Optional session identifier  
        details: Optional additional details
    """
    message_parts = ["👤", action]
    
    if user_id:
        message_parts.append(f"user:{user_id}")
    if session_id:
        message_parts.append(f"session:{session_id}")
    
    if details:
        detail_str = ", ".join(f"{k}={v}" for k, v in details.items())
        message_parts.append(f"details:[{detail_str}]")
    
    message = " | ".join(message_parts)
    logger.info(message)


def log_performance_metric(logger, metric_name: str, value: float, unit: str = "", 
                          threshold: Optional[float] = None) -> None:
    """Log performance metric with threshold checking.
    
    Args:
        logger: Logger instance
        metric_name: Name of the metric
        value: Metric value
        unit: Unit of measurement
        threshold: Optional threshold for warning
    """
    message = f"📊 {metric_name}: {value:.2f}{unit}"
    
    if threshold and value > threshold:
        logger.warning(f"{message} (exceeds threshold {threshold:.2f}{unit})")
    else:
        logger.info(message)


class StructuredLogger:
    """Structured logger that provides consistent formatting and context."""
    
    def __init__(self, name: str):
        """Initialize structured logger.
        
        Args:
            name: Logger name (typically service or module name)
        """
        self.logger = get_logger(name)
        self.name = name
    
    def log_operation_start(self, operation: str, **context) -> None:
        """Log the start of an operation."""
        context_str = " | ".join(f"{k}={v}" for k, v in context.items()) if context else ""
        message = f"🔄 Starting {operation}"
        if context_str:
            message += f" | {context_str}"
        self.logger.info(message)
    
    def log_operation_success(self, operation: str, duration: float, **context) -> None:
        """Log successful completion of an operation."""
        context_str = " | ".join(f"{k}={v}" for k, v in context.items()) if context else ""
        message = f"✅ {operation} completed in {duration:.2f}s"
        if context_str:
            message += f" | {context_str}"
        self.logger.info(message)
    
    def log_operation_error(self, operation: str, error: Exception, duration: float, **context) -> None:
        """Log operation failure."""
        context_str = " | ".join(f"{k}={v}" for k, v in context.items()) if context else ""
        message = f"❌ {operation} failed after {duration:.2f}s: {error}"
        if context_str:
            message += f" | {context_str}"
        self.logger.error(message)
    
    def log_config_value(self, key: str, value: Any, sensitive: bool = False) -> None:
        """Log configuration value."""
        if sensitive:
            display_value = "***" if value else "not set"
        else:
            display_value = value
        self.logger.info(f"⚙️  Config {key}: {display_value}")
    
    def log_external_call(self, service: str, operation: str, success: bool, 
                         duration: Optional[float] = None, **context) -> None:
        """Log external service call."""
        status_emoji = "✅" if success else "❌"
        message = f"{status_emoji} {service}: {operation}"
        
        if duration:
            message += f" | {duration:.2f}s"
        
        if context:
            context_str = " | ".join(f"{k}={v}" for k, v in context.items())
            message += f" | {context_str}"
        
        if success:
            self.logger.info(message)
        else:
            self.logger.error(message)
