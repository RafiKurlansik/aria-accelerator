"""Base service class with standardized error handling patterns.

This module provides a base class that all ARIA services should inherit from
to ensure consistent error handling, logging, and response patterns.
"""

from abc import ABC
from typing import Dict, List, Any, Optional, Tuple
from aria.core.logging_config import get_logger
from aria.core.error_handler import ARIAErrorHandler, ErrorInfo
from aria.config import settings


class BaseService(ABC):
    """Base class for all ARIA services with standardized error handling."""
    
    def __init__(self):
        """Initialize the base service."""
        self.settings = settings
        self.logger = get_logger(self.__class__.__name__)
        self.error_handler = ARIAErrorHandler()
    
    def _create_info_dict(self, method: str = "unknown") -> Dict[str, Any]:
        """Create a standardized info dictionary for service responses.
        
        Args:
            method: The method/operation being performed
            
        Returns:
            Dictionary with standard structure for error tracking
        """
        return {
            "method": method,
            "processing_time": 0.0,
            "errors": [],
            "user_errors": [],  # User-friendly error messages
            "admin_errors": [],  # Technical error details for logging
        }
    
    def _handle_api_error(
        self, 
        status_code: int, 
        response_text: str, 
        operation: str,
        context: str = "",
        info_dict: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, str]:
        """Handle API errors consistently across all services.
        
        Args:
            status_code: HTTP status code
            response_text: Response body text
            operation: Name of the operation (e.g., "Question Extraction")
            context: Additional context for the error
            info_dict: Optional info dictionary to populate with error details
            
        Returns:
            Tuple of (success=False, user_friendly_error_message)
        """
        error_info = self.error_handler.handle_api_error(
            status_code, response_text, operation, context
        )
        
        # Log technical details
        admin_msg = self.error_handler.format_admin_error(error_info)
        self.logger.error(admin_msg)
        
        # Get user-friendly message
        user_msg = self.error_handler.format_user_error(error_info)
        
        # Populate info dict if provided
        if info_dict is not None:
            info_dict["errors"].append(admin_msg)
            info_dict["user_errors"].append(user_msg)
            info_dict["admin_errors"].append(admin_msg)
        
        return False, user_msg
    
    def _handle_exception(
        self, 
        exception: Exception, 
        operation: str,
        context: str = "",
        info_dict: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, str]:
        """Handle exceptions consistently across all services.
        
        Args:
            exception: The exception that occurred
            operation: Name of the operation
            context: Additional context
            info_dict: Optional info dictionary to populate
            
        Returns:
            Tuple of (success=False, user_friendly_error_message)
        """
        error_msg = str(exception)
        
        # Check for timeout errors
        if any(keyword in error_msg.lower() for keyword in ["timeout", "timed out", "upstream response timeout"]):
            user_msg = "Request timed out - try reducing the amount of content being processed or try again later."
            admin_msg = f"Timeout in {operation}: {error_msg}"
        else:
            user_msg = f"An error occurred during {operation.lower()}. Please try again."
            admin_msg = f"Error in {operation}: {error_msg}"
        
        self.logger.error(admin_msg)
        
        # Populate info dict if provided
        if info_dict is not None:
            info_dict["errors"].append(admin_msg)
            info_dict["user_errors"].append(user_msg)
            info_dict["admin_errors"].append(admin_msg)
        
        return False, user_msg
    
    def _get_auth_headers(self) -> Optional[Dict[str, str]]:
        """Get authentication headers with consistent error handling.
        
        Returns:
            Auth headers if available, None if authentication not configured
        """
        try:
            return self.settings.get_auth_headers()
        except Exception as e:
            self.logger.error(f"Failed to get authentication headers: {e}")
            return None
    
    def _check_authentication(self, info_dict: Optional[Dict[str, Any]] = None) -> Tuple[bool, Optional[Dict[str, str]]]:
        """Check authentication and return headers with consistent error handling.
        
        Args:
            info_dict: Optional info dictionary to populate with error details
            
        Returns:
            Tuple of (success, auth_headers)
        """
        auth_headers = self._get_auth_headers()
        if not auth_headers:
            error_info = self.error_handler._handle_authentication_error(
                "No authentication headers available", 
                self.__class__.__name__
            )
            
            admin_msg = self.error_handler.format_admin_error(error_info)
            user_msg = self.error_handler.format_user_error(error_info)
            
            self.logger.error(admin_msg)
            
            if info_dict is not None:
                info_dict["errors"].append(admin_msg)
                info_dict["user_errors"].append(user_msg)
                info_dict["admin_errors"].append(admin_msg)
            
            return False, None
        
        return True, auth_headers
