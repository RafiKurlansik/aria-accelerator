"""Custom exceptions for ARIA application.

This module defines custom exception classes to provide better error handling
and more informative error messages throughout the application.
"""

from typing import Optional, Any, Dict


class AriaBaseException(Exception):
    """Base exception class for all ARIA-specific exceptions."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        """Initialize the exception with a message and optional details.
        
        Args:
            message: Human-readable error message
            details: Optional dictionary with additional error context
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}
    
    def __str__(self) -> str:
        """Return a string representation of the exception."""
        if self.details:
            details_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            return f"{self.message} ({details_str})"
        return self.message


class ConfigurationError(AriaBaseException):
    """Raised when there's an issue with application configuration."""
    pass


class AuthenticationError(AriaBaseException):
    """Raised when authentication fails or credentials are invalid."""
    pass


class FileProcessingError(AriaBaseException):
    """Raised when there's an error processing uploaded files."""
    pass


class UnsupportedFileTypeError(FileProcessingError):
    """Raised when an unsupported file type is uploaded."""
    pass


class QuestionExtractionError(AriaBaseException):
    """Raised when question extraction from documents fails."""
    pass


class AnswerGenerationError(AriaBaseException):
    """Raised when answer generation fails."""
    pass


class DatabricksAPIError(AriaBaseException):
    """Raised when Databricks API calls fail."""
    
    def __init__(
        self, 
        message: str, 
        status_code: Optional[int] = None,
        response_text: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        """Initialize with API-specific details.
        
        Args:
            message: Human-readable error message
            status_code: HTTP status code from the API response
            response_text: Response body text
            details: Additional error context
        """
        api_details = details or {}
        if status_code:
            api_details["status_code"] = status_code
        if response_text:
            api_details["response_text"] = response_text[:200]  # Truncate long responses
            
        super().__init__(message, api_details)
        self.status_code = status_code
        self.response_text = response_text


class ModelInvocationError(DatabricksAPIError):
    """Raised when model invocation fails."""
    pass


class DataValidationError(AriaBaseException):
    """Raised when data validation fails."""
    pass


class SessionStateError(AriaBaseException):
    """Raised when there's an issue with session state management."""
    pass


class ExportError(AriaBaseException):
    """Raised when export operations fail."""
    pass


class TemporaryServiceError(AriaBaseException):
    """Raised when a service is temporarily unavailable and should be retried.
    
    This exception is used for temporary failures like 503 errors, timeouts,
    or other retryable conditions across all services.
    """
    pass 