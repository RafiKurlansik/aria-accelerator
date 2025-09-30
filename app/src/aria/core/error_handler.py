"""Centralized error handling for ARIA application.

This module provides consistent, user-friendly error messages across all services
that interact with Databricks APIs and endpoints.
"""

from enum import Enum
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
import re


class ErrorCategory(Enum):
    """Categories of errors for consistent handling."""
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    NETWORK = "network"
    SERVICE_UNAVAILABLE = "service_unavailable"
    RATE_LIMIT = "rate_limit"
    VALIDATION = "validation"
    MODEL_ERROR = "model_error"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


class ErrorSeverity(Enum):
    """Severity levels for errors."""
    LOW = "low"          # User can likely resolve
    MEDIUM = "medium"    # May need retry or minor action
    HIGH = "high"        # Requires admin intervention
    CRITICAL = "critical"  # System-level issue


@dataclass
class ErrorInfo:
    """Structured error information for consistent handling."""
    category: ErrorCategory
    severity: ErrorSeverity
    user_message: str
    admin_message: str
    suggested_actions: list[str]
    is_retryable: bool
    retry_delay_seconds: Optional[int] = None
    error_code: Optional[str] = None


class ARIAErrorHandler:
    """Centralized error handler for consistent user experience."""
    
    def __init__(self):
        """Initialize the error handler with predefined error patterns."""
        self._error_patterns = self._build_error_patterns()
    
    def handle_api_error(
        self,
        status_code: int,
        response_text: str,
        service_name: str,
        operation: str
    ) -> ErrorInfo:
        """Handle API errors and return structured error information.
        
        Args:
            status_code: HTTP status code
            response_text: Response body text
            service_name: Name of the service (e.g., "Chat", "Question Extraction")
            operation: What operation was being performed
            
        Returns:
            ErrorInfo with user-friendly messages and guidance
        """
        # Determine error category based on status code and response
        if status_code == 401:
            return self._handle_authentication_error(response_text, service_name)
        elif status_code == 403:
            return self._handle_authorization_error(response_text, service_name)
        elif status_code == 404:
            return self._handle_not_found_error(response_text, service_name)
        elif status_code == 429:
            return self._handle_rate_limit_error(response_text, service_name)
        elif status_code in [500, 502, 503, 504]:
            return self._handle_server_error(status_code, response_text, service_name)
        elif status_code == 400:
            return self._handle_bad_request_error(response_text, service_name, operation)
        else:
            return self._handle_unknown_error(status_code, response_text, service_name)
    
    def handle_network_error(self, error: Exception, service_name: str) -> ErrorInfo:
        """Handle network-related errors.
        
        Args:
            error: The network exception
            service_name: Name of the service
            
        Returns:
            ErrorInfo with network-specific guidance
        """
        error_str = str(error).lower()
        
        if "timeout" in error_str:
            return ErrorInfo(
                category=ErrorCategory.TIMEOUT,
                severity=ErrorSeverity.MEDIUM,
                user_message=f"⏱️ {service_name} request timed out. This often happens when the AI model needs to wake up after being idle.",
                admin_message=f"Timeout error in {service_name}: {error}",
                suggested_actions=[
                    "The system will automatically retry in a few moments",
                    "This is normal for the first request after a period of inactivity",
                    "For chat requests, this usually resolves within 30-60 seconds"
                ],
                is_retryable=True,
                retry_delay_seconds=30,
                error_code="MODEL_COLD_START"
            )
        elif "connection" in error_str:
            return ErrorInfo(
                category=ErrorCategory.NETWORK,
                severity=ErrorSeverity.HIGH,
                user_message=f"🌐 Unable to connect to {service_name}. Please check your network connection.",
                admin_message=f"Network connection error in {service_name}: {error}",
                suggested_actions=[
                    "Check your internet connection",
                    "Try refreshing the page",
                    "Contact your IT administrator if the problem persists"
                ],
                is_retryable=True,
                retry_delay_seconds=10
            )
        else:
            return ErrorInfo(
                category=ErrorCategory.NETWORK,
                severity=ErrorSeverity.MEDIUM,
                user_message=f"🔌 Network error while connecting to {service_name}. Please try again.",
                admin_message=f"Network error in {service_name}: {error}",
                suggested_actions=[
                    "Try again in a few moments",
                    "Check your network connection",
                    "Contact support if the issue continues"
                ],
                is_retryable=True,
                retry_delay_seconds=15
            )
    
    def _handle_authentication_error(self, response_text: str, service_name: str) -> ErrorInfo:
        """Handle 401 authentication errors."""
        return ErrorInfo(
            category=ErrorCategory.AUTHENTICATION,
            severity=ErrorSeverity.HIGH,
            user_message=f"🔐 Authentication failed for {service_name}. Your session may have expired.",
            admin_message=f"Authentication error in {service_name}: {response_text}",
            suggested_actions=[
                "Try refreshing the page to get a new session",
                "Contact your administrator to verify your access credentials",
                "Check if your Databricks token is still valid"
            ],
            is_retryable=False,
            error_code="AUTH_FAILED"
        )
    
    def _handle_authorization_error(self, response_text: str, service_name: str) -> ErrorInfo:
        """Handle 403 authorization errors."""
        # Check for specific permission patterns
        if "endpoint" in response_text.lower() or "serving" in response_text.lower():
            return ErrorInfo(
                category=ErrorCategory.AUTHORIZATION,
                severity=ErrorSeverity.HIGH,
                user_message=f"🚫 You don't have permission to access the {service_name} model endpoint.",
                admin_message=f"Model endpoint permission error in {service_name}: {response_text}",
                suggested_actions=[
                    "Contact your Databricks administrator",
                    "Request access to the model serving endpoint",
                    "Verify your service principal has the correct permissions"
                ],
                is_retryable=False,
                error_code="ENDPOINT_ACCESS_DENIED"
            )
        else:
            return ErrorInfo(
                category=ErrorCategory.AUTHORIZATION,
                severity=ErrorSeverity.HIGH,
                user_message=f"🚫 You don't have permission to use {service_name}.",
                admin_message=f"Authorization error in {service_name}: {response_text}",
                suggested_actions=[
                    "Contact your Databricks administrator",
                    "Request the necessary permissions for ARIA",
                    "Check your workspace access level"
                ],
                is_retryable=False,
                error_code="ACCESS_DENIED"
            )
    
    def _handle_not_found_error(self, response_text: str, service_name: str) -> ErrorInfo:
        """Handle 404 not found errors."""
        if "endpoint" in response_text.lower() or "serving" in response_text.lower():
            return ErrorInfo(
                category=ErrorCategory.MODEL_ERROR,
                severity=ErrorSeverity.HIGH,
                user_message=f"📍 The {service_name} model endpoint was not found. It may not be deployed or configured correctly.",
                admin_message=f"Model endpoint not found for {service_name}: {response_text}",
                suggested_actions=[
                    "Contact your administrator to verify the model endpoint is deployed",
                    "Check the model configuration in ARIA settings",
                    "Verify the endpoint name is correct"
                ],
                is_retryable=False,
                error_code="ENDPOINT_NOT_FOUND"
            )
        else:
            return ErrorInfo(
                category=ErrorCategory.MODEL_ERROR,
                severity=ErrorSeverity.HIGH,
                user_message=f"📍 {service_name} service endpoint not found.",
                admin_message=f"Service endpoint not found for {service_name}: {response_text}",
                suggested_actions=[
                    "Contact your administrator",
                    "Check the service configuration",
                    "Verify ARIA is properly deployed"
                ],
                is_retryable=False,
                error_code="SERVICE_NOT_FOUND"
            )
    
    def _handle_rate_limit_error(self, response_text: str, service_name: str) -> ErrorInfo:
        """Handle 429 rate limit errors."""
        # Extract retry-after if available
        retry_delay = 60  # Default
        if "retry-after" in response_text.lower():
            try:
                match = re.search(r'retry-after[:\s]+(\d+)', response_text.lower())
                if match:
                    retry_delay = int(match.group(1))
            except:
                pass
        
        return ErrorInfo(
            category=ErrorCategory.RATE_LIMIT,
            severity=ErrorSeverity.MEDIUM,
            user_message=f"⏳ {service_name} is currently rate limited. Too many requests have been made.",
            admin_message=f"Rate limit exceeded for {service_name}: {response_text}",
            suggested_actions=[
                f"Wait {retry_delay} seconds before trying again",
                "Try processing smaller batches of data",
                "Contact your administrator about increasing rate limits"
            ],
            is_retryable=True,
            retry_delay_seconds=retry_delay,
            error_code="RATE_LIMITED"
        )
    
    def _handle_server_error(self, status_code: int, response_text: str, service_name: str) -> ErrorInfo:
        """Handle 5xx server errors."""
        if status_code == 503:
            return ErrorInfo(
                category=ErrorCategory.SERVICE_UNAVAILABLE,
                severity=ErrorSeverity.MEDIUM,
                user_message=f"🔧 {service_name} is temporarily unavailable. The service may be restarting or under maintenance.",
                admin_message=f"Service unavailable (503) for {service_name}: {response_text}",
                suggested_actions=[
                    "Try again in a few minutes",
                    "Check Databricks status page for maintenance notifications",
                    "Contact support if the issue persists"
                ],
                is_retryable=True,
                retry_delay_seconds=120
            )
        elif status_code == 504:
            return ErrorInfo(
                category=ErrorCategory.TIMEOUT,
                severity=ErrorSeverity.MEDIUM,
                user_message=f"🌅 {service_name} timed out. This usually happens when the AI model needs to wake up after being idle.",
                admin_message=f"Gateway timeout (504) for {service_name}: {response_text}",
                suggested_actions=[
                    "The system will automatically retry in a few moments",
                    "This is normal for the first request after a period of inactivity",
                    "Contact support if this continues for more than 2 minutes"
                ],
                is_retryable=True,
                retry_delay_seconds=30,
                error_code="MODEL_COLD_START"
            )
        else:
            return ErrorInfo(
                category=ErrorCategory.SERVICE_UNAVAILABLE,
                severity=ErrorSeverity.HIGH,
                user_message=f"🔧 {service_name} encountered an internal server error. This is likely a temporary issue.",
                admin_message=f"Server error ({status_code}) for {service_name}: {response_text}",
                suggested_actions=[
                    "Try again in a few minutes",
                    "Contact your administrator if this continues",
                    "Report this issue with the error details"
                ],
                is_retryable=True,
                retry_delay_seconds=60
            )
    
    def _handle_bad_request_error(self, response_text: str, service_name: str, operation: str) -> ErrorInfo:
        """Handle 400 bad request errors."""
        response_lower = response_text.lower()
        
        if "too large" in response_lower or "payload" in response_lower:
            return ErrorInfo(
                category=ErrorCategory.VALIDATION,
                severity=ErrorSeverity.MEDIUM,
                user_message=f"📄 Your document is too large for {service_name} to process.",
                admin_message=f"Payload too large for {service_name}: {response_text}",
                suggested_actions=[
                    "Try processing a smaller document",
                    "Break your document into sections",
                    "Remove unnecessary content before uploading"
                ],
                is_retryable=False,
                error_code="PAYLOAD_TOO_LARGE"
            )
        elif "token" in response_lower or "length" in response_lower:
            return ErrorInfo(
                category=ErrorCategory.VALIDATION,
                severity=ErrorSeverity.MEDIUM,
                user_message=f"📝 Your request is too long for {service_name} to process.",
                admin_message=f"Token length error for {service_name}: {response_text}",
                suggested_actions=[
                    "Try with a shorter document or question",
                    "Break your content into smaller parts",
                    "Remove unnecessary details"
                ],
                is_retryable=False,
                error_code="INPUT_TOO_LONG"
            )
        else:
            return ErrorInfo(
                category=ErrorCategory.VALIDATION,
                severity=ErrorSeverity.MEDIUM,
                user_message=f"❌ Invalid request to {service_name}. Please check your input.",
                admin_message=f"Bad request for {service_name}: {response_text}",
                suggested_actions=[
                    "Check your input format",
                    "Try uploading a different file",
                    "Contact support if the problem continues"
                ],
                is_retryable=False,
                error_code="INVALID_REQUEST"
            )
    
    def _handle_unknown_error(self, status_code: int, response_text: str, service_name: str) -> ErrorInfo:
        """Handle unexpected errors."""
        return ErrorInfo(
            category=ErrorCategory.UNKNOWN,
            severity=ErrorSeverity.MEDIUM,
            user_message=f"❓ {service_name} returned an unexpected error (status {status_code}).",
            admin_message=f"Unexpected error ({status_code}) for {service_name}: {response_text}",
            suggested_actions=[
                "Try again in a few moments",
                "Contact support with the error details",
                "Check if the issue affects other users"
            ],
            is_retryable=True,
            retry_delay_seconds=30,
            error_code=f"HTTP_{status_code}"
        )
    
    def _build_error_patterns(self) -> Dict[str, ErrorCategory]:
        """Build patterns for categorizing errors based on response text."""
        return {
            "jwt": ErrorCategory.AUTHENTICATION,
            "token": ErrorCategory.AUTHENTICATION,
            "unauthorized": ErrorCategory.AUTHENTICATION,
            "forbidden": ErrorCategory.AUTHORIZATION,
            "permission": ErrorCategory.AUTHORIZATION,
            "access denied": ErrorCategory.AUTHORIZATION,
            "rate limit": ErrorCategory.RATE_LIMIT,
            "too many requests": ErrorCategory.RATE_LIMIT,
            "service unavailable": ErrorCategory.SERVICE_UNAVAILABLE,
            "timeout": ErrorCategory.TIMEOUT,
            "connection": ErrorCategory.NETWORK,
            "network": ErrorCategory.NETWORK,
        }
    
    def format_user_error(self, error_info: ErrorInfo) -> str:
        """Format error for display to users.
        
        Args:
            error_info: The error information
            
        Returns:
            Formatted error message for users
        """
        message_parts = [error_info.user_message]
        
        if error_info.suggested_actions:
            message_parts.append("\n\n💡 **What you can do:**")
            for i, action in enumerate(error_info.suggested_actions, 1):
                message_parts.append(f"{i}. {action}")
        
        if error_info.is_retryable and error_info.retry_delay_seconds:
            message_parts.append(f"\n\n🔄 You can try again in {error_info.retry_delay_seconds} seconds.")
        
        return "\n".join(message_parts)
    
    def format_admin_error(self, error_info: ErrorInfo) -> str:
        """Format error for admin/logging purposes.
        
        Args:
            error_info: The error information
            
        Returns:
            Formatted error message for admins/logs
        """
        parts = [
            f"Error Category: {error_info.category.value}",
            f"Severity: {error_info.severity.value}",
            f"Retryable: {error_info.is_retryable}",
            f"Admin Message: {error_info.admin_message}"
        ]
        
        if error_info.error_code:
            parts.append(f"Error Code: {error_info.error_code}")
        
        if error_info.retry_delay_seconds:
            parts.append(f"Retry Delay: {error_info.retry_delay_seconds}s")
        
        return " | ".join(parts)


# Global error handler instance
error_handler = ARIAErrorHandler()
