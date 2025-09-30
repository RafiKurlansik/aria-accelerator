"""Core module for ARIA application.

This module contains core utilities including exceptions, logging configuration,
and type definitions.
"""

from .exceptions import (
    AriaBaseException,
    ConfigurationError, 
    AuthenticationError,
    FileProcessingError,
    UnsupportedFileTypeError,
    QuestionExtractionError,
    AnswerGenerationError,
    DatabricksAPIError,
    ModelInvocationError,
    DataValidationError,
    SessionStateError,
    ExportError,
    TemporaryServiceError,
)
from .logging_config import setup_logging, get_logger, log_info, log_warning, log_error, log_success
from .logging_utils import log_operation, log_api_request, log_service_status, log_user_action, log_performance_metric, StructuredLogger
from .base_service import BaseService
from .types import (
    FileType,
    ProcessingStep,
    AuthMode,
    UploadedFile,
    Question,
    Answer,
    QuestionAnswerPair,
    DocumentMetadata,
    ProcessingSession,
    APIRequest,
    APIResponse,
    ExportData,
    TrackingData,
    DataFrameType,
    SessionState,
    ConfigDict,
)

__all__ = [
    # Exceptions
    "AriaBaseException",
    "ConfigurationError", 
    "AuthenticationError",
    "FileProcessingError",
    "UnsupportedFileTypeError",
    "QuestionExtractionError",
    "AnswerGenerationError",
    "DatabricksAPIError",
    "ModelInvocationError",
    "DataValidationError",
    "SessionStateError",
    "ExportError",
    "TemporaryServiceError",
    
    # Logging
    "setup_logging",
    "get_logger",
    "log_info",
    "log_warning", 
    "log_error",
    "log_success",
    "log_operation",
    "log_api_request",
    "log_service_status", 
    "log_user_action",
    "log_performance_metric",
    "StructuredLogger",
    
    # Base Service
    "BaseService",
    
    # Types
    "FileType",
    "ProcessingStep",
    "AuthMode",
    "UploadedFile",
    "Question",
    "Answer",
    "QuestionAnswerPair",
    "DocumentMetadata",
    "ProcessingSession",
    "APIRequest",
    "APIResponse",
    "ExportData",
    "TrackingData",
    "DataFrameType",
    "SessionState",
    "ConfigDict",
]
