"""Type definitions and data models for ARIA application.

This module contains Pydantic models and type definitions used throughout
the application for data validation and type safety.
"""

from typing import Optional, List, Dict, Any, Union
from enum import Enum
from datetime import datetime
from pydantic import BaseModel, Field, validator
import pandas as pd


class FileType(str, Enum):
    """Supported file types for document upload."""
    CSV = "csv"
    HTML = "html"
    HTM = "htm"


class ProcessingStep(int, Enum):
    """Application processing steps."""
    UPLOAD = 1
    EXTRACT = 2
    GENERATE = 3
    DOWNLOAD = 4


class AuthMode(str, Enum):
    """Authentication modes for Databricks."""
    TOKEN = "token"
    SERVICE_PRINCIPAL = "service_principal"


class UploadedFile(BaseModel):
    """Model representing an uploaded file."""
    
    name: str = Field(..., description="Original filename")
    size: int = Field(..., description="File size in bytes")
    type: FileType = Field(..., description="File type")
    path: str = Field(..., description="Local file path")
    mime_type: str = Field(..., description="MIME type of the file")
    
    @validator("size")
    def validate_file_size(cls, v: int) -> int:
        """Validate file size is within limits."""
        max_size = 50 * 1024 * 1024  # 50MB in bytes
        if v > max_size:
            raise ValueError(f"File size {v} exceeds maximum allowed size of {max_size} bytes")
        return v
    
    @validator("type")
    def validate_file_type(cls, v: FileType) -> FileType:
        """Validate file type is supported."""
        if v not in [FileType.CSV, FileType.HTML, FileType.HTM]:
            raise ValueError(f"Unsupported file type: {v}")
        return v


class Question(BaseModel):
    """Model representing a question extracted from a document."""
    
    id: str = Field(..., description="Unique question identifier")
    topic: Optional[str] = Field(None, description="Question topic/category")
    sub_question: Optional[str] = Field(None, description="Sub-question identifier")
    text: str = Field(..., description="Question text content")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    @validator("text")
    def validate_text_not_empty(cls, v: str) -> str:
        """Ensure question text is not empty."""
        if not v.strip():
            raise ValueError("Question text cannot be empty")
        return v.strip()


class Answer(BaseModel):
    """Model representing an AI-generated answer."""
    
    question_id: str = Field(..., description="ID of the question being answered")
    text: str = Field(..., description="Answer text content")
    confidence: Optional[float] = Field(None, description="Confidence score (0-1)")
    generated_at: datetime = Field(default_factory=datetime.utcnow, description="Timestamp when answer was generated")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    @validator("confidence")
    def validate_confidence(cls, v: Optional[float]) -> Optional[float]:
        """Validate confidence score is between 0 and 1."""
        if v is not None and (v < 0 or v > 1):
            raise ValueError("Confidence score must be between 0 and 1")
        return v


class QuestionAnswerPair(BaseModel):
    """Model representing a question-answer pair."""
    
    question: Question = Field(..., description="The question")
    answer: Optional[Answer] = Field(None, description="The answer (if generated)")
    selected: bool = Field(True, description="Whether this pair is selected for processing")


class DocumentMetadata(BaseModel):
    """Model representing document metadata."""
    
    name: str = Field(..., description="Document name")
    file_type: FileType = Field(..., description="Document file type")
    size: int = Field(..., description="File size in bytes")
    upload_time: datetime = Field(default_factory=datetime.utcnow, description="Upload timestamp")
    processing_time: Optional[float] = Field(None, description="Processing time in seconds")
    question_count: Optional[int] = Field(None, description="Number of questions extracted")
    
    @validator("name")
    def validate_name_not_empty(cls, v: str) -> str:
        """Ensure document name is not empty."""
        if not v.strip():
            raise ValueError("Document name cannot be empty")
        return v.strip()


class ProcessingSession(BaseModel):
    """Model representing a complete processing session."""
    
    session_id: str = Field(..., description="Unique session identifier")
    document: DocumentMetadata = Field(..., description="Document metadata")
    questions: List[Question] = Field(default_factory=list, description="Extracted questions")
    answers: List[Answer] = Field(default_factory=list, description="Generated answers")
    current_step: ProcessingStep = Field(ProcessingStep.UPLOAD, description="Current processing step")
    custom_prompt: Optional[str] = Field(None, description="Custom prompt for AI processing")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Session creation time")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Last update time")


class APIRequest(BaseModel):
    """Model for API request payloads."""
    
    messages: List[Dict[str, str]] = Field(..., description="Messages for the AI model")
    max_tokens: int = Field(15000, description="Maximum tokens to generate")
    temperature: float = Field(0.1, description="Temperature for generation")
    
    @validator("max_tokens")
    def validate_max_tokens(cls, v: int) -> int:
        """Validate max tokens is positive."""
        if v <= 0:
            raise ValueError("Max tokens must be positive")
        return v
    
    @validator("temperature")
    def validate_temperature(cls, v: float) -> float:
        """Validate temperature is between 0 and 2."""
        if v < 0 or v > 2:
            raise ValueError("Temperature must be between 0 and 2")
        return v


class APIResponse(BaseModel):
    """Model for API response data."""
    
    choices: List[Dict[str, Any]] = Field(..., description="Response choices")
    usage: Optional[Dict[str, int]] = Field(None, description="Token usage information")
    model: Optional[str] = Field(None, description="Model name used")
    
    def get_text_content(self) -> Optional[str]:
        """Extract text content from the response."""
        if not self.choices:
            return None
        
        choice = self.choices[0]
        
        # Check for message format (OpenAI/Claude style API)
        if 'message' in choice and 'content' in choice['message']:
            return choice['message']['content']
        
        # Check for text format (older API style)
        if 'text' in choice:
            return choice['text']
        
        return None


class ExportData(BaseModel):
    """Model for export data structure."""
    
    question_id: str = Field(..., description="Question identifier")
    topic: Optional[str] = Field(None, description="Question topic")
    sub_question_id: Optional[str] = Field(None, description="Sub-question identifier")
    question_text: str = Field(..., description="Question text")
    answer: str = Field(..., description="Generated answer")
    
    class Config:
        """Pydantic configuration."""
        # Allow conversion from pandas DataFrame
        arbitrary_types_allowed = True


class TrackingData(BaseModel):
    """Model for usage tracking data."""
    
    customer: str = Field(..., description="Customer name")
    date_processed: datetime = Field(..., description="Processing date")
    user_email: Optional[str] = Field(None, description="User email")
    input_file: str = Field(..., description="Input file path")
    output_file: str = Field(..., description="Output file path")
    execution_time: float = Field(..., description="Execution time in minutes")
    record_count: int = Field(..., description="Number of records processed")
    volume_path: str = Field(..., description="Volume path used")
    timezone: str = Field(..., description="Timezone")


# Type aliases for common types
DataFrameType = pd.DataFrame
SessionState = Dict[str, Any]
ConfigDict = Dict[str, Any] 