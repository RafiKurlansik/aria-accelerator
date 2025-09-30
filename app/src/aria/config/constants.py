"""Application constants for ARIA.

This module contains all application-wide constants including file types,
limits, and configuration defaults.
"""

from typing import Final

# File processing constants
SUPPORTED_FILE_TYPES: Final[list[str]] = [".csv", ".html", ".htm"]
MAX_FILE_SIZE_MB: Final[int] = 50
MAX_QUESTIONS_PER_BATCH: Final[int] = 100

# Available Claude models for question extraction
AVAILABLE_CLAUDE_MODELS: Final[dict[str, str]] = {
    "databricks-claude-sonnet-4": "Claude Sonnet 4 (Latest - Fast & Balanced)",
    "databricks-claude-3-7-sonnet": "Claude 3.7 Sonnet (Hybrid Reasoning)",
}

# Default model selection
DEFAULT_QUESTION_EXTRACTION_MODEL: Final[str] = "databricks-claude-3-7-sonnet"

# API constants
DEFAULT_TIMEOUT_SECONDS: Final[int] = 480  # 8 minutes - increased from 297s to handle complex extractions
MAX_RETRIES: Final[int] = 3
RETRY_WAIT_SECONDS: Final[int] = 2

# UI constants (removed Streamlit-specific constants - now using HTML/CSS)

# Model parameters - removed artificial limits for large context window models
DEFAULT_MAX_TOKENS: Final[int] = 40000  # High limit for large context models
DEFAULT_TEMPERATURE: Final[float] = 0.1
BATCH_MAX_TOKENS: Final[int] = 40000  # High limit for large context models
EXTRACTION_MAX_TOKENS: Final[int] = 40000  # High limit for question extraction

# Regex patterns
QUESTION_ID_PATTERN: Final[str] = r'(\d+\.\d+):\s*(.*?)(?=\n\n\d+\.\d+:|\Z)'
FALLBACK_QUESTION_PATTERN: Final[str] = r'(\d+\.\d+)[:\.\s]+\s*((?:.|\n)*?)(?=\s*\d+\.\d+[:\.\s]+|\Z)'

# Default prompts
DEFAULT_EXTRACTION_PROMPT: Final[str] = "If a question doesn't ask how or for details, answer in one sentence. If a question includes options, answer by selecting an option."
DEFAULT_GENERATION_PROMPT: Final[str] = """You are Claude, an AI assistant specialized in analyst relations serving as an expert on enterprise software solutions. 

Your task is to generate informative, concise, and accurate responses to analyst questions about our product capabilities.

Follow these specific guidelines:
- Focus on providing direct answers based only on the information in our product capabilities
- Use confident, professional language focusing on strengths without overselling
- Be truthful and accurate about our capabilities
- Use clear, structured responses with bullet points where appropriate
- Highlight unique differentiators but avoid marketing language
- Present answers in 2-5 paragraphs typically
- Format using Markdown for clear structure"""

# Session state keys - centralized key management
SESSION_KEYS: Final[dict[str, str]] = {
    # Application state
    "TEMP_DIR": "temp_dir",
    "EXECUTION_TIME": "execution_time",
    
    # Document information  
    "RFI_NAME": "rfi_name",
    "UPLOADED_FILE": "uploaded_file",
    
    # Question extraction
    "QUESTIONS": "questions",
    "EXTRACTION_IN_PROGRESS": "extraction_in_progress", 
    "CUSTOM_EXTRACTION_PROMPT": "custom_extraction_prompt",
    "SELECTED_EXTRACTION_MODEL": "selected_extraction_model",
    
    # Answer generation
    "GENERATED_ANSWERS": "generated_answers",
    "GENERATION_IN_PROGRESS": "generation_in_progress",
    "CUSTOM_PROMPT": "custom_prompt",
    
    # Export (simplified for web interface)
    "OUTPUT_FILE_NAME": "output_file_name",
    
    # Lakebase tracking
    "CHAT_ID": "chat_id",
    "DOC_ID": "doc_id",
    "USER_ID": "user_id",
    "QUESTIONS_LOGGED": "questions_logged",
    "ANSWERS_LOGGED": "answers_logged",

    # Document checker  
    "DOC_CHECK_AUDIT_CATEGORIES": "doc_check_audit_categories",
    "DOC_CHECK_RESULT_MD": "doc_check_result_md",
    "DOC_CHECK_IN_PROGRESS": "doc_check_in_progress",
    "DOC_CHECK_MODEL": "doc_check_model",
}

# Audit Categories Configuration
AUDIT_CATEGORIES: Final[dict[str, dict[str, str]]] = {
    "factual_accuracy": {
        "label": "Factual Accuracy",
        "description": "Verify claims, statistics, product features, and technical accuracy",
        "rules": """
- Fact-check all specific claims about products, features, and capabilities
- Verify statistics, numbers, and quantitative statements
- Check product names, version numbers, and technical specifications
- Identify unsupported or potentially misleading statements
- Cross-reference against known product documentation
- Flag statements that cannot be verified through official sources
"""
    },
    "tone": {
        "label": "Tone & Professionalism", 
        "description": "Assess tone, professionalism, and appropriateness of language",
        "rules": """
- Flag unprofessional language, slang, or overly casual tone
- Identify hostile, dismissive, or condescending language
- Check for inappropriate humor or sarcasm
- Assess whether tone matches the intended audience and context
- Flag language that could be perceived as biased or discriminatory
- Ensure respectful treatment of competitors and partners
- Verify appropriate level of formality for business communications
"""
    },
    "messaging": {
        "label": "Messaging Alignment",
        "description": "Ensure content aligns with company messaging and positioning",
        "rules": """
- Verify alignment with official product positioning and key messages
- Check consistency with company values and brand voice
- Identify messaging that conflicts with strategic direction
- Flag overpromising or absolute guarantees without qualifiers
- Ensure competitive positioning is accurate and fair
- Verify alignment with approved talking points and key differentiators
- Check for consistent use of approved terminology and product names
"""
    }
}

# Error messages
ERROR_MESSAGES: Final[dict[str, str]] = {
    "FILE_NOT_FOUND": "File not found: {file_path}",
    "UNSUPPORTED_FILE_TYPE": "Unsupported file type: {file_type}. Please upload a CSV or HTML file.",
    "NO_QUESTIONS_FOUND": "No questions were extracted from the document. Please check the file format.",
    "NO_AUTH_TOKEN": "DATABRICKS_TOKEN not set. Cannot process files.",
    "API_CALL_FAILED": "API call failed with status code {status_code}",
    "EXTRACTION_ERROR": "Error extracting questions: {error}",
    "GENERATION_ERROR": "Error generating answers: {error}",
    "NO_FILE_UPLOADED": "No file has been uploaded. Please go back to Step 1.",
    "NO_QUESTIONS_AVAILABLE": "No questions available. Please go back to extract questions first.",
    "NO_ANSWERS_AVAILABLE": "No answers available for export.",
}

# Success messages
SUCCESS_MESSAGES: Final[dict[str, str]] = {
    "FILE_UPLOADED": "File uploaded successfully to Databricks in {time:.4f} seconds",
    "QUESTIONS_EXTRACTED": "Successfully extracted {count} questions from document.",
    "ANSWERS_GENERATED": "Successfully generated answers for {count} topics.",
    "EXPORT_READY": "Export data prepared: {count} rows ready for download.",
}

# CSS class names (removed - now using standard HTML/CSS classes in static files)

# File extensions
SUPPORTED_EXTENSIONS: Final[set[str]] = {".csv", ".html", ".htm"}
EXPORT_EXTENSIONS: Final[dict[str, str]] = {
    "CSV": "csv",
    "HTML": "html",
}

# Mime types
MIME_TYPES: Final[dict[str, str]] = {
    "csv": "text/csv",
    "html": "text/html",
    "htm": "text/html",
}

# Column mappings for different data formats
COLUMN_MAPPINGS: Final[dict[str, dict[str, str]]] = {
    "hierarchical": {
        "question": "question_id",
        "topic": "topic",
        "sub_question": "sub_question_id", 
        "text": "question_text",
        "response": "answer"
    },
    "legacy": {
        "ID": "question_id",
        "Question": "question_text",
        "Response": "answer"
    }
}

# AgGrid configuration removed - no longer using Streamlit AgGrid 