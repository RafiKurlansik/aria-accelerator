"""Question extraction service for ARIA application.

This module handles extracting questions from documents using AI models
for HTML files and direct extraction for CSV files.
"""

import re
import json
from typing import Dict, List, Optional, Any, Tuple
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from aria.core.logging_config import get_logger
from aria.core.base_service import BaseService
from aria.core.exceptions import TemporaryServiceError
from aria.config import settings
from aria.config.constants import (
    DEFAULT_EXTRACTION_PROMPT, 
    DEFAULT_TIMEOUT_SECONDS,
    MAX_RETRIES,
    RETRY_WAIT_SECONDS,
    EXTRACTION_MAX_TOKENS
)

logger = get_logger(__name__)


# Removed TemporaryServiceError - now using shared TemporaryServiceError from core.exceptions


class QuestionExtractionService(BaseService):
    """Service for extracting questions from documents."""
    
    def __init__(self) -> None:
        """Initialize the question extraction service."""
        super().__init__()
        self.timeout = DEFAULT_TIMEOUT_SECONDS
    
    def extract_questions(
        self, 
        content: str, 
        extraction_method: str,
        custom_prompt: str = "",
        metadata: Optional[Dict[str, Any]] = None,
        model_name: Optional[str] = None
    ) -> Tuple[bool, List[Dict[str, Any]], Dict[str, Any]]:
        """Extract questions from content.
        
        Args:
            content: The content to extract questions from
            extraction_method: Method to use ('csv_direct' or 'ai_extraction')
            custom_prompt: Custom prompt for AI extraction
            metadata: Additional metadata about the content
            model_name: Optional model name to use for AI extraction
            
        Returns:
            Tuple of (success, questions_list, extraction_info)
        """
        # Log model configuration for operational visibility
        logger.info(f"Question extraction using model: {model_name or self.settings.question_extraction_model}")
        
        extraction_info = self._create_info_dict(extraction_method)
        extraction_info.update({
            "questions_found": 0,
            "model_used": model_name or self.settings.question_extraction_model,
            "streaming_used": False
        })
        
        try:
            import time
            start_time = time.time()
            
            if extraction_method == "csv_direct":
                success, questions = self._extract_from_csv_content(content, metadata)
            elif extraction_method == "ai_extraction":
                success, questions = self._extract_with_ai(content, custom_prompt, model_name, extraction_info)
            else:
                extraction_info["errors"].append(f"Unknown extraction method: {extraction_method}")
                return False, [], extraction_info
            
            extraction_info["processing_time"] = time.time() - start_time
            extraction_info["questions_found"] = self._count_total_questions(questions) if success else 0
            
            # Streaming debug info is populated for ai_extraction method
            
            if success:
                individual_count = self._count_total_questions(questions)
                logger.info(f"Successfully extracted {len(questions)} question structures containing {individual_count} individual questions using {extraction_method}")
            else:
                logger.warning(f"Question extraction failed using {extraction_method}")
            
            return success, questions, extraction_info
            
        except TimeoutError as e:
            # TimeoutError contains user-friendly message - capture it directly
            user_friendly_msg = str(e)
            extraction_info["errors"].append(user_friendly_msg)
            logger.error(f"Question extraction failed with timeout: {user_friendly_msg}")
            return False, [], extraction_info
        except Exception as e:
            # Handle any other unexpected errors
            user_friendly_msg = "An unexpected error occurred during question extraction. Please try with a smaller document or contact support."
            extraction_info["errors"].append(user_friendly_msg)
            logger.error(f"Question extraction failed with unexpected error: {str(e)}")
            return False, [], extraction_info
    
    def _extract_from_csv_content(
        self, 
        content: str, 
        metadata: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        """Extract questions from CSV content.
        
        Args:
            content: CSV content as text
            metadata: Metadata about the CSV file
            
        Returns:
            Tuple of (success, questions_list)
        """
        try:
            questions = []
            
            # Parse the content which should be in format "Q1: question text"
            lines = content.strip().split('\n')
            
            for i, line in enumerate(lines):
                if line.strip():
                    # Extract question text (remove "Q1:" prefix if present)
                    question_text = re.sub(r'^Q\d+:\s*', '', line.strip())
                    
                    question = {
                        "id": f"Q{i+1}",
                        "question": str(i+1),
                        "topic": "General",  # CSV files don't have topic grouping by default
                        "sub_question": f"{i+1}.1",
                        "text": question_text
                    }
                    questions.append(question)
            
            logger.info(f"Extracted {len(questions)} questions from CSV content")
            return True, questions
            
        except Exception as e:
            logger.error(f"Error extracting from CSV content: {str(e)}")
            return False, []
    
    def _extract_with_ai(
        self, 
        html_content: str, 
        custom_prompt: str = "",
        model_name: Optional[str] = None,
        extraction_info: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        """Extract questions from HTML content using AI.
        
        Args:
            html_content: HTML content to process
            custom_prompt: Custom prompt for extraction
            model_name: Optional model name to use for AI extraction
            
        Returns:
            Tuple of (success, questions_list)
        """
        try:
            # Check if we have authentication configured
            auth_success, auth_headers = self._check_authentication(extraction_info)
            if not auth_success:
                return False, []
            
            # Prepare the content for AI processing
            processed_content = self._preprocess_html(html_content)
            
            # Create the prompt
            system_prompt = self._build_extraction_prompt()
            user_prompt = self._build_user_prompt(processed_content, custom_prompt)
            
            # Make API call with improved retry logic
            success, response_text = self._call_extraction_api_with_retry(system_prompt, user_prompt, auth_headers, model_name, extraction_info)
            
            if not success:
                logger.error("AI extraction failed after retries")
                return False, []
            
            # Parse the AI response
            questions = self._parse_ai_response(response_text)
            
            if not questions:
                logger.error("AI extraction succeeded but no questions were parsed from response")
                return False, []
            
            return True, questions
            
        except TimeoutError as e:
            # Timeout with user-friendly message - propagate to user
            logger.error(f"Timeout in AI extraction: {str(e)}")
            return False, []
        except Exception as e:
            logger.error(f"Error in AI extraction: {str(e)}")
            return False, []
    
    def _preprocess_html(self, html_content: str) -> str:
        """Preprocess HTML content for AI extraction.
        
        Args:
            html_content: Raw HTML content
            
        Returns:
            Processed content suitable for AI
        """
        try:
            # Try to use BeautifulSoup if available for better preprocessing
            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Remove script and style elements
                for script in soup(["script", "style"]):
                    script.decompose()
                
                # Get text content
                text_content = soup.get_text(separator=' ', strip=True)
                
                # Clean up whitespace
                text_content = re.sub(r'\s+', ' ', text_content)
                
                logger.info(f"Preprocessed HTML: {len(html_content)} -> {len(text_content)} characters")
                return text_content
                
            except ImportError as e:
                logger.warning(f"BeautifulSoup not available - using basic preprocessing. Error: {str(e)}")
                logger.warning(f"Python path: {__import__('sys').path}")
                logger.warning(f"Current working directory: {__import__('os').getcwd()}")
            except Exception as e:
                logger.error(f"BeautifulSoup import/usage failed with unexpected error: {str(e)}")
                logger.warning("Falling back to basic preprocessing")
                # Basic preprocessing without BeautifulSoup
                # Remove script and style tags
                text = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
                text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
                
                # Remove HTML tags
                text = re.sub(r'<[^>]+>', ' ', text)
                
                # Clean up whitespace
                text = re.sub(r'\s+', ' ', text).strip()
                
                return text
                
        except Exception as e:
            logger.error(f"Error preprocessing HTML: {str(e)}")
            return html_content
    
    def _count_total_questions(self, questions: List[Dict[str, Any]]) -> int:
        """Count the total number of individual questions, including nested sub-questions.
        
        Args:
            questions: List of question structures (may contain nested sub-questions)
            
        Returns:
            Total count of individual questions
        """
        total_count = 0
        
        for question in questions:
            if isinstance(question, dict):
                # Check if this has nested sub_topics structure
                if 'sub_topics' in question:
                    # Count all sub-questions within sub_topics
                    for sub_topic in question.get('sub_topics', []):
                        if isinstance(sub_topic, dict) and 'sub_questions' in sub_topic:
                            total_count += len(sub_topic.get('sub_questions', []))
                        else:
                            total_count += 1
                else:
                    # Simple question structure
                    total_count += 1
            else:
                # Simple string question
                total_count += 1
                
        return total_count
    
    def _build_extraction_prompt(self) -> str:
        """Build the system prompt for question extraction.
        
        Returns:
            System prompt for AI
        """
        # Use configured prompt if available, otherwise fallback to hardcoded prompt
        # Get custom prompt from service config if available
        service_config = self.settings.get_service_config('question_extraction')
        configured_prompt = service_config.prompt.strip() if service_config and service_config.prompt else DEFAULT_EXTRACTION_PROMPT.strip()
        if configured_prompt:
            return configured_prompt
        
        # Fallback to existing hardcoded prompt
        return """You are an expert assistant for rewriting complex forms into structured, numbered survey questions.
Your task is to convert a set of interrelated subquestions into clearly numbered and grouped questions 
using hierarchical numbering (e.g., 1.0, 1.1) and topic-based organization.

Step 1: Thematic Categorization
Before rewriting, analyze the set of subquestions and identify logical groupings or capabilities 
(e.g., "Synthetic Data Generation", "AI-Based Recommendations"). Categorize each sub-question into 
the most appropriate group. Use context clues and key phrases to determine what the question is about. 
If a question starts discussing a new feature or capability, always start a new topic.

Step 2: Structured Rewriting
Once categorized, rewrite the questions for each capability following these rules:
1. Each capability becomes a new numbered section (e.g., Section 2: Synthetic Data Generation).
2. Number the questions in order:
   • 1: Main question, often preceded by a number 
   • 1.1, 1.2, 1.3, etc.: Follow-up questions (e.g., how it's supported, documentation, expected delivery date, etc.)
3. Disambiguate vague references: Replace pronouns like "this" or "it" with clear references to the capability.
   • ❌ "How is this supported?"
   • ✅ "How is synthetic data generation supported in your platform?"
4. Preserve the original intent and detail of each question. Only rewrite to clarify or resolve ambiguity.
5. If the question expects a specific type of answer (e.g., "Options:", "max 500 characters"), include
   that instruction at the end of the question.
6. Any answer options listed after the question should be included as part of the question.
7. OUTPUT ONLY valid JSON. No extra text, no markdown, no preambles, no explanations.
8. IMPORTANT: 
   - ONLY extract questions that actually exist in the provided document
   - If no questions are found, return an empty JSON array: []
   - NEVER invent or hallucinate questions that aren't in the original text
   - Do not use the example above as a template for content, only for format
9. Example output structure:
[
  {
    "question": "1",
    "sub_topics": [
      {
        "topic": "Synthetic Data Generation",
        "sub_questions": [
          {
            "sub_question": "1.01",
            "text": "Does your platform support synthetic data generation?"
          },
          {
            "sub_question": "1.02",
            "text": "How is synthetic data generation supported in your platform?"
          },
          {
            "sub_question": "1.03",
            "text": "Please provide any further detail on how synthetic data generation is supported. (Maximum 1000 characters)"
          }
        ]
      },
      {
        "topic": "AI-Based Recommendation",
        "sub_questions": [
          {
            "sub_question": "1.04",
            "text": "Does your platform support AI-based recommendations?"
          },
          {
            "sub_question": "1.05",
            "text": "How are AI-based recommendations supported in your platform?"
          },
          {
            "sub_question": "1.06",
            "text": "Please provide any further detail on how AI-based recommendations are supported. (Maximum 1000 characters)"
          }
        ]
      }
    ]
  },
  {
    "question": "2",
    "sub_topics": [
      {
        "topic": "Data Augmentation",
        "sub_questions": [
          {
            "sub_question": "2.01",
            "text": "Does your platform support data augmentation?"
          },
          {
            "sub_question": "2.02",
            "text": "How is data augmentation supported in your platform?"
          },
          {
            "sub_question": "2.03",
            "text": "Please provide any further detail on how data augmentation is supported. (Maximum 1000 characters)"
          }
        ]
      }
    ]
  }
]"""
    
    def _build_user_prompt(self, content: str, custom_prompt: str = "") -> str:
        """Build the user prompt for question extraction.
        
        Args:
            content: Content to extract questions from
            custom_prompt: Custom instructions
            
        Returns:
            User prompt for AI
        """
        base_prompt = f"Please extract and structure questions from the following content:\n\n{content}"
        
        if custom_prompt and custom_prompt.strip():
            base_prompt += f"\n\nAdditional instructions: {custom_prompt}"
        
        # Add emphasis on JSON format
        base_prompt += "\n\nIMPORTANT: Your response must be valid JSON only. No explanations, no markdown, just the JSON array as specified in the system prompt."
        
        return base_prompt
    
    def _call_extraction_api_with_retry(
        self, 
        system_prompt: str, 
        user_prompt: str, 
        auth_headers: Dict[str, str],
        model_name: Optional[str] = None,
        extraction_info: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, str]:
        """Call the AI API with improved retry logic for 503 errors.
        
        Args:
            system_prompt: System prompt for AI
            user_prompt: User prompt with content
            auth_headers: Authentication headers
            model_name: Optional model name to use for AI extraction
            
        Returns:
            Tuple of (success, response_text)
        """
        try:
            # First attempt with standard retry logic
            return self._call_extraction_api(system_prompt, user_prompt, auth_headers, model_name, extraction_info)
        except TemporaryServiceError:
            # For 503 errors, try with more aggressive retry
            logger.warning("Service temporarily unavailable, trying with extended retry logic")
            return self._call_extraction_api_extended_retry(system_prompt, user_prompt, auth_headers, model_name, extraction_info)
        except Exception as e:
            logger.error(f"API call failed: {str(e)}")
            return False, ""
    
    @retry(
        stop=stop_after_attempt(MAX_RETRIES), 
        wait=wait_exponential(multiplier=1, min=2, max=8),  # Reduced max wait from 10 to 8 seconds
        retry=retry_if_exception_type(TemporaryServiceError)
    )
    def _call_extraction_api(
        self, 
        system_prompt: str, 
        user_prompt: str, 
        auth_headers: Dict[str, str],
        model_name: Optional[str] = None,
        extraction_info: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, str]:
        """Call the AI API for question extraction.
        
        Args:
            system_prompt: System prompt for AI
            user_prompt: User prompt with content
            auth_headers: Authentication headers
            model_name: Optional model name to use for AI extraction
            
        Returns:
            Tuple of (success, response_text)
        """
        try:
            # Use provided model name first, then fallback to configured model
            effective_model_name = model_name or self.settings.question_extraction_model
            endpoint_url = self.settings.databricks.get_model_endpoint_url(effective_model_name)
            
            # Log model selection for monitoring
            logger.info(f"Using model: {effective_model_name}")
            
            # Remove artificial token limits - let model handle its own context window
            max_tokens = EXTRACTION_MAX_TOKENS
            content_length = len(user_prompt)
            system_prompt_length = len(system_prompt)
            
            payload = {
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "max_tokens": max_tokens,
                "temperature": 0.1,
                "stream": True  # Enable streaming to prevent 60s timeout
            }
            
            logger.info(f"📊 API PAYLOAD DEBUG:")
            logger.info(f"  Model: {effective_model_name}")
            logger.info(f"  System prompt: {system_prompt_length} chars")
            logger.info(f"  User prompt: {content_length} chars")
            logger.info(f"  Total input: {system_prompt_length + content_length} chars")
            logger.info(f"  Requested max_tokens: {max_tokens}")
            logger.info(f"  Estimated total context needed: {system_prompt_length + content_length + max_tokens} tokens")
            
            response = requests.post(
                endpoint_url,
                headers=auth_headers,
                json=payload,
                timeout=self.timeout,
                stream=True  # Enable streaming response
            )
            
            if response.status_code == 200:
                # Check if response supports streaming
                content_type = response.headers.get('content-type', '')
                is_streaming = ('text/event-stream' in content_type or 
                               'chunked' in response.headers.get('transfer-encoding', ''))
                
                # Process streaming response
                extraction_info["streaming_used"] = True
                complete_response = self._process_streaming_response(response, effective_model_name)
                
                if complete_response:
                    logger.info(f"✅ {effective_model_name} streaming extraction API call successful - {len(complete_response)} chars")
                    return True, complete_response
                else:
                    # If streaming failed, maybe it's a regular JSON response
                    logger.warning("❌ Streaming response was empty, checking if it's regular JSON...")
                    try:
                        response.encoding = 'utf-8'
                        json_response = response.json()
                        if 'choices' in json_response and len(json_response['choices']) > 0:
                            content = json_response['choices'][0].get('message', {}).get('content', '')
                            if content:
                                logger.warning(f"🚨 MODEL DOESN'T SUPPORT STREAMING: Got regular JSON response instead ({len(content)} chars)")
                                return True, content
                    except Exception as e:
                        logger.error(f"Failed to parse as JSON either: {e}")
                    
                    logger.error("❌ Failed to process response as streaming OR regular JSON")
                    return False, ""
            elif response.status_code == 403:
                response_text = response.text
                
                # Check if this is a JWT expiration error
                if ("ExpiredJwtException" in response_text or 
                    "JWT expired" in response_text or
                    "token expired" in response_text.lower()):
                    
                    logger.warning("JWT token expired, attempting to refresh authentication")
                    
                    # Get fresh authentication headers
                    fresh_auth_headers = self.settings.get_auth_headers()
                    
                    if fresh_auth_headers and fresh_auth_headers != auth_headers:
                        logger.info("Retrieved fresh authentication token, retrying API call")
                        
                        # Retry with fresh token using streaming
                        retry_response = requests.post(
                            endpoint_url,
                            headers=fresh_auth_headers,
                            json=payload,
                            timeout=self.timeout,
                            stream=True  # Enable streaming for retry
                        )
                        
                        if retry_response.status_code == 200:
                            # Process streaming response for retry
                            complete_response = self._process_streaming_response(retry_response, effective_model_name)
                            if complete_response:
                                logger.info(f"{effective_model_name} streaming extraction API call successful after token refresh")
                                return True, complete_response
                            else:
                                logger.error("Failed to process streaming response after token refresh")
                                return False, ""
                        elif retry_response.status_code == 503:
                            # Raise specific exception for 503 errors to trigger retry
                            error_msg = f"Service temporarily unavailable after token refresh: {retry_response.text}"
                            logger.warning(error_msg)
                            raise TemporaryServiceError(error_msg)
                        else:
                            logger.error(f"API call failed even after token refresh with status {retry_response.status_code}: {retry_response.text}")
                            return False, ""
                    else:
                        logger.error("Could not refresh authentication token or got same token")
                        return False, ""
                else:
                    # Non-token related 403 error
                    logger.error(f"Authentication error (403): {response_text}")
                    return False, ""
            elif response.status_code == 400:
                # Handle bad request errors - often token limit issues
                error_msg = response.text
                logger.error(f"❌ API call failed with status 400: {error_msg}")
                
                # Check for specific token limit errors
                if ("maximum tokens you requested" in error_msg.lower() and "exceeds" in error_msg.lower()):
                    user_friendly_msg = ("Model's output token limit exceeded. This is a configuration issue - the system is requesting too many output tokens. "
                                        "Try reducing the document size or contact support if this persists.")
                    logger.error(f"🚫 Output token limit exceeded: {error_msg}")
                    logger.info(f"💡 User guidance: {user_friendly_msg}")
                    return False, user_friendly_msg
                elif ("context" in error_msg.lower() and ("window" in error_msg.lower() or "length" in error_msg.lower())):
                    user_friendly_msg = ("Document content exceeds model's input context window. "
                                        "Try breaking your document into smaller sections, reducing content, or removing unnecessary text.")
                    logger.error(f"🪟 Input context window exceeded: {error_msg}")
                    logger.info(f"💡 User guidance: {user_friendly_msg}")
                    return False, user_friendly_msg
                else:
                    # Generic 400 error
                    return False, "Invalid request. Please check your document format and try again."
            elif response.status_code == 503:
                # Raise specific exception for 503 errors to trigger retry
                error_msg = f"Service temporarily unavailable: {response.text}"
                logger.warning(f"🔄 503 Service Unavailable - will retry: {error_msg}")
                raise TemporaryServiceError(error_msg)
            else:
                logger.error(f"❌ API call failed with status {response.status_code}: {response.text}")
                return False, ""
                
        except TemporaryServiceError:
            # Re-raise to trigger retry
            raise
        except Exception as e:
            error_msg = str(e)
            # Enhanced token error detection - distinguish between output and input limits
            if ("maximum tokens you requested" in error_msg.lower() and "exceeds" in error_msg.lower()):
                user_friendly_msg = ("Model's output token limit exceeded. This is a configuration issue - the system is requesting too many output tokens. "
                                    "Try reducing the document size or contact support if this persists.")
                logger.error(f"🚫 Output token limit exceeded: {error_msg}")
                logger.info(f"💡 User guidance: {user_friendly_msg}")
                raise TimeoutError(user_friendly_msg)
            elif ("context" in error_msg.lower() and ("window" in error_msg.lower() or "length" in error_msg.lower())) or \
               ("context_length_exceeded" in error_msg.lower() or "input too long" in error_msg.lower()) or \
               ("too long" in error_msg.lower() and "input" in error_msg.lower()):
                user_friendly_msg = ("Document content exceeds model's input context window. "
                                    "Try breaking your document into smaller sections, reducing content, or removing unnecessary text.")
                logger.error(f"🪟 Input context window error calling {effective_model_name} extraction API: {error_msg}")
                logger.info(f"💡 User guidance: {user_friendly_msg}")
                raise TimeoutError(user_friendly_msg)
            elif "timeout" in error_msg.lower() or "timed out" in error_msg.lower() or "upstream response timeout" in error_msg.lower():
                user_friendly_msg = "Request timed out - try extracting questions from a smaller document section or reduce the amount of content being processed."
                logger.error(f"⏰ Timeout error calling {effective_model_name} extraction API after {self.timeout}s: {error_msg}")
                logger.info(f"💡 User guidance: {user_friendly_msg}")
                raise TimeoutError(user_friendly_msg)
            else:
                logger.error(f"❌ Error calling {effective_model_name} extraction API: {error_msg}")
            return False, ""
    
    @retry(
        stop=stop_after_attempt(4),  # Reduced from 6 to 4 attempts for 503 errors
        wait=wait_exponential(multiplier=1.5, min=3, max=30)  # Reduced waits: multiplier 1.5 (was 2), max 30s (was 60s)
    )
    def _call_extraction_api_extended_retry(
        self, 
        system_prompt: str, 
        user_prompt: str, 
        auth_headers: Dict[str, str],
        model_name: Optional[str] = None,
        extraction_info: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, str]:
        """Call the AI API with extended retry for persistent 503 errors.
        
        Args:
            system_prompt: System prompt for AI
            user_prompt: User prompt with content
            auth_headers: Authentication headers
            model_name: Optional model name to use for AI extraction
            
        Returns:
            Tuple of (success, response_text)
        """
        try:
            # Use provided model name first, then fallback to YAML config  
            effective_model_name = model_name or self.settings.question_extraction_model
            endpoint_url = self.settings.databricks.get_model_endpoint_url(effective_model_name)
            
            # Debug model selection for extended retry
            logger.info(f"🔍 MODEL DEBUG (extended retry): provided='{model_name}', configured='{self.settings.question_extraction_model}', effective='{effective_model_name}'")
            
            # Use conservative max_tokens for extended retry with streaming
            system_prompt_length = len(system_prompt)
            user_prompt_length = len(user_prompt)
            
            payload = {
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "max_tokens": EXTRACTION_MAX_TOKENS,  # Use high limit instead of conservative 20k
                "temperature": 0.1,
                "stream": True  # Enable streaming for extended retry
            }
            
            logger.info(f"📊 EXTENDED RETRY PAYLOAD DEBUG:")
            logger.info(f"  Model: {effective_model_name}")
            logger.info(f"  System prompt: {system_prompt_length} chars")
            logger.info(f"  User prompt: {user_prompt_length} chars")
            logger.info(f"  Total input: {system_prompt_length + user_prompt_length} chars")
            logger.info(f"  Requested max_tokens: {EXTRACTION_MAX_TOKENS}")
            logger.info(f"  Estimated total context needed: {system_prompt_length + user_prompt_length + EXTRACTION_MAX_TOKENS} tokens")
            
            logger.info(f"Extended retry - calling {effective_model_name} for question extraction with streaming: {endpoint_url}")
            
            response = requests.post(
                endpoint_url,
                headers=auth_headers,
                json=payload,
                timeout=self.timeout,
                stream=True  # Enable streaming response
            )
            
            if response.status_code == 200:
                # Process streaming response for extended retry
                if extraction_info:
                    extraction_info["streaming_used"] = True
                complete_response = self._process_streaming_response(response, effective_model_name)
                if complete_response:
                    logger.info(f"{effective_model_name} streaming extraction API call successful (extended retry)")
                    return True, complete_response
                else:
                    logger.error("Failed to process streaming response (extended retry)")
                    return False, ""
            elif response.status_code == 400:
                # Handle bad request errors - often token limit issues (extended retry)
                error_msg = response.text
                logger.error(f"❌ API call failed with status 400 (extended retry): {error_msg}")
                
                # Check for specific token limit errors
                if ("maximum tokens you requested" in error_msg.lower() and "exceeds" in error_msg.lower()):
                    user_friendly_msg = ("Model's output token limit exceeded. This is a configuration issue - the system is requesting too many output tokens. "
                                        "Try reducing the document size or contact support if this persists.")
                    logger.error(f"🚫 Output token limit exceeded (extended retry): {error_msg}")
                    logger.info(f"💡 User guidance: {user_friendly_msg}")
                    return False, user_friendly_msg
                elif ("context" in error_msg.lower() and ("window" in error_msg.lower() or "length" in error_msg.lower())):
                    user_friendly_msg = ("Document content exceeds model's input context window. "
                                        "Try breaking your document into smaller sections, reducing content, or removing unnecessary text.")
                    logger.error(f"🪟 Input context window exceeded (extended retry): {error_msg}")
                    logger.info(f"💡 User guidance: {user_friendly_msg}")
                    return False, user_friendly_msg
                else:
                    # Generic 400 error
                    return False, "Invalid request. Please check your document format and try again."
            elif response.status_code == 503:
                # Continue retrying for 503 errors
                error_msg = f"Service still unavailable (extended retry): {response.text}"
                logger.warning(error_msg)
                raise TemporaryServiceError(error_msg)
            else:
                logger.error(f"API call failed with status {response.status_code}: {response.text}")
                return False, ""
                
        except TemporaryServiceError:
            # Re-raise to trigger retry
            raise
        except Exception as e:
            error_msg = str(e)
            # Enhanced token error detection for extended retry - distinguish between output and input limits
            if ("maximum tokens you requested" in error_msg.lower() and "exceeds" in error_msg.lower()):
                user_friendly_msg = ("Model's output token limit exceeded. This is a configuration issue - the system is requesting too many output tokens. "
                                    "Try reducing the document size or contact support if this persists.")
                logger.error(f"🚫 Output token limit exceeded (extended retry): {error_msg}")
                logger.info(f"💡 User guidance: {user_friendly_msg}")
                raise TimeoutError(user_friendly_msg)
            elif ("context" in error_msg.lower() and ("window" in error_msg.lower() or "length" in error_msg.lower())) or \
               ("context_length_exceeded" in error_msg.lower() or "input too long" in error_msg.lower()) or \
               ("too long" in error_msg.lower() and "input" in error_msg.lower()):
                user_friendly_msg = ("Document content exceeds model's input context window. "
                                    "Try breaking your document into smaller sections, reducing content, or removing unnecessary text.")
                logger.error(f"🪟 Input context window error calling {effective_model_name} extraction API (extended retry): {error_msg}")
                logger.info(f"💡 User guidance: {user_friendly_msg}")
                raise TimeoutError(user_friendly_msg)
            elif "timeout" in error_msg.lower() or "timed out" in error_msg.lower() or "upstream response timeout" in error_msg.lower():
                user_friendly_msg = "Request timed out - try extracting questions from a smaller document section or reduce the amount of content being processed."
                logger.error(f"⏰ Timeout error calling {effective_model_name} extraction API (extended retry) after {self.timeout}s: {error_msg}")
                logger.info(f"💡 User guidance: {user_friendly_msg}")
                raise TimeoutError(user_friendly_msg)
            else:
                logger.error(f"❌ Error calling {effective_model_name} extraction API (extended retry): {error_msg}")
            return False, ""
    
    def _parse_ai_response(self, response_text: str) -> List[Dict[str, Any]]:
        """Parse AI response into structured questions.
        
        Args:
            response_text: Raw response from AI
            
        Returns:
            List of structured questions
        """
        try:
            logger.info(f"Raw AI response (first 500 chars): {response_text[:500]}...")
            logger.info(f"Raw AI response length: {len(response_text)}")
            
            # Clean up the response text to extract JSON
            json_text = response_text.strip()
            
            # Remove markdown code blocks if present
            if json_text.startswith('```'):
                lines = json_text.split('\n')
                # Remove first and last lines if they're markdown
                if lines[0].startswith('```'):
                    lines = lines[1:]
                if lines and lines[-1].strip() == '```':
                    lines = lines[:-1]
                json_text = '\n'.join(lines)
            
            # Find JSON array or object pattern in the text
            json_match = re.search(r'(\[.*\]|\{.*\})', json_text, re.DOTALL)
            if json_match:
                json_text = json_match.group(1)
                logger.info(f"Extracted JSON pattern (first 200 chars): {json_text[:200]}...")
            else:
                logger.warning("No JSON pattern found in AI response")
                logger.warning(f"Full response for debugging: {response_text}")
                return []
            
            # Parse JSON
            try:
                questions_data = json.loads(json_text)
                logger.info(f"Successfully parsed JSON. Type: {type(questions_data)}")
                if isinstance(questions_data, list):
                    logger.info(f"JSON is list with {len(questions_data)} items")
                elif isinstance(questions_data, dict):
                    logger.info(f"JSON is dict with keys: {list(questions_data.keys())}")
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse JSON: {str(e)}, attempting to clean and retry")
                # Try to clean up common JSON issues
                cleaned_json = re.sub(r'\\([^"\\/bfnrtu])', r'\1', json_text)
                cleaned_json = re.sub(r'\}\s*\{', '},{', cleaned_json)
                cleaned_json = re.sub(r',\s*\]', ']', cleaned_json)
                questions_data = json.loads(cleaned_json)
                logger.info("Successfully parsed cleaned JSON")
            
            # Handle different response formats
            questions = []
            
            if isinstance(questions_data, list):
                logger.info(f"Processing list with {len(questions_data)} items")
                # Handle array of questions or complex objects
                for i, item in enumerate(questions_data):
                    logger.info(f"Processing item {i}: type={type(item)}, keys={list(item.keys()) if isinstance(item, dict) else 'N/A'}")
                    
                    if isinstance(item, str):
                        # Simple string array
                        questions.append({
                            "question": str(i + 1),
                            "topic": "General",
                            "sub_question": f"{i + 1}.1",
                            "text": item
                        })
                        logger.info(f"Added string item as question {i + 1}")
                    elif isinstance(item, dict):
                        # Pass through the complex nested structure as-is
                        # The frontend will handle flattening the sub_topics/sub_questions structure
                        if 'sub_topics' in item:
                            # Keep the original nested structure intact
                            questions.append(item)
                            logger.info(f"Added nested sub_topics structure for question {item.get('question', i + 1)}")
                        # Check if it's the section/questions structure
                        elif 'section' in item and 'questions' in item:
                            section_name = item.get('section', 'General')
                            section_questions = item.get('questions', [])
                            for j, q in enumerate(section_questions):
                                if isinstance(q, dict):
                                    questions.append({
                                        "question": str(i + 1),
                                        "topic": section_name,
                                        "sub_question": q.get('number', f"{i + 1}.{j + 1}"),
                                        "text": q.get('text', q.get('question', ''))
                                    })
                                elif isinstance(q, str):
                                    questions.append({
                                        "question": str(i + 1),
                                        "topic": section_name,
                                        "sub_question": f"{i + 1}.{j + 1}",
                                        "text": q
                                    })
                        else:
                            # Simple object structure
                            questions.append({
                                "question": item.get("question", str(i + 1)),
                                "topic": item.get("topic", "General"),
                                "sub_question": item.get("sub_question", f"{i + 1}.1"),
                                "text": item.get("text", item.get("question", ""))
                            })
                            
            elif isinstance(questions_data, dict):
                # Handle object with questions array
                if 'questions' in questions_data:
                    question_list = questions_data['questions']
                    for i, q in enumerate(question_list):
                        if isinstance(q, str):
                            questions.append({
                                "question": "1",
                                "topic": "General",
                                "sub_question": f"1.{i + 1}",
                                "text": q
                            })
                        elif isinstance(q, dict):
                            questions.append({
                                "question": q.get("question", "1"),
                                "topic": q.get("topic", "General"),
                                "sub_question": q.get("sub_question", f"1.{i + 1}"),
                                "text": q.get("text", q.get("question", ""))
                            })
                else:
                    # Single question object
                    questions.append({
                        "question": questions_data.get("question", "1"),
                        "topic": questions_data.get("topic", "General"),
                        "sub_question": questions_data.get("sub_question", "1.1"),
                        "text": questions_data.get("text", questions_data.get("question", ""))
                    })
            
            logger.info(f"Successfully parsed {len(questions)} questions from AI response")
            logger.info(f"Final questions structure: {questions[:2] if questions else 'Empty list'}")  # Log first 2 items
            return questions
                
        except Exception as e:
            logger.error(f"Error parsing AI response: {str(e)}")
            logger.error(f"Raw response that caused error: {response_text[:500]}...")
            
            # Try emergency fallback parsing for simple text responses
            logger.info("Attempting emergency fallback parsing...")
            try:
                fallback_questions = self._emergency_fallback_parse(response_text)
                if fallback_questions:
                    logger.info(f"Emergency fallback parsed {len(fallback_questions)} questions")
                    return fallback_questions
            except Exception as fallback_error:
                logger.error(f"Emergency fallback also failed: {fallback_error}")
            
            return []
    
    def _emergency_fallback_parse(self, response_text: str) -> List[Dict[str, Any]]:
        """Emergency fallback parsing for non-JSON AI responses.
        
        Args:
            response_text: Raw AI response text
            
        Returns:
            List of questions extracted from plain text
        """
        questions = []
        
        # Try to extract questions from numbered lists or bullet points
        patterns = [
            r'(?:^|\n)\s*(\d+)[\.\)]\s+(.*?)(?=\n\s*\d+[\.\)]|\n\s*$|\Z)',  # 1. Question or 1) Question
            r'(?:^|\n)\s*[-\*]\s+(.*?)(?=\n\s*[-\*]|\n\s*$|\Z)',  # - Question or * Question
            r'(?:^|\n)\s*Q\d*[:\.]?\s+(.*?)(?=\n\s*Q\d*[:\.]|\n\s*$|\Z)',  # Q1: Question or Q. Question
        ]
        
        for i, pattern in enumerate(patterns):
            matches = re.findall(pattern, response_text, re.MULTILINE | re.DOTALL)
            if matches:
                logger.info(f"Emergency fallback pattern {i+1} found {len(matches)} matches")
                for j, match in enumerate(matches):
                    if isinstance(match, tuple):
                        question_text = match[1].strip() if len(match) > 1 else match[0].strip()
                    else:
                        question_text = match.strip()
                    
                    if question_text and len(question_text) > 10:  # Filter out very short matches
                        questions.append({
                            "question": "1",
                            "topic": "Emergency Parsed",
                            "sub_question": f"1.{j+1}",
                            "text": question_text
                        })
                
                if questions:
                    break  # Use first successful pattern
        
        return questions
    
    def _process_streaming_response(self, response, model_name: str) -> str:
        """Process streaming Server-Sent Events response from the model API.
        
        Args:
            response: Streaming HTTP response object
            model_name: Name of the model for logging
            
        Returns:
            Complete response content as string
        """
        complete_content = ""
        
        try:
            logger.info(f"🌊 STREAMING PROOF: Starting to process streaming response from {model_name}")
            line_count = 0
            
            # Process each line of the streaming response
            for line in response.iter_lines(decode_unicode=True):
                line_count += 1
                if not line:
                    continue
                    
                # Skip lines that don't start with "data: "
                if not line.startswith("data: "):
                    logger.debug(f"🌊 STREAMING PROOF: Skipping non-data line {line_count}: {line[:50]}...")
                    continue
                    
                # Extract the JSON data
                data_str = line[6:]  # Remove "data: " prefix
                logger.debug(f"🌊 STREAMING PROOF: Processing data chunk {line_count}: {data_str[:50]}...")
                
                # Skip the final [DONE] marker
                if data_str.strip() == "[DONE]":
                    logger.debug("Reached end of stream [DONE]")
                    break
                    
                try:
                    # Parse the JSON chunk
                    chunk_data = json.loads(data_str)
                    
                    # Extract content from the chunk
                    if "choices" in chunk_data and len(chunk_data["choices"]) > 0:
                        choice = chunk_data["choices"][0]
                        if "delta" in choice and "content" in choice["delta"]:
                            content_chunk = choice["delta"]["content"]
                            complete_content += content_chunk
                            
                            # Log progress every 500 characters to show streaming is working
                            if len(complete_content) % 500 == 0:
                                logger.info(f"🌊 STREAMING PROOF: Progress {len(complete_content)} chars from {line_count} chunks")
                                
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse streaming chunk: {data_str[:50]}... - {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error processing streaming response from {model_name}: {e}")
            return ""
        
        logger.info(f"🌊 STREAMING PROOF: Complete! {model_name} streamed {len(complete_content)} chars in {line_count} chunks")
        
        # DEFINITIVE PROOF: If this is NOT streaming, line_count would be 1 (single response)
        # Log streaming confirmation based on chunk count
        # If this IS streaming, line_count will be > 1 (multiple chunks)
        
        if line_count > 1:
            logger.info(f"✅ STREAMING CONFIRMED: Received {line_count} chunks (streaming working)")
        else:
            logger.warning(f"❌ STREAMING FAILED: Only received {line_count} chunk (not streaming)")
            
        return complete_content

 