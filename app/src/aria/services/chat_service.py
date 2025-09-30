"""Chat service for ARIA application.

This module handles chat conversations using AI models for general Q&A
and assistance with document analysis and Databricks platform questions.
"""

import json
import requests
from typing import Dict, List, Optional, Any, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from aria.core.logging_config import get_logger
from aria.core.base_service import BaseService
from aria.core.error_handler import error_handler
from aria.core.exceptions import TemporaryServiceError
from aria.config import settings
from aria.config.constants import DEFAULT_TIMEOUT_SECONDS, MAX_RETRIES

logger = get_logger(__name__)


# Removed TemporaryServiceError - now using shared TemporaryServiceError from core.exceptions


class ChatService(BaseService):
    """Service for handling chat conversations with AI models."""
    
    def __init__(self) -> None:
        """Initialize the chat service."""
        super().__init__()
        self.timeout = DEFAULT_TIMEOUT_SECONDS

    def chat(
        self, 
        question: str, 
        context: Optional[List[Dict[str, Any]]] = None,
        model_name: Optional[str] = None
    ) -> Tuple[bool, str, List[str], Dict[str, Any]]:
        """Handle a chat conversation.
        
        Args:
            question: User's question
            context: Previous conversation context (optional)
            model_name: Optional model name override
            
        Returns:
            Tuple of (success, answer, sources, chat_info)
        """
        chat_info = {
            "question": question,
            "model_used": model_name or self._get_chat_model(),
            "context_length": len(context) if context else 0,
            "sources_found": 0
        }
        
        try:
            # Get authentication headers
            auth_headers = self.settings.get_auth_headers()
            if not auth_headers:
                error_info = error_handler._handle_authentication_error("No auth headers available", "Chat")
                logger.error(error_handler.format_admin_error(error_info))
                user_message = error_handler.format_user_error(error_info)
                chat_info["error"] = error_info.admin_message
                return False, user_message, [], chat_info
            
            # Build the user prompt (no system prompt needed for agents)
            user_prompt = self._build_user_prompt(question, context)
            
            # Call the AI model (with retries)
            try:
                success, response_data = self._call_chat_api(
                    user_prompt, auth_headers, model_name
                )
                
                if not success:
                    # The _call_chat_api method now returns structured error info in response_data
                    chat_info["error"] = response_data
                    return False, response_data, [], chat_info
            except TemporaryServiceError as e:
                # All retries exhausted - format as service unavailable
                error_info = error_handler.handle_api_error(503, str(e), "Chat", "conversation")
                logger.error(error_handler.format_admin_error(error_info))
                user_message = error_handler.format_user_error(error_info)
                chat_info["error"] = error_info.admin_message
                return False, user_message, [], chat_info
            
            # Parse the response and extract answer text and references
            answer_text, sources = self._parse_chat_response(response_data)
            chat_info["sources_found"] = len(sources)
            
            logger.info(f"Chat response generated successfully for question: {question[:50]}...")
            return True, answer_text, sources, chat_info
            
        except Exception as e:
            logger.error(f"Error in chat service: {str(e)}")
            chat_info["error"] = str(e)
            user_message = f"❓ Chat service encountered an unexpected error. Please try again or contact support.\n\n💡 **What you can do:**\n1. Try again in a few moments\n2. Contact support with the error details\n3. Check if the issue affects other users"
            return False, user_message, [], chat_info

    def _get_chat_model(self) -> str:
        """Get the model to use for chat from configuration."""
        return self.settings.chat_model



    def _build_user_prompt(self, question: str, context: Optional[List[Dict[str, Any]]] = None) -> str:
        """Build the user prompt including question and context.
        
        Args:
            question: Current user question
            context: Previous conversation context
            
        Returns:
            Formatted user prompt
        """
        prompt_parts = []
        
        # Add conversation context if available
        if context and len(context) > 0:
            prompt_parts.append("**Previous Conversation:**")
            for msg in context[-5:]:  # Last 5 messages for context
                sender = msg.get('sender', 'unknown')
                content = msg.get('content', '')
                if sender == 'user':
                    prompt_parts.append(f"User: {content}")
                elif sender == 'bot':
                    prompt_parts.append(f"Assistant: {content}")
            prompt_parts.append("")  # Empty line
        
        # Add current question
        prompt_parts.append("**Current Question:**")
        prompt_parts.append(question)
        
        return "\n".join(prompt_parts)

    @retry(
        stop=stop_after_attempt(MAX_RETRIES), 
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(TemporaryServiceError)
    )
    def _call_chat_api(
        self,
        user_prompt: str,
        auth_headers: Dict[str, str],
        model_name: Optional[str] = None
    ) -> Tuple[bool, str]:
        """Call the AI API for chat response.
        
        Args:
            user_prompt: User prompt with question and context
            auth_headers: Authentication headers
            model_name: Optional model name override
            
        Returns:
            Tuple of (success, response_content)
        """
        try:
            # Use configured model or fallback
            effective_model_name = model_name or self._get_chat_model()
            endpoint_url = self.settings.databricks.get_model_endpoint_url(effective_model_name)
            
            payload = {
                "messages": [
                    {"role": "user", "content": user_prompt}
                ],
                "max_tokens": 15000,
                "temperature": 0.3  # Slightly more creative than other services
            }
            
            logger.info(f"Calling {effective_model_name} for chat: {endpoint_url}")
            
            response = requests.post(
                endpoint_url,
                headers=auth_headers,
                json=payload,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                result = response.json()
                
                # Extract the response content
                if 'choices' in result and len(result['choices']) > 0:
                    content = result['choices'][0]['message']['content']
                    return True, content.strip()
                else:
                    logger.error(f"Unexpected response format: {result}")
                    return False, "Unexpected response format from AI model"
            
            else:
                error_text = response.text
                
                # Use centralized error handler for consistent user messages
                error_info = error_handler.handle_api_error(
                    response.status_code, 
                    error_text, 
                    "Chat", 
                    "conversation"
                )
                
                logger.error(error_handler.format_admin_error(error_info))
                user_message = error_handler.format_user_error(error_info)
                
                # Check if this should be retried
                if error_info.is_retryable:
                    # For retryable errors, raise TemporaryServiceError to trigger retry
                    if response.status_code in [500, 502, 503, 504]:
                        raise TemporaryServiceError(f"Server temporarily unavailable: {error_text}")
                    elif (response.status_code == 400 and 
                          ("Search has too many filter clauses" in error_text or 
                           "long query text" in error_text)):
                        raise TemporaryServiceError(f"Vector search temporarily unavailable: {error_text}")
                    else:
                        # Other retryable errors
                        raise TemporaryServiceError(user_message)
                else:
                    # Non-retryable error - return user-friendly message
                    return False, user_message
                
        except TemporaryServiceError:
            # Re-raise to trigger retry
            raise
        except requests.exceptions.Timeout as e:
            # Handle timeout errors with user-friendly messages
            error_info = error_handler.handle_network_error(e, "Chat")
            logger.error(error_handler.format_admin_error(error_info))
            if error_info.is_retryable:
                raise TemporaryServiceError(error_handler.format_user_error(error_info))
            else:
                return False, error_handler.format_user_error(error_info)
        except requests.exceptions.RequestException as e:
            # Handle network errors with user-friendly messages
            error_info = error_handler.handle_network_error(e, "Chat")
            logger.error(error_handler.format_admin_error(error_info))
            if error_info.is_retryable:
                raise TemporaryServiceError(error_handler.format_user_error(error_info))
            else:
                return False, error_handler.format_user_error(error_info)
        except Exception as e:
            # Handle unexpected errors
            logger.error(f"Unexpected error in chat API call: {str(e)}")
            user_message = f"❓ Chat encountered an unexpected error. Please try again or contact support.\n\n💡 **What you can do:**\n1. Try again in a few moments\n2. Contact support with the error details\n3. Check if the issue affects other users"
            return False, user_message

    def _extract_sources(self, response_text: str) -> List[str]:
        """Extract potential sources from the response text.
        
        This is a simple implementation that looks for common source patterns.
        
        Args:
            response_text: The AI response text
            
        Returns:
            List of extracted sources
        """
        sources = []
        
        # Common Databricks documentation sources
        databricks_sources = [
            "docs.databricks.com",
            "databricks.com/platform",
            "databricks.com/pricing",
            "databricks.com/product/delta-lake",
            "databricks.com/product/machine-learning",
            "mlflow.org",
            "delta.io"
        ]
        
        # Check if response mentions Databricks concepts
        response_lower = response_text.lower()
        
        if any(term in response_lower for term in ['databricks', 'lakehouse', 'delta lake']):
            sources.append("docs.databricks.com")
        
        if any(term in response_lower for term in ['machine learning', 'mlflow', 'mlops']):
            sources.append("docs.databricks.com/machine-learning")
        
        if any(term in response_lower for term in ['spark', 'apache spark']):
            sources.append("spark.apache.org")
        
        if any(term in response_lower for term in ['pricing', 'cost', 'dbu']):
            sources.append("databricks.com/pricing")
        
        # Remove duplicates and return
        return list(set(sources))
    
    def _parse_chat_response(self, response_content: str) -> Tuple[str, List[str]]:
        """Parse chat response content, handling both JSON and plain text responses.
        
        If the response is JSON (from answer generation model), extract answer and references.
        If the response is plain text, use keyword-based source extraction.
        
        Args:
            response_content: Raw response content from the model
            
        Returns:
            Tuple of (answer_text, sources_list)
        """
        # First, try to parse as JSON (answer generation model response format)
        try:
            response_content_clean = response_content.strip()
            
            # Remove markdown code blocks if present
            if response_content_clean.startswith('```'):
                lines = response_content_clean.split('\n')
                if lines[0].startswith('```'):
                    lines = lines[1:]
                if lines and lines[-1].strip() == '```':
                    lines = lines[:-1]
                response_content_clean = '\n'.join(lines)
            
            # Try to parse as JSON
            response_data = json.loads(response_content_clean)
            
            # Check if it has the answer generation format
            if isinstance(response_data, dict) and 'answers' in response_data:
                logger.info("🎯 [CHAT] Detected JSON response with answers structure")
                
                # Extract the first answer (chat is typically one question)
                answers = response_data['answers']
                if answers and len(answers) > 0:
                    first_answer = answers[0]
                    answer_text = first_answer.get('answer', '').strip()
                    references = first_answer.get('references', [])
                    
                    # Clean up references - ensure they're strings
                    clean_references = []
                    for ref in references:
                        if isinstance(ref, str) and ref.strip():
                            clean_references.append(ref.strip())
                    
                    # Concatenate references to the answer text
                    if clean_references:
                        references_text = "\n\n📚 **References:**\n" + "\n".join([
                            f"[{i+1}] {ref}" for i, ref in enumerate(clean_references)
                        ])
                        answer_text += references_text
                    
                    logger.info(f"📊 [CHAT] Extracted answer ({len(answer_text)} chars) with {len(clean_references)} references concatenated")
                    return answer_text, []  # Return empty sources since they're now in the answer
                else:
                    logger.warning("⚠️ [CHAT] JSON response has no answers")
                    return response_content, []
            
            # If JSON but not answer format, treat as plain text
            else:
                logger.info("📄 [CHAT] JSON response but not answer format, treating as plain text")
                sources = self._extract_sources(response_content)
                return response_content, sources
                
        except json.JSONDecodeError:
            # Not JSON, treat as plain text
            logger.info("📄 [CHAT] Plain text response, using keyword-based source extraction")
            sources = self._extract_sources(response_content)
            return response_content, sources
        
        except Exception as e:
            logger.warning(f"⚠️ [CHAT] Error parsing response: {e}, falling back to plain text")
            sources = self._extract_sources(response_content)
            return response_content, sources
