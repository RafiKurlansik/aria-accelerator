"""Document checker service for ARIA application.

This module provides general-purpose document checking, including:
- Fact checking claims about Databricks
- Optional rules/guidelines compliance checking

Outputs Markdown with sections for Summary, Fact Checks, Rule Compliance, and Suggested Rewrites.
"""

from __future__ import annotations

from typing import Dict, Optional, Tuple, List, Any
import requests
import json
from datetime import datetime

from aria.core.logging_config import get_logger
from aria.core.base_service import BaseService
from aria.config import settings
from aria.config.constants import DEFAULT_TIMEOUT_SECONDS


# Logger provided by BaseService as self.logger


class DocumentCheckerService(BaseService):
    """Service for document auditing using the specialized audit agent.

    Uses a Databricks-hosted audit agent defined in backend_services.yaml.
    The audit agent expects input with doc_id, doc_name, and text, and returns
    structured audit results in JSON format.
    """

    def __init__(self) -> None:
        """Initialize the document checker service with configuration."""
        super().__init__()
        self.timeout = DEFAULT_TIMEOUT_SECONDS

    def check_document(
        self,
        content: str,
        doc_id: Optional[str] = None,
        doc_name: Optional[str] = None,
        include_rules_markdown: Optional[str] = None,
        only_fact_check: bool = True,
        audit_categories: Optional[list[str]] = None,
        model_name: Optional[str] = None,
    ) -> Tuple[bool, List[Dict[str, Any]], Dict[str, str]]:
        """Run document checking and return audit results.

        Args:
            content: The raw text to analyze
            doc_id: Stable identifier for the document
            doc_name: Human-readable name for the document
            include_rules_markdown: Optional Markdown with rules/guidelines (legacy)
            only_fact_check: If True, ignore provided rules (legacy)
            audit_categories: List of audit category IDs to apply (legacy)
            model_name: Optional override for the model to use

        Returns:
            Tuple of (success, audit_results_list, info)
        """
        try:
            auth_headers = self.settings.get_auth_headers()
            if not auth_headers:
                self.logger.error("No authentication configured - cannot run document checker")
                return False, [], {"error": "Authentication not configured"}

            # Build the new simplified input for the audit agent
            user_message = self._build_audit_input(content, doc_id, doc_name)

            success, audit_results = self._call_audit_model(user_message, auth_headers, model_name)
            return success, (audit_results or []), {"model_used": model_name or self.settings.document_checker_model}

        except Exception as exc:
            self.logger.error(f"Error in document checking: {exc}")
            return False, [], {"error": str(exc)}

    def _build_audit_input(self, content: str, doc_id: Optional[str] = None, doc_name: Optional[str] = None) -> str:
        """Build the audit input message according to the new schema.
        
        Args:
            content: The document text to audit
            doc_id: Stable identifier for the document
            doc_name: Human-readable name for the document
            
        Returns:
            Formatted input message for the audit agent
        """
        # Generate default values if not provided
        if not doc_id:
            doc_id = f"doc_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        if not doc_name:
            doc_name = "Uploaded Document"
            
        # Create the input according to the new schema
        input_data = {
            "doc_id": doc_id,
            "doc_name": doc_name,
            "text": content
        }
        
        # Format the input message with instructions for JSON output
        message_parts = [
            f"doc_id: {input_data['doc_id']}",
            f"doc_name: {input_data['doc_name']}",
            f"text: {input_data['text']}",
            "Do not include any markdown, explanations, or other text - return only the JSON object."
        ]
        
        return "\n".join(message_parts)



    def _call_audit_model(
        self,
        user_message: str,
        auth_headers: Dict[str, str],
        model_name: Optional[str] = None,
    ) -> Tuple[bool, Optional[List[Dict[str, Any]]]]:
        """Call the audit agent and return structured results.
        
        Args:
            user_message: The formatted input message for the audit agent
            auth_headers: Authentication headers
            model_name: Optional override for the model to use
            
        Returns:
            Tuple of (success, audit_results_list)
        """
        model = model_name or self.settings.document_checker_model
        endpoint_url = self.settings.databricks.get_model_endpoint_url(model)

        # For the new audit agent, we only send a user message (no system prompt needed)
        payload = {
            "messages": [
                {"role": "user", "content": user_message},
            ],
            "max_tokens": 15000,
            "temperature": 0.1,
        }

        self.logger.info(f"Calling audit model {model} with payload:")
        self.logger.info(json.dumps(payload, indent=2))

        response = requests.post(
            endpoint_url,
            headers=auth_headers,
            json=payload,
            timeout=self.timeout,
        )

        if response.status_code == 200:
            data = response.json()
            self.logger.info("Audit model response:")
            self.logger.info(json.dumps(data, indent=2))
            
            if "choices" in data and data["choices"]:
                choice = data["choices"][0]
                content = None
                if "message" in choice and "content" in choice["message"]:
                    content = choice["message"]["content"]
                elif "text" in choice:
                    content = choice["text"]
                
                if content:
                    try:
                        # First try to parse as direct JSON
                        result_data = json.loads(content)
                        if isinstance(result_data, dict) and "results" in result_data:
                            audit_results = result_data["results"]
                            self.logger.info(f"Successfully parsed {len(audit_results)} audit results")
                            return True, audit_results
                        else:
                            self.logger.error(f"Unexpected response format: {content}")
                            return False, None
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"Failed to parse as direct JSON: {e}")
                        self.logger.info("Attempting to extract JSON from response content...")
                        
                        # Try to extract JSON from markdown or mixed content
                        json_match = self._extract_json_from_content(content)
                        if json_match:
                            try:
                                result_data = json.loads(json_match)
                                if isinstance(result_data, dict) and "results" in result_data:
                                    audit_results = result_data["results"]
                                    self.logger.info(f"Successfully extracted and parsed {len(audit_results)} audit results")
                                    return True, audit_results
                            except json.JSONDecodeError as nested_e:
                                self.logger.error(f"Failed to parse extracted JSON: {nested_e}")
                        
                        # If JSON parsing fails completely, log the issue and return failure
                        self.logger.error(f"Could not extract valid JSON from response")
                        self.logger.error(f"Raw content preview: {content[:500]}...")
                        return False, None
                else:
                    self.logger.error("No content in model response")
                    return False, None
            else:
                self.logger.error("No choices in response")
                return False, None
        elif response.status_code == 403:
            self.logger.error(f"Authentication/authorization error calling model {model}: {response.text}")
            return False, None
        else:
            self.logger.error(f"API call failed with status {response.status_code}: {response.text}")
            return False, None

    def _extract_json_from_content(self, content: str) -> Optional[str]:
        """Extract JSON content from mixed text/markdown response.
        
        Args:
            content: Raw response content that may contain JSON
            
        Returns:
            Extracted JSON string or None if not found
        """
        import re
        
        # Try to find JSON in code blocks
        code_block_pattern = r'```(?:json)?\s*(\{.*?\})\s*```'
        match = re.search(code_block_pattern, content, re.DOTALL | re.IGNORECASE)
        if match:
            self.logger.info("Found JSON in code block")
            return match.group(1)
        
        # Try to find JSON object that starts with { and has "results"
        json_pattern = r'\{\s*"results"\s*:\s*\[.*?\]\s*\}'
        match = re.search(json_pattern, content, re.DOTALL)
        if match:
            self.logger.info("Found JSON object with results pattern")
            return match.group(0)
        
        # Try to find any JSON object in the content
        json_pattern = r'\{(?:[^{}]|{[^{}]*})*\}'
        matches = re.findall(json_pattern, content, re.DOTALL)
        for match in matches:
            try:
                # Test if it's valid JSON
                test_data = json.loads(match)
                if isinstance(test_data, dict) and "results" in test_data:
                    self.logger.info("Found valid JSON object with results")
                    return match
            except json.JSONDecodeError:
                continue
        
        self.logger.warning("No valid JSON found in content")
        return None


