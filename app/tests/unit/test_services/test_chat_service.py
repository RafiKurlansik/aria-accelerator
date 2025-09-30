"""Unit tests for ChatService.

Tests cover chat functionality, error handling, and various scenarios.
"""

from __future__ import annotations

import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from typing import TYPE_CHECKING

from aria.services.chat_service import ChatService
from aria.config import settings

if TYPE_CHECKING:
    from _pytest.capture import CaptureFixture
    from _pytest.fixtures import FixtureRequest
    from _pytest.logging import LogCaptureFixture
    from _pytest.monkeypatch import MonkeyPatch
    from pytest_mock.plugin import MockerFixture


class TestChatService:
    """Test cases for ChatService."""

    @pytest.fixture
    def chat_service(self) -> ChatService:
        """Chat service fixture."""
        return ChatService()

    @pytest.fixture
    def mock_settings(self, mocker: "MockerFixture") -> Mock:
        """Mock settings for testing."""
        mock_settings = Mock()
        mock_databricks = Mock()
        mock_databricks.warehouse_id = "test_warehouse"

        # Mock the get_model_endpoint_url method
        def mock_get_endpoint_url(model_name: str) -> str:
            return f"https://example.databricks/serving-endpoints/{model_name}/invocations"

        mock_databricks.get_model_endpoint_url = mock_get_endpoint_url
        mock_settings.databricks = mock_databricks
        mock_settings.get_auth_headers.return_value = {"Authorization": "Bearer test-token"}
        mock_settings.chat_model = "test-chat-model"

        mocker.patch('aria.services.chat_service.settings', mock_settings)
        return mock_settings

    def test_init(self, chat_service: ChatService) -> None:
        """Test ChatService initialization."""
        assert chat_service.timeout == 300  # DEFAULT_TIMEOUT_SECONDS
        assert hasattr(chat_service, 'logger')

    def test_chat_success(self, chat_service: ChatService, mock_settings: Mock, mocker: "MockerFixture") -> None:
        """Test successful chat interaction."""
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": "This is a helpful response to your question."
                    }
                }
            ]
        }

        mock_requests_post = mocker.patch('aria.services.chat_service.requests.post', return_value=mock_response)

        question = "What is machine learning?"
        success, answer, sources, chat_info = chat_service.chat(question)

        assert success is True
        assert "helpful response" in answer
        assert isinstance(sources, list)
        assert chat_info["question"] == question
        assert chat_info["model_used"] == "test-chat-model"

        # Verify the API call was made correctly
        mock_requests_post.assert_called_once()
        call_args = mock_requests_post.call_args
        assert call_args[1]["json"]["messages"][0]["content"] == question

    def test_chat_with_context(self, chat_service: ChatService, mock_settings: Mock, mocker: "MockerFixture") -> None:
        """Test chat with conversation context."""
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": "Based on our previous conversation, here's the answer."
                    }
                }
            ]
        }

        mock_requests_post = mocker.patch('aria.services.chat_service.requests.post', return_value=mock_response)

        question = "Tell me more about that topic"
        context = [
            {"role": "user", "content": "What is AI?"},
            {"role": "assistant", "content": "AI stands for Artificial Intelligence."}
        ]

        success, answer, sources, chat_info = chat_service.chat(question, context)

        assert success is True
        assert "previous conversation" in answer

        # Verify context was included in the request
        call_args = mock_requests_post.call_args
        messages = call_args[1]["json"]["messages"]
        assert len(messages) == 3  # context + new question
        assert messages[-1]["content"] == question

    def test_chat_with_model_override(self, chat_service: ChatService, mock_settings: Mock, mocker: "MockerFixture") -> None:
        """Test chat with custom model override."""
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": "Response from custom model."
                    }
                }
            ]
        }

        mock_requests_post = mocker.patch('aria.services.chat_service.requests.post', return_value=mock_response)

        question = "Test question"
        custom_model = "custom-chat-model"

        success, answer, sources, chat_info = chat_service.chat(question, model_name=custom_model)

        assert success is True
        assert chat_info["model_used"] == custom_model

        # Verify the custom model endpoint was used
        call_args = mock_requests_post.call_args
        expected_url = f"https://example.databricks/serving-endpoints/{custom_model}/invocations"
        assert call_args[0][0] == expected_url

    def test_chat_missing_question(self, chat_service: ChatService) -> None:
        """Test chat with missing or empty question."""
        success, answer, sources, chat_info = chat_service.chat("")

        assert success is False
        assert "Question is required" in answer
        assert sources == []
        assert "error" in chat_info

    def test_chat_authentication_failure(self, chat_service: ChatService, mock_settings: Mock, mocker: "MockerFixture") -> None:
        """Test chat when authentication fails."""
        # Mock failed authentication
        mock_settings.get_auth_headers.return_value = None

        question = "What is AI?"
        success, answer, sources, chat_info = chat_service.chat(question)

        assert success is False
        assert "Authentication not configured" in answer
        assert sources == []
        assert "error" in chat_info

    def test_chat_api_error_403(self, chat_service: ChatService, mock_settings: Mock, mocker: "MockerFixture") -> None:
        """Test chat with API authentication error."""
        # Mock 403 Forbidden response
        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.text = "Authentication failed"

        mocker.patch('aria.services.chat_service.requests.post', return_value=mock_response)

        question = "What is AI?"
        success, answer, sources, chat_info = chat_service.chat(question)

        assert success is False
        assert "Authentication/authorization error" in answer
        assert sources == []
        assert "error" in chat_info

    def test_chat_api_error_500(self, chat_service: ChatService, mock_settings: Mock, mocker: "MockerFixture") -> None:
        """Test chat with API server error."""
        # Mock 500 Internal Server Error response
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal server error"

        mocker.patch('aria.services.chat_service.requests.post', return_value=mock_response)

        question = "What is AI?"
        success, answer, sources, chat_info = chat_service.chat(question)

        assert success is False
        assert "API call failed with status 500" in answer
        assert sources == []
        assert "error" in chat_info

    def test_chat_network_error(self, chat_service: ChatService, mock_settings: Mock, mocker: "MockerFixture") -> None:
        """Test chat with network connectivity issues."""
        # Mock network error
        mocker.patch('aria.services.chat_service.requests.post', side_effect=Exception("Connection timeout"))

        question = "What is AI?"
        success, answer, sources, chat_info = chat_service.chat(question)

        assert success is False
        assert "Connection timeout" in answer
        assert sources == []
        assert "error" in chat_info

    def test_chat_malformed_response(self, chat_service: ChatService, mock_settings: Mock, mocker: "MockerFixture") -> None:
        """Test chat with malformed API response."""
        # Mock response with missing expected fields
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"unexpected": "format"}

        mocker.patch('aria.services.chat_service.requests.post', return_value=mock_response)

        question = "What is AI?"
        success, answer, sources, chat_info = chat_service.chat(question)

        assert success is False
        assert "No choices in response" in answer
        assert sources == []
        assert "error" in chat_info

    def test_chat_empty_choices(self, chat_service: ChatService, mock_settings: Mock, mocker: "MockerFixture") -> None:
        """Test chat with empty choices in response."""
        # Mock response with empty choices
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"choices": []}

        mocker.patch('aria.services.chat_service.requests.post', return_value=mock_response)

        question = "What is AI?"
        success, answer, sources, chat_info = chat_service.chat(question)

        assert success is False
        assert "No choices in response" in answer
        assert sources == []
        assert "error" in chat_info

    def test_chat_missing_content(self, chat_service: ChatService, mock_settings: Mock, mocker: "MockerFixture") -> None:
        """Test chat with missing content in response."""
        # Mock response with choice but no content
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "role": "assistant"
                        # Missing "content" field
                    }
                }
            ]
        }

        mocker.patch('aria.services.chat_service.requests.post', return_value=mock_response)

        question = "What is AI?"
        success, answer, sources, chat_info = chat_service.chat(question)

        assert success is False
        assert "No content in model response" in answer
        assert sources == []
        assert "error" in chat_info

    def test_chat_with_retry_success(self, chat_service: ChatService, mock_settings: Mock, mocker: "MockerFixture") -> None:
        """Test chat with successful retry after initial failure."""
        # Mock sequence: failure, then success
        mock_response_failure = Mock()
        mock_response_failure.status_code = 500
        mock_response_failure.text = "Temporary server error"

        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": "Response after retry."
                    }
                }
            ]
        }

        mock_requests_post = mocker.patch('aria.services.chat_service.requests.post')
        mock_requests_post.side_effect = [mock_response_failure, mock_response_success]

        question = "What is AI?"
        success, answer, sources, chat_info = chat_service.chat(question)

        assert success is True
        assert "Response after retry" in answer

        # Verify retry occurred
        assert mock_requests_post.call_count == 2

    def test_chat_max_retries_exceeded(self, chat_service: ChatService, mock_settings: Mock, mocker: "MockerFixture") -> None:
        """Test chat when max retries are exceeded."""
        # Mock persistent failure
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Persistent server error"

        mocker.patch('aria.services.chat_service.requests.post', return_value=mock_response)

        question = "What is AI?"
        success, answer, sources, chat_info = chat_service.chat(question)

        assert success is False
        assert "API call failed with status 500" in answer
        assert sources == []
        assert "error" in chat_info
