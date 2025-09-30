"""Unit tests for ARIA FastAPI application endpoints.

Tests cover all major API endpoints including audit, document processing,
question extraction, answer generation, chat, and analytics functionality.
"""

from __future__ import annotations

import pytest
import json
from io import BytesIO
from unittest.mock import Mock, patch, MagicMock
from typing import TYPE_CHECKING

from fastapi.testclient import TestClient
from fastapi import UploadFile

from aria.api.app import app
from aria.services.document_checker import DocumentCheckerService
from aria.services.document_processor import DocumentProcessor
from aria.services.question_extraction import QuestionExtractionService
from aria.services.answer_generation import AnswerGenerationService
from aria.services.chat_service import ChatService
from aria.services.analytics_service import AnalyticsService

if TYPE_CHECKING:
    from _pytest.capture import CaptureFixture
    from _pytest.fixtures import FixtureRequest
    from _pytest.logging import LogCaptureFixture
    from _pytest.monkeypatch import MonkeyPatch
    from pytest_mock.plugin import MockerFixture


@pytest.fixture
def client() -> TestClient:
    """FastAPI test client fixture."""
    return TestClient(app)


@pytest.fixture
def mock_services(mocker: "MockerFixture") -> dict:
    """Mock all service dependencies."""
    mocks = {}

    # Mock document checker service
    mock_doc_checker = mocker.patch('aria.api.app.DocumentCheckerService')
    mock_doc_checker.return_value.check_document.return_value = (
        True,
        [{"claim": "test", "verdict": "supported", "explanation": "test explanation"}],
        {"model_used": "test-model"}
    )
    mocks['doc_checker'] = mock_doc_checker

    # Mock document processor service
    mock_doc_processor = mocker.patch('aria.api.app.DocumentProcessor')
    mock_doc_processor.return_value.read_text_from_any.return_value = {
        "success": True,
        "content": "Processed content",
        "ready": True,
        "info": {}
    }
    mocks['doc_processor'] = mock_doc_processor

    # Mock question extraction service
    mock_qe_service = mocker.patch('aria.api.app.QuestionExtractionService')
    mock_qe_service.return_value.extract_questions.return_value = (
        True,
        [{"id": "q1", "text": "Test question"}],
        {"model_used": "test-model"}
    )
    mocks['qe_service'] = mock_qe_service

    # Mock answer generation service
    mock_ag_service = mocker.patch('aria.api.app.AnswerGenerationService')
    mock_ag_service.return_value.generate_answers.return_value = (
        True,
        [{"id": "a1", "text": "Test answer"}],
        {"model_used": "test-model"}
    )
    mocks['ag_service'] = mock_ag_service

    # Mock chat service
    mock_chat_service = mocker.patch('aria.api.app.ChatService')
    mock_chat_service.return_value.chat.return_value = (
        True,
        "Test response",
        [],
        {"model_used": "test-model"}
    )
    mocks['chat_service'] = mock_chat_service

    # Mock analytics service
    mock_analytics_service = mocker.patch('aria.api.app.AnalyticsService')
    mock_analytics_service.return_value.track_event.return_value = (
        True,
        {"event_id": "test-event-id"}
    )
    mocks['analytics_service'] = mock_analytics_service

    return mocks


class TestAuditEndpoints:
    """Test cases for document audit endpoints."""

    def test_audit_text_success(self, client: TestClient, mock_services: dict) -> None:
        """Test successful text auditing."""
        response = client.post(
            "/api/audit",
            json={
                "text": "This is a test document to audit.",
                "audit_categories": ["factual_accuracy"],
                "model_name": "test-model"
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "results_markdown" in data
        assert "info" in data
        assert "audit_items" in data["info"]

    def test_audit_text_missing_text(self, client: TestClient, mock_services: dict) -> None:
        """Test audit endpoint with missing text."""
        response = client.post(
            "/api/audit",
            json={
                "audit_categories": ["factual_accuracy"]
            }
        )

        assert response.status_code == 422  # Validation error

    def test_audit_text_empty_categories(self, client: TestClient, mock_services: dict) -> None:
        """Test audit endpoint with empty categories."""
        response = client.post(
            "/api/audit",
            json={
                "text": "Test document",
                "audit_categories": []
            }
        )

        # Empty list is allowed by FastAPI, so it should process normally
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True

    def test_audit_file_success(self, client: TestClient, mock_services: dict) -> None:
        """Test successful file auditing."""
        # Create a test file
        test_content = b"This is a test document content."
        files = {
            "file": ("test.txt", BytesIO(test_content), "text/plain")
        }
        data = {
            "audit_categories": '["factual_accuracy"]',
            "model_name": "test-model"
        }

        response = client.post("/api/audit-file", files=files, data=data)

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert "results_markdown" in response_data
        assert "info" in response_data

    def test_audit_file_no_file(self, client: TestClient, mock_services: dict) -> None:
        """Test file audit endpoint with no file."""
        response = client.post(
            "/api/audit-file",
            data={"audit_categories": '["factual_accuracy"]'}
        )

        assert response.status_code == 422  # Validation error


class TestDocumentProcessingEndpoints:
    """Test cases for document processing endpoints."""

    def test_process_document_success(self, client: TestClient, mock_services: dict) -> None:
        """Test successful document processing."""
        response = client.post(
            "/api/process-document",
            json={
                "file_path": "/tmp/test_document.txt",
                "document_name": "Test Document"
            }
        )

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert "content" in response_data
        assert response_data["ready"] is True

    def test_process_document_missing_fields(self, client: TestClient, mock_services: dict) -> None:
        """Test document processing with missing required fields."""
        response = client.post(
            "/api/process-document",
            json={"document_name": "Test Document"}  # Missing file_path
        )

        assert response.status_code == 422  # Validation error


class TestQuestionExtractionEndpoints:
    """Test cases for question extraction endpoints."""

    def test_extract_questions_success(self, client: TestClient, mock_services: dict) -> None:
        """Test successful question extraction."""
        response = client.post(
            "/api/extract-questions",
            json={
                "content": "This is test content for question extraction.",
                "document_name": "Test Document",
                "extraction_method": "ai_extraction",
                "model_name": "test-model"
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "questions" in data
        assert len(data["questions"]) > 0

    def test_extract_questions_missing_content(self, client: TestClient, mock_services: dict) -> None:
        """Test question extraction with missing content."""
        response = client.post(
            "/api/extract-questions",
            json={
                "document_name": "Test Document",
                "extraction_method": "ai_extraction"
            }
        )

        assert response.status_code == 422  # Validation error


class TestAnswerGenerationEndpoints:
    """Test cases for answer generation endpoints."""

    def test_generate_answers_success(self, client: TestClient, mock_services: dict) -> None:
        """Test successful answer generation."""
        response = client.post(
            "/api/generate-answers",
            json={
                "questions": [{"id": "q1", "text": "What is AI?"}],
                "settings": {"custom_prompt": "Test prompt"},
                "document_name": "Test Document"
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "answers" in data
        assert len(data["answers"]) > 0

    def test_generate_answers_missing_questions(self, client: TestClient, mock_services: dict) -> None:
        """Test answer generation with missing questions."""
        response = client.post(
            "/api/generate-answers",
            json={
                "settings": {"custom_prompt": "Test prompt"},
                "document_name": "Test Document"
            }
        )

        assert response.status_code == 422  # Validation error


class TestChatEndpoints:
    """Test cases for chat endpoints."""

    def test_chat_success(self, client: TestClient, mock_services: dict) -> None:
        """Test successful chat interaction."""
        response = client.post(
            "/api/chat",
            json={
                "question": "What is machine learning?",
                "context": [{"role": "user", "content": "ML is a subset of AI"}],
                "max_tokens": 1000
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "answer" in data

    def test_chat_missing_question(self, client: TestClient, mock_services: dict) -> None:
        """Test chat with missing question."""
        response = client.post(
            "/api/chat",
            json={
                "context": [{"role": "user", "content": "Test context"}],
                "max_tokens": 1000
            }
        )

        assert response.status_code == 422  # FastAPI validation error
        data = response.json()
        assert "detail" in data  # FastAPI validation error format

    def test_chat_empty_question(self, client: TestClient, mock_services: dict) -> None:
        """Test chat with empty question."""
        response = client.post(
            "/api/chat",
            json={
                "question": "",
                "context": [{"role": "user", "content": "Test context"}]
            }
        )

        # Empty string is still valid, so it should process normally
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True


class TestAnalyticsEndpoints:
    """Test cases for analytics endpoints."""

    def test_analytics_track_success(self, client: TestClient, mock_services: dict) -> None:
        """Test successful analytics tracking."""
        response = client.post(
            "/api/analytics/track",
            json={
                "event_type": "rfi_export",
                "session_id": "test-session",
                "document_name": "Test Document",
                "export_format": "csv",
                "total_questions": 5,
                "total_answers": 5
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True


class TestHealthAndDebugEndpoints:
    """Test cases for health and debug endpoints."""

    def test_healthz_endpoint(self, client: TestClient) -> None:
        """Test health check endpoint."""
        response = client.get("/healthz")

        assert response.status_code == 200
        data = response.json()
        assert "status" in data

    def test_debug_config_endpoint(self, client: TestClient) -> None:
        """Test debug config endpoint."""
        response = client.get("/debug/config")

        # Should work regardless of auth setup
        assert response.status_code in [200, 500]  # 500 if backend not available

    def test_metrics_endpoint(self, client: TestClient) -> None:
        """Test metrics endpoint."""
        response = client.get("/metrics")

        assert response.status_code == 200
        assert "text/plain" in response.headers.get("content-type", "")

    def test_debug_headers_endpoint(self, client: TestClient) -> None:
        """Test debug headers endpoint."""
        response = client.get("/debug/headers")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)


class TestErrorHandling:
    """Test cases for error handling scenarios."""

    def test_audit_service_failure(self, client: TestClient, mock_services: dict) -> None:
        """Test audit endpoint when service fails."""
        # Configure mock to return failure
        mock_services['doc_checker'].return_value.check_document.return_value = (
            False,
            [],
            {"error": "Service unavailable"}
        )

        response = client.post(
            "/api/audit",
            json={
                "text": "Test document",
                "audit_categories": ["factual_accuracy"]
            }
        )

        assert response.status_code == 200  # API returns 200 with success=false
        data = response.json()
        assert data["success"] is False

    def test_invalid_json_payload(self, client: TestClient) -> None:
        """Test endpoint with invalid JSON payload."""
        response = client.post(
            "/api/audit",
            content="invalid json {",
            headers={"Content-Type": "application/json"}
        )

        assert response.status_code == 422  # FastAPI validation error
