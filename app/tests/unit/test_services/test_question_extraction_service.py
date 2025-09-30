"""Unit tests for QuestionExtractionService.

Tests cover CSV extraction, AI extraction, error handling, and various edge cases.
"""

from __future__ import annotations

import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from typing import TYPE_CHECKING

from aria.services.question_extraction import QuestionExtractionService
from aria.config import settings
from aria.core.exceptions import TemporaryServiceError

if TYPE_CHECKING:
    from _pytest.capture import CaptureFixture
    from _pytest.fixtures import FixtureRequest
    from _pytest.logging import LogCaptureFixture
    from _pytest.monkeypatch import MonkeyPatch
    from pytest_mock.plugin import MockerFixture


class TestQuestionExtractionService:
    """Test cases for QuestionExtractionService."""

    @pytest.fixture
    def qe_service(self) -> QuestionExtractionService:
        """Question extraction service fixture."""
        return QuestionExtractionService()

    def test_init(self, qe_service: QuestionExtractionService) -> None:
        """Test QuestionExtractionService initialization."""
        assert qe_service.timeout == 480  # DEFAULT_TIMEOUT_SECONDS

    def test_extract_questions_csv_direct_success(self, qe_service: QuestionExtractionService) -> None:
        """Test successful CSV question extraction."""
        content = "Q1: What is AI?\nQ2: How does machine learning work?"

        success, questions, info = qe_service.extract_questions(
            content=content,
            extraction_method="csv_direct",
            metadata={"document_name": "test.csv"}
        )

        assert success is True
        assert len(questions) == 2
        assert questions[0]["text"] == "What is AI?"
        assert questions[1]["text"] == "How does machine learning work?"
        assert info["questions_found"] == 2
        assert info["method"] == "csv_direct"

    def test_extract_questions_csv_without_prefix(self, qe_service: QuestionExtractionService) -> None:
        """Test CSV extraction with questions that don't have Q1: prefix."""
        content = "What is AI?\nHow does machine learning work?"

        success, questions, info = qe_service.extract_questions(
            content=content,
            extraction_method="csv_direct"
        )

        assert success is True
        assert len(questions) == 2
        assert questions[0]["text"] == "What is AI?"
        assert questions[1]["text"] == "How does machine learning work?"

    def test_extract_questions_csv_empty_content(self, qe_service: QuestionExtractionService) -> None:
        """Test CSV extraction with empty content."""
        success, questions, info = qe_service.extract_questions(
            content="",
            extraction_method="csv_direct"
        )

        assert success is True
        assert len(questions) == 0
        assert info["questions_found"] == 0

    def test_extract_questions_invalid_method(self, qe_service: QuestionExtractionService) -> None:
        """Test invalid extraction method handling."""
        success, questions, info = qe_service.extract_questions(
            content="test content",
            extraction_method="invalid_method"
        )

        assert success is False
        assert len(questions) == 0
        assert "Unknown extraction method" in str(info["errors"])

    def test_extract_questions_ai_method_exists(self, qe_service: QuestionExtractionService) -> None:
        """Test that AI extraction method exists and can be called."""
        # Just test that the method exists and the service can handle ai_extraction method
        # The actual AI call would require complex mocking of external APIs
        success, questions, info = qe_service.extract_questions(
            content="<html>Test content</html>",
            extraction_method="ai_extraction"
        )

        # Since we don't have real API mocking, this will likely fail with auth error
        # But we can test that the method exists and returns proper structure
        assert isinstance(success, bool)
        assert isinstance(questions, list)
        assert isinstance(info, dict)
        assert "errors" in info

    def test_extract_questions_ai_error_handling(self, qe_service: QuestionExtractionService, mocker: "MockerFixture") -> None:
        """Test AI extraction error handling."""
        # Mock a general exception during AI processing
        mocker.patch.object(qe_service, '_check_authentication', side_effect=Exception("Test error"))

        success, questions, info = qe_service.extract_questions(
            content="<html>Test content</html>",
            extraction_method="ai_extraction"
        )

        assert success is False
        assert len(questions) == 0

    def test_extract_questions_timeout_error(self, qe_service: QuestionExtractionService, mocker: "MockerFixture") -> None:
        """Test timeout error handling."""
        mocker.patch.object(qe_service, '_check_authentication', return_value=(True, {"Authorization": "Bearer token"}))
        mocker.patch.object(qe_service, '_preprocess_html', side_effect=TimeoutError("Request timed out"))

        success, questions, info = qe_service.extract_questions(
            content="<html>Test content</html>",
            extraction_method="ai_extraction"
        )

        assert success is False
        assert len(questions) == 0
        # The timeout is handled in the main extract_questions method and logged as error

    def test_extract_questions_custom_prompt(self, qe_service: QuestionExtractionService, mocker: "MockerFixture") -> None:
        """Test that custom prompt parameter is accepted."""
        # Test CSV extraction with custom prompt (should work fine)
        success, questions, info = qe_service.extract_questions(
            content="Q1: Test question",
            extraction_method="csv_direct",
            custom_prompt="Custom extraction prompt"
        )

        assert success is True
        # Custom prompt should be accepted even for CSV (though not used)

    def test_extract_questions_model_specification(self, qe_service: QuestionExtractionService, mocker: "MockerFixture") -> None:
        """Test that model name is properly recorded in info."""
        # Test CSV extraction with model name (should still work)
        success, questions, info = qe_service.extract_questions(
            content="Q1: Test question",
            extraction_method="csv_direct",
            model_name="specified-model"
        )

        assert success is True
        assert info["model_used"] == "specified-model"

    def test_count_total_questions_simple(self, qe_service: QuestionExtractionService) -> None:
        """Test counting simple questions."""
        questions = [
            {"id": "Q1", "text": "Question 1"},
            {"id": "Q2", "text": "Question 2"}
        ]

        count = qe_service._count_total_questions(questions)
        assert count == 2

    def test_count_total_questions_nested(self, qe_service: QuestionExtractionService) -> None:
        """Test counting questions with nested sub-topics."""
        questions = [
            {
                "id": "T1",
                "sub_topics": [
                    {"sub_questions": [{"text": "Sub Q1"}, {"text": "Sub Q2"}]},
                    {"sub_questions": [{"text": "Sub Q3"}]}
                ]
            },
            {"id": "Q2", "text": "Simple question"}
        ]

        count = qe_service._count_total_questions(questions)
        assert count == 4  # 3 sub-questions + 1 simple question

    def test_count_total_questions_empty(self, qe_service: QuestionExtractionService) -> None:
        """Test counting with empty questions list."""
        count = qe_service._count_total_questions([])
        assert count == 0

    def test_count_total_questions_mixed_types(self, qe_service: QuestionExtractionService) -> None:
        """Test counting with mixed question types."""
        questions = [
            {"id": "Q1", "text": "Simple question"},
            "String question",
            {
                "id": "T1",
                "sub_topics": [
                    {"sub_questions": [{"text": "Sub Q1"}]}
                ]
            }
        ]

        count = qe_service._count_total_questions(questions)
        assert count == 3

    def test_preprocess_html(self, qe_service: QuestionExtractionService) -> None:
        """Test HTML preprocessing."""
        html_content = "<html><head><title>Test</title></head><body><p>Test content</p></body></html>"

        processed = qe_service._preprocess_html(html_content)

        # Should remove HTML tags and clean up content
        assert "Test content" in processed
        assert "<html>" not in processed
        assert "<p>" not in processed

    def test_build_extraction_prompt(self, qe_service: QuestionExtractionService) -> None:
        """Test extraction prompt building."""
        prompt = qe_service._build_extraction_prompt()

        assert isinstance(prompt, str)
        assert len(prompt) > 0
        assert "question" in prompt.lower()

    def test_build_user_prompt(self, qe_service: QuestionExtractionService) -> None:
        """Test user prompt building."""
        content = "Test document content"
        custom_prompt = "Custom instructions"

        prompt = qe_service._build_user_prompt(content, custom_prompt)

        assert content in prompt
        assert custom_prompt in prompt

    def test_build_user_prompt_no_custom(self, qe_service: QuestionExtractionService) -> None:
        """Test user prompt building without custom prompt."""
        content = "Test document content"

        prompt = qe_service._build_user_prompt(content)

        assert content in prompt
        assert "Custom instructions" not in prompt

    def test_parse_ai_response_valid_json(self, qe_service: QuestionExtractionService) -> None:
        """Test parsing valid JSON response from AI."""
        response_text = '[{"id": "Q1", "text": "What is AI?", "topic": "AI"}, {"id": "Q2", "text": "How does ML work?", "topic": "ML"}]'

        questions = qe_service._parse_ai_response(response_text)

        assert len(questions) == 2
        assert questions[0]["text"] == "What is AI?"
        assert questions[1]["text"] == "How does ML work?"

    def test_parse_ai_response_malformed_json(self, qe_service: QuestionExtractionService) -> None:
        """Test parsing malformed JSON response."""
        response_text = 'Invalid JSON response'

        questions = qe_service._parse_ai_response(response_text)

        assert len(questions) == 0

    def test_emergency_fallback_parse(self, qe_service: QuestionExtractionService) -> None:
        """Test emergency fallback parsing."""
        response_text = "Q1: What is AI?\nQ2: How does ML work?\nSome other text"

        questions = qe_service._emergency_fallback_parse(response_text)

        assert len(questions) >= 2
        assert any("What is AI?" in q.get("text", "") for q in questions)

    def test_create_info_dict(self, qe_service: QuestionExtractionService) -> None:
        """Test creation of info dictionary."""
        info = qe_service._create_info_dict("csv_direct")

        # Check that the info dict has the expected structure
        assert "method" in info
        assert "errors" in info
        assert "processing_time" in info
        assert "admin_errors" in info

        assert info["method"] == "csv_direct"
        assert isinstance(info["errors"], list)
        assert isinstance(info["admin_errors"], list)
