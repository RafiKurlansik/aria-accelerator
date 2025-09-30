"""Unit tests for AnswerGenerationService.

Tests cover question generation, topic-based processing, individual processing,
error handling, analytics integration, and various edge cases.
"""

from __future__ import annotations

import pytest
import json
from unittest.mock import Mock, patch, MagicMock, call
from typing import TYPE_CHECKING
import pandas as pd

from aria.services.answer_generation import AnswerGenerationService
from aria.config import settings
from aria.core.exceptions import TemporaryServiceError

if TYPE_CHECKING:
    from _pytest.capture import CaptureFixture
    from _pytest.fixtures import FixtureRequest
    from _pytest.logging import LogCaptureFixture
    from _pytest.monkeypatch import MonkeyPatch
    from pytest_mock.plugin import MockerFixture


class TestAnswerGenerationService:
    """Test cases for AnswerGenerationService."""

    @pytest.fixture
    def ag_service(self) -> AnswerGenerationService:
        """Answer generation service fixture."""
        return AnswerGenerationService()

    @pytest.fixture
    def mock_analytics_service(self, mocker: "MockerFixture") -> Mock:
        """Mock analytics service fixture."""
        return mocker.patch('aria.services.answer_generation.AnalyticsService')

    def test_init(self, ag_service: AnswerGenerationService) -> None:
        """Test AnswerGenerationService initialization."""
        assert ag_service.timeout == 600  # 10 minutes for cold starts

    def test_generate_answers_empty_list(self, ag_service: AnswerGenerationService, mock_analytics_service: Mock) -> None:
        """Test generating answers for empty question list."""
        success, answers, info = ag_service.generate_answers([])

        assert success is True
        assert len(answers) == 0
        assert info["questions_processed"] == 0
        assert info["answers_generated"] == 0

    def test_generate_answers_individual_questions(self, ag_service: AnswerGenerationService, mocker: "MockerFixture", mock_analytics_service: Mock) -> None:
        """Test generating answers for individual questions (no hierarchical structure)."""
        # Mock authentication
        mocker.patch.object(ag_service, '_check_authentication', return_value=(True, {"Authorization": "Bearer token"}))

        # Mock individual generation method
        mock_answers = [
            {"question_id": "q1", "answer": "Answer 1", "topic": "Topic 1"},
            {"question_id": "q2", "answer": "Answer 2", "topic": "Topic 2"}
        ]
        mocker.patch.object(ag_service, '_generate_individual_with_retry', return_value=True)

        # Mock question tracker setup
        mocker.patch.object(ag_service, '_generate_with_tracking', return_value=(True, mock_answers))

        questions = [
            {"id": "q1", "text": "What is AI?", "topic": "Topic 1"},
            {"id": "q2", "text": "How does ML work?", "topic": "Topic 2"}
        ]

        success, answers, info = ag_service.generate_answers(questions)

        assert success is True
        assert len(answers) == 2
        assert info["method"] == "answer_generation"
        assert info["questions_processed"] == 2

    def test_generate_answers_hierarchical_questions(self, ag_service: AnswerGenerationService, mocker: "MockerFixture", mock_analytics_service: Mock) -> None:
        """Test generating answers for hierarchical questions (with topics and sub-questions)."""
        # Mock authentication
        mocker.patch.object(ag_service, '_check_authentication', return_value=(True, {"Authorization": "Bearer token"}))

        # Mock hierarchical generation method
        mock_answers = [
            {"question_id": "1.1", "answer": "Sub-answer 1", "topic": "Topic 1"},
            {"question_id": "1.2", "answer": "Sub-answer 2", "topic": "Topic 1"}
        ]
        mocker.patch.object(ag_service, '_generate_by_topics_with_retry', return_value=True)
        mocker.patch.object(ag_service, '_generate_with_tracking', return_value=(True, mock_answers))

        questions = [
            {"question": "1", "topic": "Topic 1", "sub_question": "1.1", "text": "What is AI?"},
            {"question": "1", "topic": "Topic 1", "sub_question": "1.2", "text": "How does ML work?"}
        ]

        success, answers, info = ag_service.generate_answers(questions)

        assert success is True
        assert len(answers) == 2
        assert info["method"] == "answer_generation"

    def test_generate_answers_authentication_failure(self, ag_service: AnswerGenerationService, mocker: "MockerFixture", mock_analytics_service: Mock) -> None:
        """Test authentication failure handling."""
        mocker.patch.object(ag_service, '_check_authentication', return_value=(False, {}))

        questions = [{"id": "q1", "text": "Test question"}]
        success, answers, info = ag_service.generate_answers(questions)

        assert success is False
        assert len(answers) == 0

    def test_generate_answers_with_custom_prompt(self, ag_service: AnswerGenerationService, mocker: "MockerFixture", mock_analytics_service: Mock) -> None:
        """Test generating answers with custom prompt."""
        mocker.patch.object(ag_service, '_check_authentication', return_value=(True, {"Authorization": "Bearer token"}))
        mocker.patch.object(ag_service, '_generate_individual_with_retry', return_value=True)

        # Mock with proper answer structure
        mock_answers = [{"question_id": "q1", "answer": "Detailed technical answer", "topic": "Test"}]
        mocker.patch.object(ag_service, '_generate_with_tracking', return_value=(True, mock_answers))

        questions = [{"id": "q1", "text": "Test question"}]
        custom_prompt = "Please provide detailed technical answers."

        success, answers, info = ag_service.generate_answers(questions, custom_prompt=custom_prompt)

        assert success is True
        assert len(answers) == 1

    def test_generate_answers_with_progress_callback(self, ag_service: AnswerGenerationService, mocker: "MockerFixture", mock_analytics_service: Mock) -> None:
        """Test generating answers with progress callback."""
        mocker.patch.object(ag_service, '_check_authentication', return_value=(True, {"Authorization": "Bearer token"}))
        mocker.patch.object(ag_service, '_generate_individual_with_retry', return_value=True)

        mock_answers = [{"question_id": "q1", "answer": "Progress callback answer", "topic": "Test"}]
        mocker.patch.object(ag_service, '_generate_with_tracking', return_value=(True, mock_answers))

        progress_calls = []
        def progress_callback(current: int, total: int, status: str) -> None:
            progress_calls.append((current, total, status))

        questions = [{"id": "q1", "text": "Test question"}]
        success, answers, info = ag_service.generate_answers(
            questions,
            progress_callback=progress_callback
        )

        assert success is True
        assert len(answers) == 1

    def test_generate_answers_with_session_tracking(self, ag_service: AnswerGenerationService, mocker: "MockerFixture", mock_analytics_service: Mock) -> None:
        """Test generating answers with session tracking."""
        mocker.patch.object(ag_service, '_check_authentication', return_value=(True, {"Authorization": "Bearer token"}))
        mocker.patch.object(ag_service, '_generate_individual_with_retry', return_value=True)

        mock_answers = [{"question_id": "q1", "answer": "Session tracked answer", "topic": "Test"}]
        mocker.patch.object(ag_service, '_generate_with_tracking', return_value=(True, mock_answers))

        questions = [{"id": "q1", "text": "Test question"}]
        success, answers, info = ag_service.generate_answers(
            questions,
            session_id="session_123",
            document_name="test_doc.pdf"
        )

        assert success is True
        assert len(answers) == 1

    def test_generate_answers_regeneration(self, ag_service: AnswerGenerationService, mocker: "MockerFixture", mock_analytics_service: Mock) -> None:
        """Test generating answers with regeneration flag."""
        mocker.patch.object(ag_service, '_check_authentication', return_value=(True, {"Authorization": "Bearer token"}))
        mocker.patch.object(ag_service, '_generate_individual_with_retry', return_value=True)

        mock_answers = [{"question_id": "q1", "answer": "Regenerated answer", "topic": "Test"}]
        mocker.patch.object(ag_service, '_generate_with_tracking', return_value=(True, mock_answers))

        questions = [{"id": "q1", "text": "Test question"}]
        success, answers, info = ag_service.generate_answers(
            questions,
            session_id="session_123",
            is_regeneration=True
        )

        assert success is True
        assert len(answers) == 1

    def test_has_hierarchical_structure_true(self, ag_service: AnswerGenerationService) -> None:
        """Test hierarchical structure detection - positive case."""
        questions = [
            {
                "question": "1",
                "topic": "AI Fundamentals",
                "sub_question": "1.1",
                "text": "What is artificial intelligence?"
            }
        ]

        has_hierarchy = ag_service._has_hierarchical_structure(questions)
        assert has_hierarchy is True

    def test_has_hierarchical_structure_false(self, ag_service: AnswerGenerationService) -> None:
        """Test hierarchical structure detection - negative case."""
        questions = [
            {"id": "q1", "text": "What is AI?"},  # Missing required fields
            {"id": "q2", "text": "How does ML work?"}
        ]

        has_hierarchy = ag_service._has_hierarchical_structure(questions)
        assert has_hierarchy is False

    def test_has_hierarchical_structure_empty(self, ag_service: AnswerGenerationService) -> None:
        """Test hierarchical structure detection - empty list."""
        has_hierarchy = ag_service._has_hierarchical_structure([])
        assert has_hierarchy is False

    def test_get_question_id_from_sub_question(self, ag_service: AnswerGenerationService) -> None:
        """Test question ID extraction from sub_question field."""
        question = {
            "question": "1",
            "sub_question": "1.2",
            "text": "What is machine learning?"
        }

        question_id = ag_service._get_question_id(question)
        assert question_id == "1.2"

    def test_get_question_id_fallback_chain(self, ag_service: AnswerGenerationService) -> None:
        """Test question ID extraction fallback chain."""
        # Test different ID field priorities
        questions = [
            {"id": "test_id", "text": "Question 1"},
            {"ID": "test_ID", "text": "Question 2"},
            {"question_id": "test_question_id", "text": "Question 3"},
            {"text": "Question 4"}  # Should fall back to 'unknown'
        ]

        expected_ids = ["test_id", "test_ID", "test_question_id", "unknown"]

        for i, question in enumerate(questions):
            question_id = ag_service._get_question_id(question)
            assert question_id == expected_ids[i]

    def test_validate_complete_answers_success(self, ag_service: AnswerGenerationService) -> None:
        """Test answer validation - success case."""
        questions = [
            {"id": "q1", "text": "Question 1"},
            {"id": "q2", "text": "Question 2"}
        ]
        answers = [
            {"question_id": "q1", "answer": "Answer 1"},
            {"question_id": "q2", "answer": "Answer 2"}
        ]
        generation_info = {}

        is_valid = ag_service._validate_complete_answers(questions, answers, generation_info)
        assert is_valid is True

    def test_validate_complete_answers_count_mismatch(self, ag_service: AnswerGenerationService) -> None:
        """Test answer validation - count mismatch."""
        questions = [
            {"id": "q1", "text": "Question 1"},
            {"id": "q2", "text": "Question 2"}
        ]
        answers = [{"question_id": "q1", "answer": "Answer 1"}]  # Missing one answer
        generation_info = {}

        is_valid = ag_service._validate_complete_answers(questions, answers, generation_info)
        assert is_valid is False
        assert "validation_error" in generation_info

    def test_validate_complete_answers_with_errors(self, ag_service: AnswerGenerationService) -> None:
        """Test answer validation - with error answers."""
        questions = [
            {"id": "q1", "text": "Question 1"},
            {"id": "q2", "text": "Question 2"}
        ]
        answers = [
            {"question_id": "q1", "answer": "Answer 1"},
            {"question_id": "q2", "answer": "❌ Failed to generate answer after multiple retries", "error": True}
        ]
        generation_info = {}

        is_valid = ag_service._validate_complete_answers(questions, answers, generation_info)
        assert is_valid is True  # Still valid if all questions have some answer
        assert generation_info["error_answers"] == 1

    def test_build_generation_prompt(self, ag_service: AnswerGenerationService) -> None:
        """Test generation prompt building."""
        prompt = ag_service._build_generation_prompt()

        assert isinstance(prompt, str)
        assert len(prompt) > 0
        assert "answer" in prompt.lower()

    def test_build_topic_user_prompt(self, ag_service: AnswerGenerationService) -> None:
        """Test topic-based user prompt building."""
        topic = "AI Fundamentals"
        question_text = "What is artificial intelligence?"
        custom_prompt = "Provide technical details."

        prompt = ag_service._build_topic_user_prompt(topic, question_text, custom_prompt)

        assert topic in prompt
        assert question_text in prompt
        assert custom_prompt in prompt

    def test_build_topic_user_prompt_no_custom(self, ag_service: AnswerGenerationService) -> None:
        """Test topic-based user prompt building without custom prompt."""
        topic = "AI Fundamentals"
        question_text = "What is artificial intelligence?"

        prompt = ag_service._build_topic_user_prompt(topic, question_text)

        assert topic in prompt
        assert question_text in prompt

    def test_build_single_user_prompt(self, ag_service: AnswerGenerationService) -> None:
        """Test single question user prompt building."""
        question_text = "What is machine learning?"
        custom_prompt = "Be concise."

        prompt = ag_service._build_single_user_prompt(question_text, custom_prompt)

        assert question_text in prompt
        assert custom_prompt in prompt

    def test_build_json_user_prompt(self, ag_service: AnswerGenerationService) -> None:
        """Test JSON user prompt building."""
        json_payload = {
            "questions": ["What is AI?", "How does ML work?"],
            "context": "Technical documentation"
        }

        prompt = ag_service._build_json_user_prompt(json_payload)

        assert "What is AI?" in prompt
        assert "Technical documentation" in prompt

    def test_get_next_session_version_first_time(self, ag_service: AnswerGenerationService, mock_analytics_service: Mock) -> None:
        """Test session version retrieval - first time."""
        mock_analytics = mock_analytics_service.return_value
        mock_analytics.track_generation_batch.return_value = None

        # Mock as if no previous version exists
        version = ag_service._get_next_session_version("session_123", mock_analytics)

        assert version == 1

    def test_get_next_session_version_regeneration(self, ag_service: AnswerGenerationService, mock_analytics_service: Mock) -> None:
        """Test session version retrieval - regeneration."""
        mock_analytics = mock_analytics_service.return_value
        mock_analytics.track_generation_batch.return_value = None

        version = ag_service._get_next_session_version("session_123", mock_analytics, is_regeneration=True)

        assert version == 2  # Should increment for regeneration

    def test_create_info_dict(self, ag_service: AnswerGenerationService) -> None:
        """Test info dictionary creation."""
        info = ag_service._create_info_dict("test_method")

        # Check that the info dict has the expected structure
        assert "method" in info
        assert "errors" in info
        assert "processing_time" in info
        assert "admin_errors" in info

        assert info["method"] == "test_method"
        assert isinstance(info["errors"], list)
        assert isinstance(info["admin_errors"], list)
        assert info["processing_time"] == 0.0
