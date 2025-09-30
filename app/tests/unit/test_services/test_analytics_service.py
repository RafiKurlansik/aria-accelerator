"""Unit tests for AnalyticsService.

Tests cover initialization, table management, tracking methods,
error handling, and Unity Catalog integration.
"""

from __future__ import annotations

import pytest
import json
from unittest.mock import Mock, patch, MagicMock, call
from typing import TYPE_CHECKING
from datetime import datetime

from aria.services.analytics_service import AnalyticsService
from aria.config import settings
from databricks.sdk.errors import DatabricksError

if TYPE_CHECKING:
    from _pytest.capture import CaptureFixture
    from _pytest.fixtures import FixtureRequest
    from _pytest.logging import LogCaptureFixture
    from _pytest.monkeypatch import MonkeyPatch
    from pytest_mock.plugin import MockerFixture


class TestAnalyticsService:
    """Test cases for AnalyticsService."""

    @pytest.fixture
    def analytics_service(self) -> AnalyticsService:
        """Analytics service fixture using real Databricks authentication."""
        # Use real authentication since it's already configured
        service = AnalyticsService()
        return service

    def test_init_disabled_analytics(self, mocker: "MockerFixture") -> None:
        """Test initialization with analytics disabled."""
        mock_settings = mocker.patch('aria.services.analytics_service.settings')
        mock_settings._raw_yaml = {
            'analytics': {
                'enabled': False,
                'unity_catalog': {'catalog': 'users', 'schema': 'test_user'}
            }
        }

        service = AnalyticsService()

        assert service.analytics_enabled is False
        assert service.workspace_client is None

    def test_init_enabled_analytics(self, mocker: "MockerFixture") -> None:
        """Test initialization with analytics enabled."""
        mock_settings = mocker.patch('aria.services.analytics_service.settings')
        mock_settings._raw_yaml = {
            'analytics': {
                'enabled': True,
                'unity_catalog': {'catalog': 'users', 'schema': 'test_user'}
            }
        }
        mock_workspace_client = mocker.patch('aria.services.analytics_service.settings.get_workspace_client')
        mock_workspace_client.return_value = Mock()

        service = AnalyticsService()

        assert service.analytics_enabled is True
        assert service.catalog == 'users'
        assert service.schema == 'test_user'

    def test_table_name_construction(self, analytics_service: AnalyticsService) -> None:
        """Test that table names are constructed correctly."""
        # Use real values from your Databricks setup
        expected_tables = {
            'rfi_uploads_table': 'users.rafi_kurlansik.rfi_uploads',
            'rfi_extractions_table': 'users.rafi_kurlansik.rfi_extractions',
            'rfi_exports_table': 'users.rafi_kurlansik.rfi_exports',
            'chat_sessions_table': 'users.rafi_kurlansik.chat_sessions',
            'chat_questions_table': 'users.rafi_kurlansik.chat_questions',
            'rfi_generation_batches_table': 'users.rafi_kurlansik.rfi_generation_batches'
        }

        for attr_name, expected_table in expected_tables.items():
            assert getattr(analytics_service, attr_name) == expected_table

    def test_ensure_tables_exist_success(self, analytics_service: AnalyticsService, mocker: "MockerFixture") -> None:
        """Test successful table creation."""
        # Mock workspace client
        mock_client = Mock()
        analytics_service.workspace_client = mock_client

        # Mock successful schema validation
        mock_statement = Mock()
        mock_client.statement_execution.execute_statement.return_value = mock_statement

        # Mock table creation calls
        mock_client.statement_execution.execute_statement.return_value = Mock()

        analytics_service._ensure_tables_exist()

        # Verify schema validation was called
        mock_client.statement_execution.execute_statement.assert_called()

    def test_ensure_tables_exist_no_client(self, analytics_service: AnalyticsService) -> None:
        """Test table creation with no workspace client."""
        analytics_service.workspace_client = None

        # Should not raise exception
        analytics_service._ensure_tables_exist()

    def test_ensure_tables_exist_no_warehouse(self, analytics_service: AnalyticsService, mocker: "MockerFixture") -> None:
        """Test table creation with no warehouse ID."""
        mock_client = Mock()
        analytics_service.workspace_client = mock_client

        # Mock settings with no warehouse ID
        mock_settings = mocker.patch.object(analytics_service, 'settings')
        mock_settings.databricks.warehouse_id = None

        analytics_service._ensure_tables_exist()

        # Should not attempt table creation
        mock_client.statement_execution.execute_statement.assert_not_called()

    def test_ensure_columns_exist_success(self, analytics_service: AnalyticsService, mocker: "MockerFixture") -> None:
        """Test successful column creation."""
        mock_client = Mock()
        analytics_service.workspace_client = mock_client

        # Mock successful column operations
        mock_client.statement_execution.execute_statement.return_value = Mock()

        analytics_service._ensure_columns_exist()

        # Verify column operations were attempted
        assert mock_client.statement_execution.execute_statement.call_count > 0

    def test_track_rfi_upload_success(self, analytics_service: AnalyticsService) -> None:
        """Test successful RFI upload tracking."""
        # Skip if analytics is not enabled (no real Databricks connection)
        if not analytics_service.is_enabled():
            pytest.skip("Analytics not enabled or not properly configured")

        result = analytics_service.track_rfi_upload(
            session_id="test_session_analytics",
            document_name="test_document.pdf",
            file_size_bytes=1024,
            file_type="pdf",
            processing_method="upload",
            headers={"User-Agent": "test-agent"}
        )

        # With real authentication, this should work if tables exist
        assert result is True

    def test_track_rfi_upload_no_client(self, analytics_service: AnalyticsService) -> None:
        """Test RFI upload tracking with no workspace client."""
        analytics_service.workspace_client = None

        result = analytics_service.track_rfi_upload(
            session_id="session_123",
            document_name="test.pdf",
            file_size_bytes=1024,
            file_type="pdf",
            processing_method="upload"
        )

        assert result is False

    def test_track_rfi_upload_database_error(self, analytics_service: AnalyticsService, mocker: "MockerFixture") -> None:
        """Test RFI upload tracking with database error."""
        mock_client = Mock()
        analytics_service.workspace_client = mock_client

        # Mock database error
        mock_client.statement_execution.execute_statement.side_effect = DatabricksError("Database error")

        result = analytics_service.track_rfi_upload(
            session_id="session_123",
            document_name="test.pdf",
            file_size_bytes=1024,
            file_type="pdf",
            processing_method="upload"
        )

        assert result is False

    def test_track_rfi_extraction_success(self, analytics_service: AnalyticsService) -> None:
        """Test successful RFI extraction tracking."""
        if not analytics_service.is_enabled():
            pytest.skip("Analytics not enabled or not properly configured")

        result = analytics_service.track_rfi_extraction(
            session_id="test_session_extraction",
            document_name="test_document.pdf",
            extraction_method="ai_extraction",
            questions_extracted=5,
            model_used="test-model",
            custom_prompt_used=True,
            processing_time_seconds=2.5,
            headers={"User-Agent": "test-agent"}
        )

        assert result is True

    def test_track_generation_batch_success(self, analytics_service: AnalyticsService) -> None:
        """Test successful generation batch tracking."""
        if not analytics_service.is_enabled():
            pytest.skip("Analytics not enabled or not properly configured")

        result = analytics_service.track_generation_batch(
            session_id="test_session_batch",
            session_version=1,
            topic_name="AI Fundamentals",
            batch_attempt_number=1,
            batch_status="success",
            batch_processing_time_seconds=10.5,
            model_used="test-model",
            custom_prompt_used=True,
            questions_in_batch=[
                {"id": "q1", "text": "What is AI?", "topic": "AI Fundamentals"}
            ],
            answers_in_batch=[
                {"question_id": "q1", "answer": "AI is artificial intelligence"}
            ],
            document_name="test_document.pdf"
        )

        assert result is True

    def test_track_rfi_export_success(self, analytics_service: AnalyticsService) -> None:
        """Test successful RFI export tracking."""
        if not analytics_service.is_enabled():
            pytest.skip("Analytics not enabled or not properly configured")

        result = analytics_service.track_rfi_export(
            session_id="test_session_export",
            document_name="test_document.pdf",
            export_format="csv",
            total_questions=10,
            total_answers=10,
            headers={"User-Agent": "test-agent"}
        )

        assert result is True

    def test_track_chat_session_success(self, analytics_service: AnalyticsService) -> None:
        """Test successful chat session tracking."""
        if not analytics_service.is_enabled():
            pytest.skip("Analytics not enabled or not properly configured")

        result = analytics_service.track_chat_session(
            session_id="test_session_chat",
            chat_session_id="chat_session_123",
            total_questions=3,
            total_responses=3,
            session_duration_seconds=120,
            final_status="completed",
            headers={"User-Agent": "test-agent"}
        )

        assert result is True

    def test_track_chat_question_success(self, analytics_service: AnalyticsService) -> None:
        """Test successful chat question tracking."""
        if not analytics_service.is_enabled():
            pytest.skip("Analytics not enabled or not properly configured")

        question_data = {
            "text": "What is AI?",
            "length_chars": 10
        }

        response_data = {
            "text": "AI stands for Artificial Intelligence",
            "length_chars": 35,
            "model_used": "test-model",
            "response_time_seconds": 5.2,
            "copied_to_clipboard": False
        }

        result = analytics_service.track_chat_question(
            session_id="test_session_question",
            chat_session_id="chat_session_123",
            question_data=question_data,
            response_data=response_data,
            headers={"User-Agent": "test-agent"}
        )

        assert result is True

    def test_track_chat_question_with_copy(self, analytics_service: AnalyticsService) -> None:
        """Test chat question tracking with clipboard copy."""
        if not analytics_service.is_enabled():
            pytest.skip("Analytics not enabled or not properly configured")

        question_data = {
            "text": "What is AI?",
            "length_chars": 10
        }

        response_data = {
            "text": "AI stands for Artificial Intelligence",
            "length_chars": 35,
            "model_used": "test-model",
            "response_time_seconds": 5.2,
            "copied_to_clipboard": True
        }

        result = analytics_service.track_chat_question(
            session_id="test_session_copy",
            chat_session_id="chat_session_123",
            question_data=question_data,
            response_data=response_data
        )

        assert result is True

    def test_multiple_tracking_calls_success(self, analytics_service: AnalyticsService) -> None:
        """Test successful multiple tracking method calls."""
        if not analytics_service.is_enabled():
            pytest.skip("Analytics not enabled or not properly configured")

        # Test multiple successful tracking calls
        upload_result = analytics_service.track_rfi_upload(
            session_id="test_multi_1",
            document_name="doc1.pdf",
            file_size_bytes=1024,
            file_type="pdf",
            processing_method="upload"
        )

        extraction_result = analytics_service.track_rfi_extraction(
            session_id="test_multi_2",
            document_name="doc2.pdf",
            extraction_method="ai_extraction",
            questions_extracted=5,
            model_used="test-model",
            custom_prompt_used=False,
            processing_time_seconds=2.5
        )

        export_result = analytics_service.track_rfi_export(
            session_id="test_multi_3",
            document_name="doc3.pdf",
            export_format="csv",
            total_questions=10,
            total_answers=10
        )

        assert upload_result is True
        assert extraction_result is True
        assert export_result is True

    def test_large_data_handling(self, analytics_service: AnalyticsService) -> None:
        """Test handling of large data payloads."""
        if not analytics_service.is_enabled():
            pytest.skip("Analytics not enabled or not properly configured")

        # Test with large text content
        large_question = "What is AI?" * 100  # Very long question
        large_response = "AI is..." * 100     # Very long response

        question_data = {
            "text": large_question,
            "length_chars": len(large_question)
        }

        response_data = {
            "text": large_response,
            "length_chars": len(large_response),
            "model_used": "test-model",
            "response_time_seconds": 5.2,
            "copied_to_clipboard": False
        }

        result = analytics_service.track_chat_question(
            session_id="test_large_data",
            chat_session_id="chat_large",
            question_data=question_data,
            response_data=response_data
        )

        assert result is True

    def test_special_characters_in_data(self, analytics_service: AnalyticsService) -> None:
        """Test handling of special characters in tracking data."""
        if not analytics_service.is_enabled():
            pytest.skip("Analytics not enabled or not properly configured")

        # Test with special characters that might break SQL
        special_question = "What is AI? Special chars: ' \" ; -- /* */"
        special_response = "AI is great! More chars: @#$%^&*()"

        question_data = {
            "text": special_question,
            "length_chars": len(special_question)
        }

        response_data = {
            "text": special_response,
            "length_chars": len(special_response),
            "model_used": "test-model",
            "response_time_seconds": 5.2,
            "copied_to_clipboard": False
        }

        result = analytics_service.track_chat_question(
            session_id="test_special_chars",
            chat_session_id="chat_special",
            question_data=question_data,
            response_data=response_data
        )

        assert result is True

    def test_concurrent_tracking_simulation(self, analytics_service: AnalyticsService) -> None:
        """Test simulation of concurrent tracking calls."""
        if not analytics_service.is_enabled():
            pytest.skip("Analytics not enabled or not properly configured")

        # Simulate multiple concurrent calls
        results = []
        for i in range(3):  # Reduced to 3 to avoid overwhelming the service
            result = analytics_service.track_rfi_upload(
                session_id=f"session_{i}",
                document_name=f"test_{i}.pdf",
                file_size_bytes=1024 + i * 100,
                file_type="pdf",
                processing_method="upload"
            )
            results.append(result)

        assert all(results)

    def test_disabled_analytics_tracking(self, mocker: "MockerFixture") -> None:
        """Test that disabled analytics doesn't perform tracking."""
        # Create a service with analytics disabled by mocking the config
        mock_settings = mocker.patch('aria.services.analytics_service.settings')
        mock_settings._raw_yaml = {
            'analytics': {
                'enabled': False,
                'unity_catalog': {'catalog': 'users', 'schema': 'test_user'}
            }
        }

        service = AnalyticsService()

        # All tracking methods should return False when disabled
        upload_result = service.track_rfi_upload(
            session_id="session_123",
            document_name="test.pdf",
            file_size_bytes=1024,
            file_type="pdf",
            processing_method="upload"
        )
        extraction_result = service.track_rfi_extraction(
            session_id="session_123",
            document_name="test.pdf",
            extraction_method="ai_extraction",
            questions_extracted=5,
            model_used="test-model",
            custom_prompt_used=False,
            processing_time_seconds=2.5
        )
        export_result = service.track_rfi_export(
            session_id="session_123",
            document_name="test.pdf",
            export_format="csv",
            total_questions=10,
            total_answers=10
        )

        assert upload_result is False
        assert extraction_result is False
        assert export_result is False

    def test_service_functionality_verification(self, analytics_service: AnalyticsService) -> None:
        """Test that the analytics service is properly initialized and functional."""
        # Test basic service properties
        assert hasattr(analytics_service, 'workspace_client')
        assert hasattr(analytics_service, 'analytics_enabled')
        assert hasattr(analytics_service, 'catalog')
        assert hasattr(analytics_service, 'schema')

        # Test is_enabled method
        enabled_result = analytics_service.is_enabled()
        assert isinstance(enabled_result, bool)

        # Test that table names are properly constructed
        assert analytics_service.rfi_uploads_table.endswith('rfi_uploads')
        assert analytics_service.rfi_extractions_table.endswith('rfi_extractions')
        assert analytics_service.rfi_exports_table.endswith('rfi_exports')
        assert analytics_service.chat_sessions_table.endswith('chat_sessions')
        assert analytics_service.chat_questions_table.endswith('chat_questions')
