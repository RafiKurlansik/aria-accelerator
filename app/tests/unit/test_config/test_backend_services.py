"""Tests for unified configuration functionality."""

import pytest
import yaml
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
from typing import TYPE_CHECKING

from aria.config import settings, UnifiedSettings

if TYPE_CHECKING:
    from _pytest.capture import CaptureFixture
    from _pytest.fixtures import FixtureRequest
    from _pytest.logging import LogCaptureFixture
    from _pytest.monkeypatch import MonkeyPatch
    from pytest_mock.plugin import MockerFixture


class TestUnifiedConfiguration:
    """Test cases for UnifiedSettings."""
    
    def test_settings_models_accessible(self) -> None:
        """Test that model settings are accessible through unified config."""
        # Test that all model properties are accessible
        assert hasattr(settings, 'question_extraction_model')
        assert hasattr(settings, 'answer_generation_model') 
        assert hasattr(settings, 'document_checker_model')
        assert hasattr(settings, 'chat_model')
        
        # Test that models are strings
        assert isinstance(settings.question_extraction_model, str)
        assert isinstance(settings.answer_generation_model, str)
        assert isinstance(settings.document_checker_model, str)
        assert isinstance(settings.chat_model, str)
    
    def test_databricks_settings_accessible(self) -> None:
        """Test that Databricks settings are accessible."""
        assert hasattr(settings, 'databricks')
        assert hasattr(settings.databricks, 'host')
        assert hasattr(settings.databricks, 'warehouse_id')
        assert hasattr(settings.databricks, 'get_model_endpoint_url')
        
        # Test that get_model_endpoint_url is callable
        assert callable(settings.databricks.get_model_endpoint_url)
    
    def test_service_config_accessible(self) -> None:
        """Test that service configurations are accessible."""
        assert hasattr(settings, 'get_service_config')
        assert callable(settings.get_service_config)
        
        # Test getting service configs
        qe_config = settings.get_service_config('question_extraction')
        assert qe_config is not None
        
        ag_config = settings.get_service_config('answer_generation')  
        assert ag_config is not None
    
    def test_auth_headers_accessible(self) -> None:
        """Test that authentication headers are accessible."""
        assert hasattr(settings, 'get_auth_headers')
        assert callable(settings.get_auth_headers)
        
        # Test that it returns headers (may be empty if not configured)
        headers = settings.get_auth_headers()
        assert isinstance(headers, dict)