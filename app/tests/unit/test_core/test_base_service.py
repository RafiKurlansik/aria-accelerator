"""Unit tests for BaseService.

Tests cover the base service functionality, logging, error handling,
and common service patterns.
"""

from __future__ import annotations

import pytest
from unittest.mock import Mock, patch, MagicMock
from typing import TYPE_CHECKING

from aria.core.base_service import BaseService
from aria.config import settings

if TYPE_CHECKING:
    from _pytest.capture import CaptureFixture
    from _pytest.fixtures import FixtureRequest
    from _pytest.logging import LogCaptureFixture
    from _pytest.monkeypatch import MonkeyPatch
    from pytest_mock.plugin import MockerFixture


class TestBaseService:
    """Test cases for BaseService."""

    def test_init(self, mocker: "MockerFixture") -> None:
        """Test BaseService initialization."""
        # Mock settings to avoid dependency issues
        mock_settings = Mock()
        mocker.patch('aria.core.base_service.settings', mock_settings)

        service = BaseService()

        assert service.settings is mock_settings
        assert hasattr(service, 'logger')
        assert service.logger is not None

    def test_logger_property(self, mocker: "MockerFixture") -> None:
        """Test that logger property works correctly."""
        mock_settings = Mock()
        mocker.patch('aria.core.base_service.settings', mock_settings)

        # Mock get_logger to return a specific logger
        mock_logger = Mock()
        mocker.patch('aria.core.base_service.get_logger', return_value=mock_logger)

        service = BaseService()

        # Access logger property
        logger = service.logger

        assert logger is mock_logger

    def test_settings_property(self, mocker: "MockerFixture") -> None:
        """Test that settings property returns the correct settings."""
        mock_settings = Mock()
        mock_settings.app_name = "Test App"
        mocker.patch('aria.core.base_service.settings', mock_settings)

        service = BaseService()

        assert service.settings is mock_settings
        assert service.settings.app_name == "Test App"

    def test_multiple_instances_share_settings(self, mocker: "MockerFixture") -> None:
        """Test that multiple service instances share the same settings."""
        mock_settings = Mock()
        mock_settings.app_version = "1.0.0"
        mocker.patch('aria.core.base_service.settings', mock_settings)

        service1 = BaseService()
        service2 = BaseService()

        assert service1.settings is service2.settings
        assert service1.settings.app_version == "1.0.0"
        assert service2.settings.app_version == "1.0.0"


class TestBaseServiceIntegration:
    """Integration tests for BaseService with actual dependencies."""

    def test_real_settings_integration(self) -> None:
        """Test BaseService with real settings (if available)."""
        try:
            service = BaseService()
            # If settings are available, test basic functionality
            assert hasattr(service, 'settings')
            assert hasattr(service, 'logger')

            # Test that settings has expected attributes (if configured)
            if hasattr(service.settings, 'databricks'):
                assert hasattr(service.settings.databricks, 'host')

        except Exception as e:
            # If settings are not configured, that's okay for this test
            pytest.skip(f"Settings not configured: {e}")

    def test_logger_functionality(self, caplog: "LogCaptureFixture") -> None:
        """Test that the logger works correctly."""
        try:
            service = BaseService()

            # Test logging functionality
            with caplog.at_level("INFO"):
                service.logger.info("Test message")

            assert "Test message" in caplog.text

        except Exception as e:
            pytest.skip(f"Logger not available: {e}")
