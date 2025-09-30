#!/usr/bin/env python3
"""Integration test to verify imports work correctly."""

import sys
import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from _pytest.capture import CaptureFixture
    from _pytest.fixtures import FixtureRequest
    from _pytest.logging import LogCaptureFixture
    from _pytest.monkeypatch import MonkeyPatch
    from pytest_mock.plugin import MockerFixture

import pytest

# Ensure PYTHONPATH is set correctly for testing
src_path = Path(__file__).parent.parent / "src"
if "PYTHONPATH" not in os.environ:
    os.environ["PYTHONPATH"] = str(src_path)
    sys.path.insert(0, str(src_path))


def test_basic_imports() -> None:
    """Test that basic dependencies can be imported."""
    try:
        import uvicorn
        assert uvicorn is not None
    except ImportError:
        pytest.fail("Failed to import uvicorn")

    try:
        import fastapi
        assert fastapi is not None
    except ImportError:
        pytest.fail("Failed to import fastapi")

    try:
        import pydantic
        assert pydantic is not None
    except ImportError:
        pytest.fail("Failed to import pydantic")

    try:
        import pandas
        assert pandas is not None
    except ImportError:
        pytest.fail("Failed to import pandas")


def test_databricks_imports() -> None:
    """Test that Databricks SDK can be imported."""
    try:
        from databricks.sdk import WorkspaceClient
        assert WorkspaceClient is not None
    except ImportError:
        pytest.fail("Failed to import Databricks SDK")


def test_aria_core_imports() -> None:
    """Test that ARIA core modules can be imported."""
    try:
        from aria.core import get_logger, BaseService
        assert get_logger is not None
        assert BaseService is not None
    except ImportError as e:
        pytest.fail(f"Failed to import ARIA core modules: {e}")


def test_aria_config_imports() -> None:
    """Test that ARIA configuration can be imported."""
    try:
        from aria.config import settings
        assert settings is not None
    except ImportError as e:
        pytest.fail(f"Failed to import ARIA config: {e}")


def test_aria_services_imports() -> None:
    """Test that ARIA services can be imported."""
    try:
        from aria.services.question_extraction import QuestionExtractionService
        from aria.services.answer_generation import AnswerGenerationService
        from aria.services.chat_service import ChatService
        
        assert QuestionExtractionService is not None
        assert AnswerGenerationService is not None
        assert ChatService is not None
    except ImportError as e:
        pytest.fail(f"Failed to import ARIA services: {e}")


def test_aria_api_imports() -> None:
    """Test that ARIA API can be imported."""
    try:
        from aria.api.app import app
        assert app is not None
    except ImportError as e:
        pytest.fail(f"Failed to import ARIA API: {e}")


def test_configuration_loading() -> None:
    """Test that configuration loads without errors."""
    try:
        from aria.config import settings
        
        # Test basic configuration access
        assert hasattr(settings, 'databricks')
        assert hasattr(settings, 'app')
        assert hasattr(settings, 'tracking')
        
        # Test model configuration
        assert hasattr(settings, 'question_extraction_model')
        assert hasattr(settings, 'answer_generation_model')
        
    except Exception as e:
        pytest.fail(f"Failed to load configuration: {e}")


if __name__ == "__main__":
    """Allow running this test directly for debugging."""
    print("Testing imports...")
    print(f"PYTHONPATH: {os.environ.get('PYTHONPATH', 'Not set')}")
    print(f"Current directory: {os.getcwd()}")
    print(f"Source path: {src_path}")
    print(f"Source path exists: {src_path.exists()}")

    # Run tests manually
    try:
        test_basic_imports()
        print("✅ Basic imports test passed")
    except Exception as e:
        print(f"❌ Basic imports test failed: {e}")

    try:
        test_databricks_imports()
        print("✅ Databricks imports test passed")
    except Exception as e:
        print(f"❌ Databricks imports test failed: {e}")

    try:
        test_aria_core_imports()
        print("✅ ARIA core imports test passed")
    except Exception as e:
        print(f"❌ ARIA core imports test failed: {e}")

    try:
        test_aria_config_imports()
        print("✅ ARIA config imports test passed")
    except Exception as e:
        print(f"❌ ARIA config imports test failed: {e}")

    try:
        test_aria_services_imports()
        print("✅ ARIA services imports test passed")
    except Exception as e:
        print(f"❌ ARIA services imports test failed: {e}")

    try:
        test_aria_api_imports()
        print("✅ ARIA API imports test passed")
    except Exception as e:
        print(f"❌ ARIA API imports test failed: {e}")

    try:
        test_configuration_loading()
        print("✅ Configuration loading test passed")
    except Exception as e:
        print(f"❌ Configuration loading test failed: {e}")

    print("Import testing completed!")
