"""Unit tests for `DocumentCheckerService` and related configuration.

These tests validate that configuration exposes the document checker model
and that the service can call the Databricks endpoint and return Markdown.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import json
import pytest

from aria.config import settings
from aria.services.document_checker import DocumentCheckerService

if TYPE_CHECKING:
    from _pytest.capture import CaptureFixture  # noqa: F401
    from _pytest.fixtures import FixtureRequest  # noqa: F401
    from _pytest.logging import LogCaptureFixture  # noqa: F401
    from _pytest.monkeypatch import MonkeyPatch  # noqa: F401
    from pytest_mock.plugin import MockerFixture  # noqa: F401


def test_document_checker_config_exposed() -> None:
    """Ensure document checker model and endpoint are exposed via settings."""
    model = settings.document_checker_model
    assert isinstance(model, str) and model

    endpoint = settings.databricks.get_model_endpoint_url(model)
    assert model in endpoint
    assert endpoint.endswith("/invocations")


def test_document_checker_success(
    monkeypatch: "MonkeyPatch",
    mocker: "MockerFixture",
) -> None:
    """Service should return Markdown when the API responds successfully."""
    # Arrange
    service = DocumentCheckerService()

    # Mock auth headers
    mocker.patch.object(settings, "get_auth_headers", return_value={"Authorization": "Bearer TOKEN", "Content-Type": "application/json"})

    # Mock the get_model_endpoint_url method at the class level
    from aria.config.settings import DatabricksSettings
    mocker.patch.object(DatabricksSettings, "get_model_endpoint_url", return_value="https://example.databricks/serving-endpoints/test-model/invocations")

    # Mock requests.post - return JSON format that service expects
    fake_response = {
        "choices": [
            {"message": {"content": '{"results": [{"claim": "Test claim", "verdict": "supported", "reason": "Test reason"}]}'}}
        ]
    }

    class _Resp:
        status_code = 200

        def json(self) -> dict:
            return fake_response

    mocker.patch("aria.services.document_checker.requests.post", return_value=_Resp())

    # Act
    ok, results, info = service.check_document(content="Some text")

    # Assert
    assert ok is True
    assert isinstance(results, list)
    assert len(results) > 0
    assert "model_used" in info


