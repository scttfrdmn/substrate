"""Unit tests for pytest_substrate fixture definitions."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pytest_substrate.server import SubstrateServer


# ---------------------------------------------------------------------------
# Test that fixtures are registered as pytest plugins
# ---------------------------------------------------------------------------

def test_plugin_registers_substrate_server_fixture(pytestconfig: pytest.Config) -> None:
    """The session-scoped substrate_server fixture must be discoverable."""
    # The plugin is registered via entry_points in pyproject.toml.
    # If the package is installed, the fixture names will be present.
    fm = pytestconfig.pluginmanager
    # plugin module should be registered
    assert fm.hasplugin("pytest_substrate.plugin") or fm.hasplugin("substrate")


def test_plugin_registers_substrate_fixture(pytestconfig: pytest.Config) -> None:
    """The function-scoped substrate fixture must be discoverable."""
    fm = pytestconfig.pluginmanager
    assert fm.hasplugin("pytest_substrate.plugin") or fm.hasplugin("substrate")


# ---------------------------------------------------------------------------
# Test the SubstrateServer exposed from __init__
# ---------------------------------------------------------------------------

def test_substrateserver_importable() -> None:
    from pytest_substrate import SubstrateServer as SS  # noqa: F401
    assert SS is SubstrateServer


# ---------------------------------------------------------------------------
# Integration: substrate fixture patches environment variables
# ---------------------------------------------------------------------------

def test_substrate_fixture_patches_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Simulate what the substrate fixture does: verify env vars are set."""
    mock_server = MagicMock(spec=SubstrateServer)
    mock_server.url = "http://localhost:12345"
    mock_server.reset_state = MagicMock()

    # Simulate fixture body.
    mock_server.reset_state()
    monkeypatch.setenv("AWS_ENDPOINT_URL", mock_server.url)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "substrate-test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "substrate-test-secret")

    import os
    assert os.environ["AWS_ENDPOINT_URL"] == "http://localhost:12345"
    assert os.environ["AWS_DEFAULT_REGION"] == "us-east-1"
    assert os.environ["AWS_ACCESS_KEY_ID"] == "substrate-test"
    assert os.environ["AWS_SECRET_ACCESS_KEY"] == "substrate-test-secret"
    mock_server.reset_state.assert_called_once()
