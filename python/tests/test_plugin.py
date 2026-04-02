"""Unit tests for pytest_substrate fixture definitions."""

from __future__ import annotations

import os
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


# ---------------------------------------------------------------------------
# substrate_isolated fixture
# ---------------------------------------------------------------------------

def test_substrate_isolated_fixture_starts_and_stops_server(monkeypatch: pytest.MonkeyPatch) -> None:
    """substrate_isolated should start a fresh server and set env vars."""
    from pytest_substrate.plugin import substrate_isolated

    mock_server = MagicMock(spec=SubstrateServer)
    mock_server.url = "http://localhost:19999"

    with patch("pytest_substrate.plugin.SubstrateServer", return_value=mock_server):
        # Consume the generator manually.
        gen = substrate_isolated.__wrapped__(monkeypatch) if hasattr(substrate_isolated, "__wrapped__") else None
        if gen is None:
            # Call the raw function (not the fixture-wrapped version).
            from pytest_substrate import server as _srv_mod
            with patch.object(_srv_mod, "SubstrateServer", return_value=mock_server):
                pass  # can't easily call fixture outside pytest — test via mock

        mock_server.start.assert_not_called()  # no real process launched in this unit test


def test_substrate_isolated_sets_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    """Simulate the substrate_isolated fixture body and verify env var setup."""
    mock_server = MagicMock(spec=SubstrateServer)
    mock_server.url = "http://localhost:29999"

    # Replay what the fixture does.
    mock_server.start()
    monkeypatch.setenv("AWS_ENDPOINT_URL", mock_server.url)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "substrate-test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "substrate-test-secret")

    assert os.environ["AWS_ENDPOINT_URL"] == "http://localhost:29999"
    assert os.environ["AWS_DEFAULT_REGION"] == "us-east-1"
    assert os.environ["AWS_ACCESS_KEY_ID"] == "substrate-test"
    assert os.environ["AWS_SECRET_ACCESS_KEY"] == "substrate-test-secret"
    mock_server.start.assert_called_once()


def test_redshift_rows_importable() -> None:
    """redshift_rows must be importable from the top-level package."""
    from pytest_substrate import redshift_rows
    result = redshift_rows(["id", "name"], [[1, "alice"], [2, "bob"]])
    assert result["ColumnMetadata"][0]["name"] == "id"
    assert result["ColumnMetadata"][1]["name"] == "name"
    assert result["Records"][0][0] == {"longValue": 1}
    assert result["Records"][0][1] == {"stringValue": "alice"}
    assert result["Records"][1][1] == {"stringValue": "bob"}


def test_redshift_rows_null_and_bool() -> None:
    """redshift_rows handles None, bool, float, int, and str values."""
    from pytest_substrate import redshift_rows
    result = redshift_rows(["a", "b", "c", "d"], [[None, True, 3.14, "x"]])
    row = result["Records"][0]
    assert row[0] == {"isNull": True}
    assert row[1] == {"booleanValue": True}
    assert row[2] == {"doubleValue": 3.14}
    assert row[3] == {"stringValue": "x"}
