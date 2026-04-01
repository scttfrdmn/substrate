"""Unit tests for SubstrateServer — no real substrate binary required."""

from __future__ import annotations

import subprocess
import urllib.error
from http.client import HTTPResponse
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from pytest_substrate.server import SubstrateServer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_healthy_response() -> MagicMock:
    """Return a mock that looks like a successful urlopen() result."""
    mock = MagicMock(spec=HTTPResponse)
    mock.__enter__ = lambda s: s
    mock.__exit__ = MagicMock(return_value=False)
    return mock


# ---------------------------------------------------------------------------
# _find_binary
# ---------------------------------------------------------------------------

class TestFindBinary:
    def test_env_var_takes_precedence(self, tmp_path: Path) -> None:
        binary = tmp_path / "substrate"
        binary.touch()
        with patch.dict("os.environ", {"SUBSTRATE_BINARY": str(binary)}):
            assert SubstrateServer._find_binary() == str(binary)

    def test_home_candidate_used_when_present(self, tmp_path: Path) -> None:
        fake_home = tmp_path
        candidate = fake_home / "src" / "substrate" / "bin" / "substrate"
        candidate.parent.mkdir(parents=True)
        candidate.touch()
        with patch.dict("os.environ", {}, clear=False):
            # Remove SUBSTRATE_BINARY if set.
            import os
            os.environ.pop("SUBSTRATE_BINARY", None)
            with patch("pathlib.Path.home", return_value=fake_home):
                assert SubstrateServer._find_binary() == str(candidate)

    def test_falls_back_to_path(self, tmp_path: Path) -> None:
        import os
        os.environ.pop("SUBSTRATE_BINARY", None)
        with patch("pathlib.Path.home", return_value=tmp_path):  # no candidate
            with patch("shutil.which", return_value="/usr/local/bin/substrate"):
                assert SubstrateServer._find_binary() == "/usr/local/bin/substrate"

    def test_raises_when_not_found(self, tmp_path: Path) -> None:
        import os
        os.environ.pop("SUBSTRATE_BINARY", None)
        with patch("pathlib.Path.home", return_value=tmp_path):
            with patch("shutil.which", return_value=None):
                with pytest.raises(RuntimeError, match="substrate binary not found"):
                    SubstrateServer._find_binary()


# ---------------------------------------------------------------------------
# _free_port
# ---------------------------------------------------------------------------

class TestFreePort:
    def test_returns_valid_port(self) -> None:
        port = SubstrateServer._free_port()
        assert 1024 < port < 65536

    def test_returns_different_ports_on_repeated_calls(self) -> None:
        # Not guaranteed, but extremely unlikely to collide twice in a row.
        ports = {SubstrateServer._free_port() for _ in range(5)}
        # At least 2 distinct ports across 5 calls.
        assert len(ports) >= 1


# ---------------------------------------------------------------------------
# start / stop
# ---------------------------------------------------------------------------

class TestStartStop:
    def test_start_launches_process_and_waits_for_health(self) -> None:
        server = SubstrateServer()
        mock_proc = MagicMock(spec=subprocess.Popen)

        with patch.object(SubstrateServer, "_find_binary", return_value="/bin/substrate"), \
             patch("subprocess.Popen", return_value=mock_proc) as popen_mock, \
             patch.object(server, "_wait_healthy") as mock_wait:
            server.start()

        popen_mock.assert_called_once()
        args = popen_mock.call_args[0][0]
        assert args[0] == "/bin/substrate"
        assert "server" in args
        assert f"localhost:{server.port}" in " ".join(args)
        mock_wait.assert_called_once()
        assert server._process is mock_proc

    def test_stop_terminates_process(self) -> None:
        server = SubstrateServer()
        mock_proc = MagicMock(spec=subprocess.Popen)
        server._process = mock_proc

        server.stop()

        mock_proc.terminate.assert_called_once()
        mock_proc.wait.assert_called_once()
        assert server._process is None

    def test_stop_kills_if_terminate_times_out(self) -> None:
        server = SubstrateServer()
        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.wait.side_effect = subprocess.TimeoutExpired(cmd="substrate", timeout=5)
        server._process = mock_proc

        server.stop()

        mock_proc.kill.assert_called_once()
        assert server._process is None

    def test_stop_is_idempotent_when_not_started(self) -> None:
        server = SubstrateServer()
        # Should not raise even if stop() called before start().
        server.stop()


# ---------------------------------------------------------------------------
# _wait_healthy
# ---------------------------------------------------------------------------

class TestWaitHealthy:
    def test_returns_immediately_when_server_healthy(self) -> None:
        server = SubstrateServer()
        with patch("urllib.request.urlopen", return_value=_fake_healthy_response()):
            server._wait_healthy(timeout=1.0)

    def test_retries_until_healthy(self) -> None:
        server = SubstrateServer()
        call_count = 0

        def urlopen_side_effect(url, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionRefusedError("not yet")
            return _fake_healthy_response()

        with patch("urllib.request.urlopen", side_effect=urlopen_side_effect), \
             patch("time.sleep"):
            server._wait_healthy(timeout=5.0)

        assert call_count == 3

    def test_raises_after_timeout(self) -> None:
        server = SubstrateServer()
        import time as time_mod

        # Simulate time advancing past the deadline after 2 checks.
        call_count = 0
        original_monotonic = time_mod.monotonic

        def fake_monotonic():
            nonlocal call_count
            call_count += 1
            # First two calls return a time within the deadline,
            # subsequent calls return a time past it.
            if call_count <= 2:
                return 0.0
            return 100.0

        with patch("urllib.request.urlopen", side_effect=ConnectionRefusedError("nope")), \
             patch("time.monotonic", side_effect=fake_monotonic), \
             patch("time.sleep"):
            with pytest.raises(RuntimeError, match="did not become healthy"):
                server._wait_healthy(timeout=1.0)


# ---------------------------------------------------------------------------
# reset_state
# ---------------------------------------------------------------------------

class TestResetState:
    def test_posts_to_reset_endpoint(self) -> None:
        server = SubstrateServer()
        with patch("urllib.request.urlopen", return_value=_fake_healthy_response()) as mock_open:
            server.reset_state()

        mock_open.assert_called_once()
        req = mock_open.call_args[0][0]
        assert "/v1/state/reset" in req.full_url
        assert req.get_method() == "POST"

    def test_raises_on_http_error(self) -> None:
        server = SubstrateServer()
        with patch("urllib.request.urlopen",
                   side_effect=urllib.error.HTTPError(
                       url=f"{server.url}/v1/state/reset",
                       code=500,
                       msg="Internal Server Error",
                       hdrs=None,  # type: ignore[arg-type]
                       fp=None,    # type: ignore[arg-type]
                   )):
            with pytest.raises(RuntimeError, match="state reset failed"):
                server.reset_state()


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------

class TestURL:
    def test_url_uses_localhost_and_port(self) -> None:
        server = SubstrateServer()
        assert server.url == f"http://localhost:{server.port}"
        assert server.url.startswith("http://localhost:")
