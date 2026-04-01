"""SubstrateServer — manages a substrate process for use in tests."""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path


class SubstrateServer:
    """Lifecycle manager for a substrate server process.

    Typical usage::

        server = SubstrateServer()
        server.start()          # launches process, waits for /health
        server.reset_state()    # wipe all recorded state
        server.stop()           # terminate the process
    """

    def __init__(self) -> None:
        self.port: int = self._free_port()
        self.url: str = f"http://localhost:{self.port}"
        self._process: subprocess.Popen[bytes] | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the substrate server and block until healthy."""
        binary = self._find_binary()
        self._process = subprocess.Popen(
            [binary, "server", "--address", f"localhost:{self.port}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self._wait_healthy()

    def stop(self) -> None:
        """Terminate the substrate server process."""
        if self._process is not None:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
            self._process = None

    def reset_state(self) -> None:
        """Reset all substrate state (equivalent to a fresh start)."""
        req = urllib.request.Request(
            f"{self.url}/v1/state/reset",
            method="POST",
            data=b"",
        )
        try:
            urllib.request.urlopen(req, timeout=5)
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"substrate state reset failed: {exc.code} {exc.reason}") from exc

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _find_binary() -> str:
        """Locate the substrate binary.

        Search order:
        1. ``SUBSTRATE_BINARY`` environment variable
        2. ``~/src/substrate/bin/substrate``
        3. ``substrate`` on PATH
        """
        if env := os.environ.get("SUBSTRATE_BINARY"):
            return env
        candidate = Path.home() / "src" / "substrate" / "bin" / "substrate"
        if candidate.exists():
            return str(candidate)
        found = shutil.which("substrate")
        if found:
            return found
        raise RuntimeError(
            "substrate binary not found. Set SUBSTRATE_BINARY env var, "
            "place the binary at ~/src/substrate/bin/substrate, "
            "or add it to PATH."
        )

    @staticmethod
    def _free_port() -> int:
        """Return a free TCP port on localhost."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            return int(s.getsockname()[1])

    def _wait_healthy(self, timeout: float = 10.0) -> None:
        """Poll GET /health until substrate responds or timeout expires."""
        deadline = time.monotonic() + timeout
        last_exc: Exception | None = None
        while time.monotonic() < deadline:
            try:
                urllib.request.urlopen(f"{self.url}/health", timeout=1)
                return
            except Exception as exc:
                last_exc = exc
                time.sleep(0.05)
        raise RuntimeError(
            f"substrate did not become healthy within {timeout}s. "
            f"Last error: {last_exc}"
        )
