"""SubstrateServer — manages a substrate process for use in tests."""

from __future__ import annotations

import datetime
import json
import os
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# The body field (and DELETE query parameter) each result-seeding endpoint keys a seed by.
# Athena and Redshift Data take ``sql``; Timestream's Query seed takes ``queryString``, the
# member name of the Query request it answers, and ignores ``sql`` entirely.
_SEED_QUERY_FIELD: dict[str, str] = {"timestream-query": "queryString"}

# Service names accepted as aliases for a seeding endpoint's path segment.
_SEED_SERVICE_ALIASES: dict[str, str] = {"timestream": "timestream-query"}

#: The request-ID seed a server this package launches gets unless told otherwise, so a test's
#: request IDs — and every identifier substrate mints from them — are the same on every run.
#: Pass ``seed=0`` for the unseeded, wall-clock IDs substrate assigns by default (#1598).
DEFAULT_SEED = 1

#: The instant ``deterministic=True`` pins the simulated clock at.
DEFAULT_EPOCH = "2026-01-01T00:00:00Z"

# Distinguishes "not passed" from None in :meth:`SubstrateServer.set_run`.
_UNSET: object = object()


def _rfc3339(when: datetime.datetime | str) -> str:
    """Render ``when`` as the RFC 3339 string substrate's control endpoints accept.

    A naive datetime is read as UTC; a string is passed through for substrate to validate.
    """
    if isinstance(when, str):
        return when
    if when.tzinfo is None:
        when = when.replace(tzinfo=datetime.UTC)
    return when.astimezone(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _seed_endpoint(service: str) -> tuple[str, str]:
    """Return the ``/v1/{segment}/results`` path segment and the query field for ``service``."""
    segment = _SEED_SERVICE_ALIASES.get(service, service)
    return segment, _SEED_QUERY_FIELD.get(segment, "sql")


class SubstrateServer:
    """Lifecycle manager for a substrate server process.

    Typical usage::

        server = SubstrateServer()
        server.start()          # launches process, waits for /health
        server.reset_state()    # wipe all recorded state
        server.stop()           # terminate the process

    The server is launched with ``SUBSTRATE_RUN_SEED`` set to ``seed`` (default
    :data:`DEFAULT_SEED`), so its request IDs and the identifiers minted from them repeat from
    run to run; ``seed=0`` leaves them unseeded. ``epoch`` additionally freezes the simulated
    clock at that instant (``SUBSTRATE_RUN_EPOCH``), and :meth:`reset_state` returns it there,
    so two runs of the same calls answer byte-identical bodies (#1598).
    """

    def __init__(self, seed: int = DEFAULT_SEED, epoch: datetime.datetime | str | None = None) -> None:
        self.port: int = self._free_port()
        self.url: str = f"http://localhost:{self.port}"
        self.seed: int = seed
        self.epoch: str | None = _rfc3339(epoch) if epoch is not None else None
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
            env=self._env(),
        )
        self._wait_healthy()

    def _env(self) -> dict[str, str]:
        """Return the environment the server process is launched with: this one, plus the run settings."""
        env = dict(os.environ)
        env["SUBSTRATE_RUN_SEED"] = str(self.seed)
        if self.epoch is not None:
            env["SUBSTRATE_RUN_EPOCH"] = self.epoch
        return env

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
        """Reset all substrate state (equivalent to a fresh start).

        The reset also rewinds the request-ID sequence and returns a pinned clock to its epoch, so the
        next test sees the identifiers and timestamps a freshly started server would give it.
        """
        req = urllib.request.Request(
            f"{self.url}/v1/state/reset",
            method="POST",
            data=b"",
        )
        try:
            urllib.request.urlopen(req, timeout=5)  # nosemgrep
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"substrate state reset failed: {exc.code} {exc.reason}") from exc

    def now(self) -> datetime.datetime:
        """Return substrate's simulated clock (``GET /v1/control/time``) as an aware UTC datetime."""
        with urllib.request.urlopen(f"{self.url}/v1/control/time", timeout=5) as resp:  # nosemgrep
            body = json.loads(resp.read())
        return _parse_rfc3339(body["simulated_time"])

    def set_time(self, when: datetime.datetime | str) -> datetime.datetime:
        """Set substrate's simulated clock (``POST /v1/control/time``) and return the clock it reports.

        ``when`` is an aware :class:`~datetime.datetime` or an RFC 3339 string such as
        ``"2026-11-16T12:00:00Z"``; a naive datetime is read as UTC. Setting the clock is how a
        test reaches a future instant — a schedule's due time, a lease's expiry — without waiting
        for it. The clock keeps running from the instant set.
        """
        if isinstance(when, datetime.datetime):
            if when.tzinfo is None:
                when = when.replace(tzinfo=datetime.UTC)
            when = when.astimezone(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        body = json.dumps({"time": when}).encode()
        req = urllib.request.Request(
            f"{self.url}/v1/control/time",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:  # nosemgrep
                reported = json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"substrate set_time failed: {exc.code} {exc.read().decode(errors='replace')}") from exc
        return _parse_rfc3339(reported["simulated_time"])

    def freeze_time(self, when: datetime.datetime | str | None = None) -> datetime.datetime:
        """Stop substrate's simulated clock — at ``when`` exactly, if given — and return the clock it reports.

        Every timestamp substrate renders is then that one instant until :meth:`unfreeze_time`;
        :meth:`set_time` and :meth:`advance_time` still move it, and it stays frozen.
        """
        body: dict[str, object] = {"frozen": True}
        if when is not None:
            body["time"] = _rfc3339(when)
        return _parse_rfc3339(self._post_json("/v1/control/time", body, "freeze_time")["simulated_time"])

    def unfreeze_time(self) -> datetime.datetime:
        """Resume substrate's simulated clock from where it was frozen, and return the clock it reports."""
        return _parse_rfc3339(self._post_json("/v1/control/time", {"frozen": False}, "unfreeze_time")["simulated_time"])

    def run_settings(self) -> dict:
        """Return the deterministic-run settings (``GET /v1/control/run``).

        The dict carries ``seed`` (0 when unseeded), ``epoch`` (present only while the clock is
        pinned) and ``drawn``, the number of request IDs assigned since the seed was set or the
        state last reset.
        """
        with urllib.request.urlopen(f"{self.url}/v1/control/run", timeout=5) as resp:  # nosemgrep
            return json.loads(resp.read())

    def set_run(self, seed: object = _UNSET, epoch: object = _UNSET) -> dict:
        """Change the deterministic-run settings of the running server (``POST /v1/control/run``).

        ``seed`` reseeds the request IDs and restarts their sequence; 0 unseeds them. ``epoch``
        pins the simulated clock at an instant (a datetime or an RFC 3339 string) and makes
        :meth:`reset_state` return it there; ``None`` unpins it. An argument not passed is left
        as it is. Returns the settings the server reports afterwards.
        """
        body: dict[str, object] = {}
        if seed is not _UNSET:
            body["seed"] = seed
        if epoch is not _UNSET:
            body["epoch"] = "" if epoch is None else _rfc3339(epoch)  # type: ignore[arg-type]
        return self._post_json("/v1/control/run", body, "set_run")

    def _post_json(self, path: str, body: dict, what: str) -> dict:
        """POST ``body`` as JSON to ``path`` and return the decoded reply, raising on an HTTP error."""
        req = urllib.request.Request(
            f"{self.url}{path}",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:  # nosemgrep
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"substrate {what} failed: {exc.code} {exc.read().decode(errors='replace')}") from exc

    def advance_time(self, delta: datetime.timedelta | None = None, **kwargs: float) -> datetime.datetime:
        """Move substrate's simulated clock forward and return the clock it reports.

        Pass a :class:`~datetime.timedelta`, or its keyword arguments directly
        (``advance_time(hours=1)``). Substrate has no advance endpoint, so this reads the clock and
        sets it to that instant plus ``delta``.
        """
        if delta is None:
            delta = datetime.timedelta(**kwargs)
        elif kwargs:
            raise TypeError("pass either a timedelta or its keyword arguments, not both")
        if delta < datetime.timedelta(0):
            raise ValueError("advance_time moves the clock forward; use set_time to move it back")
        return self.set_time(self.now() + delta)

    def seed_result(self, service: str, result: dict, sql: str = "*") -> None:
        """Seed a query result for the given service and SQL pattern.

        The ``result`` dict must match the service's result shape
        (e.g. ``{"ColumnMetadata": [...], "Records": [[...]]}`` for redshift-data).
        Use :func:`redshift_rows` to build a Redshift-compatible result dict.
        The ``sql`` pattern defaults to ``"*"`` (wildcard — matches any statement).

        For ``"timestream-query"`` (or its alias ``"timestream"``) the pattern is sent as
        ``queryString``, the field that endpoint reads, and the result shape is
        ``{"Rows": [...], "ColumnInfo": [...]}``.
        """
        segment, field = _seed_endpoint(service)
        body = json.dumps({field: sql, "result": result}).encode()
        req = urllib.request.Request(
            f"{self.url}/v1/{segment}/results",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:  # nosemgrep
            resp.read()

    def set_status(self, service: str, status: str, error_message: str = "") -> None:
        """Set the default statement execution status for the given service.

        Valid statuses for redshift-data: ``"FINISHED"``, ``"FAILED"``,
        ``"ABORTED"``, ``"STARTED"``.  Pass ``error_message`` to attach an
        error description when ``status="FAILED"``.
        """
        body = json.dumps({"status": status, "errorMessage": error_message}).encode()
        req = urllib.request.Request(
            f"{self.url}/v1/{service}/status",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:  # nosemgrep
            resp.read()

    def clear_seeds(self, service: str, sql: str | None = None) -> None:
        """Clear seeded results for the given service.

        Pass ``sql`` to remove only that specific SQL pattern; omit it to
        clear all seeded results for the service.
        """
        segment, field = _seed_endpoint(service)
        url = f"{self.url}/v1/{segment}/results"
        if sql is not None:
            url += f"?{field}={urllib.parse.quote(sql, safe='')}"
        req = urllib.request.Request(url, method="DELETE")
        with urllib.request.urlopen(req) as resp:  # nosemgrep
            resp.read()

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
                urllib.request.urlopen(f"{self.url}/health", timeout=1)  # nosemgrep
                return
            except Exception as exc:  # noqa: BLE001 - any startup error means "not ready yet"; keep polling
                last_exc = exc
                time.sleep(0.05)
        raise RuntimeError(
            f"substrate did not become healthy within {timeout}s. "
            f"Last error: {last_exc}"
        )


def _parse_rfc3339(value: str) -> datetime.datetime:
    """Parse the RFC 3339 instant substrate's clock endpoint reports into an aware UTC datetime.

    Go renders nanoseconds, which :meth:`datetime.datetime.fromisoformat` does not accept, so the
    fraction is cut to microseconds first.
    """
    value = value.replace("Z", "+00:00")
    if "." in value:
        head, rest = value.split(".", 1)
        n = 0
        while n < len(rest) and rest[n].isdigit():
            n += 1
        value = f"{head}.{rest[:n][:6].ljust(6, '0')}{rest[n:]}"
    return datetime.datetime.fromisoformat(value).astimezone(datetime.UTC)
