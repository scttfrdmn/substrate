"""pytest fixtures for substrate integration tests."""

from __future__ import annotations

import datetime
from typing import Any

import pytest

from pytest_substrate.server import DEFAULT_EPOCH, DEFAULT_SEED, SubstrateServer

_MARKER = "substrate"


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register the ini options that configure the session-scoped server."""
    parser.addini(
        "substrate_seed",
        f"request-ID seed for the session's substrate server; 0 is unseeded (default {DEFAULT_SEED})",
        default=str(DEFAULT_SEED),
    )
    parser.addini(
        "substrate_epoch",
        "RFC 3339 instant to pin the session server's clock at; empty leaves it running",
        default="",
    )
    parser.addini(
        "substrate_deterministic",
        f"pin the session server's clock at {DEFAULT_EPOCH} (substrate_epoch takes precedence)",
        type="bool",
        default=False,
    )


def pytest_configure(config: pytest.Config) -> None:
    """Register the ``substrate`` marker."""
    config.addinivalue_line(
        "markers",
        f"{_MARKER}(deterministic=False, epoch=None, seed=None): run settings for the substrate "
        "and substrate_isolated fixtures — deterministic=True pins the clock at "
        f"{DEFAULT_EPOCH}, epoch pins it at the instant given, seed seeds request IDs (0 unseeds them)",
    )


def _resolve_run(settings: dict[str, Any]) -> tuple[int | None, datetime.datetime | str | None]:
    """Return the (seed, epoch) a run-settings dict asks for; None means "not asked for"."""
    epoch = settings.get("epoch")
    if epoch is None and settings.get("deterministic"):
        epoch = DEFAULT_EPOCH
    seed = settings.get("seed")
    return (int(seed) if seed is not None else None), epoch


def _requested_run(request: pytest.FixtureRequest) -> dict[str, Any]:
    """Merge the run settings a test asked for: its ``substrate`` marker, then an indirect parameter.

    An indirect parameter is a dict of the marker's keyword arguments, or a bare bool meaning
    ``deterministic``.
    """
    settings: dict[str, Any] = {}
    node = getattr(request, "node", None)
    marker = node.get_closest_marker(_MARKER) if node is not None else None
    if marker is not None:
        settings.update(marker.kwargs)
    param = getattr(request, "param", None)
    if isinstance(param, bool):
        settings["deterministic"] = param
    elif isinstance(param, dict):
        settings.update(param)
    return settings


@pytest.fixture(scope="session")
def substrate_server(request: pytest.FixtureRequest) -> SubstrateServer:  # type: ignore[misc]
    """Session-scoped fixture that starts one substrate server for the whole test run.

    The server's request IDs are seeded (ini ``substrate_seed``, default 1), so identifiers
    repeat from run to run. Set ini ``substrate_deterministic = true``, or ``substrate_epoch``,
    to pin its clock as well; or parametrize it indirectly with ``{"deterministic": True}``.
    """
    config = request.config
    settings: dict[str, Any] = {"seed": config.getini("substrate_seed")}
    if config.getini("substrate_epoch"):
        settings["epoch"] = config.getini("substrate_epoch")
    if config.getini("substrate_deterministic"):
        settings["deterministic"] = True
    param = getattr(request, "param", None)
    if isinstance(param, bool):
        settings["deterministic"] = param
    elif isinstance(param, dict):
        settings.update(param)
    seed, epoch = _resolve_run(settings)
    server = SubstrateServer(seed=DEFAULT_SEED if seed is None else seed, epoch=epoch)
    server.start()
    yield server  # type: ignore[misc]
    server.stop()


def _set_aws_env(monkeypatch: pytest.MonkeyPatch, server: SubstrateServer) -> None:
    monkeypatch.setenv("AWS_ENDPOINT_URL", server.url)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "substrate-test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "substrate-test-secret")


@pytest.fixture()
def substrate(
    request: pytest.FixtureRequest,
    substrate_server: SubstrateServer,
    monkeypatch: pytest.MonkeyPatch,
) -> SubstrateServer:  # type: ignore[misc]
    """Function-scoped fixture that resets substrate state and configures boto3 env vars.

    The reset also rewinds the request-ID sequence, so each test sees the identifiers a fresh
    server would give it. Mark a test ``@pytest.mark.substrate(deterministic=True)`` (or
    ``epoch=...``, ``seed=...``) to pin the shared server's clock for that test alone; the
    server's own settings are restored afterwards.

    Usage::

        def test_something(substrate):
            s3 = boto3.client("s3")   # hits substrate automatically
            s3.create_bucket(Bucket="my-bucket")
            ...
    """
    seed, epoch = _resolve_run(_requested_run(request))
    changes: dict[str, Any] = {}
    if seed is not None:
        changes["seed"] = seed
    if epoch is not None:
        changes["epoch"] = epoch
    if changes:
        substrate_server.set_run(**changes)
    substrate_server.reset_state()
    _set_aws_env(monkeypatch, substrate_server)
    yield substrate_server  # type: ignore[misc]
    if changes:
        # Back to the session's own settings, so the next test is unaffected.
        restore: dict[str, Any] = {}
        if "seed" in changes:
            restore["seed"] = substrate_server.seed
        if "epoch" in changes:
            restore["epoch"] = substrate_server.epoch
        substrate_server.set_run(**restore)
        if "epoch" in changes and substrate_server.epoch is None:
            # Unpinning resumes the clock from the test's epoch; put it back on the wall
            # clock, where an unpinned session server's clock started.
            substrate_server.set_time(datetime.datetime.now(datetime.UTC))


@pytest.fixture()
def substrate_isolated(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> SubstrateServer:  # type: ignore[misc]
    """Function-scoped fixture that starts a fresh substrate server per test.

    Unlike :func:`substrate`, which resets state on a shared session-scoped
    server, this fixture launches and tears down a new process for each test.
    Use it when you need full process-level isolation (e.g. different startup
    flags, or when shared-server state leakage is a concern).

    The server is seeded like every server this package launches. Mark the test
    ``@pytest.mark.substrate(deterministic=True)`` — or pass ``epoch=`` or ``seed=`` — or
    parametrize the fixture indirectly with the same keys, to pin its clock from startup.

    Usage::

        @pytest.mark.substrate(deterministic=True)
        def test_something(substrate_isolated):
            s3 = boto3.client("s3")   # hits the isolated substrate
            s3.create_bucket(Bucket="my-bucket")
            ...
    """
    seed, epoch = _resolve_run(_requested_run(request))
    server = SubstrateServer(seed=DEFAULT_SEED if seed is None else seed, epoch=epoch)
    server.start()
    _set_aws_env(monkeypatch, server)
    yield server  # type: ignore[misc]
    server.stop()
