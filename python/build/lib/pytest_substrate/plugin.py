"""pytest fixtures for substrate integration tests."""

from __future__ import annotations

import pytest

from pytest_substrate.server import SubstrateServer


@pytest.fixture(scope="session")
def substrate_server() -> SubstrateServer:  # type: ignore[misc]
    """Session-scoped fixture that starts one substrate server for the whole test run."""
    server = SubstrateServer()
    server.start()
    yield server  # type: ignore[misc]
    server.stop()


@pytest.fixture()
def substrate(substrate_server: SubstrateServer, monkeypatch: pytest.MonkeyPatch) -> SubstrateServer:  # type: ignore[misc]
    """Function-scoped fixture that resets substrate state and configures boto3 env vars.

    Usage::

        def test_something(substrate):
            s3 = boto3.client("s3")   # hits substrate automatically
            s3.create_bucket(Bucket="my-bucket")
            ...
    """
    substrate_server.reset_state()
    monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_server.url)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "substrate-test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "substrate-test-secret")
    yield substrate_server  # type: ignore[misc]
