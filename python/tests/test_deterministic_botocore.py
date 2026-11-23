"""Deterministic runs against a real substrate process (#1598).

Two fresh servers given the same calls answer the same identifiers, and with the clock
pinned the same timestamps too. The calls go through boto3, so the assertion is about what
a consumer's own client observes.
"""

from __future__ import annotations

import datetime

import pytest

from pytest_substrate import DEFAULT_EPOCH, SubstrateServer

boto3 = pytest.importorskip("boto3", reason="boto3 drives the calls whose answers are compared")
from botocore.config import Config  # noqa: E402  (imported after the skip guard)


def _client(service: str, server: SubstrateServer):  # type: ignore[no-untyped-def]
    return boto3.client(
        service,
        endpoint_url=server.url,
        region_name="us-east-1",
        aws_access_key_id="substrate-test",
        aws_secret_access_key="substrate-test-secret",
        config=Config(retries={"max_attempts": 1, "mode": "standard"}),
    )


def _observe(server: SubstrateServer) -> tuple:
    """Create a VPC, a KMS key and an IAM user, and return what substrate answered for each."""
    vpc = _client("ec2", server).create_vpc(CidrBlock="10.50.0.0/16")["Vpc"]["VpcId"]
    key = _client("kms", server).create_key(Description="det")["KeyMetadata"]
    user = _client("iam", server).create_user(UserName="det-user")["User"]
    return vpc, key["KeyId"], key["CreationDate"], user["UserId"], user["CreateDate"]


def _fresh(epoch: str | None) -> tuple:
    server = SubstrateServer(epoch=epoch)
    server.start()
    try:
        return _observe(server)
    finally:
        server.stop()


def test_two_fresh_pinned_servers_answer_identically() -> None:
    first, second = _fresh(DEFAULT_EPOCH), _fresh(DEFAULT_EPOCH)
    assert first == second
    epoch = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    assert first[2] == epoch, "KMS CreationDate is the pinned epoch"
    assert first[4] == epoch, "IAM CreateDate is the pinned epoch"


def test_seeding_alone_repeats_identifiers() -> None:
    first, second = _fresh(None), _fresh(None)
    assert first[0] == second[0], "the VPC ID derives from the seeded request ID"
    assert first[1] == second[1]
    assert first[3] == second[3]


@pytest.mark.substrate(deterministic=True)
def test_isolated_marker_pins_the_clock(substrate_isolated: SubstrateServer) -> None:
    assert substrate_isolated.run_settings()["epoch"] == DEFAULT_EPOCH
    assert substrate_isolated.now() == datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)


@pytest.mark.substrate(deterministic=True)
def test_shared_marker_pins_and_reset_rewinds(substrate: SubstrateServer) -> None:
    first = _observe(substrate)
    assert substrate.run_settings()["drawn"] == 3
    substrate.reset_state()
    assert substrate.run_settings()["drawn"] == 0
    assert _observe(substrate) == first, "a reset gives the identifiers a fresh server would"


def test_shared_server_is_unpinned_again_after_a_marked_test(substrate: SubstrateServer) -> None:
    settings = substrate.run_settings()
    assert "epoch" not in settings
    assert settings["seed"] == substrate.seed
