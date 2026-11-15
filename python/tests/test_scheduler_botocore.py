"""boto3-driven EventBridge Scheduler coverage: the deployment-lease scenario (#1501, #1475).

aws-elastic-globus-compute's lease creates one-time ``at()`` schedules in a tagged schedule group,
whose universal targets write an SSM parameter and scale an Auto Scaling group to zero if no engine
renews them. Here the simulated clock is set past the lease's instant through
``POST /v1/control/time`` rather than waited for, so nothing depends on real elapsed time.
"""

from __future__ import annotations

import json
import urllib.request

import pytest

boto3 = pytest.importorskip("boto3", reason="boto3 is needed to drive the Scheduler REST-JSON path")
from botocore.config import Config  # noqa: E402  (imported after the skip guard)
from botocore.exceptions import ClientError  # noqa: E402

ROLE = "arn:aws:iam::123456789012:role/aegc/aegc-dev-scheduler-us-west-2"


def _set_time(url: str, when: str) -> None:
    req = urllib.request.Request(
        f"{url}/v1/control/time",
        data=json.dumps({"time": when}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(req, timeout=5)  # nosemgrep  # noqa: S310 - a local test server


@pytest.fixture()
def clients(substrate_isolated, monkeypatch):  # type: ignore[no-untyped-def]
    """boto3 Scheduler and SSM clients against a per-test substrate, retries off."""
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    cfg = Config(retries={"max_attempts": 1, "mode": "standard"})
    make = lambda svc: boto3.client(svc, endpoint_url=substrate_isolated.url, config=cfg)  # noqa: E731
    return substrate_isolated, make("scheduler"), make("ssm")


def test_lease_expires_when_the_clock_passes_it(clients) -> None:  # type: ignore[no-untyped-def]
    server, sch, ssm = clients
    _set_time(server.url, "2026-11-16T11:00:00Z")

    sch.create_schedule_group(Name="aegc-dev", Tags=[{"Key": "aegc:managed", "Value": "true"}])
    group = sch.get_schedule_group(Name="aegc-dev")
    assert group["State"] == "ACTIVE"
    tags = sch.list_tags_for_resource(ResourceArn=group["Arn"])["Tags"]
    assert tags == [{"Key": "aegc:managed", "Value": "true"}]

    for name, delete in (("lease-state", "NONE"), ("lease-once", "DELETE")):
        sch.create_schedule(
            Name=name,
            GroupName="aegc-dev",
            ScheduleExpression="at(2026-11-16T12:00:00)",
            FlexibleTimeWindow={"Mode": "OFF"},
            ActionAfterCompletion=delete,
            Target={
                "Arn": "arn:aws:scheduler:::aws-sdk:ssm:putParameter",
                "RoleArn": ROLE,
                "Input": json.dumps(
                    {"Name": f"/aegc/dev/{name}", "Value": '{"mode": "expired"}', "Type": "String", "Overwrite": True}
                ),
            },
        )
    listed = sch.list_schedules(GroupName="aegc-dev")["Schedules"]
    assert sorted(s["Name"] for s in listed) == ["lease-once", "lease-state"]

    with pytest.raises(ClientError) as before:
        ssm.get_parameter(Name="/aegc/dev/lease-state")
    assert before.value.response["Error"]["Code"] == "ParameterNotFound"

    _set_time(server.url, "2026-11-16T12:00:05Z")
    value = ssm.get_parameter(Name="/aegc/dev/lease-state")["Parameter"]["Value"]
    assert json.loads(value) == {"mode": "expired"}
    assert ssm.get_parameter(Name="/aegc/dev/lease-once")["Parameter"]["Value"]

    # NONE keeps the schedule; DELETE removed it after its one invocation.
    assert sch.get_schedule(Name="lease-state", GroupName="aegc-dev")["State"] == "ENABLED"
    with pytest.raises(ClientError) as gone:
        sch.get_schedule(Name="lease-once", GroupName="aegc-dev")
    assert gone.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert [s["Name"] for s in sch.list_schedules(GroupName="aegc-dev")["Schedules"]] == ["lease-state"]

    sch.delete_schedule_group(Name="aegc-dev")
    with pytest.raises(ClientError) as deleted:
        sch.get_schedule_group(Name="aegc-dev")
    assert deleted.value.response["Error"]["Code"] == "ResourceNotFoundException"
