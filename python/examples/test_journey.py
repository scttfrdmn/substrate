"""A worked pytest-substrate journey: testing infrastructure code against a per-test emulator (#1518).

The code under test is the small release script below. It uploads an artifact to S3, runs a health
check on a host through SSM Run Command, waits for the command to finish, and records the outcome in
an SSM parameter. A maintenance window is a one-time EventBridge Scheduler schedule that flips
another parameter when it falls due.

Each test shows one part of testing that script against substrate:

* ``substrate_isolated`` starts a fresh substrate process for the test and points boto3 at it via
  ``AWS_ENDPOINT_URL``, so the clients below need no endpoint or credentials of their own.
* A **seed** sets an outcome the script's failure path handles. Substrate does not run the command;
  ``POST /v1/ssm/command-invocation`` sets what ``GetCommandInvocation`` reports, so the failure path
  runs on the first try, every time.
* The **simulated clock** is set with ``SubstrateServer.set_time`` and moved with
  ``SubstrateServer.advance_time``, so a schedule an hour away fires without waiting an hour.
* Assertions read **state** back through the same APIs the script uses.

Run it with ``pytest python/examples`` once the substrate binary is built (``make build``) or
``SUBSTRATE_BINARY`` points at it.
"""

from __future__ import annotations

import datetime
import json
import urllib.request

import pytest

boto3 = pytest.importorskip("boto3", reason="the journey drives substrate through boto3")
from botocore.exceptions import ClientError  # noqa: E402  (imported after the skip guard)

HOST = "i-0123456789abcdef0"
HEALTH_CHECK = "curl -fsS http://localhost:8080/healthz"
SCHEDULER_ROLE = "arn:aws:iam::123456789012:role/release-scheduler"
START = datetime.datetime(2026, 11, 16, 9, 0, tzinfo=datetime.UTC)
TERMINAL = {"Success", "Failed", "Cancelled", "TimedOut"}


# --- The code under test ----------------------------------------------------------------------------


def wait_for_command(ssm, command_id: str, instance_id: str, attempts: int = 30) -> dict:
    """Poll GetCommandInvocation until the command reaches a terminal status."""
    for _ in range(attempts):
        invocation = ssm.get_command_invocation(CommandId=command_id, InstanceId=instance_id)
        if invocation["Status"] in TERMINAL:
            return invocation
    raise TimeoutError(f"command {command_id} did not finish on {instance_id}")


def release(s3, ssm, bucket: str, version: str) -> bool:
    """Upload the artifact, health-check the host, and record the outcome. Returns whether it passed."""
    s3.put_object(Bucket=bucket, Key=f"releases/{version}.tar.gz", Body=b"artifact-bytes")
    command = ssm.send_command(
        InstanceIds=[HOST],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [HEALTH_CHECK]},
    )["Command"]
    invocation = wait_for_command(ssm, command["CommandId"], HOST)
    passed = invocation["Status"] == "Success"
    record = {"version": version, "status": "ok" if passed else "failed"}
    if not passed:
        record["reason"] = invocation["StandardErrorContent"]
    ssm.put_parameter(Name="/app/release", Value=json.dumps(record), Type="String", Overwrite=True)
    return passed


def schedule_maintenance(scheduler, at: datetime.datetime) -> None:
    """Create a one-time schedule that turns maintenance mode on at ``at``."""
    scheduler.create_schedule(
        Name="maintenance-window",
        ScheduleExpression=f"at({at.strftime('%Y-%m-%dT%H:%M:%S')})",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={
            "Arn": "arn:aws:scheduler:::aws-sdk:ssm:putParameter",
            "RoleArn": SCHEDULER_ROLE,
            "Input": json.dumps({"Name": "/app/maintenance", "Value": "on", "Type": "String", "Overwrite": True}),
        },
    )


# --- Fixtures ---------------------------------------------------------------------------------------


@pytest.fixture()
def aws(substrate_isolated):  # type: ignore[no-untyped-def]
    """A per-test substrate with its clock at START and an artifact bucket, plus the clients."""
    substrate_isolated.set_time(START)
    clients = {name: boto3.client(name) for name in ("s3", "ssm", "scheduler")}
    clients["s3"].create_bucket(Bucket="release-artifacts")
    return substrate_isolated, clients


def seed_command_outcome(server, *, status: str, stderr: str = "", exit_code: int = 0) -> None:
    """Seed the outcome every matching SendCommand reports (POST /v1/ssm/command-invocation)."""
    body = {
        "documentName": "AWS-RunShellScript",
        "paramMatch": "healthz",
        "status": status,
        "stderr": stderr,
        "exitCode": exit_code,
    }
    req = urllib.request.Request(
        f"{server.url}/v1/ssm/command-invocation",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:  # nosemgrep  # noqa: S310 - a local test server
        assert json.loads(resp.read())["ok"] is True


def released(ssm) -> dict:
    return json.loads(ssm.get_parameter(Name="/app/release")["Parameter"]["Value"])


# --- The journey ------------------------------------------------------------------------------------


def test_a_healthy_release_is_recorded(aws) -> None:  # type: ignore[no-untyped-def]
    _, c = aws
    # Unseeded, a command takes its nominal path: Success.
    assert release(c["s3"], c["ssm"], "release-artifacts", "1.4.0") is True
    assert released(c["ssm"]) == {"version": "1.4.0", "status": "ok"}

    # The artifact is stamped with the simulated clock, not the machine's.
    head = c["s3"].head_object(Bucket="release-artifacts", Key="releases/1.4.0.tar.gz")
    assert START <= head["LastModified"] < START + datetime.timedelta(minutes=1)


def test_a_failed_health_check_is_recorded_with_its_reason(aws) -> None:  # type: ignore[no-untyped-def]
    server, c = aws
    seed_command_outcome(server, status="Failed", stderr="curl: (7) Failed to connect", exit_code=7)

    assert release(c["s3"], c["ssm"], "release-artifacts", "1.5.0") is False
    assert released(c["ssm"]) == {"version": "1.5.0", "status": "failed", "reason": "curl: (7) Failed to connect"}

    # The artifact was still uploaded: the failure is the health check's, not the upload's.
    keys = [o["Key"] for o in c["s3"].list_objects_v2(Bucket="release-artifacts")["Contents"]]
    assert keys == ["releases/1.5.0.tar.gz"]


def test_the_maintenance_window_opens_when_the_clock_reaches_it(aws) -> None:  # type: ignore[no-untyped-def]
    server, c = aws
    window = START + datetime.timedelta(hours=1)
    schedule_maintenance(c["scheduler"], window)

    # Before the window, the schedule has not fired.
    with pytest.raises(ClientError) as before:
        c["ssm"].get_parameter(Name="/app/maintenance")
    assert before.value.response["Error"]["Code"] == "ParameterNotFound"

    # An hour passes on the simulated clock in one call.
    now = server.advance_time(hours=1, seconds=5)
    assert now >= window + datetime.timedelta(seconds=5)
    assert c["ssm"].get_parameter(Name="/app/maintenance")["Parameter"]["Value"] == "on"
