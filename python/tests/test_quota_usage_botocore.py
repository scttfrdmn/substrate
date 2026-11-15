"""boto3-driven EC2 vCPU quota preflight against a real substrate process (#1498).

A Spot consumer checks headroom before launching a fleet: it reads the Spot vCPU quota
from Service Quotas, follows the quota's ``UsageMetric`` to CloudWatch's ``AWS/Usage``
namespace, and compares ``quota - usage`` with the vCPUs it is about to ask for. boto3
speaks Service Quotas' JSON protocol and CloudWatch's ``awsJson1_0``, so this is the path
such a consumer exercises.

The simulated clock is set to a fixed instant and slowed to a crawl through the control
plane, so the datapoint's timestamp is exact rather than dependent on how long the test
took.
"""

from __future__ import annotations

import datetime as dt
import json
import urllib.request

import pytest

boto3 = pytest.importorskip("boto3", reason="boto3 is needed to drive the botocore JSON paths")
from botocore.config import Config  # noqa: E402  (imported after the skip guard)

NOW = dt.datetime(2026, 3, 1, 0, 7, 30, tzinfo=dt.UTC)
AMI_PARAMETER = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"


def _control(url: str, path: str, body: dict) -> None:
    req = urllib.request.Request(
        f"{url}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:  # nosemgrep
        resp.read()


@pytest.fixture()
def clients(substrate_isolated):  # type: ignore[no-untyped-def]
    """boto3 clients for the preflight's three services, with the clock pinned at NOW."""
    _control(substrate_isolated.url, "/v1/control/time", {"time": NOW.isoformat()})
    _control(substrate_isolated.url, "/v1/control/scale", {"scale": 1e-9})

    def client(name: str):  # type: ignore[no-untyped-def]
        return boto3.client(
            name,
            endpoint_url=substrate_isolated.url,
            region_name="us-east-1",
            aws_access_key_id="substrate-test",
            aws_secret_access_key="substrate-test-secret",
            config=Config(retries={"max_attempts": 1, "mode": "standard"}),
        )

    return {name: client(name) for name in ("service-quotas", "ec2", "cloudwatch", "ssm")}


def test_spot_quota_and_its_default(clients) -> None:  # type: ignore[no-untyped-def]
    sq = clients["service-quotas"]
    applied = sq.get_service_quota(ServiceCode="ec2", QuotaCode="L-34B43A08")["Quota"]
    assert applied["Value"] == 5.0
    assert applied["QuotaName"] == "All Standard (A, C, D, H, I, M, R, T, Z) Spot Instance Requests"
    assert applied["UsageMetric"]["MetricDimensions"]["Class"] == "Standard/Spot"

    default = sq.get_aws_default_service_quota(ServiceCode="ec2", QuotaCode="L-34B43A08")["Quota"]
    assert default["Value"] == 5.0
    # A default belongs to no account, so its ARN names none.
    assert default["QuotaArn"] == "arn:aws:servicequotas:us-east-1::ec2/L-34B43A08"

    pages = sq.get_paginator("list_aws_default_service_quotas").paginate(
        ServiceCode="ec2", PaginationConfig={"PageSize": 5}
    )
    codes = [q["QuotaCode"] for page in pages for q in page["Quotas"]]
    assert len(codes) == 12
    assert {"L-1216C47A", "L-34B43A08", "L-3819A6DF", "L-7212CCBC"} <= set(codes)


def test_usage_metric_reports_running_spot_vcpus(clients) -> None:  # type: ignore[no-untyped-def]
    image = clients["ssm"].get_parameter(Name=AMI_PARAMETER)["Parameter"]["Value"]
    # Three m5.large Spot instances: 6 vCPU, one over the 5-vCPU default.
    clients["ec2"].run_instances(
        ImageId=image,
        InstanceType="m5.large",
        MinCount=3,
        MaxCount=3,
        InstanceMarketOptions={"MarketType": "spot"},
    )

    quota = clients["service-quotas"].get_service_quota(ServiceCode="ec2", QuotaCode="L-34B43A08")["Quota"]
    metric = quota["UsageMetric"]
    resp = clients["cloudwatch"].get_metric_data(
        MetricDataQueries=[
            {
                "Id": "u",
                "MetricStat": {
                    "Metric": {
                        "Namespace": metric["MetricNamespace"],
                        "MetricName": metric["MetricName"],
                        "Dimensions": [{"Name": k, "Value": v} for k, v in metric["MetricDimensions"].items()],
                    },
                    "Period": 300,
                    "Stat": metric["MetricStatisticRecommendation"],
                },
            }
        ],
        StartTime=NOW - dt.timedelta(minutes=15),
        EndTime=NOW,
    )
    (result,) = resp["MetricDataResults"]
    assert result["Values"] == [6.0]
    assert result["Timestamps"] == [dt.datetime(2026, 3, 1, 0, 7, tzinfo=dt.UTC)]
    assert quota["Value"] - result["Values"][0] == -1.0

    listed = clients["cloudwatch"].list_metrics(Namespace="AWS/Usage")["Metrics"]
    assert [d["Value"] for m in listed for d in m["Dimensions"] if d["Name"] == "Class"] == ["Standard/Spot"]
