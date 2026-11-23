"""botocore-driven CloudWatch coverage against a real substrate process.

This is the tier the Go end-to-end suite structurally cannot provide. CloudWatch's
service shape declares ``awsQuery``, ``awsJson1_0``, ``rpcv2Cbor`` and
``awsQueryCompatible`` at once, and clients disagree about which to use: ``aws-sdk-go-v2``
takes the CBOR path, botocore resolves ``json`` first and posts
``X-Amz-Target: GraniteServiceVersion20100801.{Op}``. Every Go test therefore exercises
CBOR and none exercises JSON, which is how substrate shipped a CloudWatch that answered
XML to the AWS CLI: the call returned HTTP 200, boto3's JSON parser found no members, and
``aws cloudwatch list-metrics`` printed nothing at all rather than failing (#757, #785).

These tests drive boto3 through a real substrate process, so a regression on the JSON path
fails here rather than being noticed by a downstream consumer.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

boto3 = pytest.importorskip("boto3", reason="boto3 is needed to drive the botocore JSON path")
from botocore.config import Config  # noqa: E402  (imported after the skip guard)
from botocore.exceptions import ClientError  # noqa: E402

NAMESPACE = "Substrate/Botocore"


@pytest.fixture()
def cloudwatch(substrate_isolated):  # type: ignore[no-untyped-def]
    """A boto3 CloudWatch client pointed at a per-test substrate process.

    Retries are off so each assertion is about the first response rather than whatever
    the retry loop settled on.
    """
    return boto3.client(
        "cloudwatch",
        endpoint_url=substrate_isolated.url,
        region_name="us-east-1",
        aws_access_key_id="substrate-test",
        aws_secret_access_key="substrate-test-secret",
        config=Config(retries={"max_attempts": 1, "mode": "standard"}),
    )


def test_put_metric_data_body_is_parsed(cloudwatch) -> None:  # type: ignore[no-untyped-def]
    """A JSON-1.0 PutMetricData body reaches state, rather than being silently dropped.

    The handler reads ``MetricData.member.1.MetricName``; botocore sends a JSON array. Every
    input parameter used to go unread, so ListMetrics reported nothing had been published.
    """
    cloudwatch.put_metric_data(
        Namespace=NAMESPACE,
        MetricData=[
            {"MetricName": "Requests", "Value": 3.0, "Unit": "Count"},
            {"MetricName": "Errors", "Value": 0.5, "Unit": "Percent"},
        ],
    )

    listed = cloudwatch.list_metrics(Namespace=NAMESPACE)
    names = sorted(m["MetricName"] for m in listed["Metrics"])
    assert names == ["Errors", "Requests"]
    # Present and empty, not absent: the metrics were published without dimensions, so a
    # caller ranging over Dimensions gets an empty list rather than a KeyError.
    assert listed["Metrics"][0]["Dimensions"] == []


def test_list_metrics_honours_the_metric_name_filter(cloudwatch) -> None:  # type: ignore[no-untyped-def]
    cloudwatch.put_metric_data(
        Namespace=NAMESPACE,
        MetricData=[
            {"MetricName": "Alpha", "Value": 1.0},
            {"MetricName": "Beta", "Value": 2.0},
        ],
    )
    filtered = cloudwatch.list_metrics(Namespace=NAMESPACE, MetricName="Alpha")
    assert [m["MetricName"] for m in filtered["Metrics"]] == ["Alpha"]


def test_alarm_round_trip_keeps_modeled_types(cloudwatch) -> None:  # type: ignore[no-untyped-def]
    """An alarm's members survive the JSON round trip with their modeled types.

    A fractional threshold is the assertion that matters: a renderer that wrote every number
    as an integer would return 80 here and no round-trip test in another protocol would
    notice.
    """
    cloudwatch.put_metric_alarm(
        AlarmName="botocore-alarm",
        AlarmDescription="set by the botocore journey",
        MetricName="Requests",
        Namespace=NAMESPACE,
        Statistic="Average",
        ComparisonOperator="GreaterThanThreshold",
        Threshold=80.5,
        EvaluationPeriods=2,
        Period=300,
        AlarmActions=["arn:aws:sns:us-east-1:123456789012:botocore"],
    )

    alarms = cloudwatch.describe_alarms()["MetricAlarms"]
    assert len(alarms) == 1
    alarm = alarms[0]
    assert alarm["AlarmName"] == "botocore-alarm"
    assert alarm["AlarmDescription"] == "set by the botocore journey"
    assert alarm["Threshold"] == 80.5
    assert alarm["EvaluationPeriods"] == 2
    assert alarm["Period"] == 300
    assert alarm["ActionsEnabled"] is True
    assert alarm["StateValue"] == "INSUFFICIENT_DATA"
    assert alarm["AlarmActions"] == ["arn:aws:sns:us-east-1:123456789012:botocore"]
    # Absent rather than empty: no OK actions were configured, and a caller can tell.
    assert "OKActions" not in alarm

    cloudwatch.set_alarm_state(
        AlarmName="botocore-alarm",
        StateValue="ALARM",
        StateReason="botocore said so",
        StateReasonData='{"botocore": true}',
    )
    updated = cloudwatch.describe_alarms(AlarmNames=["botocore-alarm"])["MetricAlarms"][0]
    assert updated["StateValue"] == "ALARM"
    assert updated["StateReason"] == "botocore said so"
    assert updated["StateReasonData"] == '{"botocore": true}'

    for_metric = cloudwatch.describe_alarms_for_metric(MetricName="Requests", Namespace=NAMESPACE)
    assert [a["AlarmName"] for a in for_metric["MetricAlarms"]] == ["botocore-alarm"]
    # DescribeAlarmsForMetric does not paginate, so its output has no token to return.
    assert "NextToken" not in for_metric

    cloudwatch.disable_alarm_actions(AlarmNames=["botocore-alarm"])
    assert cloudwatch.describe_alarms()["MetricAlarms"][0]["ActionsEnabled"] is False
    cloudwatch.enable_alarm_actions(AlarmNames=["botocore-alarm"])
    assert cloudwatch.describe_alarms()["MetricAlarms"][0]["ActionsEnabled"] is True

    cloudwatch.delete_alarms(AlarmNames=["botocore-alarm"])
    # Present and empty once the last alarm is gone: a caller polling until its alarm has
    # been deleted reads a zero-length list rather than a missing key.
    assert cloudwatch.describe_alarms()["MetricAlarms"] == []


def test_get_metric_data_reports_no_results_rather_than_nothing(cloudwatch) -> None:  # type: ignore[no-untyped-def]
    """GetMetricData answers one result per query, with modeled empty data points.

    Nothing was published in the window, so "no data points" is the honest answer — but a
    caller has to be able to tell it from "the operation returned nothing", which a bare HTTP
    200 with an unparseable body could not. The result itself is present, as CloudWatch answers
    for a metric with no data in the window (#1498).
    """
    out = cloudwatch.get_metric_data(
        MetricDataQueries=[
            {
                "Id": "q0",
                "MetricStat": {
                    "Metric": {"Namespace": NAMESPACE, "MetricName": "Requests"},
                    "Period": 300,
                    "Stat": "Average",
                },
            }
        ],
        StartTime="2024-01-01T00:00:00Z",
        EndTime="2024-01-02T00:00:00Z",
    )
    (result,) = out["MetricDataResults"]
    assert result["Id"] == "q0"
    assert result["StatusCode"] == "Complete"
    assert result["Timestamps"] == []
    assert result["Values"] == []
    assert out["Messages"] == []


def test_published_datapoints_read_back(cloudwatch) -> None:  # type: ignore[no-untyped-def]
    """PutMetricData's values come back through GetMetricData and GetMetricStatistics (#1595).

    The timestamps are relative to now because PutMetricData refuses one more than two weeks
    before the server's clock, and the server here runs on the wall clock.
    """
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    dims = [{"Name": "Pool", "Value": "gpu"}]
    cloudwatch.put_metric_data(
        Namespace=NAMESPACE,
        MetricData=[
            {"MetricName": "Tasks", "Dimensions": dims, "Value": 2.0, "Timestamp": now - timedelta(minutes=3)},
            {"MetricName": "Tasks", "Dimensions": dims, "Value": 5.0, "Timestamp": now - timedelta(minutes=3)},
        ],
    )
    out = cloudwatch.get_metric_data(
        MetricDataQueries=[
            {
                "Id": "q0",
                "MetricStat": {
                    "Metric": {"Namespace": NAMESPACE, "MetricName": "Tasks", "Dimensions": dims},
                    "Period": 60,
                    "Stat": "Sum",
                },
            }
        ],
        StartTime=now - timedelta(minutes=10),
        EndTime=now,
    )
    (result,) = out["MetricDataResults"]
    assert result["Values"] == [7.0]
    assert result["Timestamps"] == [now - timedelta(minutes=3)]

    stats = cloudwatch.get_metric_statistics(
        Namespace=NAMESPACE,
        MetricName="Tasks",
        Dimensions=dims,
        StartTime=now - timedelta(minutes=10),
        EndTime=now,
        Period=600,
        ExtendedStatistics=["p50"],
    )
    (point,) = stats["Datapoints"]
    assert point["ExtendedStatistics"] == {"p50": 2.0}

    (listed,) = cloudwatch.list_metrics(Namespace=NAMESPACE, MetricName="Tasks")["Metrics"]
    assert listed["Dimensions"] == dims


def test_alarm_tags_and_missing_data(cloudwatch) -> None:  # type: ignore[no-untyped-def]
    """An alarm's tags and TreatMissingData survive the JSON path (#1618, #1620)."""
    cloudwatch.put_metric_alarm(
        AlarmName="tagged-alarm",
        MetricName="Heartbeat",
        Namespace=NAMESPACE,
        Statistic="Sum",
        ComparisonOperator="LessThanThreshold",
        Threshold=1,
        EvaluationPeriods=3,
        DatapointsToAlarm=2,
        Period=60,
        TreatMissingData="breaching",
        Tags=[{"Key": "aegc:managed", "Value": "true"}],
    )
    alarm = cloudwatch.describe_alarms(AlarmNames=["tagged-alarm"])["MetricAlarms"][0]
    assert alarm["TreatMissingData"] == "breaching"
    assert alarm["DatapointsToAlarm"] == 2
    arn = alarm["AlarmArn"]
    assert cloudwatch.list_tags_for_resource(ResourceARN=arn)["Tags"] == [{"Key": "aegc:managed", "Value": "true"}]
    cloudwatch.tag_resource(ResourceARN=arn, Tags=[{"Key": "team", "Value": "hpc"}])
    cloudwatch.untag_resource(ResourceARN=arn, TagKeys=["aegc:managed"])
    assert cloudwatch.list_tags_for_resource(ResourceARN=arn)["Tags"] == [{"Key": "team", "Value": "hpc"}]


def test_a_refusal_reports_its_code_and_message(cloudwatch) -> None:  # type: ignore[no-untyped-def]
    """A JSON-RPC refusal carries the code in the body, where botocore's parser reads it.

    Shaped as REST/JSON the code went into a header botocore's JSON parser never looks at, so
    a refused call surfaced as ``An error occurred (404)`` — the stringified HTTP status — and
    the caller's ``except`` branch keyed on the code was unreachable.
    """
    with pytest.raises(ClientError) as caught:
        cloudwatch.set_alarm_state(
            AlarmName="no-such-alarm",
            StateValue="OK",
            StateReason="nothing to set",
        )
    err = caught.value.response["Error"]
    assert err["Code"] == "ResourceNotFoundException"
    assert "no-such-alarm" in err["Message"]
    assert caught.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
