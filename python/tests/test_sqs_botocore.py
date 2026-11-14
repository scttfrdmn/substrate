"""boto3-driven SQS coverage against a real substrate process (#1441, #1462).

boto3 speaks SQS's JSON protocol, the path a worker that renews visibility in batches
and fails a task after too many deliveries actually exercises. None of these tests wait
on a clock: a per-receive ``VisibilityTimeout=0`` makes a message visible again at once,
which is itself one of the behaviours under test.
"""

from __future__ import annotations

import pytest

boto3 = pytest.importorskip("boto3", reason="boto3 is needed to drive the SQS JSON path")
from botocore.config import Config  # noqa: E402  (imported after the skip guard)


@pytest.fixture()
def sqs(substrate_isolated):  # type: ignore[no-untyped-def]
    """A boto3 SQS client pointed at a per-test substrate process, retries off."""
    return boto3.client(
        "sqs",
        endpoint_url=substrate_isolated.url,
        region_name="us-east-1",
        aws_access_key_id="substrate-test",
        aws_secret_access_key="substrate-test-secret",
        config=Config(retries={"max_attempts": 1, "mode": "standard"}),
    )


def test_receive_returns_approximate_receive_count(sqs) -> None:  # type: ignore[no-untyped-def]
    q = sqs.create_queue(QueueName="botocore-rc")["QueueUrl"]
    sqs.send_message(QueueUrl=q, MessageBody="x")

    first = sqs.receive_message(QueueUrl=q, AttributeNames=["All"], VisibilityTimeout=0)["Messages"][0]
    assert first["Attributes"]["ApproximateReceiveCount"] == "1"
    assert {"SentTimestamp", "SenderId", "ApproximateFirstReceiveTimestamp"} <= set(first["Attributes"])

    # VisibilityTimeout=0 left it visible, so the next receive gets it again.
    second = sqs.receive_message(
        QueueUrl=q, MessageSystemAttributeNames=["ApproximateReceiveCount"]
    )["Messages"][0]
    assert second["Attributes"] == {"ApproximateReceiveCount": "2"}

    # Attributes are absent unless requested.
    sqs.change_message_visibility(QueueUrl=q, ReceiptHandle=second["ReceiptHandle"], VisibilityTimeout=0)
    third = sqs.receive_message(QueueUrl=q)["Messages"][0]
    assert "Attributes" not in third


def test_queue_attribute_counts(sqs) -> None:  # type: ignore[no-untyped-def]
    q = sqs.create_queue(QueueName="botocore-counts")["QueueUrl"]
    sqs.send_message(QueueUrl=q, MessageBody="a")
    sqs.send_message(QueueUrl=q, MessageBody="b")
    sqs.send_message(QueueUrl=q, MessageBody="c", DelaySeconds=900)
    sqs.receive_message(QueueUrl=q, MaxNumberOfMessages=1, VisibilityTimeout=600)

    names = [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed",
    ]
    attrs = sqs.get_queue_attributes(QueueUrl=q, AttributeNames=names)["Attributes"]
    assert attrs == dict(zip(names, ["1", "1", "1"]))


def test_change_message_visibility_batch(sqs) -> None:  # type: ignore[no-untyped-def]
    q = sqs.create_queue(QueueName="botocore-cmvb")["QueueUrl"]
    sqs.send_message(QueueUrl=q, MessageBody="x")
    msg = sqs.receive_message(QueueUrl=q, VisibilityTimeout=600)["Messages"][0]

    out = sqs.change_message_visibility_batch(
        QueueUrl=q,
        Entries=[
            {"Id": "ok", "ReceiptHandle": msg["ReceiptHandle"], "VisibilityTimeout": 0},
            {"Id": "bad", "ReceiptHandle": "not-a-handle", "VisibilityTimeout": 0},
        ],
    )
    assert [e["Id"] for e in out["Successful"]] == ["ok"]
    assert [(e["Id"], e["Code"], e["SenderFault"]) for e in out["Failed"]] == [
        ("bad", "ReceiptHandleIsInvalid", True)
    ]
    # Released by the batch, the message is visible again.
    assert sqs.receive_message(QueueUrl=q)["Messages"][0]["Body"] == "x"
