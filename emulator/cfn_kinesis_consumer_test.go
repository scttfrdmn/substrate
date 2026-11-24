package emulator_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #1613: AWS::Kinesis::StreamConsumer deploys through RegisterStreamConsumer and deletes through
// DeregisterStreamConsumer.

const cfnKinesisConsumerTemplate = `{
	"AWSTemplateFormatVersion": "2010-09-09",
	"Resources": {
		"Stream": {"Type": "AWS::Kinesis::Stream", "Properties": {"Name": "cfn-fan", "ShardCount": 1}},
		"Consumer": {
			"Type": "AWS::Kinesis::StreamConsumer",
			"Properties": {
				"ConsumerName": "cfn-app",
				"StreamARN": {"Fn::GetAtt": ["Stream", "Arn"]},
				"Tags": [{"Key": "team", "Value": "data"}]
			}
		}
	},
	"Outputs": {
		"Ref": {"Value": {"Ref": "Consumer"}},
		"ConsumerARN": {"Value": {"Fn::GetAtt": ["Consumer", "ConsumerARN"]}},
		"ConsumerName": {"Value": {"Fn::GetAtt": ["Consumer", "ConsumerName"]}},
		"ConsumerStatus": {"Value": {"Fn::GetAtt": ["Consumer", "ConsumerStatus"]}},
		"StreamARN": {"Value": {"Fn::GetAtt": ["Consumer", "StreamARN"]}}
	}
}`

// cfnKinesisCall dispatches one Kinesis operation through the deployer.
func cfnKinesisCall(t *testing.T, d *emulator.StackDeployer, op string, body map[string]any) (*emulator.AWSResponse, error) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	return d.DispatchForTest(context.Background(), &emulator.AWSRequest{
		Service: "kinesis", Operation: op, Body: raw,
		Headers: map[string]string{"X-Amz-Target": "Kinesis_20131202." + op}, Params: map[string]string{},
	}, "probe")
}

func TestCFN_KinesisStreamConsumer(t *testing.T) {
	t.Parallel()
	d := newFullTestDeployer(t)
	ctx := context.Background()

	result, err := d.Deploy(ctx, cfnKinesisConsumerTemplate, "kinesis-consumer", nil)
	require.NoError(t, err)
	streamARN := "arn:aws:kinesis:us-east-1:123456789012:stream/cfn-fan"
	assert.Regexp(t, `^`+streamARN+`/consumer/cfn-app:[0-9]+$`, result.Outputs["Ref"])
	assert.Equal(t, result.Outputs["Ref"], result.Outputs["ConsumerARN"])
	assert.Equal(t, "cfn-app", result.Outputs["ConsumerName"])
	assert.Equal(t, "ACTIVE", result.Outputs["ConsumerStatus"])
	assert.Equal(t, streamARN, result.Outputs["StreamARN"], "the stream's ARN, not the consumer's")

	resp, err := cfnKinesisCall(t, d, "ListTagsForResource", map[string]any{"ResourceARN": result.Outputs["Ref"]})
	require.NoError(t, err)
	assert.JSONEq(t, `{"Tags":[{"Key":"team","Value":"data"}]}`, string(resp.Body))

	require.NoError(t, d.DeleteStack(ctx, "kinesis-consumer"))
	_, err = cfnKinesisCall(t, d, "DescribeStream", map[string]any{"StreamName": "cfn-fan"})
	var awsErr *emulator.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "ResourceNotFoundException", awsErr.Code,
		"the consumer is deregistered first, so the stream's delete is not refused")
}
