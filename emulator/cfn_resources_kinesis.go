package emulator

// StackDeployer helpers for CloudFormation's Kinesis resource types.

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// ----- Kinesis -------------------------------------------------------------

// deployKinesisStream creates a Kinesis data stream for the given CFN resource.
func (d *StackDeployer) deployKinesisStream(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)

	// ShardCount, StreamModeDetails and MaxRecordSizeInKiB are forwarded as declared (#1118, #1418).
	// All three are Required: No on AWS::Kinesis::Stream and the page publishes no default for
	// ShardCount, so a provisioned stream declaring none keeps the one shard substrate has always
	// given it, and an ON_DEMAND stream declaring none sends none, as CreateStream requires.
	body := map[string]interface{}{"StreamName": name}
	mode := ""
	if details, ok := props["StreamModeDetails"].(map[string]interface{}); ok {
		mode = resolveStringProp(details, "StreamMode", "", cctx)
		body["StreamModeDetails"] = map[string]string{"StreamMode": mode}
	}
	if n, ok := cfnIntProp(props, "ShardCount", cctx); ok {
		body["ShardCount"] = n
	} else if mode != kinesisStreamModeOnDemand {
		body["ShardCount"] = 1
	}
	if n, ok := cfnIntProp(props, "MaxRecordSizeInKiB", cctx); ok {
		body["MaxRecordSizeInKiB"] = n
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployKinesisStream marshal: %w", err)
	}

	req := &AWSRequest{
		Service:   "kinesis",
		Operation: "CreateStream",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "Kinesis_20131202.CreateStream"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	arn := "arn:aws:kinesis:" + cctx.region + ":" + cctx.accountID + ":stream/" + name
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Kinesis::Stream",
		PhysicalID: name,
		ARN:        arn,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployKinesisStreamConsumer registers an enhanced fan-out consumer for an
// AWS::Kinesis::StreamConsumer. Ref is the consumer ARN, so it is the physical ID; the five
// attributes are recorded from RegisterStreamConsumer's response. StreamARN is recorded because the
// generic ARN rule would otherwise answer the consumer's own ARN for it.
func (d *StackDeployer) deployKinesisStreamConsumer(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	streamARN := resolveStringProp(props, "StreamARN", "", cctx)
	body := map[string]interface{}{
		"StreamARN":    streamARN,
		"ConsumerName": resolveStringProp(props, "ConsumerName", logicalID, cctx),
	}
	if tags, ok := props["Tags"]; ok {
		body["Tags"] = cfnTagMap(tags, cctx)
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployKinesisStreamConsumer marshal: %w", err)
	}
	req := &AWSRequest{
		Service:   "kinesis",
		Operation: "RegisterStreamConsumer",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "Kinesis_20131202.RegisterStreamConsumer"},
		Params:    map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID: logicalID,
		Type:      "AWS::Kinesis::StreamConsumer",
		Metadata:  map[string]interface{}{"StreamARN": streamARN},
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
		return dr, cost, nil
	}
	var out struct {
		Consumer kinesisConsumerView `json:"Consumer"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployKinesisStreamConsumer unmarshal: %w", err)
	}
	dr.PhysicalID = out.Consumer.ConsumerARN
	dr.ARN = out.Consumer.ConsumerARN
	dr.Metadata["ConsumerARN"] = out.Consumer.ConsumerARN
	dr.Metadata["ConsumerName"] = out.Consumer.ConsumerName
	dr.Metadata["ConsumerStatus"] = out.Consumer.ConsumerStatus
	dr.Metadata["ConsumerCreationTimestamp"] = time.Time(out.Consumer.ConsumerCreationTimestamp).UTC().Format(time.RFC3339)
	return dr, cost, nil
}
