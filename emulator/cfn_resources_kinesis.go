package emulator

// StackDeployer helpers for CloudFormation's Kinesis resource types.

import (
	"context"
	"encoding/json"
	"fmt"
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
