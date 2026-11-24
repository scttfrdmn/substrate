package emulator

// StackDeployer helpers for CloudFormation's Firehose resource type.

import (
	"context"
	"encoding/json"
	"fmt"
)

// deployFirehoseDeliveryStream creates a Firehose delivery stream for the given CFN resource.
func (d *StackDeployer) deployFirehoseDeliveryStream(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	streamName := logicalID
	if name, ok := props["DeliveryStreamName"].(string); ok && name != "" {
		streamName = name
	}

	body := map[string]interface{}{
		"DeliveryStreamName": streamName,
		"DeliveryStreamType": "DirectPut",
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal firehose delivery stream body: %w", err)
	}

	req := &AWSRequest{
		Service:   "firehose",
		Operation: "CreateDeliveryStream",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "Firehose_20150804.CreateDeliveryStream"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::KinesisFirehose::DeliveryStream",
		PhysicalID: streamName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	_ = cctx
	return dr, cost, nil
}
