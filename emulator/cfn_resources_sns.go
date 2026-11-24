package emulator

// StackDeployer helpers for CloudFormation's SNS resource types. The rules every in-place updater
// follows are stated with cfnUpdateUnmodelledProperties.

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

// ----- AWS::SNS::Topic ------------------------------------------------------------------

// updateSNSTopic applies a changed AWS::SNS::Topic's DisplayName, the one attribute the create
// forwards, through SetTopicAttributes. TopicName and FifoTopic are Replacement.
//
// A removed DisplayName is set to the empty string, which is what a topic created without one
// reports.
func (d *StackDeployer) updateSNSTopic(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	if !slices.Contains(changed, "DisplayName") {
		return dr, 0, nil
	}
	arn := prior.PhysicalID
	if !strings.HasPrefix(arn, "arn:") {
		arn = fmt.Sprintf("arn:aws:sns:%s:%s:%s", cctx.region, cctx.accountID, arn)
	}
	var cost float64
	if routeErr := d.cfnUpdateDispatch(ctx, &AWSRequest{Service: "sns", Operation: "SetTopicAttributes",
		Headers: map[string]string{},
		Params: map[string]string{
			"Action": "SetTopicAttributes", "TopicArn": arn, "AttributeName": "DisplayName",
			"AttributeValue": resolveStringProp(props, "DisplayName", "", cctx),
		}}, streamID, &cost); routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}
