package emulator

// StackDeployer helpers for CloudFormation's DynamoDB resource types. The rules every in-place
// updater follows are stated with cfnUpdateUnmodelledProperties.

import (
	"context"
	"slices"
)

// ----- AWS::DynamoDB::Table -------------------------------------------------------------

// updateDynamoDBTable applies a changed AWS::DynamoDB::Table through UpdateTable (BillingMode and
// ProvisionedThroughput) and UpdateTimeToLive (TimeToLiveSpecification). TableName and
// ImportSourceSpecification are Replacement; the key schema, index and stream properties are in
// cfnUpdateUnmodelledProperties.
//
// A removed BillingMode resets to PROVISIONED — "If not specified, the default is PROVISIONED." A
// removed TimeToLiveSpecification disables TTL on the attribute the previous declaration named, and
// only when that declaration enabled it: a TTL already off, or one naming no attribute, needs no
// call.
func (d *StackDeployer) updateDynamoDBTable(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	var cost float64
	if cfnChangedAny(changed, "BillingMode", "ProvisionedThroughput") {
		body := map[string]interface{}{
			"TableName":   prior.PhysicalID,
			"BillingMode": resolveStringProp(props, "BillingMode", "PROVISIONED", cctx),
		}
		if pt, ok := props["ProvisionedThroughput"]; ok {
			body["ProvisionedThroughput"] = resolveNested(pt, cctx)
		}
		req, err := cfnJSONRequest("dynamodb", "UpdateTable", body)
		if err != nil {
			return DeployedResource{}, 0, err
		}
		if routeErr := d.cfnUpdateDispatch(ctx, req, streamID, &cost); routeErr != nil {
			dr.Error = routeErr.Error()
			return dr, cost, nil //nolint:nilerr // A refused update is reported on the resource.
		}
	}
	if slices.Contains(changed, "TimeToLiveSpecification") {
		var ttl interface{}
		if spec, declared := cfnResolveStructured(props["TimeToLiveSpecification"], cctx).(map[string]interface{}); declared {
			ttl = spec
		} else {
			old, _ := cctx.priorProps["TimeToLiveSpecification"].(map[string]interface{})
			attr, _ := old["AttributeName"].(string)
			if on, _ := cfnBoolValue(old["Enabled"]); !on || attr == "" {
				return dr, cost, nil
			}
			ttl = map[string]interface{}{"AttributeName": attr, "Enabled": false}
		}
		req, err := cfnJSONRequest("dynamodb", "UpdateTimeToLive",
			map[string]interface{}{"TableName": prior.PhysicalID, "TimeToLiveSpecification": ttl})
		if err != nil {
			return DeployedResource{}, 0, err
		}
		if routeErr := d.cfnUpdateDispatch(ctx, req, streamID, &cost); routeErr != nil {
			dr.Error = routeErr.Error()
			return dr, cost, nil //nolint:nilerr // A refused update is reported on the resource.
		}
	}
	return dr, cost, nil
}
