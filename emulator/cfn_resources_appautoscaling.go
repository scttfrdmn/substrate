package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// CloudFormation's two Application Auto Scaling types (#1661). Both operations they dispatch to are
// create-or-update, so a re-deploy of a changed declaration updates the target or the policy in
// place under the same physical ID; a change to a property whose "Update requires" is Replacement
// is a replacement (see cfnRecreationProperties).

// ----- AWS::ApplicationAutoScaling::ScalableTarget ------------------------------------------------

// cfnScalableTargetID renders the ID CloudFormation generates for a scalable target, which Ref
// returns: "CloudFormation uses the following format to generate the ID:
// service/resource_ID|scalable_dimension|service_namespace" (the type's Return values), whose
// example is `service/ecsStack-MyECSCluster-AB12CDE3F4GH/ecsStack-MyECSService-AB12CDE3F4GH|ecs:service:DesiredCount|ecs`.
func cfnScalableTargetID(resourceID, dimension, namespace string) string {
	return resourceID + "|" + dimension + "|" + namespace
}

// cfnSplitScalableTargetID splits a ScalingTargetId back into its resource ID, dimension and
// namespace. The resource ID is the part that may hold the separator, so the split is from the right.
func cfnSplitScalableTargetID(id string) (resourceID, dimension, namespace string, ok bool) {
	last := strings.LastIndexByte(id, '|')
	if last < 0 {
		return "", "", "", false
	}
	mid := strings.LastIndexByte(id[:last], '|')
	if mid < 0 {
		return "", "", "", false
	}
	return id[:mid], id[mid+1 : last], id[last+1:], true
}

// cfnAASRequest builds an Application Auto Scaling request carrying body.
func cfnAASRequest(operation string, body map[string]interface{}) (*AWSRequest, error) {
	return cfnJSONRequest(appAutoScalingNamespace, operation, body)
}

// deployApplicationAutoScalingScalableTarget registers the target with RegisterScalableTarget and
// puts each declared ScheduledActions entry with PutScheduledAction. A scheduled action the target
// holds and the declaration does not is deleted, so a re-deploy leaves exactly the declared set.
func (d *StackDeployer) deployApplicationAutoScalingScalableTarget(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	namespace := resolveStringProp(props, "ServiceNamespace", "", cctx)
	dimension := resolveStringProp(props, "ScalableDimension", "", cctx)
	resourceID := resolveStringProp(props, "ResourceId", "", cctx)
	dr := DeployedResource{
		LogicalID: logicalID, Type: "AWS::ApplicationAutoScaling::ScalableTarget",
		PhysicalID: cfnScalableTargetID(resourceID, dimension, namespace),
		Metadata: map[string]interface{}{
			"ServiceNamespace": namespace, "ScalableDimension": dimension, "ResourceId": resourceID,
		},
	}
	body := map[string]interface{}{
		"ServiceNamespace": namespace, "ScalableDimension": dimension, "ResourceId": resourceID,
	}
	for _, k := range []string{"MinCapacity", "MaxCapacity"} {
		if n, ok := cfnIntProp(props, k, cctx); ok {
			body[k] = n
		}
	}
	if role := resolveStringProp(props, "RoleARN", "", cctx); role != "" {
		body["RoleARN"] = role
	}
	if s, ok := cfnResolveStructured(props["SuspendedState"], cctx).(map[string]interface{}); ok {
		body["SuspendedState"] = cfnAASCoerce(s)
	}
	if tags := cfnTagMap(cfnResolveStructured(props["Tags"], cctx), cctx); len(tags) > 0 {
		body["Tags"] = tags
	}
	req, err := cfnAASRequest("RegisterScalableTarget", body)
	if err != nil {
		return DeployedResource{}, 0, err
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	if routeErr != nil {
		dr.Error = routeErr.Error()
		return dr, cost, nil //nolint:nilerr // A refused call is reported on the resource.
	}
	var out struct {
		ScalableTargetARN string `json:"ScalableTargetARN"`
	}
	if resp != nil {
		if err := json.Unmarshal(resp.Body, &out); err != nil {
			return DeployedResource{}, cost, fmt.Errorf("cfn deployScalableTarget decode: %w", err)
		}
	}
	dr.ARN = out.ScalableTargetARN

	actionCost, actionErr := d.syncScheduledActions(ctx, props, namespace, dimension, resourceID, streamID, cctx)
	cost += actionCost
	if actionErr != nil {
		dr.Error = actionErr.Error()
	}
	return dr, cost, nil
}

// syncScheduledActions puts each ScheduledActions entry on the target and deletes the target's
// actions the declaration does not name.
func (d *StackDeployer) syncScheduledActions(
	ctx context.Context, props map[string]interface{}, namespace, dimension, resourceID, streamID string, cctx *cfnContext,
) (float64, error) {
	var cost float64
	declared := map[string]bool{}
	entries, _ := cfnResolveStructured(props["ScheduledActions"], cctx).([]interface{})
	for _, e := range entries {
		m, ok := e.(map[string]interface{})
		if !ok {
			continue
		}
		body := cfnAASCoerce(m)
		body["ServiceNamespace"], body["ScalableDimension"], body["ResourceId"] = namespace, dimension, resourceID
		for _, k := range []string{"StartTime", "EndTime"} {
			if v, ok := body[k].(string); ok && v != "" {
				if t, ok := cfnAASTimestamp(v); ok {
					body[k] = float64(t.Unix())
				}
			}
		}
		name, _ := body["ScheduledActionName"].(string)
		declared[name] = true
		req, err := cfnAASRequest("PutScheduledAction", body)
		if err != nil {
			return cost, err
		}
		_, c, routeErr := d.dispatch(ctx, req, streamID)
		cost += c
		if routeErr != nil {
			return cost, routeErr
		}
	}
	req, err := cfnAASRequest("DescribeScheduledActions", map[string]interface{}{
		"ServiceNamespace": namespace, "ScalableDimension": dimension, "ResourceId": resourceID,
	})
	if err != nil {
		return cost, err
	}
	resp, c, routeErr := d.dispatch(ctx, req, streamID)
	cost += c
	if routeErr != nil || resp == nil {
		return cost, routeErr
	}
	var out struct {
		ScheduledActions []struct {
			ScheduledActionName string `json:"ScheduledActionName"`
		} `json:"ScheduledActions"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		return cost, fmt.Errorf("cfn syncScheduledActions decode: %w", err)
	}
	for _, a := range out.ScheduledActions {
		if declared[a.ScheduledActionName] {
			continue
		}
		del, err := cfnAASRequest("DeleteScheduledAction", map[string]interface{}{
			"ServiceNamespace": namespace, "ScalableDimension": dimension, "ResourceId": resourceID,
			"ScheduledActionName": a.ScheduledActionName,
		})
		if err != nil {
			return cost, err
		}
		_, c, routeErr := d.dispatch(ctx, del, streamID)
		cost += c
		if routeErr != nil {
			return cost, routeErr
		}
	}
	return cost, nil
}

// cfnScalableTargetDeleter deregisters the target, which deletes its policies and scheduled
// actions with it.
func cfnScalableTargetDeleter(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
	namespace := cfnRefMetadata(dr, "ServiceNamespace")
	if namespace == "" {
		return nil
	}
	req, err := cfnAASRequest("DeregisterScalableTarget", map[string]interface{}{
		"ServiceNamespace": namespace, "ScalableDimension": cfnRefMetadata(dr, "ScalableDimension"),
		"ResourceId": cfnRefMetadata(dr, "ResourceId"),
	})
	if err != nil {
		return nil
	}
	return req
}

// ----- AWS::ApplicationAutoScaling::ScalingPolicy -------------------------------------------------

// cfnScalingPolicyTarget resolves which target a policy names: "You must specify either the
// ScalingTargetId property, or the ResourceId, ScalableDimension, and ServiceNamespace properties,
// but not both" (the type's ScalingTargetId).
func cfnScalingPolicyTarget(props map[string]interface{}, cctx *cfnContext) (resourceID, dimension, namespace string, err error) {
	id := resolveStringProp(props, "ScalingTargetId", "", cctx)
	resourceID = resolveStringProp(props, "ResourceId", "", cctx)
	dimension = resolveStringProp(props, "ScalableDimension", "", cctx)
	namespace = resolveStringProp(props, "ServiceNamespace", "", cctx)
	explicit := resourceID != "" || dimension != "" || namespace != ""
	switch {
	case id != "" && explicit:
		return "", "", "", fmt.Errorf("specify either ScalingTargetId or ResourceId, ScalableDimension and ServiceNamespace, but not both")
	case id != "":
		r, dim, ns, ok := cfnSplitScalableTargetID(id)
		if !ok {
			return "", "", "", fmt.Errorf("ScalingTargetId %q is not a scalable target ID", id)
		}
		return r, dim, ns, nil
	case resourceID == "" || dimension == "" || namespace == "":
		return "", "", "", fmt.Errorf("specify either ScalingTargetId or ResourceId, ScalableDimension and ServiceNamespace")
	}
	return resourceID, dimension, namespace, nil
}

// deployApplicationAutoScalingScalingPolicy puts the policy with PutScalingPolicy. "Ref returns the
// Application Auto Scaling scaling policy Amazon Resource Name (ARN)", which is the physical ID and
// Fn::GetAtt Arn.
func (d *StackDeployer) deployApplicationAutoScalingScalingPolicy(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ApplicationAutoScaling::ScalingPolicy"}
	resourceID, dimension, namespace, err := cfnScalingPolicyTarget(props, cctx)
	if err != nil {
		dr.Error = err.Error()
		return dr, 0, nil //nolint:nilerr // A template naming no target is reported on the resource.
	}
	name := resolveStringProp(props, "PolicyName", "", cctx)
	dr.Metadata = map[string]interface{}{
		"ServiceNamespace": namespace, "ScalableDimension": dimension, "ResourceId": resourceID, "PolicyName": name,
	}
	body := map[string]interface{}{
		"PolicyName": name, "PolicyType": resolveStringProp(props, "PolicyType", "", cctx),
		"ServiceNamespace": namespace, "ScalableDimension": dimension, "ResourceId": resourceID,
	}
	for _, k := range []string{
		"StepScalingPolicyConfiguration", "TargetTrackingScalingPolicyConfiguration", "PredictiveScalingPolicyConfiguration",
	} {
		if m, ok := cfnResolveStructured(props[k], cctx).(map[string]interface{}); ok {
			body[k] = cfnAASCoerce(m)
		}
	}
	req, err := cfnAASRequest("PutScalingPolicy", body)
	if err != nil {
		return DeployedResource{}, 0, err
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	if routeErr != nil {
		dr.Error = routeErr.Error()
		return dr, cost, nil //nolint:nilerr // A refused call is reported on the resource.
	}
	var out struct {
		PolicyARN string `json:"PolicyARN"`
	}
	if resp != nil {
		if err := json.Unmarshal(resp.Body, &out); err != nil {
			return DeployedResource{}, cost, fmt.Errorf("cfn deployScalingPolicy decode: %w", err)
		}
	}
	dr.PhysicalID, dr.ARN = out.PolicyARN, out.PolicyARN
	dr.Metadata["Arn"] = out.PolicyARN
	return dr, cost, nil
}

// cfnScalingPolicyDeleter deletes the policy with DeleteScalingPolicy. A policy whose target was
// deregistered first is already gone, which ObjectNotFoundException reports.
func cfnScalingPolicyDeleter(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
	name := cfnRefMetadata(dr, "PolicyName")
	if name == "" {
		return nil
	}
	req, err := cfnAASRequest("DeleteScalingPolicy", map[string]interface{}{
		"PolicyName": name, "ServiceNamespace": cfnRefMetadata(dr, "ServiceNamespace"),
		"ScalableDimension": cfnRefMetadata(dr, "ScalableDimension"), "ResourceId": cfnRefMetadata(dr, "ResourceId"),
	})
	if err != nil {
		return nil
	}
	return req
}

// cfnAASNumericMembers are the members of the Application Auto Scaling configuration structures
// the model types as numbers. A template that sets one through a Ref to a parameter resolves it to
// a string, which the JSON protocol would refuse, so it is converted back.
var cfnAASNumericMembers = map[string]bool{ //nolint:gochecknoglobals // immutable lookup table
	"TargetValue": true, "ScaleInCooldown": true, "ScaleOutCooldown": true, "Cooldown": true,
	"MinAdjustmentMagnitude": true, "ScalingAdjustment": true, "MetricIntervalLowerBound": true,
	"MetricIntervalUpperBound": true, "MaxCapacityBuffer": true, "SchedulingBufferTime": true,
	"MinCapacity": true, "MaxCapacity": true,
}

// cfnAASBoolMembers are the boolean members of the same structures.
var cfnAASBoolMembers = map[string]bool{ //nolint:gochecknoglobals // immutable lookup table
	"DisableScaleIn": true, "ReturnData": true, "DynamicScalingInSuspended": true,
	"DynamicScalingOutSuspended": true, "ScheduledScalingSuspended": true,
}

// cfnAASCoerce copies a resolved configuration, converting the string spellings of numeric and
// boolean members back to their JSON types, recursively.
func cfnAASCoerce(m map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = cfnAASCoerceValue(k, v)
	}
	return out
}

// cfnAASCoerceValue converts one member's value; see [cfnAASCoerce].
func cfnAASCoerceValue(key string, v interface{}) interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		return cfnAASCoerce(x)
	case []interface{}:
		list := make([]interface{}, len(x))
		for i, e := range x {
			list[i] = cfnAASCoerceValue(key, e)
		}
		return list
	case string:
		if cfnAASNumericMembers[key] {
			if f, err := strconv.ParseFloat(strings.TrimSpace(x), 64); err == nil {
				return f
			}
		}
		if cfnAASBoolMembers[key] {
			if b, err := strconv.ParseBool(strings.TrimSpace(x)); err == nil {
				return b
			}
		}
	}
	return v
}

// cfnAASTimestamp parses a ScheduledAction's StartTime or EndTime, which a template writes as an
// ISO 8601 timestamp, with or without a zone.
func cfnAASTimestamp(v string) (time.Time, bool) {
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05"} {
		if t, err := time.Parse(layout, strings.TrimSpace(v)); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}
