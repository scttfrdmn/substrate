package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// CloudFormation's two Application Auto Scaling types (#1661). Both operations they dispatch to are
// create-or-update: a create refuses a target or policy that already exists rather than adopting it,
// and an update applies a changed No interruption property in place under the same physical ID; a
// change to a property whose "Update requires" is Replacement is a replacement (see
// cfnRecreationProperties).

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

// cfnAASTargetBody renders the RegisterScalableTarget body a declaration describes. Tags are sent
// only on a create, because RegisterScalableTarget ignores them on an update ("To tag an existing
// scalable target, use the TagResource operation"). On an update (withTags false), a SuspendedState
// or RoleARN the previous declaration set and this one removes is reset — every flag false, and the
// namespace's service-linked role — because RegisterScalableTarget leaves an omitted member as it was.
func cfnAASTargetBody(props map[string]interface{}, cctx *cfnContext, namespace, dimension, resourceID string, withTags bool) map[string]interface{} {
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
	if tags := cfnTagMap(cfnResolveStructured(props["Tags"], cctx), cctx); withTags && len(tags) > 0 {
		body["Tags"] = tags
	}
	if !withTags && cctx.priorProps != nil {
		if _, had := cctx.priorProps["SuspendedState"]; had && body["SuspendedState"] == nil {
			body["SuspendedState"] = map[string]interface{}{
				"DynamicScalingInSuspended": false, "DynamicScalingOutSuspended": false, "ScheduledScalingSuspended": false,
			}
		}
		if _, had := cctx.priorProps["RoleARN"]; had && body["RoleARN"] == nil {
			if role := aasServiceLinkedRoleARN(cctx.accountID, namespace); role != "" {
				body["RoleARN"] = role
			}
		}
	}
	return body
}

// cfnAASTagDiff returns the tags to add and the keys to remove to take a target from the previous
// declaration's Tags to this one's.
func cfnAASTagDiff(props map[string]interface{}, cctx *cfnContext) (map[string]string, []string) {
	now := cfnTagMap(cfnResolveStructured(props["Tags"], cctx), cctx)
	var before map[string]string
	if cctx.priorProps != nil {
		before = cfnTagMap(cfnResolveStructured(cctx.priorProps["Tags"], cctx), cctx)
	}
	add := map[string]string{}
	for k, v := range now {
		if old, ok := before[k]; !ok || old != v {
			add[k] = v
		}
	}
	var remove []string
	for k := range before {
		if _, ok := now[k]; !ok {
			remove = append(remove, k)
		}
	}
	sort.Strings(remove)
	return add, remove
}

// cfnAASCall dispatches an Application Auto Scaling request and decodes its answer into out, which
// may be nil.
func (d *StackDeployer) cfnAASCall(ctx context.Context, operation string, body map[string]interface{}, out any, streamID string) (float64, error) {
	req, err := cfnAASRequest(operation, body)
	if err != nil {
		return 0, err
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	if routeErr != nil {
		return cost, routeErr
	}
	if out != nil && resp != nil {
		if err := json.Unmarshal(resp.Body, out); err != nil {
			return cost, fmt.Errorf("cfn application-autoscaling %s decode: %w", operation, err)
		}
	}
	return cost, nil
}

// cfnAASListed reports whether a Describe operation lists anything under member for body.
func (d *StackDeployer) cfnAASListed(ctx context.Context, operation, member string, body map[string]interface{}, streamID string) (bool, float64, error) {
	var out map[string][]json.RawMessage
	cost, err := d.cfnAASCall(ctx, operation, body, &out, streamID)
	return len(out[member]) > 0, cost, err
}

// deployApplicationAutoScalingScalableTarget registers the target with RegisterScalableTarget and
// puts each declared ScheduledActions entry with PutScheduledAction.
//
// RegisterScalableTarget creates or updates, so a create would otherwise adopt a target that already
// exists — one another stack or caller registered — and the stack's delete would then deregister it,
// with its policies and alarms. A create of a target that already exists is refused instead (the
// message is substrate's own). A create whose scheduled actions fail deregisters the target it
// registered, so the failed resource leaves nothing behind for a rollback to miss.
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
	exists, cost, err := d.cfnAASListed(ctx, "DescribeScalableTargets", "ScalableTargets", map[string]interface{}{
		"ServiceNamespace": namespace, "ScalableDimension": dimension, "ResourceIds": []string{resourceID},
	}, streamID)
	if err != nil {
		dr.Error = err.Error()
		return dr, cost, nil //nolint:nilerr // A refused call is reported on the resource.
	}
	if exists {
		dr.Error = fmt.Sprintf("scalable target %s already exists", dr.PhysicalID)
		return dr, cost, nil
	}
	var out struct {
		ScalableTargetARN string `json:"ScalableTargetARN"`
	}
	c, err := d.cfnAASCall(ctx, "RegisterScalableTarget", cfnAASTargetBody(props, cctx, namespace, dimension, resourceID, true), &out, streamID)
	cost += c
	if err != nil {
		dr.Error = err.Error()
		return dr, cost, nil //nolint:nilerr // A refused call is reported on the resource.
	}
	dr.ARN = out.ScalableTargetARN
	names, c, err := d.syncScheduledActions(ctx, props, namespace, dimension, resourceID, nil, streamID, cctx)
	cost += c
	if err != nil {
		dr.Error = err.Error()
		if req := cfnScalableTargetDeleter(d, dr, props, cctx); req != nil {
			_, c, undoErr := d.dispatch(ctx, req, streamID)
			cost += c
			if undoErr != nil {
				dr.Error += "; deregistering the target also failed: " + undoErr.Error()
			}
		}
		return dr, cost, nil
	}
	dr.Metadata["ScheduledActionNames"] = names
	return dr, cost, nil
}

// updateApplicationAutoScalingScalableTarget applies a changed No interruption property —
// MinCapacity, MaxCapacity, RoleARN, SuspendedState, ScheduledActions — through
// RegisterScalableTarget's update and the scheduled-action sync. A failure is reported on the
// resource, and the update's rollback applies the previous declaration the same way.
func (d *StackDeployer) updateApplicationAutoScalingScalableTarget(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, _ []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	dr.Error = ""
	dr.Metadata = make(map[string]interface{}, len(prior.Metadata))
	for k, v := range prior.Metadata {
		dr.Metadata[k] = v
	}
	namespace, dimension, resourceID := cfnRefMetadata(prior, "ServiceNamespace"), cfnRefMetadata(prior, "ScalableDimension"),
		cfnRefMetadata(prior, "ResourceId")
	cost, err := d.cfnAASCall(ctx, "RegisterScalableTarget", cfnAASTargetBody(props, cctx, namespace, dimension, resourceID, false), nil, streamID)
	if err != nil {
		dr.Error = err.Error()
		return dr, cost, nil //nolint:nilerr // A refused call is reported on the resource.
	}
	if add, remove := cfnAASTagDiff(props, cctx); prior.ARN != "" && (len(add) > 0 || len(remove) > 0) {
		if len(add) > 0 {
			c, err := d.cfnAASCall(ctx, "TagResource", map[string]interface{}{"ResourceARN": prior.ARN, "Tags": add}, nil, streamID)
			cost += c
			if err != nil {
				dr.Error = err.Error()
				return dr, cost, nil //nolint:nilerr // A refused call is reported on the resource.
			}
		}
		if len(remove) > 0 {
			c, err := d.cfnAASCall(ctx, "UntagResource", map[string]interface{}{"ResourceARN": prior.ARN, "TagKeys": remove}, nil, streamID)
			cost += c
			if err != nil {
				dr.Error = err.Error()
				return dr, cost, nil //nolint:nilerr // A refused call is reported on the resource.
			}
		}
	}
	previous, _ := cfnMetadataList(prior, "ScheduledActionNames")
	names, c, err := d.syncScheduledActions(ctx, props, namespace, dimension, resourceID, previous, streamID, cctx)
	cost += c
	if err != nil {
		// The actions this update added are deleted again, so the rollback — which re-applies the
		// previous declaration — finds only the actions that declaration names. The record keeps
		// the union, so any of them this cleanup could not delete is still the stack's to delete.
		had := map[string]bool{}
		for _, n := range previous {
			had[n] = true
		}
		var left []string
		for _, n := range names {
			if had[n] {
				continue
			}
			c, delErr := d.cfnAASCall(ctx, "DeleteScheduledAction", map[string]interface{}{
				"ServiceNamespace": namespace, "ScalableDimension": dimension, "ResourceId": resourceID,
				"ScheduledActionName": n,
			}, nil, streamID)
			cost += c
			if delErr != nil && !cfnDeleteIsAbsent(delErr) {
				left = append(left, n)
			}
		}
		dr.Metadata["ScheduledActionNames"] = cfnAASUnion(previous, left)
		dr.Error = err.Error()
		return dr, cost, nil //nolint:nilerr // A refused call is reported on the resource.
	}
	dr.Metadata["ScheduledActionNames"] = names
	return dr, cost, nil
}

// cfnAASUnion returns the sorted, deduplicated union of two name lists.
func cfnAASUnion(a, b []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, n := range append(append([]string{}, a...), b...) {
		if n != "" && !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

// syncScheduledActions puts each ScheduledActions entry on the target, and deletes the actions the
// previous declaration named and this one does not. An action the stack never declared — one made
// outside it — is left alone. It returns the names this declaration holds.
func (d *StackDeployer) syncScheduledActions(
	ctx context.Context, props map[string]interface{}, namespace, dimension, resourceID string, previous []string,
	streamID string, cctx *cfnContext,
) ([]string, float64, error) {
	var cost float64
	var names []string
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
		names = append(names, name)
		c, err := d.cfnAASCall(ctx, "PutScheduledAction", body, nil, streamID)
		cost += c
		if err != nil {
			return names, cost, err
		}
	}
	for _, name := range previous {
		if declared[name] {
			continue
		}
		c, err := d.cfnAASCall(ctx, "DeleteScheduledAction", map[string]interface{}{
			"ServiceNamespace": namespace, "ScalableDimension": dimension, "ResourceId": resourceID,
			"ScheduledActionName": name,
		}, nil, streamID)
		cost += c
		if err != nil && !cfnDeleteIsAbsent(err) {
			return names, cost, err
		}
	}
	sort.Strings(names)
	return names, cost, nil
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

// cfnAASPolicyBody renders the PutScalingPolicy body a declaration describes.
func cfnAASPolicyBody(props map[string]interface{}, cctx *cfnContext, name, namespace, dimension, resourceID string) map[string]interface{} {
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
	return body
}

// deployApplicationAutoScalingScalingPolicy puts the policy with PutScalingPolicy. "Ref returns the
// Application Auto Scaling scaling policy Amazon Resource Name (ARN)", which is the physical ID and
// Fn::GetAtt Arn. PutScalingPolicy creates or updates, so a create of a policy the target already
// holds under the name is refused rather than adopted (the message is substrate's own).
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
	var listed struct {
		ScalingPolicies []struct {
			PolicyARN string `json:"PolicyARN"`
		} `json:"ScalingPolicies"`
	}
	cost, err := d.cfnAASCall(ctx, "DescribeScalingPolicies", map[string]interface{}{
		"ServiceNamespace": namespace, "ScalableDimension": dimension, "ResourceId": resourceID, "PolicyNames": []string{name},
	}, &listed, streamID)
	if err != nil {
		dr.Error = err.Error()
		return dr, cost, nil //nolint:nilerr // A refused call is reported on the resource.
	}
	// The policy this stack already holds under the logical ID is its own: a replacement that keeps
	// the policy's name and target — a ScalingTargetId swapped for the explicit three members — is
	// the same policy, updated in place.
	own := len(listed.ScalingPolicies) == 1 && cctx.priorPhysicalID != "" && listed.ScalingPolicies[0].PolicyARN == cctx.priorPhysicalID
	if len(listed.ScalingPolicies) > 0 && !own {
		dr.Error = fmt.Sprintf("scaling policy %s already exists on %s", name, cfnScalableTargetID(resourceID, dimension, namespace))
		return dr, cost, nil
	}
	var out struct {
		PolicyARN string `json:"PolicyARN"`
	}
	c, err := d.cfnAASCall(ctx, "PutScalingPolicy", cfnAASPolicyBody(props, cctx, name, namespace, dimension, resourceID), &out, streamID)
	cost += c
	if err != nil {
		dr.Error = err.Error()
		return dr, cost, nil //nolint:nilerr // A refused call is reported on the resource.
	}
	dr.PhysicalID, dr.ARN = out.PolicyARN, out.PolicyARN
	dr.Metadata["Arn"] = out.PolicyARN
	return dr, cost, nil
}

// updateApplicationAutoScalingScalingPolicy applies a changed PolicyType or configuration — the No
// interruption properties — through PutScalingPolicy's update, which keeps the policy's ARN.
func (d *StackDeployer) updateApplicationAutoScalingScalingPolicy(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, _ []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	dr.Error = ""
	body := cfnAASPolicyBody(props, cctx, cfnRefMetadata(prior, "PolicyName"), cfnRefMetadata(prior, "ServiceNamespace"),
		cfnRefMetadata(prior, "ScalableDimension"), cfnRefMetadata(prior, "ResourceId"))
	cost, err := d.cfnAASCall(ctx, "PutScalingPolicy", body, nil, streamID)
	if err != nil {
		dr.Error = err.Error()
	}
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
