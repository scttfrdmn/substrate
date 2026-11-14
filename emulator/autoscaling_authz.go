package emulator

import (
	"sort"
)

// The resource an Auto Scaling request is authorized against (#1460).
//
// The Service Authorization Reference (vendored as emulator/authzref/autoscaling.json) gives
// every mutating action the autoScalingGroup resource type and every describe none, so a
// describe is decided against "*" and a mutation against the group's ARN:
//
//	arn:${Partition}:autoscaling:${Region}:${Account}:autoScalingGroup:${GroupId}:autoScalingGroupName/${GroupFriendlyName}
//
// The GroupId is a UUID minted at creation and known only to the stored record, so the ARN
// is read rather than assembled. A group that does not exist yet — CreateAutoScalingGroup's
// — has none, and is named with the UUID segment as the literal "*": the user guide says that
// to name a group in a CreateAutoScalingGroup policy "you must replace the UUID with a
// wildcard (*)", so a statement written that way matches, and so does one naming the whole
// type.
//
// # Which group a request is about
//
// The decision must be about the group the handler will act on, or a policy scoped to one
// group is a way into another. So:
//
//   - **TerminateInstanceInAutoScalingGroup by InstanceId** is about the group that owns the
//     instance, whatever AutoScalingGroupName says — the handler terminates the owner's
//     instance, and refuses a request whose AutoScalingGroupName names a different group.
//   - **CreateOrUpdateTags and DeleteTags** are about every group a Tags.member.N.ResourceId
//     names, each decided separately ([asgAuthzTagResources]); one denial denies the request,
//     which writes nothing.
//   - Everything else is about AutoScalingGroupName.

// asgAuthzGroupName returns the name of the one group a request is about, or "".
func asgAuthzGroupName(state StateManager, reqCtx *RequestContext, req *AWSRequest) string {
	if req.Operation == "TerminateInstanceInAutoScalingGroup" {
		if id := req.Params["InstanceId"]; id != "" {
			if g, err := asgGroupForInstance(state, reqCtx, id); err == nil && g != nil {
				return g.Name
			}
		}
	}
	if name := req.Params["AutoScalingGroupName"]; name != "" {
		return name
	}
	if tags := asgParseTags(req.Params); len(tags) > 0 && tags[0].resourceID != "" {
		return tags[0].resourceID
	}
	return ""
}

// asgAuthzGroup returns the stored group a request is about, or nil — for an action AWS
// scopes to "*", for a request naming no group, and for a group that does not exist.
func asgAuthzGroup(state StateManager, reqCtx *RequestContext, req *AWSRequest) *AutoScalingGroup {
	if !authzActionSupportsResourceType(autoscalingNamespace, req.Operation, "autoScalingGroup") {
		return nil
	}
	name := asgAuthzGroupName(state, reqCtx, req)
	if name == "" {
		return nil
	}
	g, err := asgLoadGroup(state, reqCtx, name)
	if err != nil {
		return nil
	}
	return g
}

// asgAuthzResourceARN is the resource an Auto Scaling request is decided against.
func asgAuthzResourceARN(state StateManager, reqCtx *RequestContext, req *AWSRequest) string {
	if !authzActionSupportsResourceType(autoscalingNamespace, req.Operation, "autoScalingGroup") {
		return "*"
	}
	if g := asgAuthzGroup(state, reqCtx, req); g != nil {
		return g.ARN
	}
	name := asgAuthzGroupName(state, reqCtx, req)
	if name == "" {
		return "*"
	}
	return asgGroupARN(reqCtx.Region, reqCtx.AccountID, "*", name)
}

// asgAuthzTagResources resolves a CreateOrUpdateTags or DeleteTags request to one resource per
// distinct group its Tags members name, in first-mention order. Each carries that group's own
// ARN and stored tags, and — in its Context and Multi — only the request tags aimed at it as
// aws:RequestTag/<key> and aws:TagKeys, so a tag applied to one group cannot satisfy or fail a
// condition evaluated for another. The request-level arm of [addRequestTags] contributes nothing
// for these two operations, which is what keeps the scoping exact.
//
// It returns nil for any other operation, or a request naming no group, leaving the
// single-resource path to answer.
func asgAuthzTagResources(state StateManager, reqCtx *RequestContext, req *AWSRequest) []authzResource {
	if req.Operation != "CreateOrUpdateTags" && req.Operation != "DeleteTags" {
		return nil
	}
	var order []string
	byGroup := map[string]map[string]string{}
	for _, t := range asgParseTags(req.Params) {
		if t.resourceID == "" {
			continue
		}
		if _, ok := byGroup[t.resourceID]; !ok {
			order = append(order, t.resourceID)
			byGroup[t.resourceID] = map[string]string{}
		}
		byGroup[t.resourceID][t.Key] = t.Value
	}
	out := make([]authzResource, 0, len(order))
	for _, name := range order {
		res := authzResource{ARN: asgGroupARN(reqCtx.Region, reqCtx.AccountID, "*", name)}
		if g, err := asgLoadGroup(state, reqCtx, name); err == nil && g != nil {
			res.ARN = g.ARN
			res.Tags = g.tagMap()
		}
		res.Context = make(map[string]string, len(byGroup[name]))
		keys := make([]string, 0, len(byGroup[name]))
		for k, v := range byGroup[name] {
			res.Context["aws:RequestTag/"+k] = v
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if len(keys) > 0 {
			res.Multi = map[string][]string{"aws:TagKeys": keys}
		}
		out = append(out, res)
	}
	return out
}
