package emulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// ApplicationAutoScalingPlugin emulates Application Auto Scaling, the JSON 1.1 API whose target
// prefix is AnyScaleFrontendService. It records scalable targets, their scaling policies and their
// scheduled actions, and answers them back; none of them changes a resource's capacity, because
// what Application Auto Scaling would do to the resource is the workload the scope rule leaves out.
// A target tracking policy creates the two CloudWatch alarms AWS creates for it, through the
// CloudWatch plugin, so DescribeAlarms shows what the policy's Alarms member names.
type ApplicationAutoScalingPlugin struct {
	state    StateManager
	logger   Logger
	tc       *TimeController
	registry *PluginRegistry

	// mu serializes every write to a target record: the operations' own and the tagging API's.
	mu sync.Mutex
}

// Name returns the service name "application-autoscaling".
func (p *ApplicationAutoScalingPlugin) Name() string { return appAutoScalingNamespace }

// Initialize sets up the plugin with the provided configuration.
func (p *ApplicationAutoScalingPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	if r, ok := cfg.Options["registry"].(*PluginRegistry); ok {
		p.registry = r
	}
	return nil
}

// Shutdown is a no-op.
func (p *ApplicationAutoScalingPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an Application Auto Scaling request on the X-Amz-Target suffix.
func (p *ApplicationAutoScalingPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op := req.Operation
	if target := req.Headers["X-Amz-Target"]; target != "" {
		if dot := strings.LastIndexByte(target, '.'); dot >= 0 {
			op = target[dot+1:]
		}
	}
	switch op {
	case "RegisterScalableTarget":
		return p.registerScalableTarget(ctx, req)
	case "DescribeScalableTargets":
		return p.describeScalableTargets(ctx, req)
	case "DeregisterScalableTarget":
		return p.deregisterScalableTarget(ctx, req)
	case "PutScalingPolicy":
		return p.putScalingPolicy(ctx, req)
	case "DescribeScalingPolicies":
		return p.describeScalingPolicies(ctx, req)
	case "DeleteScalingPolicy":
		return p.deleteScalingPolicy(ctx, req)
	case "PutScheduledAction":
		return p.putScheduledAction(ctx, req)
	case "DescribeScheduledActions":
		return p.describeScheduledActions(ctx, req)
	case "DeleteScheduledAction":
		return p.deleteScheduledAction(ctx, req)
	case "DescribeScalingActivities":
		return p.describeScalingActivities(ctx, req)
	case "TagResource":
		return p.tagResource(ctx, req)
	case "UntagResource":
		return p.untagResource(ctx, req)
	case "ListTagsForResource":
		return p.listTagsForResource(ctx, req)
	default:
		return nil, unknownActionError(p.Name(), op)
	}
}

// aasDecode reads a JSON request body into dst. An empty body is the empty object, which is what an
// SDK sends for an operation whose input members are all absent.
func aasDecode(req *AWSRequest, dst any) error {
	body := req.Body
	if len(body) == 0 {
		body = []byte("{}")
	}
	if err := json.Unmarshal(body, dst); err != nil {
		return &AWSError{
			Code: "SerializationException", Message: "The request body is not well-formed JSON.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return nil
}

// aasJSON renders v as an application/x-amz-json-1.1 response.
func aasJSON(v any) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("application-autoscaling marshal response: %w", err)
	}
	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}

// loadTarget reads the target stored at key, reporting false when there is none.
func (p *ApplicationAutoScalingPlugin) loadTarget(key string) (*aasTarget, bool, error) {
	raw, err := p.state.Get(context.Background(), appAutoScalingNamespace, key)
	if err != nil {
		return nil, false, fmt.Errorf("application-autoscaling load target: %w", err)
	}
	if raw == nil {
		return nil, false, nil
	}
	var t aasTarget
	if err := json.Unmarshal(raw, &t); err != nil {
		return nil, false, fmt.Errorf("application-autoscaling decode target: %w", err)
	}
	return &t, true, nil
}

// storeTarget writes t at key.
func (p *ApplicationAutoScalingPlugin) storeTarget(key string, t *aasTarget) error {
	return aasStoreTarget(context.Background(), p.state, key, t)
}

// aasStoreTarget writes t at key in state.
func aasStoreTarget(ctx context.Context, state StateManager, key string, t *aasTarget) error {
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("application-autoscaling marshal target: %w", err)
	}
	if err := state.Put(ctx, appAutoScalingNamespace, key, data); err != nil {
		return fmt.Errorf("application-autoscaling store target: %w", err)
	}
	return nil
}

// listTargets returns every target whose key begins with prefix, in key order.
func aasListTargets(ctx context.Context, state StateManager, prefix string) ([]*aasTarget, []string, error) {
	keys, err := state.List(ctx, appAutoScalingNamespace, prefix)
	if err != nil {
		return nil, nil, fmt.Errorf("application-autoscaling list targets: %w", err)
	}
	sort.Strings(keys)
	targets := make([]*aasTarget, 0, len(keys))
	kept := make([]string, 0, len(keys))
	for _, k := range keys {
		raw, err := state.Get(ctx, appAutoScalingNamespace, k)
		if err != nil {
			return nil, nil, fmt.Errorf("application-autoscaling get target %s: %w", k, err)
		}
		if raw == nil {
			continue
		}
		var t aasTarget
		if err := json.Unmarshal(raw, &t); err != nil {
			return nil, nil, fmt.Errorf("application-autoscaling decode target %s: %w", k, err)
		}
		targets = append(targets, &t)
		kept = append(kept, k)
	}
	return targets, kept, nil
}

// aasPageSize reads a Describe operation's MaxResults: absent is def, and a value outside 1..max is
// refused. The bounds are each operation's own ("This value can be between 1 and 50. The default
// value is 50."); the message is substrate's own.
func aasPageSize(maxResults *int32, def, maxSize int32) (int, *AWSError) {
	if maxResults == nil {
		return int(def), nil
	}
	if *maxResults < 1 || *maxResults > maxSize {
		return 0, aasValidation(fmt.Sprintf("MaxResults must be between 1 and %d.", maxSize))
	}
	return int(*maxResults), nil
}

// ----- Scalable targets ----------------------------------------------------------------------------

// aasRegisterInput is RegisterScalableTarget's input.
type aasRegisterInput struct {
	ServiceNamespace  string  `json:"ServiceNamespace"`
	ResourceID        string  `json:"ResourceId"`
	ScalableDimension string  `json:"ScalableDimension"`
	MinCapacity       *int32  `json:"MinCapacity"`
	MaxCapacity       *int32  `json:"MaxCapacity"`
	RoleARN           *string `json:"RoleARN"`
	SuspendedState    *struct {
		DynamicScalingInSuspended  *bool `json:"DynamicScalingInSuspended"`
		DynamicScalingOutSuspended *bool `json:"DynamicScalingOutSuspended"`
		ScheduledScalingSuspended  *bool `json:"ScheduledScalingSuspended"`
	} `json:"SuspendedState"`
	Tags map[string]string `json:"Tags"`
}

// registerScalableTarget handles RegisterScalableTarget, which "Registers or updates a scalable
// target". A new target needs both capacities ("This property is required when registering a new
// scalable target"); an update changes only the members it names ("Any parameters that you don't
// specify are not changed by this update request"), and never the tags ("To tag an existing
// scalable target, use the TagResource operation").
func (p *ApplicationAutoScalingPlugin) registerScalableTarget(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in aasRegisterInput
	if err := aasDecode(req, &in); err != nil {
		return nil, err
	}
	if awsErr := aasCheckTarget(in.ServiceNamespace, in.ScalableDimension, in.ResourceID); awsErr != nil {
		return nil, awsErr
	}
	if in.MinCapacity != nil && *in.MinCapacity < 0 {
		return nil, aasValidation("MinCapacity must be greater than or equal to 0.")
	}
	if in.MaxCapacity != nil && *in.MaxCapacity < 0 {
		return nil, aasValidation("MaxCapacity must be greater than or equal to 0.")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	key := aasTargetKey(ctx.AccountID, ctx.Region, in.ServiceNamespace, in.ScalableDimension, in.ResourceID)
	t, found, err := p.loadTarget(key)
	if err != nil {
		return nil, err
	}
	if !found {
		if in.MinCapacity == nil || in.MaxCapacity == nil {
			return nil, aasValidation("MinCapacity and MaxCapacity are required when registering a new scalable target.")
		}
		if awsErr := aasCheckTags(in.Tags, nil); awsErr != nil {
			return nil, awsErr
		}
		role := aasServiceLinkedRoleARN(ctx.AccountID, in.ServiceNamespace)
		if in.RoleARN != nil {
			role = *in.RoleARN
		}
		if role == "" {
			return nil, aasValidation("RoleARN is required for service namespace " + in.ServiceNamespace + ".")
		}
		if err := p.checkResourceExists(ctx, in.ServiceNamespace, in.ResourceID); err != nil {
			return nil, err
		}
		t = &aasTarget{
			ServiceNamespace:  in.ServiceNamespace,
			ResourceID:        in.ResourceID,
			ScalableDimension: in.ScalableDimension,
			RoleARN:           role,
			SuspendedState:    &aasSuspendedState{},
			ScalableTargetARN: fmt.Sprintf("arn:aws:application-autoscaling:%s:%s:scalable-target/%s",
				ctx.Region, ctx.AccountID, ctx.IDs.Hex(18)),
			CreationTime: p.tc.Now().UTC(),
		}
		if len(in.Tags) > 0 {
			t.Tags = in.Tags
		}
	} else if in.RoleARN != nil {
		t.RoleARN = *in.RoleARN
	}
	if in.MinCapacity != nil {
		t.MinCapacity = *in.MinCapacity
	}
	if in.MaxCapacity != nil {
		t.MaxCapacity = *in.MaxCapacity
	}
	if t.MinCapacity > t.MaxCapacity {
		return nil, aasValidation(fmt.Sprintf("Maximum capacity (%d) cannot be less than minimum capacity (%d).",
			t.MaxCapacity, t.MinCapacity))
	}
	if s := in.SuspendedState; s != nil {
		if t.SuspendedState == nil {
			t.SuspendedState = &aasSuspendedState{}
		}
		if s.DynamicScalingInSuspended != nil {
			t.SuspendedState.DynamicScalingInSuspended = *s.DynamicScalingInSuspended
		}
		if s.DynamicScalingOutSuspended != nil {
			t.SuspendedState.DynamicScalingOutSuspended = *s.DynamicScalingOutSuspended
		}
		if s.ScheduledScalingSuspended != nil {
			t.SuspendedState.ScheduledScalingSuspended = *s.ScheduledScalingSuspended
		}
	}
	if err := p.storeTarget(key, t); err != nil {
		return nil, err
	}
	return aasJSON(struct {
		ScalableTargetARN string `json:"ScalableTargetARN"`
	}{t.ScalableTargetARN})
}

// checkResourceExists asks the owning plugin whether the resource a new DynamoDB or ECS target
// names exists. AWS checks the resource for these namespaces: an ECS target for a missing service is
// refused with a ValidationException whose message contains "ECS service doesn't exist", which the
// Terraform AWS provider retries on (internal/service/appautoscaling/target.go,
// registerScalableTarget). The other namespaces' resources are not emulated, or not checked, so
// their targets register whatever they name. Without a registry nothing is checked.
//
// Only an answer that the resource is absent becomes the ValidationException; any other failure
// of the owning plugin is returned as it is.
func (p *ApplicationAutoScalingPlugin) checkResourceExists(ctx *RequestContext, namespace, resourceID string) error {
	if p.registry == nil {
		return nil
	}
	switch namespace {
	case "dynamodb":
		parts := strings.Split(strings.TrimPrefix(resourceID, "table/"), "/index/")
		exists, err := p.dynamoDBResourceExists(ctx, parts)
		if err != nil {
			return err
		}
		if !exists {
			// The message is substrate's own.
			return aasValidation(fmt.Sprintf("DynamoDB resource doesn't exist: %s", resourceID))
		}
	case "ecs":
		parts := strings.SplitN(strings.TrimPrefix(resourceID, "service/"), "/", 2)
		exists := false
		if len(parts) == 2 {
			var err error
			if exists, err = p.ecsServiceExists(ctx, parts[0], parts[1]); err != nil {
				return err
			}
		}
		if !exists {
			// "ECS service doesn't exist" is the substring the Terraform AWS provider matches on AWS's
			// answer; the rest of the message is substrate's own.
			return aasValidation(fmt.Sprintf("ECS service doesn't exist: %s", resourceID))
		}
	}
	return nil
}

// aasRouteJSON sends a JSON request to another plugin. An AWS refusal whose code is in absent is
// reported as found false; any other failure is returned.
func (p *ApplicationAutoScalingPlugin) aasRouteJSON(ctx *RequestContext, service, op, contentType string, body any,
	absent ...string,
) ([]byte, bool, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, false, fmt.Errorf("application-autoscaling marshal %s: %w", op, err)
	}
	resp, err := p.registry.RouteRequest(ctx, &AWSRequest{
		Service: service, Operation: op, Body: raw,
		Headers: map[string]string{"Content-Type": contentType}, Params: map[string]string{},
	})
	var awsErr *AWSError
	if errors.As(err, &awsErr) && slices.Contains(absent, awsErr.Code) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("application-autoscaling %s %s: %w", service, op, err)
	}
	if resp == nil {
		return nil, false, fmt.Errorf("application-autoscaling %s %s: no response", service, op)
	}
	return resp.Body, true, nil
}

// dynamoDBResourceExists reports whether the table parts[0] exists and, when parts names an index,
// whether the table has that global secondary index. DescribeTable's ResourceNotFoundException is
// the table's absence.
func (p *ApplicationAutoScalingPlugin) dynamoDBResourceExists(ctx *RequestContext, parts []string) (bool, error) {
	body, found, err := p.aasRouteJSON(ctx, "dynamodb", "DescribeTable", "application/x-amz-json-1.0",
		map[string]string{"TableName": parts[0]}, "ResourceNotFoundException")
	if err != nil || !found {
		return false, err
	}
	if len(parts) < 2 {
		return true, nil
	}
	var out struct {
		Table struct {
			GlobalSecondaryIndexes []struct {
				IndexName string `json:"IndexName"`
			} `json:"GlobalSecondaryIndexes"`
		} `json:"Table"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return false, fmt.Errorf("application-autoscaling decode DescribeTable: %w", err)
	}
	for _, gsi := range out.Table.GlobalSecondaryIndexes {
		if gsi.IndexName == parts[1] {
			return true, nil
		}
	}
	return false, nil
}

// ecsServiceExists reports whether ECS knows an active service named svc in cluster. A missing
// cluster (ClusterNotFoundException), a service DescribeServices reports as a failure, and an
// INACTIVE service are absences.
func (p *ApplicationAutoScalingPlugin) ecsServiceExists(ctx *RequestContext, cluster, svc string) (bool, error) {
	body, found, err := p.aasRouteJSON(ctx, "ecs", "DescribeServices", "application/x-amz-json-1.1",
		map[string]any{"cluster": cluster, "services": []string{svc}}, "ClusterNotFoundException")
	if err != nil || !found {
		return false, err
	}
	var out struct {
		Services []struct {
			Status string `json:"status"`
		} `json:"services"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return false, fmt.Errorf("application-autoscaling decode DescribeServices: %w", err)
	}
	return len(out.Services) == 1 && out.Services[0].Status != "INACTIVE", nil
}

// aasDescribeTargetsInput is DescribeScalableTargets' input.
type aasDescribeTargetsInput struct {
	ServiceNamespace  string   `json:"ServiceNamespace"`
	ResourceIDs       []string `json:"ResourceIds"`
	ScalableDimension string   `json:"ScalableDimension"`
	MaxResults        *int32   `json:"MaxResults"`
	NextToken         string   `json:"NextToken"`
}

// aasCheckFilter validates a Describe operation's namespace and optional dimension filter.
func aasCheckFilter(namespace, dimension string) *AWSError {
	if namespace == "" {
		return aasValidation("ServiceNamespace is required.")
	}
	if _, ok := aasServiceLinkedRoles[namespace]; !ok && namespace != "elasticmapreduce" {
		return aasValidation(fmt.Sprintf("Unsupported service namespace: %s.", namespace))
	}
	if dimension != "" {
		if _, ok := aasDimensions[dimension]; !ok || !strings.HasPrefix(dimension, namespace+":") {
			return aasValidation(fmt.Sprintf("Unsupported scalable dimension: %s.", dimension))
		}
	}
	return nil
}

// describeScalableTargets handles DescribeScalableTargets: the namespace's targets, narrowed by
// ResourceIds and ScalableDimension, in resource ID order, 50 to a page by default.
func (p *ApplicationAutoScalingPlugin) describeScalableTargets(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in aasDescribeTargetsInput
	if err := aasDecode(req, &in); err != nil {
		return nil, err
	}
	if awsErr := aasCheckFilter(in.ServiceNamespace, in.ScalableDimension); awsErr != nil {
		return nil, awsErr
	}
	if len(in.ResourceIDs) > 50 {
		return nil, aasValidation("ResourceIds must contain no more than 50 items.")
	}
	size, awsErr := aasPageSize(in.MaxResults, 50, 50)
	if awsErr != nil {
		return nil, awsErr
	}
	preq := pageRequest{
		Op: "DescribeScalableTargets", Token: in.NextToken, Size: size,
		Filter: map[string][]string{
			"ServiceNamespace": {in.ServiceNamespace}, "ResourceIds": in.ResourceIDs,
			"ScalableDimension": {in.ScalableDimension},
		},
	}
	if err := preq.validate(); err != nil {
		return nil, aasInvalidNextToken()
	}
	targets, _, err := aasListTargets(context.Background(), p.state, aasNamespacePrefix(ctx.AccountID, ctx.Region, in.ServiceNamespace))
	if err != nil {
		return nil, err
	}
	wanted := make(map[string]bool, len(in.ResourceIDs))
	for _, id := range in.ResourceIDs {
		wanted[id] = true
	}
	var matched []*aasTarget
	for _, t := range targets {
		if (len(wanted) == 0 || wanted[t.ResourceID]) && (in.ScalableDimension == "" || t.ScalableDimension == in.ScalableDimension) {
			matched = append(matched, t)
		}
	}
	res, err := page(matched, func(t *aasTarget) cursorKey {
		return cursorKey{Sort: t.ResourceID, Tie: t.ScalableDimension}
	}, preq)
	if err != nil {
		return nil, aasInvalidNextToken()
	}
	out := aasDescribeTargetsOut{ScalableTargets: make([]aasTargetOut, 0, len(res.Items)), NextToken: res.Next}
	for _, t := range res.Items {
		out.ScalableTargets = append(out.ScalableTargets, aasTargetToWire(t))
	}
	return aasJSON(out)
}

// aasTargetRef is the input members that name a target.
type aasTargetRef struct {
	ServiceNamespace  string `json:"ServiceNamespace"`
	ResourceID        string `json:"ResourceId"`
	ScalableDimension string `json:"ScalableDimension"`
}

// deregisterScalableTarget handles DeregisterScalableTarget, which "deletes the scaling policies and
// the scheduled actions that are associated with it" and, with them, the alarms the target tracking
// policies created.
func (p *ApplicationAutoScalingPlugin) deregisterScalableTarget(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in aasTargetRef
	if err := aasDecode(req, &in); err != nil {
		return nil, err
	}
	if awsErr := aasCheckTarget(in.ServiceNamespace, in.ScalableDimension, in.ResourceID); awsErr != nil {
		return nil, awsErr
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	key := aasTargetKey(ctx.AccountID, ctx.Region, in.ServiceNamespace, in.ScalableDimension, in.ResourceID)
	t, found, err := p.loadTarget(key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, aasTargetNotFound(in.ServiceNamespace, in.ResourceID, in.ScalableDimension)
	}
	if err := p.state.Delete(context.Background(), appAutoScalingNamespace, key); err != nil {
		return nil, fmt.Errorf("application-autoscaling deregister: %w", err)
	}
	for _, pol := range t.Policies {
		p.deleteAlarms(ctx, pol.Alarms)
	}
	return aasJSON(struct{}{})
}

// describeScalingActivities handles DescribeScalingActivities. Substrate scales nothing, so no
// target has an activity, and the answer is an empty list after the request's members are checked.
func (p *ApplicationAutoScalingPlugin) describeScalingActivities(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		ServiceNamespace  string `json:"ServiceNamespace"`
		ResourceID        string `json:"ResourceId"`
		ScalableDimension string `json:"ScalableDimension"`
		MaxResults        *int32 `json:"MaxResults"`
		NextToken         string `json:"NextToken"`
	}
	if err := aasDecode(req, &in); err != nil {
		return nil, err
	}
	if awsErr := aasCheckFilter(in.ServiceNamespace, in.ScalableDimension); awsErr != nil {
		return nil, awsErr
	}
	if _, awsErr := aasPageSize(in.MaxResults, 50, 50); awsErr != nil {
		return nil, awsErr
	}
	if in.NextToken != "" {
		// No page was ever issued, so no token can resume one.
		return nil, aasInvalidNextToken()
	}
	return aasJSON(struct {
		ScalingActivities []struct{} `json:"ScalingActivities"`
	}{ScalingActivities: []struct{}{}})
}
