package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// cfnTemplate is the top-level CloudFormation template structure.
type cfnTemplate struct {
	AWSTemplateFormatVersion string                 `json:"AWSTemplateFormatVersion" yaml:"AWSTemplateFormatVersion"`
	Description              string                 `json:"Description,omitempty"    yaml:"Description,omitempty"`
	Resources                map[string]cfnResource `json:"Resources"                yaml:"Resources"`
}

// cfnResource is a single CloudFormation resource declaration.
type cfnResource struct {
	Type       string                 `json:"Type"                yaml:"Type"`
	Properties map[string]interface{} `json:"Properties"          yaml:"Properties"`
	DependsOn  interface{}            `json:"DependsOn,omitempty" yaml:"DependsOn,omitempty"`
}

// typePriority determines deployment order for CloudFormation resources.
// Lower numbers deploy first.
var typePriority = map[string]int{
	"AWS::IAM::Policy":      0,
	"AWS::IAM::Role":        1,
	"AWS::S3::Bucket":       2,
	"AWS::Lambda::Function": 3,
}

// StackDeployer parses and deploys a CloudFormation template using in-process
// plugin dispatch.
type StackDeployer struct {
	registry *PluginRegistry
	store    *EventStore
	tc       *TimeController
	logger   Logger
	costs    *CostController
}

// Deploy parses cfn and deploys all resources, returning a DeployResult.
// Resources are deployed in type-priority order (IAM::Policy → IAM::Role →
// S3::Bucket → Lambda::Function). Unknown resource types are skipped with a
// warning logged. AWS::Lambda::Function returns NotImplemented; remaining
// resources in the template still deploy.
func (d *StackDeployer) Deploy(ctx context.Context, cfn, streamID string) (*DeployResult, error) {
	tmpl, err := parseCFNTemplate(cfn)
	if err != nil {
		return nil, fmt.Errorf("parse template: %w", err)
	}

	// Determine stack name from stream ID.
	stackName := streamID

	start := d.tc.Now()

	// Sort logical IDs by type priority, then alphabetically for stability.
	type entry struct {
		logicalID string
		resource  cfnResource
		priority  int
	}
	entries := make([]entry, 0, len(tmpl.Resources))
	for logicalID, res := range tmpl.Resources {
		p, ok := typePriority[res.Type]
		if !ok {
			p = 99
		}
		entries = append(entries, entry{logicalID: logicalID, resource: res, priority: p})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].priority != entries[j].priority {
			return entries[i].priority < entries[j].priority
		}
		return entries[i].logicalID < entries[j].logicalID
	})

	resources := make([]DeployedResource, 0, len(entries))
	var totalCost float64

	for _, e := range entries {
		dr, cost, deployErr := d.deployResource(ctx, e.logicalID, e.resource, streamID)
		if deployErr != nil {
			return nil, fmt.Errorf("deploy resource %s: %w", e.logicalID, deployErr)
		}
		totalCost += cost
		resources = append(resources, dr)
	}

	duration := d.tc.Now().Sub(start)

	return &DeployResult{
		StackName: stackName,
		Resources: resources,
		StreamID:  streamID,
		TotalCost: totalCost,
		Duration:  duration,
	}, nil
}

// deployResource dispatches a single CFN resource to the correct deploy helper.
// It returns the DeployedResource, the request cost, and any infrastructure error.
func (d *StackDeployer) deployResource(
	ctx context.Context,
	logicalID string,
	res cfnResource,
	streamID string,
) (DeployedResource, float64, error) {
	switch res.Type {
	case "AWS::IAM::Policy":
		return d.deployIAMPolicy(ctx, logicalID, res.Properties, streamID)
	case "AWS::IAM::Role":
		return d.deployIAMRole(ctx, logicalID, res.Properties, streamID)
	case "AWS::S3::Bucket":
		return d.deployS3Bucket(ctx, logicalID, res.Properties, streamID)
	case "AWS::Lambda::Function":
		return d.deployLambdaFunction(logicalID)
	default:
		d.logger.Warn("unknown CloudFormation resource type; skipping",
			"logical_id", logicalID,
			"type", res.Type,
		)
		return DeployedResource{
			LogicalID: logicalID,
			Type:      res.Type,
		}, 0, nil
	}
}

// deployS3Bucket creates an S3 bucket for the given CFN resource.
func (d *StackDeployer) deployS3Bucket(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
) (DeployedResource, float64, error) {
	bucketName := strings.ToLower(stringProp(props, "BucketName", logicalID))

	req := &AWSRequest{
		Service:   "s3",
		Operation: "PUT",
		Path:      "/" + bucketName,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)

	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::S3::Bucket",
		PhysicalID: bucketName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	_ = resp

	return dr, cost, nil
}

// deployIAMRole creates an IAM role for the given CFN resource.
func (d *StackDeployer) deployIAMRole(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
) (DeployedResource, float64, error) {
	roleName := stringProp(props, "RoleName", logicalID)

	body := map[string]string{
		"RoleName":                 roleName,
		"Path":                     stringProp(props, "Path", "/"),
		"AssumeRolePolicyDocument": marshalToJSON(props["AssumeRolePolicyDocument"]),
		"Description":              stringProp(props, "Description", ""),
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal role body: %w", err)
	}

	req := &AWSRequest{
		Service:   "iam",
		Operation: "CreateRole",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)

	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::IAM::Role",
		PhysicalID: roleName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		// Try to extract the ARN from the response body.
		var result struct {
			Role struct {
				ARN string `json:"Arn"`
			} `json:"Role"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			dr.ARN = result.Role.ARN
		}
	}

	return dr, cost, nil
}

// deployIAMPolicy creates an IAM managed policy for the given CFN resource.
func (d *StackDeployer) deployIAMPolicy(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
) (DeployedResource, float64, error) {
	policyName := stringProp(props, "PolicyName", logicalID)

	body := map[string]string{
		"PolicyName":     policyName,
		"Path":           stringProp(props, "Path", "/"),
		"PolicyDocument": marshalToJSON(props["PolicyDocument"]),
		"Description":    stringProp(props, "Description", ""),
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal policy body: %w", err)
	}

	req := &AWSRequest{
		Service:   "iam",
		Operation: "CreatePolicy",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)

	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::IAM::Policy",
		PhysicalID: policyName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		// Try to extract the ARN from the response body.
		var result struct {
			Policy struct {
				ARN string `json:"Arn"`
			} `json:"Policy"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			dr.ARN = result.Policy.ARN
		}
	}

	return dr, cost, nil
}

// deployLambdaFunction returns a NotImplemented error for Lambda resources.
// Other resources in the same template still deploy.
func (d *StackDeployer) deployLambdaFunction(logicalID string) (DeployedResource, float64, error) {
	awsErr := &AWSError{
		Code:       "NotImplemented",
		Message:    "AWS::Lambda::Function is not yet supported",
		HTTPStatus: 501,
	}
	return DeployedResource{
		LogicalID: logicalID,
		Type:      "AWS::Lambda::Function",
		Error:     awsErr.Error(),
	}, 0, nil
}

// dispatch performs in-process request routing, records the event, and returns
// the response, the estimated cost, and any routing error.
func (d *StackDeployer) dispatch(
	ctx context.Context,
	req *AWSRequest,
	streamID string,
) (*AWSResponse, float64, error) {
	reqCtx := &RequestContext{
		RequestID: generateRequestID(),
		AccountID: testAccountID,
		Region:    defaultRegion,
		Timestamp: d.tc.Now(),
		Metadata:  map[string]interface{}{"stream_id": streamID},
	}

	start := d.tc.Now()
	resp, routeErr := d.registry.RouteRequest(reqCtx, req)
	duration := time.Since(start)
	cost := d.costs.CostForRequest(req)

	_ = d.store.RecordRequest(ctx, reqCtx, req, resp, duration, cost, routeErr)

	return resp, cost, routeErr
}

// parseCFNTemplate attempts JSON then YAML unmarshalling of a CloudFormation template.
func parseCFNTemplate(cfn string) (*cfnTemplate, error) {
	var tmpl cfnTemplate
	if err := json.Unmarshal([]byte(cfn), &tmpl); err == nil {
		if len(tmpl.Resources) > 0 {
			return &tmpl, nil
		}
	}
	if err := yaml.Unmarshal([]byte(cfn), &tmpl); err == nil {
		if len(tmpl.Resources) > 0 {
			return &tmpl, nil
		}
	}
	// Try once more with JSON for better error messages on empty templates.
	if err := json.Unmarshal([]byte(cfn), &tmpl); err != nil {
		return nil, fmt.Errorf("invalid CloudFormation template (JSON: %w)", err)
	}
	return &tmpl, nil
}

// stringProp returns the string value of key from props, or fallback when absent.
func stringProp(props map[string]interface{}, key, fallback string) string {
	if props == nil {
		return fallback
	}
	v, ok := props[key]
	if !ok {
		return fallback
	}
	s, ok := v.(string)
	if !ok {
		return fallback
	}
	return s
}

// marshalToJSON marshals v to a JSON string. Returns "" on nil or error.
func marshalToJSON(v interface{}) string {
	if v == nil {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}
