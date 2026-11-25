package emulator

// StackDeployer helpers for AWS::FIS::ExperimentTemplate (#1619): its deploy, in-place update and
// delete, through the FIS API.
//
// The template's properties are the API's members in Pascal case (the AWS::FIS::ExperimentTemplate
// reference), so the body is the declared properties with each structural key's first letter
// lowered. Keys the caller chose — target and action names, tag keys, action and target
// parameter names, an action's target keys — are kept as declared. "Ref returns the ID", and Id is
// the one attribute, so the physical ID is the template's ID. Every property but Tags updates in
// place through UpdateExperimentTemplate; a change to Tags goes through TagResource and
// UntagResource.

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// fisCFNLowerKeys lowers the first letter of every key of a structural object, recursively.
func fisCFNLowerKeys(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			if k == "" {
				continue
			}
			out[strings.ToLower(k[:1])+k[1:]] = fisCFNLowerKeys(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = fisCFNLowerKeys(val)
		}
		return out
	}
	return v
}

// fisCFNNamedMap lowers the keys of each value of a caller-keyed map, keeping the caller's keys
// and, inside each value, the keys of the members named in keep.
func fisCFNNamedMap(v any, keep ...string) map[string]any {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]any, len(m))
	for name, item := range m {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}
		conv := make(map[string]any, len(obj))
		for k, val := range obj {
			if k == "" {
				continue
			}
			lower := strings.ToLower(k[:1]) + k[1:]
			if slices.Contains(keep, k) {
				conv[lower] = val
				continue
			}
			conv[lower] = fisCFNLowerKeys(val)
		}
		out[name] = conv
	}
	return out
}

// cfnFISTemplateBody renders the template's properties, intrinsics resolved, as the body of
// CreateExperimentTemplate (create) or UpdateExperimentTemplate.
//
// Actions is optional in the resource type and a member every create sends, so an undeclared
// Actions is sent as {}. An update sends every optional property, an undeclared one as {}, because
// UpdateExperimentTemplate keeps a member it is not sent: a property removed from the template
// must clear what the previous deployment set, not survive it.
func cfnFISTemplateBody(props map[string]any, cctx *cfnContext, create bool) map[string]any {
	body := map[string]any{}
	if v := resolveNested(props["Description"], cctx); v != nil {
		body["description"] = v
	}
	if v := resolveNested(props["RoleArn"], cctx); v != nil {
		body["roleArn"] = v
	}
	if v := resolveNested(props["StopConditions"], cctx); v != nil {
		body["stopConditions"] = fisCFNLowerKeys(v)
	}
	body["actions"] = fisNonNil(fisCFNNamedMap(resolveNested(props["Actions"], cctx), "Parameters", "Targets"))
	if v := fisCFNNamedMap(resolveNested(props["Targets"], cctx), "ResourceTags", "Parameters"); v != nil || !create {
		body["targets"] = fisNonNil(v)
	}
	for _, name := range []string{"LogConfiguration", "ExperimentReportConfiguration"} {
		v := resolveNested(props[name], cctx)
		if v == nil && create {
			continue
		}
		if v == nil {
			v = map[string]any{}
		}
		body[strings.ToLower(name[:1])+name[1:]] = fisCFNLowerKeys(v)
	}
	options, _ := fisCFNLowerKeys(resolveNested(props["ExperimentOptions"], cctx)).(map[string]any)
	switch {
	case create && options != nil:
		body["experimentOptions"] = options
	case !create:
		// UpdateExperimentTemplateExperimentOptionsInput carries emptyTargetResolutionMode alone;
		// an AccountTargeting change replaces the template (see updateFISExperimentTemplate).
		update := map[string]any{}
		if v, ok := options["emptyTargetResolutionMode"]; ok {
			update["emptyTargetResolutionMode"] = v
		}
		body["experimentOptions"] = update
	}
	if create {
		if v := resolveNested(props["Tags"], cctx); v != nil {
			body["tags"] = v
		}
	}
	return body
}

// cfnFISAccountTargetingKey is the resource metadata key the deployed AccountTargeting is
// recorded under, which an update compares the new declaration against.
const cfnFISAccountTargetingKey = "AccountTargeting"

// cfnFISAccountTargeting is the AccountTargeting a template's properties declare, "" for none.
func cfnFISAccountTargeting(props map[string]any, cctx *cfnContext) string {
	options, _ := resolveNested(props["ExperimentOptions"], cctx).(map[string]any)
	v, _ := options["AccountTargeting"].(string)
	return v
}

// cfnFISRequest builds a FIS REST-JSON request.
func cfnFISRequest(method, path string, body any) (*AWSRequest, error) {
	req := &AWSRequest{Service: fisNamespace, Operation: method, Path: path,
		Headers: map[string]string{"Content-Type": "application/json"}, Params: map[string]string{}}
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("cfn fis marshal: %w", err)
		}
		req.Body = data
	}
	return req, nil
}

// deployFISExperimentTemplate deploys an AWS::FIS::ExperimentTemplate through
// CreateExperimentTemplate.
func (d *StackDeployer) deployFISExperimentTemplate(
	ctx context.Context, logicalID string, props map[string]any, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::FIS::ExperimentTemplate"}
	req, err := cfnFISRequest("POST", "/experimentTemplates", cfnFISTemplateBody(props, cctx, true))
	if err != nil {
		return DeployedResource{}, 0, err
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	if routeErr != nil {
		dr.Error = routeErr.Error()
		return dr, cost, nil //nolint:nilerr // A refused create is reported on the resource.
	}
	var out struct {
		ExperimentTemplate struct {
			ID  string `json:"id"`
			Arn string `json:"arn"`
		} `json:"experimentTemplate"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployFISExperimentTemplate unmarshal: %w", err)
	}
	dr.PhysicalID = out.ExperimentTemplate.ID
	dr.ARN = out.ExperimentTemplate.Arn
	dr.Metadata = map[string]any{"Id": out.ExperimentTemplate.ID,
		cfnFISAccountTargetingKey: cfnFISAccountTargeting(props, cctx)}
	return dr, cost, nil
}

// updateFISExperimentTemplate applies a changed AWS::FIS::ExperimentTemplate in place, except
// for a change to ExperimentOptions' AccountTargeting, whose "Update requires" is Replacement
// (the ExperimentTemplateExperimentOptions reference): that creates a new template, and the
// update's sweep deletes the old one as it deletes any replaced resource.
func (d *StackDeployer) updateFISExperimentTemplate(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]any, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	dr.Error = ""
	var cost float64
	// The deployed AccountTargeting is the one the previous declaration recorded on the resource,
	// so the decision reads no live state and records no request.
	if declared, _ := prior.Metadata[cfnFISAccountTargetingKey].(string); slices.Contains(changed, "ExperimentOptions") &&
		declared != cfnFISAccountTargeting(props, cctx) {
		return d.deployFISExperimentTemplate(ctx, logicalID, props, streamID, cctx)
	}
	if slices.ContainsFunc(changed, func(p string) bool { return p != "Tags" }) {
		req, err := cfnFISRequest("PATCH", "/experimentTemplates/"+prior.PhysicalID, cfnFISTemplateBody(props, cctx, false))
		if err != nil {
			return DeployedResource{}, 0, err
		}
		if err := d.cfnUpdateDispatch(ctx, req, streamID, &cost); err != nil {
			dr.Error = err.Error()
			return dr, cost, nil //nolint:nilerr // A refused update is reported on the resource.
		}
	}
	if !slices.Contains(changed, "Tags") {
		return dr, cost, nil
	}
	tags, _ := resolveNested(props["Tags"], cctx).(map[string]any)
	arn := fisTemplateARN(d.identity.region, d.identity.accountID, prior.PhysicalID)
	resp, c, err := d.dispatch(ctx, &AWSRequest{Service: fisNamespace, Operation: "GET", Path: "/tags/" + arn,
		Headers: map[string]string{}, Params: map[string]string{}}, streamID)
	cost += c
	if err != nil {
		dr.Error = err.Error()
		return dr, cost, nil //nolint:nilerr // A refused update is reported on the resource.
	}
	var current struct {
		Tags map[string]string `json:"tags"`
	}
	if err := json.Unmarshal(resp.Body, &current); err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn updateFISExperimentTemplate unmarshal: %w", err)
	}
	var remove []string
	for k := range current.Tags {
		if _, keep := tags[k]; !keep {
			remove = append(remove, k)
		}
	}
	if len(remove) > 0 {
		slices.Sort(remove)
		untag := &AWSRequest{Service: fisNamespace, Operation: "DELETE", Path: "/tags/" + arn,
			Headers: map[string]string{}, Params: map[string]string{}, MultiValueParams: map[string][]string{"tagKeys": remove}}
		if err := d.cfnUpdateDispatch(ctx, untag, streamID, &cost); err != nil {
			dr.Error = err.Error()
			return dr, cost, nil //nolint:nilerr // A refused update is reported on the resource.
		}
	}
	if len(tags) > 0 {
		req, err := cfnFISRequest("POST", "/tags/"+arn, map[string]any{"tags": tags})
		if err != nil {
			return DeployedResource{}, 0, err
		}
		if err := d.cfnUpdateDispatch(ctx, req, streamID, &cost); err != nil {
			dr.Error = err.Error()
		}
	}
	return dr, cost, nil
}
