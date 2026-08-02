package emulator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// cfnXMLNS is the CloudFormation Query-protocol XML namespace.
const cfnXMLNS = "http://cloudformation.amazonaws.com/doc/2010-05-15/"

// CloudFormationPlugin exposes substrate's CloudFormation model over the wire
// using the AWS Query protocol.
//
// It is deliberately a thin adapter: every handler translates query parameters
// in and XML out, and all stack, change-set and drift behavior lives in
// [StackDeployer], which substrate has always exposed to in-process Go callers
// through [BettyClient]. Before #483 that model was reachable only from Go, so a
// consumer driving the emulator with the AWS CLI or an SDK — the way CDK,
// CloudFormation and Terraform users actually do — got ServiceNotAvailable from
// [PluginRegistry.RouteRequest] for a service README.md advertises as a
// first-class target.
//
// Because the deployer dispatches each resource back through the same
// *PluginRegistry the server routes with, a stack created over the wire creates
// real resources in the other plugins: the bucket in an AWS::S3::Bucket is a
// bucket the S3 plugin serves, not a private CloudFormation-only record.
type CloudFormationPlugin struct {
	state    StateManager
	logger   Logger
	tc       *TimeController
	deployer *StackDeployer
}

// Name returns the service name "cloudformation".
func (p *CloudFormationPlugin) Name() string { return "cloudformation" }

// Initialize sets up the CloudFormationPlugin with the provided configuration.
//
// Options["registry"] is required: the plugin's [StackDeployer] deploys each
// template resource by routing a synthetic request through the registry, so
// without one there is nothing to deploy into. The registry is captured as a
// pointer and read at request time, so plugins registered after this one are
// still reachable.
//
// Options["event_store"] and Options["cost_controller"] are optional but are
// substituted with disabled instances when absent, never left nil:
// [StackDeployer.dispatch] dereferences both on every resource it deploys, and
// callers of [RegisterDefaultPlugins] are permitted to pass a nil *EventStore.
func (p *CloudFormationPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}

	registry, ok := cfg.Options["registry"].(*PluginRegistry)
	if !ok || registry == nil {
		return fmt.Errorf("cloudformation plugin: Options[registry] must hold a non-nil *PluginRegistry")
	}

	store, ok := cfg.Options["event_store"].(*EventStore)
	if !ok || store == nil {
		store = NewEventStore(EventStoreConfig{Enabled: false})
	}
	costs, ok := cfg.Options["cost_controller"].(*CostController)
	if !ok || costs == nil {
		costs = NewCostController(CostConfig{Enabled: false})
	}

	p.deployer = NewStackDeployer(registry, store, cfg.State, p.tc, cfg.Logger, costs)
	return nil
}

// Shutdown is a no-op for CloudFormationPlugin.
func (p *CloudFormationPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a CloudFormation query-protocol request to the
// appropriate handler.
func (p *CloudFormationPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	action := req.Operation
	if action == "" {
		action = req.Params["Action"]
	}
	switch action {
	case "CreateStack":
		return p.createStack(ctx, req)
	case "UpdateStack":
		return p.updateStack(ctx, req)
	case "DeleteStack":
		return p.deleteStack(ctx, req)
	case "DescribeStacks":
		return p.describeStacks(ctx, req)
	case "ListStacks":
		return p.listStacks(ctx, req)
	case "DescribeStackResources":
		return p.describeStackResources(ctx, req)
	case "GetTemplate":
		return p.getTemplate(ctx, req)
	case "CreateChangeSet":
		return p.createChangeSet(ctx, req)
	case "DescribeChangeSet":
		return p.describeChangeSet(ctx, req)
	case "ExecuteChangeSet":
		return p.executeChangeSet(ctx, req)
	case "ListChangeSets":
		return p.listChangeSets(ctx, req)
	case "DeleteChangeSet":
		return p.deleteChangeSet(ctx, req)
	case "DetectStackDrift":
		return p.detectStackDrift(ctx, req)
	case "DescribeStackResourceDrifts":
		return p.describeStackResourceDrifts(ctx, req)
	case "DescribeStackDriftDetectionStatus":
		return p.describeStackDriftDetectionStatus(ctx, req)
	case "DescribeStackEvents":
		// Scoped out deliberately rather than fabricated. CFNStackState carries a
		// single Status string; there is no per-resource event model behind it, so
		// synthesizing a plausible CREATE_IN_PROGRESS/CREATE_COMPLETE pair per
		// resource would invent observations a real stack never produced — the
		// opposite of the provenance rule in docs/fidelity.md. A caller polling
		// events gets a clear refusal instead and can poll DescribeStacks. See #501.
		//
		// UnsupportedOperation is not a documented CloudFormation error code; it is
		// substrate's own signal that the operation exists but is unmodelled, chosen
		// over inventing a stack-event stream.
		return nil, &AWSError{
			Code:       "UnsupportedOperation",
			Message:    "CloudFormation DescribeStackEvents is not emulated: substrate models stack status, not per-resource stack events",
			HTTPStatus: http.StatusBadRequest,
		}
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "CloudFormationPlugin: unknown action " + action,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- Stack operations ---

func (p *CloudFormationPlugin) createStack(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["StackName"]
	if name == "" {
		return nil, cfnMissingParameter("StackName")
	}
	if req.Params["TemplateBody"] == "" && req.Params["TemplateURL"] != "" {
		return nil, cfnTemplateURLUnsupported()
	}
	body := req.Params["TemplateBody"]
	if body == "" {
		return nil, cfnMissingParameter("TemplateBody")
	}

	existing, err := p.findStack(name)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		// AlreadyExists is CreateStack's documented duplicate-name code (400). The
		// message is substrate's own; no capture of the real wording was taken.
		return nil, &AWSError{
			Code:       "AlreadyExists",
			Message:    fmt.Sprintf("Stack [%s] already exists", name),
			HTTPStatus: http.StatusBadRequest,
		}
	}

	// The stack name is passed as the deployer's stream ID because
	// StackDeployer.Deploy derives the persisted stack name from it. BettyClient
	// instead passes deploy-<unixnano>, which is right for a one-shot in-process
	// validation run but would make a wire-created stack undiscoverable by the
	// name the caller asked for.
	if _, err := p.deployer.Deploy(context.Background(), body, name,
		cfnRequestParameters(req.Params, nil)); err != nil {
		return nil, cfnMapDeployerError(err)
	}

	type result struct {
		StackID string `xml:"StackId"`
	}
	type response struct {
		XMLName  xml.Name            `xml:"CreateStackResponse"`
		XMLNS    string              `xml:"xmlns,attr"`
		Result   result              `xml:"CreateStackResult"`
		Metadata cfnResponseMetadata `xml:"ResponseMetadata"`
	}
	return cfnXMLResponse(http.StatusOK, response{
		XMLNS:    cfnXMLNS,
		Result:   result{StackID: cfnStackID(reqCtx, name)},
		Metadata: cfnMetadata(reqCtx),
	})
}

func (p *CloudFormationPlugin) updateStack(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["StackName"]
	if name == "" {
		return nil, cfnMissingParameter("StackName")
	}
	if req.Params["TemplateBody"] == "" && req.Params["TemplateURL"] != "" {
		return nil, cfnTemplateURLUnsupported()
	}

	stack, err := p.findStack(name)
	if err != nil {
		return nil, err
	}
	if stack == nil {
		return nil, cfnStackNotFound(name)
	}

	body := req.Params["TemplateBody"]
	if body == "" {
		// UsePreviousTemplate is the documented way to change only parameters.
		body = stack.TemplateBody
	}
	if _, err := p.deployer.UpdateStack(context.Background(), body, name,
		cfnRequestParameters(req.Params, stack.Parameters)); err != nil {
		return nil, cfnMapDeployerError(err)
	}

	type result struct {
		StackID string `xml:"StackId"`
	}
	type response struct {
		XMLName  xml.Name            `xml:"UpdateStackResponse"`
		XMLNS    string              `xml:"xmlns,attr"`
		Result   result              `xml:"UpdateStackResult"`
		Metadata cfnResponseMetadata `xml:"ResponseMetadata"`
	}
	return cfnXMLResponse(http.StatusOK, response{
		XMLNS:    cfnXMLNS,
		Result:   result{StackID: cfnStackID(reqCtx, name)},
		Metadata: cfnMetadata(reqCtx),
	})
}

func (p *CloudFormationPlugin) deleteStack(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["StackName"]
	if name == "" {
		return nil, cfnMissingParameter("StackName")
	}
	// DeleteStack documents no not-found error: a deleted stack simply stops
	// appearing in DescribeStacks, so deleting an absent stack succeeds.
	if err := p.deployer.DeleteStack(context.Background(), name); err != nil {
		return nil, cfnMapDeployerError(err)
	}

	type response struct {
		XMLName  xml.Name            `xml:"DeleteStackResponse"`
		XMLNS    string              `xml:"xmlns,attr"`
		Metadata cfnResponseMetadata `xml:"ResponseMetadata"`
	}
	return cfnXMLResponse(http.StatusOK, response{XMLNS: cfnXMLNS, Metadata: cfnMetadata(reqCtx)})
}

// cfnStackItem is the wire shape of a CloudFormation Stack structure. Only the
// members substrate can answer from CFNStackState are emitted; CreationTime,
// StackName and StackStatus are the three the API marks required.
type cfnStackItem struct {
	StackID         string            `xml:"StackId"`
	StackName       string            `xml:"StackName"`
	CreationTime    string            `xml:"CreationTime"`
	LastUpdatedTime string            `xml:"LastUpdatedTime,omitempty"`
	StackStatus     string            `xml:"StackStatus"`
	DisableRollback bool              `xml:"DisableRollback"`
	Parameters      []cfnParameterXML `xml:"Parameters>member,omitempty"`
	Outputs         []cfnOutputXML    `xml:"Outputs>member,omitempty"`
}

// cfnParameterXML is a resolved template parameter as DescribeStacks reports it.
type cfnParameterXML struct {
	ParameterKey   string `xml:"ParameterKey"`
	ParameterValue string `xml:"ParameterValue"`
}

// cfnOutputXML is a resolved template output as DescribeStacks reports it.
type cfnOutputXML struct {
	OutputKey   string `xml:"OutputKey"`
	OutputValue string `xml:"OutputValue"`
}

func (p *CloudFormationPlugin) describeStacks(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["StackName"]

	var stacks []CFNStackState
	if name == "" {
		all, err := p.listAllStacks()
		if err != nil {
			return nil, err
		}
		stacks = all
	} else {
		stack, err := p.findStack(name)
		if err != nil {
			return nil, err
		}
		if stack == nil {
			return nil, cfnStackNotFound(name)
		}
		stacks = []CFNStackState{*stack}
	}

	type result struct {
		Stacks []cfnStackItem `xml:"Stacks>member"`
	}
	type response struct {
		XMLName  xml.Name            `xml:"DescribeStacksResponse"`
		XMLNS    string              `xml:"xmlns,attr"`
		Result   result              `xml:"DescribeStacksResult"`
		Metadata cfnResponseMetadata `xml:"ResponseMetadata"`
	}
	resp := response{XMLNS: cfnXMLNS, Metadata: cfnMetadata(reqCtx)}
	for _, s := range stacks {
		resp.Result.Stacks = append(resp.Result.Stacks, cfnStackItem{
			StackID:         cfnStackID(reqCtx, s.StackName),
			StackName:       s.StackName,
			CreationTime:    cfnTime(s.CreatedAt),
			LastUpdatedTime: cfnTime(s.UpdatedAt),
			StackStatus:     s.Status,
			Parameters:      cfnParametersXML(s.Parameters),
			Outputs:         cfnOutputsXML(s.Outputs),
		})
	}
	return cfnXMLResponse(http.StatusOK, resp)
}

func (p *CloudFormationPlugin) listStacks(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	filters := extractIndexedParams(req.Params, "StackStatusFilter.member")
	stacks, err := p.listAllStacks()
	if err != nil {
		return nil, err
	}

	type summary struct {
		StackID         string `xml:"StackId"`
		StackName       string `xml:"StackName"`
		CreationTime    string `xml:"CreationTime"`
		LastUpdatedTime string `xml:"LastUpdatedTime,omitempty"`
		StackStatus     string `xml:"StackStatus"`
	}
	type result struct {
		Summaries []summary `xml:"StackSummaries>member"`
	}
	type response struct {
		XMLName  xml.Name            `xml:"ListStacksResponse"`
		XMLNS    string              `xml:"xmlns,attr"`
		Result   result              `xml:"ListStacksResult"`
		Metadata cfnResponseMetadata `xml:"ResponseMetadata"`
	}
	resp := response{XMLNS: cfnXMLNS, Metadata: cfnMetadata(reqCtx)}
	for _, s := range stacks {
		if len(filters) > 0 && !containsStr(filters, s.Status) {
			continue
		}
		resp.Result.Summaries = append(resp.Result.Summaries, summary{
			StackID:         cfnStackID(reqCtx, s.StackName),
			StackName:       s.StackName,
			CreationTime:    cfnTime(s.CreatedAt),
			LastUpdatedTime: cfnTime(s.UpdatedAt),
			StackStatus:     s.Status,
		})
	}
	return cfnXMLResponse(http.StatusOK, resp)
}

func (p *CloudFormationPlugin) describeStackResources(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["StackName"]
	physicalID := req.Params["PhysicalResourceId"]
	logicalID := req.Params["LogicalResourceId"]

	// The API refuses both together: "you cannot specify StackName and
	// PhysicalResourceId in the same request".
	if name != "" && physicalID != "" {
		return nil, &AWSError{
			Code:       "ValidationError",
			Message:    "StackName and PhysicalResourceId cannot both be specified",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if name == "" && physicalID == "" {
		return nil, cfnMissingParameter("StackName")
	}

	var stacks []CFNStackState
	if name != "" {
		stack, err := p.findStack(name)
		if err != nil {
			return nil, err
		}
		if stack == nil {
			return nil, cfnStackNotFound(name)
		}
		stacks = []CFNStackState{*stack}
	} else {
		all, err := p.listAllStacks()
		if err != nil {
			return nil, err
		}
		stacks = all
	}

	type resourceItem struct {
		StackID              string `xml:"StackId"`
		StackName            string `xml:"StackName"`
		LogicalResourceID    string `xml:"LogicalResourceId"`
		PhysicalResourceID   string `xml:"PhysicalResourceId,omitempty"`
		ResourceType         string `xml:"ResourceType"`
		Timestamp            string `xml:"Timestamp"`
		ResourceStatus       string `xml:"ResourceStatus"`
		ResourceStatusReason string `xml:"ResourceStatusReason,omitempty"`
	}
	type result struct {
		Resources []resourceItem `xml:"StackResources>member"`
	}
	type response struct {
		XMLName  xml.Name            `xml:"DescribeStackResourcesResponse"`
		XMLNS    string              `xml:"xmlns,attr"`
		Result   result              `xml:"DescribeStackResourcesResult"`
		Metadata cfnResponseMetadata `xml:"ResponseMetadata"`
	}
	resp := response{XMLNS: cfnXMLNS, Metadata: cfnMetadata(reqCtx)}
	for _, s := range stacks {
		for _, r := range s.Resources {
			if physicalID != "" && r.PhysicalID != physicalID {
				continue
			}
			if logicalID != "" && r.LogicalID != logicalID {
				continue
			}
			// The per-resource status is derived from whether the resource actually
			// deployed, not invented: DeployedResource.Error is set when a resource
			// failed, and empty when it succeeded.
			status, reason := "CREATE_COMPLETE", ""
			if r.Error != "" {
				status, reason = "CREATE_FAILED", r.Error
			}
			resp.Result.Resources = append(resp.Result.Resources, resourceItem{
				StackID:              cfnStackID(reqCtx, s.StackName),
				StackName:            s.StackName,
				LogicalResourceID:    r.LogicalID,
				PhysicalResourceID:   r.PhysicalID,
				ResourceType:         r.Type,
				Timestamp:            cfnTime(s.UpdatedAt),
				ResourceStatus:       status,
				ResourceStatusReason: reason,
			})
		}
	}
	return cfnXMLResponse(http.StatusOK, resp)
}

func (p *CloudFormationPlugin) getTemplate(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	stackName := req.Params["StackName"]
	changeSetName := cfnChangeSetName(req.Params["ChangeSetName"])

	var body string
	switch {
	case changeSetName != "":
		cs, err := p.findChangeSet(stackName, changeSetName)
		if err != nil {
			return nil, err
		}
		if cs == nil {
			return nil, cfnChangeSetNotFound(changeSetName)
		}
		body = cs.TemplateBody
	case stackName != "":
		stack, err := p.findStack(stackName)
		if err != nil {
			return nil, err
		}
		if stack == nil {
			return nil, cfnStackNotFound(stackName)
		}
		body = stack.TemplateBody
	default:
		return nil, cfnMissingParameter("StackName")
	}

	type result struct {
		TemplateBody    string   `xml:"TemplateBody"`
		StagesAvailable []string `xml:"StagesAvailable>member"`
	}
	type response struct {
		XMLName  xml.Name            `xml:"GetTemplateResponse"`
		XMLNS    string              `xml:"xmlns,attr"`
		Result   result              `xml:"GetTemplateResult"`
		Metadata cfnResponseMetadata `xml:"ResponseMetadata"`
	}
	// Only Original is available: substrate applies no template transforms, so
	// there is no processed template distinct from the one that was submitted.
	return cfnXMLResponse(http.StatusOK, response{
		XMLNS:    cfnXMLNS,
		Result:   result{TemplateBody: body, StagesAvailable: []string{"Original"}},
		Metadata: cfnMetadata(reqCtx),
	})
}

// --- Change-set operations ---

func (p *CloudFormationPlugin) createChangeSet(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	stackName := req.Params["StackName"]
	changeSetName := req.Params["ChangeSetName"]
	if changeSetName == "" {
		return nil, cfnMissingParameter("ChangeSetName")
	}
	if stackName == "" {
		return nil, cfnMissingParameter("StackName")
	}
	if req.Params["TemplateBody"] == "" && req.Params["TemplateURL"] != "" {
		return nil, cfnTemplateURLUnsupported()
	}
	// ChangeSetType=CREATE would have to persist a stack in REVIEW_IN_PROGRESS
	// carrying no template, a state CFNStackState cannot represent and
	// StackDeployer.CreateChangeSet cannot diff against. Refuse it rather than
	// half-model it; the change-set family operates on existing stacks.
	if t := req.Params["ChangeSetType"]; t != "" && t != "UPDATE" {
		return nil, &AWSError{
			Code:       "ValidationError",
			Message:    "ChangeSetType " + t + " is not emulated: substrate supports UPDATE change sets on an existing stack",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	stack, err := p.findStack(stackName)
	if err != nil {
		return nil, err
	}
	if stack == nil {
		return nil, cfnStackNotFound(stackName)
	}
	if existing, err := p.findChangeSet(stackName, changeSetName); err != nil {
		return nil, err
	} else if existing != nil {
		return nil, &AWSError{
			Code:       "AlreadyExists",
			Message:    fmt.Sprintf("ChangeSet [%s] already exists for stack [%s]", changeSetName, stackName),
			HTTPStatus: http.StatusBadRequest,
		}
	}

	body := req.Params["TemplateBody"]
	if body == "" {
		body = stack.TemplateBody
	}
	if _, err := p.deployer.CreateChangeSet(context.Background(), stackName, changeSetName, body,
		cfnRequestParameters(req.Params, stack.Parameters)); err != nil {
		return nil, cfnMapDeployerError(err)
	}

	type result struct {
		ID      string `xml:"Id"`
		StackID string `xml:"StackId"`
	}
	type response struct {
		XMLName  xml.Name            `xml:"CreateChangeSetResponse"`
		XMLNS    string              `xml:"xmlns,attr"`
		Result   result              `xml:"CreateChangeSetResult"`
		Metadata cfnResponseMetadata `xml:"ResponseMetadata"`
	}
	return cfnXMLResponse(http.StatusOK, response{
		XMLNS: cfnXMLNS,
		Result: result{
			ID:      cfnChangeSetID(reqCtx, stackName, changeSetName),
			StackID: cfnStackID(reqCtx, stackName),
		},
		Metadata: cfnMetadata(reqCtx),
	})
}

func (p *CloudFormationPlugin) describeChangeSet(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	changeSetName := cfnChangeSetName(req.Params["ChangeSetName"])
	if changeSetName == "" {
		return nil, cfnMissingParameter("ChangeSetName")
	}
	cs, err := p.findChangeSet(req.Params["StackName"], changeSetName)
	if err != nil {
		return nil, err
	}
	if cs == nil {
		return nil, cfnChangeSetNotFound(changeSetName)
	}

	type resourceChange struct {
		Action             string `xml:"Action"`
		LogicalResourceID  string `xml:"LogicalResourceId"`
		PhysicalResourceID string `xml:"PhysicalResourceId,omitempty"`
		ResourceType       string `xml:"ResourceType"`
		Replacement        string `xml:"Replacement,omitempty"`
	}
	type change struct {
		Type           string         `xml:"Type"`
		ResourceChange resourceChange `xml:"ResourceChange"`
	}
	type result struct {
		ChangeSetID     string            `xml:"ChangeSetId"`
		ChangeSetName   string            `xml:"ChangeSetName"`
		StackID         string            `xml:"StackId"`
		StackName       string            `xml:"StackName"`
		Status          string            `xml:"Status"`
		ExecutionStatus string            `xml:"ExecutionStatus"`
		CreationTime    string            `xml:"CreationTime"`
		Parameters      []cfnParameterXML `xml:"Parameters>member,omitempty"`
		Changes         []change          `xml:"Changes>member"`
	}
	type response struct {
		XMLName  xml.Name            `xml:"DescribeChangeSetResponse"`
		XMLNS    string              `xml:"xmlns,attr"`
		Result   result              `xml:"DescribeChangeSetResult"`
		Metadata cfnResponseMetadata `xml:"ResponseMetadata"`
	}
	resp := response{
		XMLNS: cfnXMLNS,
		Result: result{
			ChangeSetID:   cfnChangeSetID(reqCtx, cs.StackName, cs.ChangeSetName),
			ChangeSetName: cs.ChangeSetName,
			StackID:       cfnStackID(reqCtx, cs.StackName),
			StackName:     cs.StackName,
			Status:        cs.Status,
			// A change set is deleted when it is executed, so any change set that
			// still resolves is one that has not run yet and is available to run.
			ExecutionStatus: "AVAILABLE",
			CreationTime:    cfnTime(cs.CreatedAt),
			Parameters:      cfnParametersXML(cs.Parameters),
		},
		Metadata: cfnMetadata(reqCtx),
	}
	for _, c := range cs.Changes {
		resp.Result.Changes = append(resp.Result.Changes, change{
			Type: "Resource",
			ResourceChange: resourceChange{
				Action:            c.Action,
				LogicalResourceID: c.LogicalID,
				ResourceType:      c.ResourceType,
				Replacement:       c.Replacement,
			},
		})
	}
	return cfnXMLResponse(http.StatusOK, resp)
}

func (p *CloudFormationPlugin) executeChangeSet(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	changeSetName := cfnChangeSetName(req.Params["ChangeSetName"])
	if changeSetName == "" {
		return nil, cfnMissingParameter("ChangeSetName")
	}
	cs, err := p.findChangeSet(req.Params["StackName"], changeSetName)
	if err != nil {
		return nil, err
	}
	if cs == nil {
		return nil, cfnChangeSetNotFound(changeSetName)
	}
	if _, err := p.deployer.ExecuteChangeSet(context.Background(), cs.StackName, cs.ChangeSetName); err != nil {
		return nil, cfnMapDeployerError(err)
	}

	type response struct {
		XMLName  xml.Name            `xml:"ExecuteChangeSetResponse"`
		XMLNS    string              `xml:"xmlns,attr"`
		Result   struct{}            `xml:"ExecuteChangeSetResult"`
		Metadata cfnResponseMetadata `xml:"ResponseMetadata"`
	}
	return cfnXMLResponse(http.StatusOK, response{XMLNS: cfnXMLNS, Metadata: cfnMetadata(reqCtx)})
}

func (p *CloudFormationPlugin) listChangeSets(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	stackName := req.Params["StackName"]
	if stackName == "" {
		return nil, cfnMissingParameter("StackName")
	}
	stack, err := p.findStack(stackName)
	if err != nil {
		return nil, err
	}
	if stack == nil {
		return nil, cfnStackNotFound(stackName)
	}
	sets, err := p.deployer.ListChangeSets(context.Background(), stackName)
	if err != nil {
		return nil, cfnMapDeployerError(err)
	}

	type summary struct {
		ChangeSetID   string `xml:"ChangeSetId"`
		ChangeSetName string `xml:"ChangeSetName"`
		StackID       string `xml:"StackId"`
		StackName     string `xml:"StackName"`
		Status        string `xml:"Status"`
		CreationTime  string `xml:"CreationTime"`
	}
	type result struct {
		Summaries []summary `xml:"Summaries>member"`
	}
	type response struct {
		XMLName  xml.Name            `xml:"ListChangeSetsResponse"`
		XMLNS    string              `xml:"xmlns,attr"`
		Result   result              `xml:"ListChangeSetsResult"`
		Metadata cfnResponseMetadata `xml:"ResponseMetadata"`
	}
	resp := response{XMLNS: cfnXMLNS, Metadata: cfnMetadata(reqCtx)}
	for _, cs := range sets {
		resp.Result.Summaries = append(resp.Result.Summaries, summary{
			ChangeSetID:   cfnChangeSetID(reqCtx, cs.StackName, cs.ChangeSetName),
			ChangeSetName: cs.ChangeSetName,
			StackID:       cfnStackID(reqCtx, cs.StackName),
			StackName:     cs.StackName,
			Status:        cs.Status,
			CreationTime:  cfnTime(cs.CreatedAt),
		})
	}
	return cfnXMLResponse(http.StatusOK, resp)
}

func (p *CloudFormationPlugin) deleteChangeSet(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	changeSetName := cfnChangeSetName(req.Params["ChangeSetName"])
	if changeSetName == "" {
		return nil, cfnMissingParameter("ChangeSetName")
	}
	// DeleteChangeSet documents no not-found error, so deleting an absent change
	// set succeeds; only a wrong-status change set is refused, and substrate has
	// no in-progress status to refuse.
	if cs, err := p.findChangeSet(req.Params["StackName"], changeSetName); err != nil {
		return nil, err
	} else if cs != nil {
		if err := p.deployer.DeleteChangeSet(context.Background(), cs.StackName, cs.ChangeSetName); err != nil {
			return nil, cfnMapDeployerError(err)
		}
	}

	type response struct {
		XMLName  xml.Name            `xml:"DeleteChangeSetResponse"`
		XMLNS    string              `xml:"xmlns,attr"`
		Result   struct{}            `xml:"DeleteChangeSetResult"`
		Metadata cfnResponseMetadata `xml:"ResponseMetadata"`
	}
	return cfnXMLResponse(http.StatusOK, response{XMLNS: cfnXMLNS, Metadata: cfnMetadata(reqCtx)})
}

// --- Drift operations ---

func (p *CloudFormationPlugin) detectStackDrift(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	stackName := req.Params["StackName"]
	if stackName == "" {
		return nil, cfnMissingParameter("StackName")
	}
	detectionID, err := p.deployer.StartStackDriftDetection(context.Background(), stackName)
	if err != nil {
		return nil, cfnMapDeployerError(err)
	}

	type result struct {
		StackDriftDetectionID string `xml:"StackDriftDetectionId"`
	}
	type response struct {
		XMLName  xml.Name            `xml:"DetectStackDriftResponse"`
		XMLNS    string              `xml:"xmlns,attr"`
		Result   result              `xml:"DetectStackDriftResult"`
		Metadata cfnResponseMetadata `xml:"ResponseMetadata"`
	}
	return cfnXMLResponse(http.StatusOK, response{
		XMLNS:    cfnXMLNS,
		Result:   result{StackDriftDetectionID: detectionID},
		Metadata: cfnMetadata(reqCtx),
	})
}

func (p *CloudFormationPlugin) describeStackResourceDrifts(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	stackName := req.Params["StackName"]
	if stackName == "" {
		return nil, cfnMissingParameter("StackName")
	}
	filters := extractIndexedParams(req.Params, "StackResourceDriftStatusFilters.member")
	drifts, err := p.deployer.DescribeStackResourceDrifts(context.Background(), stackName, filters)
	if err != nil {
		return nil, cfnMapDeployerError(err)
	}

	type propertyDifference struct {
		PropertyPath   string `xml:"PropertyPath"`
		ExpectedValue  string `xml:"ExpectedValue"`
		ActualValue    string `xml:"ActualValue"`
		DifferenceType string `xml:"DifferenceType"`
	}
	type driftItem struct {
		StackID                  string               `xml:"StackId"`
		LogicalResourceID        string               `xml:"LogicalResourceId"`
		PhysicalResourceID       string               `xml:"PhysicalResourceId,omitempty"`
		ResourceType             string               `xml:"ResourceType"`
		StackResourceDriftStatus string               `xml:"StackResourceDriftStatus"`
		Timestamp                string               `xml:"Timestamp"`
		PropertyDifferences      []propertyDifference `xml:"PropertyDifferences>member,omitempty"`
	}
	type result struct {
		Drifts []driftItem `xml:"StackResourceDrifts>member"`
	}
	type response struct {
		XMLName  xml.Name            `xml:"DescribeStackResourceDriftsResponse"`
		XMLNS    string              `xml:"xmlns,attr"`
		Result   result              `xml:"DescribeStackResourceDriftsResult"`
		Metadata cfnResponseMetadata `xml:"ResponseMetadata"`
	}
	now := cfnTime(p.tc.Now())
	resp := response{XMLNS: cfnXMLNS, Metadata: cfnMetadata(reqCtx)}
	for _, d := range drifts {
		item := driftItem{
			StackID:                  cfnStackID(reqCtx, stackName),
			LogicalResourceID:        d.LogicalID,
			PhysicalResourceID:       d.PhysicalID,
			ResourceType:             d.ResourceType,
			StackResourceDriftStatus: d.DriftStatus,
			Timestamp:                now,
		}
		for _, diff := range d.PropertyDifferences {
			// The wire struct differs from CFNPropertyDiff only in its tags, which
			// a conversion ignores. If the two ever diverge in field names, types
			// or order this stops compiling, which is the right place to notice.
			item.PropertyDifferences = append(item.PropertyDifferences, propertyDifference(diff))
		}
		resp.Result.Drifts = append(resp.Result.Drifts, item)
	}
	return cfnXMLResponse(http.StatusOK, resp)
}

func (p *CloudFormationPlugin) describeStackDriftDetectionStatus(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	detectionID := req.Params["StackDriftDetectionId"]
	if detectionID == "" {
		return nil, cfnMissingParameter("StackDriftDetectionId")
	}
	status, err := p.deployer.DescribeStackDriftDetectionStatus(context.Background(), detectionID)
	if err != nil {
		return nil, cfnMapDeployerError(err)
	}

	type result struct {
		StackID                   string `xml:"StackId"`
		StackDriftDetectionID     string `xml:"StackDriftDetectionId"`
		DetectionStatus           string `xml:"DetectionStatus"`
		StackDriftStatus          string `xml:"StackDriftStatus,omitempty"`
		DriftedStackResourceCount int    `xml:"DriftedStackResourceCount"`
		Timestamp                 string `xml:"Timestamp"`
	}
	type response struct {
		XMLName  xml.Name            `xml:"DescribeStackDriftDetectionStatusResponse"`
		XMLNS    string              `xml:"xmlns,attr"`
		Result   result              `xml:"DescribeStackDriftDetectionStatusResult"`
		Metadata cfnResponseMetadata `xml:"ResponseMetadata"`
	}
	return cfnXMLResponse(http.StatusOK, response{
		XMLNS: cfnXMLNS,
		Result: result{
			StackID:                   cfnStackID(reqCtx, status.StackName),
			StackDriftDetectionID:     status.StackDriftDetectionID,
			DetectionStatus:           status.DetectionStatus,
			StackDriftStatus:          status.StackDriftStatus,
			DriftedStackResourceCount: status.DriftedStackResourceCount,
			Timestamp:                 cfnTime(status.Timestamp),
		},
		Metadata: cfnMetadata(reqCtx),
	})
}

// --- Lookups ---

// listAllStacks returns every persisted stack.
func (p *CloudFormationPlugin) listAllStacks() ([]CFNStackState, error) {
	stacks, err := p.deployer.ListStacks(context.Background())
	if err != nil {
		return nil, cfnMapDeployerError(err)
	}
	return stacks, nil
}

// findStack returns the stack named name, or nil when no such stack exists.
//
// The lookup goes through StackDeployer.ListStacks rather than reading the
// cfn namespace directly so the state key layout stays betty_cfn.go's business.
func (p *CloudFormationPlugin) findStack(name string) (*CFNStackState, error) {
	stacks, err := p.listAllStacks()
	if err != nil {
		return nil, err
	}
	for i := range stacks {
		if stacks[i].StackName == name {
			return &stacks[i], nil
		}
	}
	return nil, nil
}

// findChangeSet resolves a change set by name. An empty stackName searches every
// stack, which is what lets DescribeChangeSet and ExecuteChangeSet accept a
// change-set ARN with no StackName the way the API documents.
func (p *CloudFormationPlugin) findChangeSet(stackName, changeSetName string) (*CFNChangeSet, error) {
	stackNames := []string{stackName}
	if stackName == "" {
		stacks, err := p.listAllStacks()
		if err != nil {
			return nil, err
		}
		stackNames = stackNames[:0]
		for _, s := range stacks {
			stackNames = append(stackNames, s.StackName)
		}
	}
	for _, sn := range stackNames {
		sets, err := p.deployer.ListChangeSets(context.Background(), sn)
		if err != nil {
			return nil, cfnMapDeployerError(err)
		}
		for i := range sets {
			if sets[i].ChangeSetName == changeSetName {
				return &sets[i], nil
			}
		}
	}
	return nil, nil
}

// --- Errors ---

// cfnMissingParameter reports an absent required query parameter. MissingParameter
// is a documented CloudFormation common error whose description is exactly this
// case: a required parameter for the specified action is not included.
func cfnMissingParameter(name string) *AWSError {
	return &AWSError{
		Code:       "MissingParameter",
		Message:    "Missing required parameter " + name,
		HTTPStatus: http.StatusBadRequest,
	}
}

// cfnStackNotFound reports an unknown stack. DescribeStacks and GetTemplate both
// document ValidationError for this case; the message wording is substrate's own.
func cfnStackNotFound(name string) *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    fmt.Sprintf("Stack with id %s does not exist", name),
		HTTPStatus: http.StatusBadRequest,
	}
}

// cfnChangeSetNotFound reports an unknown change set. ChangeSetNotFound is
// documented at HTTP 404, unlike the stack case.
func cfnChangeSetNotFound(name string) *AWSError {
	return &AWSError{
		Code:       "ChangeSetNotFound",
		Message:    fmt.Sprintf("ChangeSet [%s] does not exist", name),
		HTTPStatus: http.StatusNotFound,
	}
}

// cfnTemplateURLUnsupported refuses TemplateURL. Fetching a template from S3 or
// an HTTP URL is a network read substrate deliberately does not perform; a
// caller passes TemplateBody instead.
func cfnTemplateURLUnsupported() *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    "TemplateURL is not emulated: pass the template inline with TemplateBody",
		HTTPStatus: http.StatusBadRequest,
	}
}

// cfnMapDeployerError maps a StackDeployer failure onto a wire error code.
//
// StackDeployer reports failures as fmt.Errorf strings, so the mapping matches on
// the sentinel text it emits. The handlers above pre-flight the not-found cases
// themselves, so those codes are right by construction and this is the fallback
// for anything that escapes; typed errors in betty_cfn.go would remove the string
// matching entirely and are tracked in #502.
func cfnMapDeployerError(err error) *AWSError {
	if err == nil {
		return nil
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "not found"):
		return &AWSError{Code: "ValidationError", Message: msg, HTTPStatus: http.StatusBadRequest}
	case strings.Contains(msg, "parse template"),
		strings.Contains(msg, "parse old template"),
		strings.Contains(msg, "parse new template"):
		return &AWSError{
			Code:       "ValidationError",
			Message:    "Template format error: " + msg,
			HTTPStatus: http.StatusBadRequest,
		}
	default:
		return &AWSError{Code: "InternalFailure", Message: msg, HTTPStatus: http.StatusInternalServerError}
	}
}

// --- Wire helpers ---

// cfnResponseMetadata is the ResponseMetadata element every CloudFormation
// Query response carries.
type cfnResponseMetadata struct {
	RequestID string `xml:"RequestId"`
}

func cfnMetadata(reqCtx *RequestContext) cfnResponseMetadata {
	return cfnResponseMetadata{RequestID: reqCtx.RequestID}
}

// cfnXMLResponse serializes v to XML and returns an AWSResponse.
func cfnXMLResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("cloudformation xml marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
		Body:       append([]byte(xml.Header), body...),
	}, nil
}

// cfnTime formats t the way CloudFormation reports a timestamp. A zero time
// yields the empty string so an omitempty member is dropped rather than
// reporting year 1.
func cfnTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// cfnStackID builds the stack ARN.
//
// The region and account come from the request, not from the deployer: a caller
// signing for eu-west-1 should see a eu-west-1 stack ARN. Note that
// StackDeployer.Deploy resolves the AWS::Region and AWS::AccountId
// pseudo-parameters inside the template against substrate's defaults instead, so
// the two can disagree for a non-default region.
func cfnStackID(reqCtx *RequestContext, stackName string) string {
	return fmt.Sprintf("arn:aws:cloudformation:%s:%s:stack/%s/%s",
		reqCtx.Region, reqCtx.AccountID, stackName,
		cfnDeterministicUUID(reqCtx.Region, reqCtx.AccountID, stackName))
}

// cfnChangeSetID builds the change-set ARN.
func cfnChangeSetID(reqCtx *RequestContext, stackName, changeSetName string) string {
	return fmt.Sprintf("arn:aws:cloudformation:%s:%s:changeSet/%s/%s",
		reqCtx.Region, reqCtx.AccountID, changeSetName,
		cfnDeterministicUUID(reqCtx.Region, reqCtx.AccountID, stackName, changeSetName))
}

// cfnDeterministicUUID derives a stable UUID-shaped suffix from parts, so the
// same stack reports the same ARN on every call and across a replay.
func cfnDeterministicUUID(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "/")))
	h := hex.EncodeToString(sum[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// cfnChangeSetName accepts either a bare change-set name or a change-set ARN and
// returns the name, which is how StackDeployer keys change sets.
func cfnChangeSetName(nameOrARN string) string {
	const token = ":changeSet/"
	idx := strings.Index(nameOrARN, token)
	if idx < 0 {
		return nameOrARN
	}
	rest := nameOrARN[idx+len(token):]
	if slash := strings.IndexByte(rest, '/'); slash >= 0 {
		return rest[:slash]
	}
	return rest
}

// cfnRequestParameters parses Parameters.member.N.ParameterKey and
// .ParameterValue into the map StackDeployer takes.
//
// A member with UsePreviousValue=true takes its value from previous — the
// currently deployed stack's resolved parameters — rather than sending an empty
// string, which would silently overwrite the stored value with nothing.
func cfnRequestParameters(params, previous map[string]string) map[string]string {
	out := make(map[string]string)
	for i := 1; ; i++ {
		key, ok := params[fmt.Sprintf("Parameters.member.%d.ParameterKey", i)]
		if !ok {
			break
		}
		if key == "" {
			continue
		}
		if params[fmt.Sprintf("Parameters.member.%d.UsePreviousValue", i)] == "true" {
			if prev, found := previous[key]; found {
				out[key] = prev
			}
			continue
		}
		out[key] = params[fmt.Sprintf("Parameters.member.%d.ParameterValue", i)]
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// cfnParametersXML renders a resolved parameter map, sorted by key so a response
// is byte-identical across calls.
func cfnParametersXML(params map[string]string) []cfnParameterXML {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]cfnParameterXML, 0, len(keys))
	for _, k := range keys {
		out = append(out, cfnParameterXML{ParameterKey: k, ParameterValue: params[k]})
	}
	return out
}

// cfnOutputsXML renders a resolved output map, sorted by key.
func cfnOutputsXML(outputs map[string]string) []cfnOutputXML {
	keys := make([]string, 0, len(outputs))
	for k := range outputs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]cfnOutputXML, 0, len(keys))
	for _, k := range keys {
		out = append(out, cfnOutputXML{OutputKey: k, OutputValue: outputs[k]})
	}
	return out
}
