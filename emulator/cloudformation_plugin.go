package emulator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
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
// through [Client]. Before #483 that model was reachable only from Go, so a
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
	state  StateManager
	logger Logger
	tc     *TimeController

	// deployer serves the paths that touch no resource: the stack and change-set
	// records themselves, which are stored unpartitioned, so its identity never
	// matters. CreateChangeSet is among them — it parses and diffs two templates
	// without dispatching anything. Every path that deploys or reads a resource
	// uses deployerFor instead.
	deployer *StackDeployer

	// registry, store and costs are kept so deployerFor can build a deployer
	// carrying the requesting caller's identity. A deployer is six pointer copies
	// with no I/O, so building one per deploying request is cheaper than the
	// alternative of making one deployer's identity mutable and therefore racy.
	registry *PluginRegistry
	store    *EventStore
	costs    *CostController
}

// deployerFor returns a [StackDeployer] that deploys into the requesting
// caller's account and region.
//
// Identity is a property of the request, not of the plugin, so it cannot be fixed
// at Initialize time. A caller signing for another account used to get a stack
// whose ARN named that account while the resources inside it were written into
// substrate's defaults, which put them in a partition that caller could not read.
func (p *CloudFormationPlugin) deployerFor(reqCtx *RequestContext) *StackDeployer {
	return NewStackDeployer(p.registry, p.store, p.state, p.tc, p.logger, p.costs,
		WithDeployerIdentity(reqCtx.AccountID, reqCtx.Region))
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

	p.registry = registry
	p.store = store
	p.costs = costs
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
	case "ListExports":
		return p.listExports(ctx, req)
	case "ListImports":
		return p.listImports(ctx, req)
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
	// Not run through [cfnStackName], and deliberately: CreateStack's StackName is
	// "the name that's associated with the stack" with no unique-stack-ID
	// alternative, because the ID does not exist until this call mints it.
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

	// Whether a failed create rolls back is the caller's choice, and the two
	// parameters that express it are mutually exclusive. Presence, not value,
	// decides: the CLI's --no-disable-rollback sends DisableRollback=false
	// explicitly, so testing the value would accept a pairing the API rejects.
	disableRollback, disableRollbackPresent := req.Params["DisableRollback"]
	opts, err := cfnResolveFailureOptions(req.Params["OnFailure"], disableRollback,
		disableRollbackPresent)
	if err != nil {
		return nil, cfnMapDeployerError(err)
	}

	// The stack name is passed as the deployer's stream ID because
	// StackDeployer.Deploy derives the persisted stack name from it. [Client]
	// instead passes deploy-<unixnano>, which is right for a one-shot in-process
	// validation run but would make a wire-created stack undiscoverable by the
	// name the caller asked for.
	//
	// A failed resource is not an error here: the create's outcome is the stack's
	// status, which is where the API puts it — real CreateStack has returned its
	// StackId before any rollback happens — so a rolled-back stack answers 200 and
	// says ROLLBACK_COMPLETE through DescribeStacks.
	if _, err := p.deployerFor(reqCtx).DeployWithOptions(context.Background(), body, name,
		cfnRequestParameters(req.Params, nil), opts); err != nil {
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
	// "The name or unique stack ID of the stack to update."
	name, arnErr := cfnStackName(reqCtx, req.Params["StackName"])
	if arnErr != nil {
		return nil, arnErr
	}
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
	if _, err := p.deployerFor(reqCtx).UpdateStack(context.Background(), body, name,
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
	// "The name or the unique stack ID that's associated with the stack."
	//
	// Resolving here, ahead of the deployer, is the whole fix for #544's silent
	// success: DeleteStack's absent-stack tolerance below is right for a stack that
	// is genuinely gone and wrong for an identifier the lookup merely failed to
	// parse, and an ARN reaching the deployer verbatim matches no record, sweeps
	// nothing, and answers 200.
	name, arnErr := cfnStackName(reqCtx, req.Params["StackName"])
	if arnErr != nil {
		return nil, arnErr
	}
	if name == "" {
		return nil, cfnMissingParameter("StackName")
	}
	// DeleteStack documents no not-found error: a deleted stack simply stops
	// appearing in DescribeStacks, so deleting an absent stack succeeds.
	//
	// The caller's identity is needed even though the stack record itself is
	// unpartitioned: the delete is refused when another stack imports one of this
	// stack's exports, and export visibility is scoped per account and Region, so
	// the default identity would consult the wrong namespace.
	if err := p.deployerFor(reqCtx).DeleteStack(context.Background(), name); err != nil {
		// A sweep that could not remove a resource is reported the way the API
		// reports it: DeleteStack "returns success" and the stack reaches
		// DELETE_FAILED, which the caller learns by polling DescribeStacks. Raising
		// it on the call instead would be wrong twice over — a caller following AWS
		// semantics gets an exception where the API gives it a status to poll, and
		// the 500 it would have to be makes an SDK *retry* the delete, running the
		// sweep again against a stack already in DELETE_FAILED.
		//
		// This mirrors CreateStack, where a refused resource is CREATE_FAILED on the
		// resource and 200 on the call. The in-process deployer still returns
		// [ErrCFNDeleteFailed], so a Go caller sees the failure directly.
		if !errors.Is(err, ErrCFNDeleteFailed) {
			return nil, cfnMapDeployerError(err)
		}
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
	StackID         string `xml:"StackId"`
	StackName       string `xml:"StackName"`
	CreationTime    string `xml:"CreationTime"`
	LastUpdatedTime string `xml:"LastUpdatedTime,omitempty"`
	StackStatus     string `xml:"StackStatus"`

	// StackStatusReason is the "success/failure message associated with the stack
	// status", omitted when there is none — a CREATE_COMPLETE stack has no reason
	// to report, and emitting an empty one would suggest otherwise.
	StackStatusReason string `xml:"StackStatusReason,omitempty"`

	// DisableRollback is derived from the stack's stored OnFailure rather than being
	// stored beside it: DisableRollback=true *is* DO_NOTHING, and a record holding
	// both could contradict itself.
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

	// ExportName is the name under which the output is exported, omitted for an
	// output the template declares no Export for. It is reported here as well as
	// by ListExports because that is how a caller holding a DescribeStacks
	// response learns the name to import, without a second call.
	ExportName string `xml:"ExportName,omitempty"`
}

func (p *CloudFormationPlugin) describeStacks(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// "Running stacks: You can specify either the stack's name or its unique stack
	// ID." Substrate keeps no record of a deleted stack, so the deleted-stack case —
	// where the API requires the ID — reports the same not-found either way.
	name, arnErr := cfnStackName(reqCtx, req.Params["StackName"])
	if arnErr != nil {
		return nil, arnErr
	}

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
			StackID:           cfnStackID(reqCtx, s.StackName),
			StackName:         s.StackName,
			CreationTime:      cfnTime(s.CreatedAt),
			LastUpdatedTime:   cfnTime(s.UpdatedAt),
			StackStatus:       s.Status,
			StackStatusReason: s.StatusReason,
			DisableRollback:   cfnStackDisablesRollback(s.OnFailure),
			Parameters:        cfnParametersXML(s.Parameters),
			Outputs:           cfnOutputsXML(s.Outputs, s.ExportNames),
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
	// "Running stacks: You can specify either the stack's name or its unique stack
	// ID." Resolved after the two parameter-shape checks above, which are about
	// which parameters were sent rather than what they contain.
	name, arnErr := cfnStackName(reqCtx, name)
	if arnErr != nil {
		return nil, arnErr
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
			// A rollback swept the resources the failed create had created, so
			// reporting CREATE_COMPLETE for one would claim a resource that is gone.
			// The sweep's own record is what says which, and why.
			if del := cfnDeletionFor(s.ResourceDeletions, r.LogicalID); del != nil && r.Error == "" {
				status = del.Status
				reason = del.Reason
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
	// "Running stacks: You can specify either the stack's name or its unique stack
	// ID." The change-set branch takes a name or ARN too, which cfnChangeSetName
	// already handled; this is the stack half of the same courtesy.
	stackName, arnErr := cfnStackName(reqCtx, req.Params["StackName"])
	if arnErr != nil {
		return nil, arnErr
	}
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
	// "The name or the unique ID of the stack for which you are creating a change
	// set", and the parameter's own pattern admits an arn: form.
	stackName, arnErr := cfnStackName(reqCtx, req.Params["StackName"])
	if arnErr != nil {
		return nil, arnErr
	}
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
	// "If you specified the name of a change set, specify the stack name or Amazon
	// Resource Name (ARN)". An empty StackName searches every stack, which
	// findChangeSet already supports.
	stackName, arnErr := cfnStackName(reqCtx, req.Params["StackName"])
	if arnErr != nil {
		return nil, arnErr
	}
	cs, err := p.findChangeSet(stackName, changeSetName)
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
	// "If you specified the name of a change set, specify the stack name or Amazon
	// Resource Name (ARN) that's associated with the change set you want to execute."
	stackName, arnErr := cfnStackName(reqCtx, req.Params["StackName"])
	if arnErr != nil {
		return nil, arnErr
	}
	cs, err := p.findChangeSet(stackName, changeSetName)
	if err != nil {
		return nil, err
	}
	if cs == nil {
		return nil, cfnChangeSetNotFound(changeSetName)
	}
	// ExecuteChangeSet routes through UpdateStack to Deploy, so it deploys
	// resources and takes the executing caller's identity like the other two.
	if _, err := p.deployerFor(reqCtx).ExecuteChangeSet(context.Background(), cs.StackName, cs.ChangeSetName); err != nil {
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
	// "The name or the Amazon Resource Name (ARN) of the stack for which you want to
	// list change sets."
	stackName, arnErr := cfnStackName(reqCtx, req.Params["StackName"])
	if arnErr != nil {
		return nil, arnErr
	}
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
	// "If you specified the name of a change set to delete, specify the stack name or
	// ID (ARN) that's associated with it."
	stackName, arnErr := cfnStackName(reqCtx, req.Params["StackName"])
	if arnErr != nil {
		return nil, arnErr
	}
	// DeleteChangeSet documents no not-found error, so deleting an absent change
	// set succeeds; only a wrong-status change set is refused, and substrate has
	// no in-progress status to refuse.
	if cs, err := p.findChangeSet(stackName, changeSetName); err != nil {
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

// --- Export operations ---

// listExports lists every exported output value in the caller's account and
// Region.
//
// The caller's own identity is used rather than the plugin's default, for the same
// reason DescribeStacks does: exports are scoped per account and Region, so a
// eu-west-1 caller asking what it can import must not be shown us-east-1's
// exports. The API documents no service-specific errors and paginates above 100
// exports; substrate returns them all in one page and emits no NextToken, which is
// the documented answer for a result that fits.
func (p *CloudFormationPlugin) listExports(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	exports, err := p.deployerFor(reqCtx).Exports(context.Background())
	if err != nil {
		return nil, cfnMapDeployerError(err)
	}

	type member struct {
		Name             string `xml:"Name"`
		Value            string `xml:"Value"`
		ExportingStackID string `xml:"ExportingStackId"`
	}
	type result struct {
		Exports []member `xml:"Exports>member"`
	}
	type response struct {
		XMLName  xml.Name            `xml:"ListExportsResponse"`
		XMLNS    string              `xml:"xmlns,attr"`
		Result   result              `xml:"ListExportsResult"`
		Metadata cfnResponseMetadata `xml:"ResponseMetadata"`
	}
	resp := response{XMLNS: cfnXMLNS, Metadata: cfnMetadata(reqCtx)}
	for _, e := range exports {
		resp.Result.Exports = append(resp.Result.Exports, member{
			Name:  e.Name,
			Value: e.Value,
			// The stack ARN is built here rather than persisted with the export:
			// this is the only place that knows the requesting caller's partition,
			// and the ARN DescribeStacks reports for the same stack has to agree.
			ExportingStackID: cfnStackID(reqCtx, e.ExportingStackName),
		})
	}
	return cfnXMLResponse(http.StatusOK, resp)
}

// listImports lists the stacks importing an exported output value.
//
// ExportName is required and the API documents no service-specific errors, so an
// export nothing imports — including one that does not exist — is an empty list
// rather than a refusal. That is the same answer the real API gives, and it is the
// one a caller checking "may I delete this stack" needs.
func (p *CloudFormationPlugin) listImports(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	exportName := req.Params["ExportName"]
	if exportName == "" {
		return nil, cfnMissingParameter("ExportName")
	}
	importers, err := p.deployerFor(reqCtx).Imports(context.Background(), exportName)
	if err != nil {
		return nil, cfnMapDeployerError(err)
	}

	type result struct {
		Imports []string `xml:"Imports>member"`
	}
	type response struct {
		XMLName  xml.Name            `xml:"ListImportsResponse"`
		XMLNS    string              `xml:"xmlns,attr"`
		Result   result              `xml:"ListImportsResult"`
		Metadata cfnResponseMetadata `xml:"ResponseMetadata"`
	}
	// Stack *names*, not ARNs: "a list of stack names that are importing the
	// specified exported output value".
	return cfnXMLResponse(http.StatusOK, response{
		XMLNS:    cfnXMLNS,
		Result:   result{Imports: importers},
		Metadata: cfnMetadata(reqCtx),
	})
}

// --- Drift operations ---

func (p *CloudFormationPlugin) detectStackDrift(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// The parameter's pattern admits an arn: form even though its prose says only
	// "the name of the stack for which you want to detect drift".
	stackName, arnErr := cfnStackName(reqCtx, req.Params["StackName"])
	if arnErr != nil {
		return nil, arnErr
	}
	if stackName == "" {
		return nil, cfnMissingParameter("StackName")
	}
	// Drift resolves each resource by a state key that embeds the account and
	// region, so detection has to look where the resources were actually
	// deployed. On the default identity it would report every resource of another
	// caller's stack as DELETED.
	detectionID, err := p.deployerFor(reqCtx).StartStackDriftDetection(context.Background(), stackName)
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
	// Same pattern as DetectStackDrift, and the pair is usually called together with
	// whichever identifier the caller already has.
	stackName, arnErr := cfnStackName(reqCtx, req.Params["StackName"])
	if arnErr != nil {
		return nil, arnErr
	}
	if stackName == "" {
		return nil, cfnMissingParameter("StackName")
	}
	filters := extractIndexedParams(req.Params, "StackResourceDriftStatusFilters.member")
	// Recomputes drift, so it needs the caller's identity for the same reason
	// detectStackDrift does.
	drifts, err := p.deployerFor(reqCtx).DescribeStackResourceDrifts(context.Background(), stackName, filters)
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
// cfn namespace directly so the state key layout stays cfn_deployer.go's business.
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

// cfnStackARNNotInScope refuses a stack ARN that is not one substrate would have
// minted for this caller — a foreign account or Region, a foreign partition, or a
// UUID that does not match the name it is attached to.
//
// It reports exactly what an absent stack reports, and deliberately so: a stack
// outside the caller's account and Region is a stack the caller cannot observe, so
// telling the two cases apart would disclose whether some other scope holds a
// stack by that name. Real CloudFormation answers a cross-account stack ID the
// same way, since credentials scope the request before the identifier is read.
func cfnStackARNNotInScope(nameOrARN string) *AWSError {
	return cfnStackNotFound(nameOrARN)
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
// The classification comes from the sentinels StackDeployer wraps, read with
// errors.Is, not from the message. Matching substrings was how this worked
// through v0.87.1 and it coupled two things that have no business being coupled
// (#502): rewording a deployer message silently moved a consumer's error code and
// HTTP status, and a resource-level deploy failure whose wrapped cause happened
// to contain "not found" was reported as though the request had named an absent
// stack. Both are gone — a message may now be reworded freely.
//
// The handlers above pre-flight the not-found cases themselves, so those codes
// are right by construction and this is the fallback for anything that escapes.
func cfnMapDeployerError(err error) *AWSError {
	if err == nil {
		return nil
	}
	msg := err.Error()
	switch {
	case errors.Is(err, ErrCFNStackNotFound),
		errors.Is(err, ErrCFNChangeSetNotFound),
		errors.Is(err, ErrCFNDriftDetectionNotFound),
		// An export still imported, or a name another stack already exports, is
		// the caller's request being invalid rather than substrate failing —
		// "all the imports must be removed before you can delete the exporting
		// stack" is an instruction to the caller — so both are 400s.
		errors.Is(err, ErrCFNExportInUse),
		errors.Is(err, ErrCFNExportNameConflict),
		// An OnFailure outside the three values, a non-boolean DisableRollback, or
		// both parameters at once, are all the request being malformed. The API
		// documents no code of its own for them.
		errors.Is(err, ErrCFNInvalidOnFailure):
		return &AWSError{Code: "ValidationError", Message: msg, HTTPStatus: http.StatusBadRequest}
	case errors.Is(err, ErrCFNTemplateInvalid):
		return &AWSError{
			Code:       "ValidationError",
			Message:    "Template format error: " + msg,
			HTTPStatus: http.StatusBadRequest,
		}
	default:
		// A resource that failed to deploy lands here, as does anything
		// unclassified: the template parsed and the request named things that exist,
		// so the failure is substrate's rather than the caller's.
		//
		// [ErrCFNDeleteFailed] deliberately does not reach here — deleteStack
		// answers 200 and leaves the stack in DELETE_FAILED, as the API does.
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
// The region and account come from the request: a caller signing for eu-west-1
// sees a eu-west-1 stack ARN. The deployer takes the same identity, so the ARN and
// the resources inside the stack cannot disagree.
//
// They used to. #483 shipped this comment claiming the disagreement was confined
// to the AWS::Region and AWS::AccountId pseudo-parameters, which understated it
// considerably: the deployer dispatched every resource under substrate's default
// account and region, and most state keys embed both, so a stack created by a
// caller in any other partition wrote its EC2 instances, ECS clusters and log
// groups where that caller could not see them. Fixed in #517.
// The ARN is built by cfnStackARN, which the AWS::StackId pseudo-parameter also
// uses (#522): a template writing its own stack ID into a resource property and a
// caller reading StackId off CreateStack must describe the same stack.
func cfnStackID(reqCtx *RequestContext, stackName string) string {
	return cfnStackARN(reqCtx.Region, reqCtx.AccountID, stackName)
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

// cfnStackName resolves the StackName a caller sent — a bare stack name, or the
// stack ARN CreateStack handed back as its StackId — to the name StackDeployer
// keys stacks by.
//
// Every stack-scoped operation documents both forms: "The name or the unique
// stack ID that's associated with the stack", and CreateChangeSet, ExecuteChangeSet,
// ListChangeSets, DetectStackDrift and DescribeStackResourceDrifts spell the ARN
// alternative out in the parameter's own pattern,
// ([a-zA-Z][-a-zA-Z0-9]*)|(arn:\b(aws|aws-us-gov|aws-cn)\b:[-a-zA-Z0-9:/._+]*).
// Until #544 substrate looked the caller's string up verbatim, so the identifier
// it had just minted resolved to nothing.
//
// The ARN is verified, not merely parsed. [cfnStackARN] builds it from the
// partition, Region, account and a [cfnDeterministicUUID] over those plus the
// name, so all of it is recomputable here — and comparing the whole string
// against the one substrate would have minted checks every component at once,
// with no way for a later field to be forgotten. Lifting the name out without
// that check would let a caller reach, and DeleteStack tear down, a stack in
// another account or Region by hand-writing an ARN: a worse defect than the
// silent no-op #544 reported.
//
// CreateStack deliberately does not use this. Its StackName is documented as "The
// name that's associated with the stack", with no ARN alternative — a stack ID
// does not exist yet at the moment the stack is created.
func cfnStackName(reqCtx *RequestContext, nameOrARN string) (string, *AWSError) {
	const token = ":stack/"
	idx := strings.Index(nameOrARN, token)
	if idx < 0 {
		// A stack name is "only alphanumeric characters (case sensitive) and
		// hyphens", so no name can be mistaken for an ARN. An empty StackName stays
		// empty: whether it is required is each operation's business, not this one.
		return nameOrARN, nil
	}
	name := nameOrARN[idx+len(token):]
	if slash := strings.IndexByte(name, '/'); slash >= 0 {
		name = name[:slash]
	}
	if name == "" || nameOrARN != cfnStackARN(reqCtx.Region, reqCtx.AccountID, name) {
		return "", cfnStackARNNotInScope(nameOrARN)
	}
	return name, nil
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

// cfnOutputsXML renders a resolved output map, sorted by key, carrying each
// output's export name where the template declared one.
func cfnOutputsXML(outputs, exportNames map[string]string) []cfnOutputXML {
	keys := make([]string, 0, len(outputs))
	for k := range outputs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]cfnOutputXML, 0, len(keys))
	for _, k := range keys {
		out = append(out, cfnOutputXML{
			OutputKey:   k,
			OutputValue: outputs[k],
			ExportName:  exportNames[k],
		})
	}
	return out
}
