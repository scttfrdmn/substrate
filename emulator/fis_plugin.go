package emulator

// AWS Fault Injection Service (#1619): experiment templates, experiments that run on the
// simulated clock, and the EC2 actions an experiment takes.
//
// FIS is a REST-JSON service (botocore fis 2020-12-01). An experiment's actions change EC2 state
// through EC2's internal path (ec2_internal_actions.go), and an experiment progresses in
// [FISPlugin.RunDue] (fis_clock.go), so its effects are a function of the simulated clock and
// the recorded requests, and a replay reproduces them. Target selection is deterministic
// (fis_targets.go), and an experiment's terminal state is seedable (fis_control.go).
//
// Error codes and statuses are the model's: ValidationException 400, ResourceNotFoundException
// 404, ConflictException 409. The API reference publishes no message text for them, so the
// messages are substrate's own, except where a comment cites an observed one.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FISPlugin emulates AWS Fault Injection Service. It is a [ClockDrivenPlugin]: a running
// experiment's actions start, act on EC2 and complete as the simulated clock passes.
type FISPlugin struct {
	state    StateManager
	logger   Logger
	tc       *TimeController
	registry *PluginRegistry
	// mu serializes every write to an experiment or template with the clock-driven work, so an
	// experiment is never advanced against a record being replaced.
	mu sync.Mutex
}

// Name returns the service name "fis".
func (p *FISPlugin) Name() string { return fisNamespace }

// Initialize sets up the plugin with the provided configuration.
func (p *FISPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	p.registry, _ = cfg.Options["registry"].(*PluginRegistry)
	return nil
}

// Shutdown is a no-op for FISPlugin.
func (p *FISPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a FIS REST-JSON request by method and path.
func (p *FISPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op := parseFISOperation(requestMethod(req), req.Path)
	switch op {
	case "CreateExperimentTemplate":
		return p.createExperimentTemplate(ctx, req)
	case "GetExperimentTemplate":
		return p.getExperimentTemplate(ctx, req)
	case "ListExperimentTemplates":
		return p.listExperimentTemplates(ctx, req)
	case "UpdateExperimentTemplate":
		return p.updateExperimentTemplate(ctx, req)
	case "DeleteExperimentTemplate":
		return p.deleteExperimentTemplate(ctx, req)
	case "StartExperiment":
		return p.startExperiment(ctx, req)
	case "GetExperiment":
		return p.getExperiment(ctx, req)
	case "ListExperiments":
		return p.listExperiments(ctx, req)
	case "StopExperiment":
		return p.stopExperiment(ctx, req)
	case "ListExperimentResolvedTargets":
		return p.listExperimentResolvedTargets(ctx, req)
	case "ListActions":
		return p.listActions(ctx, req)
	case "GetAction":
		return p.getAction(ctx, req)
	case "ListTargetResourceTypes":
		return p.listTargetResourceTypes(ctx, req)
	case "GetTargetResourceType":
		return p.getTargetResourceType(ctx, req)
	case "TagResource":
		return p.tagResource(ctx, req)
	case "UntagResource":
		return p.untagResource(ctx, req)
	case "ListTagsForResource":
		return p.listTagsForResource(ctx, req)
	default:
		return nil, unknownRouteError(p.Name(), requestMethod(req), req.Path)
	}
}

// parseFISOperation maps a method and path to a FIS operation, "" for a route FIS does not
// serve. The routes are the model's requestUri values.
func parseFISOperation(method, path string) string {
	segs := strings.Split(strings.TrimPrefix(path, "/"), "/")
	switch {
	case strings.HasPrefix(path, "/tags/") && len(path) > len("/tags/"):
		// A resource ARN carries "/" once decoded, so everything after /tags/ is the ARN.
		return map[string]string{http.MethodPost: "TagResource", http.MethodGet: "ListTagsForResource",
			http.MethodDelete: "UntagResource"}[method]
	case strings.HasPrefix(path, "/actions/") && len(path) > len("/actions/"):
		if method == http.MethodGet {
			return "GetAction"
		}
		return ""
	}
	switch len(segs) {
	case 1:
		return map[string]string{
			http.MethodPost + " experimentTemplates": "CreateExperimentTemplate",
			http.MethodGet + " experimentTemplates":  "ListExperimentTemplates",
			http.MethodPost + " experiments":         "StartExperiment",
			http.MethodGet + " experiments":          "ListExperiments",
			http.MethodGet + " actions":              "ListActions",
			http.MethodGet + " targetResourceTypes":  "ListTargetResourceTypes",
		}[method+" "+segs[0]]
	case 2:
		if segs[1] == "" {
			return ""
		}
		return map[string]string{
			http.MethodGet + " experimentTemplates":    "GetExperimentTemplate",
			http.MethodPatch + " experimentTemplates":  "UpdateExperimentTemplate",
			http.MethodDelete + " experimentTemplates": "DeleteExperimentTemplate",
			http.MethodGet + " experiments":            "GetExperiment",
			http.MethodDelete + " experiments":         "StopExperiment",
			http.MethodGet + " targetResourceTypes":    "GetTargetResourceType",
		}[method+" "+segs[0]]
	case 3:
		if method == http.MethodGet && segs[0] == "experiments" && segs[1] != "" && segs[2] == "resolvedTargets" {
			return "ListExperimentResolvedTargets"
		}
	}
	return ""
}

// fisJSONResponse renders v as a 200 JSON response.
func fisJSONResponse(v any) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("fis marshal response: %w", err)
	}
	return &AWSResponse{StatusCode: http.StatusOK, Headers: map[string]string{"Content-Type": "application/json"}, Body: body}, nil
}

// fisValidation is ValidationException, "The specified input is not valid, or fails to
// satisfy the constraints for the request" (the model's documentation).
func fisValidation(msg string) *AWSError {
	return &AWSError{Code: "ValidationException", Message: msg, HTTPStatus: http.StatusBadRequest}
}

// fisNotFound is ResourceNotFoundException, "The specified resource cannot be found".
func fisNotFound(msg string) *AWSError {
	return &AWSError{Code: "ResourceNotFoundException", Message: msg, HTTPStatus: http.StatusNotFound}
}

// fisConflict is ConflictException, "The request could not be processed because of a
// conflict".
func fisConflict(msg string) *AWSError {
	return &AWSError{Code: "ConflictException", Message: msg, HTTPStatus: http.StatusConflict}
}

// fisDefaultMaxResults is the page size a listing uses when the request names none. The API
// reference publishes no default; 100 is the published maximum.
const fisDefaultMaxResults = 100

// fisMaxResults reads maxResults, which every FIS listing bounds to 1 through 100.
func fisMaxResults(req *AWSRequest) (int, *AWSError) {
	raw := req.Params["maxResults"]
	if raw == "" {
		return fisDefaultMaxResults, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > 100 {
		return 0, fisValidation(fmt.Sprintf("1 validation error detected: Value '%s' at 'maxResults' failed to "+
			"satisfy constraint: Member must have value between 1 and 100", raw))
	}
	return n, nil
}

// fisDecodeBody decodes a JSON request body into v. An empty body decodes as {}.
func fisDecodeBody(req *AWSRequest, v any) *AWSError {
	if len(strings.TrimSpace(string(req.Body))) == 0 {
		return nil
	}
	if err := json.Unmarshal(req.Body, v); err != nil {
		return fisValidation("The request body is not valid JSON: " + err.Error())
	}
	return nil
}

// State keys. Templates and experiments are keyed by owner and ID; the idempotency indexes map
// a client token to the ID it created.
func fisTemplateKey(accountID, region, id string) string {
	return "template:" + accountID + "/" + region + "/" + id
}

func fisExperimentKey(accountID, region, id string) string {
	return "experiment:" + accountID + "/" + region + "/" + id
}

func fisTokenKey(kind, accountID, region, token string) string {
	return kind + "-token:" + accountID + "/" + region + "/" + token
}

// fisTemplateARN and fisExperimentARN are the resource ARNs the service authorization reference
// gives: experiment-template/{Id} and experiment/{Id}.
func fisTemplateARN(region, accountID, id string) string {
	return "arn:aws:fis:" + region + ":" + accountID + ":experiment-template/" + id
}

func fisExperimentARN(region, accountID, id string) string {
	return "arn:aws:fis:" + region + ":" + accountID + ":experiment/" + id
}

// fisIDAlphabet is the alphabet of the 14 characters after an ID's prefix. AWS's IDs are EXT
// or EXP followed by mixed-case letters and digits (the Terraform import example names
// EXT123AbCdEfGhIjK); the length is that example's.
const fisIDAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

// loadJSON reads the record at key into v, reporting whether it exists.
func (p *FISPlugin) loadJSON(key string, v any) (bool, error) {
	data, err := p.state.Get(context.Background(), fisNamespace, key)
	if err != nil {
		return false, fmt.Errorf("fis get %s: %w", key, err)
	}
	if data == nil {
		return false, nil
	}
	if err := json.Unmarshal(data, v); err != nil {
		return false, fmt.Errorf("fis decode %s: %w", key, err)
	}
	return true, nil
}

// putJSON writes v at key.
func (p *FISPlugin) putJSON(key string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("fis encode %s: %w", key, err)
	}
	if err := p.state.Put(context.Background(), fisNamespace, key, data); err != nil {
		return fmt.Errorf("fis put %s: %w", key, err)
	}
	return nil
}

// loadTemplate reads a template, nil when absent.
func (p *FISPlugin) loadTemplate(accountID, region, id string) (*FISTemplate, error) {
	var t FISTemplate
	found, err := p.loadJSON(fisTemplateKey(accountID, region, id), &t)
	if err != nil || !found {
		return nil, err
	}
	return &t, nil
}

// requireTemplate reads a template, refusing an absent one.
func (p *FISPlugin) requireTemplate(ctx *RequestContext, id string) (*FISTemplate, error) {
	t, err := p.loadTemplate(ctx.AccountID, ctx.Region, id)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, fisNotFound("Experiment template " + id + " not found.")
	}
	return t, nil
}

// fisTemplateInput is CreateExperimentTemplateRequest and UpdateExperimentTemplateRequest.
// Every member is a pointer or a map so an update can tell an absent member, which it keeps,
// from a present one, which replaces the stored value.
type fisTemplateInput struct {
	ClientToken                   string                `json:"clientToken"`
	Description                   *string               `json:"description"`
	StopConditions                *[]FISStopCondition   `json:"stopConditions"`
	Targets                       *map[string]FISTarget `json:"targets"`
	Actions                       *map[string]FISAction `json:"actions"`
	RoleArn                       *string               `json:"roleArn"`
	Tags                          map[string]string     `json:"tags"`
	LogConfiguration              map[string]any        `json:"logConfiguration"`
	ExperimentOptions             *FISExperimentOptions `json:"experimentOptions"`
	ExperimentReportConfiguration map[string]any        `json:"experimentReportConfiguration"`
}

// fisRequiredMember refuses a request missing a required member, in the form AWS's validation
// messages take.
func fisRequiredMember(member string) *AWSError {
	return fisValidation("1 validation error detected: Value null at '" + member +
		"' failed to satisfy constraint: Member must not be null")
}

func (p *FISPlugin) createExperimentTemplate(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in fisTemplateInput
	if awsErr := fisDecodeBody(req, &in); awsErr != nil {
		return nil, awsErr
	}
	switch {
	case in.Description == nil:
		return nil, fisRequiredMember("description")
	case in.StopConditions == nil:
		return nil, fisRequiredMember("stopConditions")
	case in.Actions == nil:
		return nil, fisRequiredMember("actions")
	case in.RoleArn == nil:
		return nil, fisRequiredMember("roleArn")
	}
	if refusal := taggingReservedKeyRefusal(in.Tags, nil); refusal != nil {
		return nil, fisValidation(refusal.Message)
	}
	t := &FISTemplate{
		Description: *in.Description, StopConditions: *in.StopConditions, Actions: *in.Actions, RoleArn: *in.RoleArn,
		Tags: in.Tags, LogConfiguration: in.LogConfiguration, ExperimentReportConfiguration: in.ExperimentReportConfiguration,
		AccountID: ctx.AccountID, Region: ctx.Region, ClientToken: in.ClientToken,
	}
	if in.Targets != nil {
		t.Targets = *in.Targets
	}
	if in.ExperimentOptions != nil {
		t.ExperimentOptions = *in.ExperimentOptions
		if t.ExperimentOptions.ActionsMode != "" {
			return nil, fisValidation("actionsMode is an option of StartExperiment, not of an experiment template.")
		}
	}
	fisApplyTemplateDefaults(t)
	if awsErr := fisValidateTemplate(t); awsErr != nil {
		return nil, awsErr
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if in.ClientToken != "" {
		var existing string
		found, err := p.loadJSON(fisTokenKey("template", ctx.AccountID, ctx.Region, in.ClientToken), &existing)
		if err != nil {
			return nil, err
		}
		if found {
			prior, err := p.loadTemplate(ctx.AccountID, ctx.Region, existing)
			if err != nil {
				return nil, err
			}
			if prior != nil {
				return fisJSONResponse(map[string]any{"experimentTemplate": prior.wire()})
			}
		}
	}
	now := p.tc.Now().UTC()
	t.ID = "EXT" + ctx.IDs.Chars(14, fisIDAlphabet)
	t.ARN = fisTemplateARN(ctx.Region, ctx.AccountID, t.ID)
	t.CreationTime, t.LastUpdateTime = now, now
	if err := p.putJSON(fisTemplateKey(ctx.AccountID, ctx.Region, t.ID), t); err != nil {
		return nil, err
	}
	if in.ClientToken != "" {
		if err := p.putJSON(fisTokenKey("template", ctx.AccountID, ctx.Region, in.ClientToken), t.ID); err != nil {
			return nil, err
		}
	}
	return fisJSONResponse(map[string]any{"experimentTemplate": t.wire()})
}

// fisApplyTemplateDefaults gives a template an empty stop-condition list rather than none.
//
// The experiment options are reported as sent, an option the request named none for absent. An
// experiment without them behaves as single-account and fail ("If no targets are found, the
// experiment fails", Targets page), but no page states that AWS reports those values back, and
// Terraform's aws_fis_experiment_template plans a change for a reported option its
// configuration left unset (account_targeting is Optional and not Computed).
func fisApplyTemplateDefaults(t *FISTemplate) {
	if t.StopConditions == nil {
		t.StopConditions = []FISStopCondition{}
	}
}

func fisPathID(path, prefix string) string {
	return strings.TrimPrefix(path, prefix)
}

func (p *FISPlugin) getExperimentTemplate(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	t, err := p.requireTemplate(ctx, fisPathID(req.Path, "/experimentTemplates/"))
	if err != nil {
		return nil, err
	}
	return fisJSONResponse(map[string]any{"experimentTemplate": t.wire()})
}

func (p *FISPlugin) listExperimentTemplates(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	keys, err := p.state.List(context.Background(), fisNamespace, fisTemplateKey(ctx.AccountID, ctx.Region, ""))
	if err != nil {
		return nil, fmt.Errorf("fis list templates: %w", err)
	}
	sort.Strings(keys)
	all := make([]fisTemplateSummaryWire, 0, len(keys))
	for _, key := range keys {
		var t FISTemplate
		found, err := p.loadJSON(key, &t)
		if err != nil {
			return nil, err
		}
		if found {
			all = append(all, t.summary())
		}
	}
	page, next, awsErr := fisListPage(req, all)
	if awsErr != nil {
		return nil, awsErr
	}
	return fisJSONResponse(struct {
		ExperimentTemplates []fisTemplateSummaryWire `json:"experimentTemplates"`
		NextToken           string                   `json:"nextToken,omitempty"`
	}{page, next})
}

func (p *FISPlugin) updateExperimentTemplate(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in fisTemplateInput
	if awsErr := fisDecodeBody(req, &in); awsErr != nil {
		return nil, awsErr
	}
	if in.Tags != nil {
		return nil, fisValidation("UpdateExperimentTemplate does not take tags; use TagResource.")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	t, err := p.requireTemplate(ctx, fisPathID(req.Path, "/experimentTemplates/"))
	if err != nil {
		return nil, err
	}
	if in.Description != nil {
		t.Description = *in.Description
	}
	if in.StopConditions != nil {
		t.StopConditions = *in.StopConditions
	}
	if in.Targets != nil {
		t.Targets = *in.Targets
	}
	if in.Actions != nil {
		t.Actions = *in.Actions
	}
	if in.RoleArn != nil {
		t.RoleArn = *in.RoleArn
	}
	if in.LogConfiguration != nil {
		t.LogConfiguration = in.LogConfiguration
	}
	if in.ExperimentReportConfiguration != nil {
		t.ExperimentReportConfiguration = in.ExperimentReportConfiguration
	}
	if in.ExperimentOptions != nil {
		// UpdateExperimentTemplateExperimentOptionsInput carries emptyTargetResolutionMode only:
		// account targeting is fixed when the template is created.
		if in.ExperimentOptions.AccountTargeting != "" || in.ExperimentOptions.ActionsMode != "" {
			return nil, fisValidation("UpdateExperimentTemplate's experimentOptions takes emptyTargetResolutionMode only.")
		}
		// The object replaces the stored options: an omitted mode reverts to the default (fail).
		t.ExperimentOptions.EmptyTargetResolutionMode = in.ExperimentOptions.EmptyTargetResolutionMode
	}
	fisApplyTemplateDefaults(t)
	if awsErr := fisValidateTemplate(t); awsErr != nil {
		return nil, awsErr
	}
	t.LastUpdateTime = p.tc.Now().UTC()
	if err := p.putJSON(fisTemplateKey(ctx.AccountID, ctx.Region, t.ID), t); err != nil {
		return nil, err
	}
	return fisJSONResponse(map[string]any{"experimentTemplate": t.wire()})
}

func (p *FISPlugin) deleteExperimentTemplate(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	t, err := p.requireTemplate(ctx, fisPathID(req.Path, "/experimentTemplates/"))
	if err != nil {
		return nil, err
	}
	if err := p.state.Delete(context.Background(), fisNamespace, fisTemplateKey(ctx.AccountID, ctx.Region, t.ID)); err != nil {
		return nil, fmt.Errorf("fis delete template: %w", err)
	}
	if t.ClientToken != "" {
		if err := p.state.Delete(context.Background(), fisNamespace,
			fisTokenKey("template", ctx.AccountID, ctx.Region, t.ClientToken)); err != nil {
			return nil, fmt.Errorf("fis delete template token: %w", err)
		}
	}
	// DeleteExperimentTemplateResponse carries the deleted template.
	return fisJSONResponse(map[string]any{"experimentTemplate": t.wire()})
}

// errFISNoEC2 reports a registry with no EC2 plugin to act through.
var errFISNoEC2 = errors.New("fis: EC2 is not registered")

// ec2 returns the registered EC2 plugin, which an experiment's EC2 actions and EC2 target
// resolution go through.
func (p *FISPlugin) ec2() (*EC2Plugin, error) {
	if p.registry == nil {
		return nil, errFISNoEC2
	}
	plugin, ok := p.registry.Plugin("ec2")
	if !ok {
		return nil, errFISNoEC2
	}
	ec2, ok := plugin.(*EC2Plugin)
	if !ok {
		return nil, fmt.Errorf("fis: the registered ec2 plugin is %T, not *EC2Plugin", plugin)
	}
	return ec2, nil
}
