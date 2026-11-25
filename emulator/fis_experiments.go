package emulator

// StartExperiment, GetExperiment, ListExperiments, StopExperiment and
// ListExperimentResolvedTargets (#1619). An experiment's progress is fis_clock.go's; these
// handlers create it, stop it and read it.
//
// The published reasons are the CLI's samples: start-experiment answers status initiating with
// "Experiment is initiating." and each action pending with "Initial state.", and
// stop-experiment answers stopping with "Stopping Experiment.".

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// The experiment and action statuses (ExperimentStatus, ExperimentActionStatus).
const (
	fisStatusPending    = "pending"
	fisStatusInitiating = "initiating"
	fisStatusRunning    = "running"
	fisStatusCompleted  = "completed"
	fisStatusStopping   = "stopping"
	fisStatusStopped    = "stopped"
	fisStatusFailed     = "failed"
	fisStatusCancelled  = "cancelled"
	fisStatusSkipped    = "skipped"
)

// The reasons the CLI samples publish.
const (
	fisReasonInitiating      = "Experiment is initiating."
	fisReasonInitialState    = "Initial state."
	fisReasonStopping        = "Stopping Experiment."
	fisReasonCompleted       = "Experiment completed."
	fisReasonActionCompleted = "Action was completed."
)

// fisTerminal reports whether an experiment status is final.
func fisTerminal(status string) bool {
	switch status {
	case fisStatusCompleted, fisStatusStopped, fisStatusFailed, fisStatusCancelled:
		return true
	}
	return false
}

// fisActionTerminal reports whether an action status is final.
func fisActionTerminal(status string) bool {
	switch status {
	case fisStatusCompleted, fisStatusStopped, fisStatusFailed, fisStatusCancelled, fisStatusSkipped:
		return true
	}
	return false
}

// fisStartInput is StartExperimentRequest.
type fisStartInput struct {
	ClientToken          string `json:"clientToken"`
	ExperimentTemplateID string `json:"experimentTemplateId"`
	ExperimentOptions    *struct {
		ActionsMode string `json:"actionsMode"`
	} `json:"experimentOptions"`
	Tags map[string]string `json:"tags"`
}

// loadExperiment reads an experiment, nil when absent.
func (p *FISPlugin) loadExperiment(accountID, region, id string) (*FISExperiment, error) {
	var e FISExperiment
	found, err := p.loadJSON(fisExperimentKey(accountID, region, id), &e)
	if err != nil || !found {
		return nil, err
	}
	return &e, nil
}

// requireExperiment reads an experiment, refusing an absent one.
func (p *FISPlugin) requireExperiment(ctx *RequestContext, id string) (*FISExperiment, error) {
	e, err := p.loadExperiment(ctx.AccountID, ctx.Region, id)
	if err != nil {
		return nil, err
	}
	if e == nil {
		return nil, fisNotFound("Experiment " + id + " not found.")
	}
	return e, nil
}

// saveExperiment writes an experiment and marks it due at due, removing it from the active set
// once it is terminal.
func (p *FISPlugin) saveExperiment(e *FISExperiment, due time.Time) error {
	key := fisExperimentKey(e.AccountID, e.Region, e.ID)
	if err := p.putJSON(key, e); err != nil {
		return err
	}
	if fisTerminal(e.State.Status) {
		return p.setActive(key, nil)
	}
	return p.setActive(key, &fisActiveEntry{Due: due, TemplateID: e.TemplateID})
}

// fisActiveKey is the state key of the active set: every non-terminal experiment's key, with
// when it is next due. [FISPlugin.RunDue] reads it on every request, so a request with no
// experiment running reads one key.
const fisActiveKey = "active"

func (p *FISPlugin) startExperiment(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in fisStartInput
	if awsErr := fisDecodeBody(req, &in); awsErr != nil {
		return nil, awsErr
	}
	if in.ExperimentTemplateID == "" {
		return nil, fisRequiredMember("experimentTemplateId")
	}
	if len(in.Tags) > fisMaxTags {
		return nil, fisValidation(fmt.Sprintf("An experiment can have at most %d tags.", fisMaxTags))
	}
	if refusal := taggingReservedKeyRefusal(in.Tags, nil); refusal != nil {
		return nil, fisValidation(refusal.Message)
	}
	actionsMode := "run-all"
	if in.ExperimentOptions != nil && in.ExperimentOptions.ActionsMode != "" {
		actionsMode = in.ExperimentOptions.ActionsMode
		if actionsMode != "run-all" && actionsMode != "skip-all" {
			return nil, fisValidation("actionsMode must be run-all or skip-all.")
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if in.ClientToken != "" {
		var existing string
		found, err := p.loadJSON(fisTokenKey("experiment", ctx.AccountID, ctx.Region, in.ClientToken), &existing)
		if err != nil {
			return nil, err
		}
		if found {
			prior, err := p.loadExperiment(ctx.AccountID, ctx.Region, existing)
			if err != nil {
				return nil, err
			}
			if prior != nil {
				if prior.TemplateID != in.ExperimentTemplateID {
					return nil, fisConflict("The client token " + in.ClientToken +
						" was used to start an experiment from a different template.")
				}
				return fisJSONResponse(map[string]any{"experiment": prior.wire()})
			}
		}
	}
	t, err := p.requireTemplate(ctx, in.ExperimentTemplateID)
	if err != nil {
		return nil, err
	}
	resolved, err := p.resolveTargets(ctx, t.Targets)
	if err != nil {
		return nil, err
	}
	now := p.tc.Now().UTC()
	e := &FISExperiment{
		ID: "EXP" + ctx.IDs.Chars(14, fisIDAlphabet), TemplateID: t.ID, RoleArn: t.RoleArn,
		State:   FISActionState{Status: fisStatusInitiating, Reason: fisReasonInitiating},
		Targets: t.Targets, Actions: make(map[string]*FISExperimentAction, len(t.Actions)),
		StopConditions: t.StopConditions, Tags: in.Tags, LogConfiguration: t.LogConfiguration,
		ExperimentReportConfiguration: t.ExperimentReportConfiguration,
		ExperimentOptions: FISExperimentOptions{
			AccountTargeting: t.ExperimentOptions.AccountTargeting, ActionsMode: actionsMode,
			EmptyTargetResolutionMode: t.ExperimentOptions.EmptyTargetResolutionMode,
		},
		CreationTime: now, StartTime: now, Resolved: resolved, ClientToken: in.ClientToken,
		AccountID: ctx.AccountID, Region: ctx.Region,
	}
	e.ARN = fisExperimentARN(ctx.Region, ctx.AccountID, e.ID)
	for name, a := range t.Actions {
		e.Actions[name] = &FISExperimentAction{
			FISAction: a, State: FISActionState{Status: fisStatusPending, Reason: fisReasonInitialState},
		}
	}
	if err := p.saveExperiment(e, now); err != nil {
		return nil, err
	}
	if in.ClientToken != "" {
		if err := p.putJSON(fisTokenKey("experiment", ctx.AccountID, ctx.Region, in.ClientToken), e.ID); err != nil {
			return nil, err
		}
	}
	return fisJSONResponse(map[string]any{"experiment": e.wire()})
}

func (p *FISPlugin) getExperiment(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	e, err := p.requireExperiment(ctx, fisPathID(req.Path, "/experiments/"))
	if err != nil {
		return nil, err
	}
	return fisJSONResponse(map[string]any{"experiment": e.wire()})
}

func (p *FISPlugin) listExperiments(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	keys, err := p.state.List(context.Background(), fisNamespace, fisExperimentKey(ctx.AccountID, ctx.Region, ""))
	if err != nil {
		return nil, fmt.Errorf("fis list experiments: %w", err)
	}
	sort.Strings(keys)
	filter := req.Params["experimentTemplateId"]
	all := make([]fisExperimentSummaryWire, 0, len(keys))
	for _, key := range keys {
		var e FISExperiment
		found, err := p.loadJSON(key, &e)
		if err != nil {
			return nil, err
		}
		if found && (filter == "" || e.TemplateID == filter) {
			all = append(all, e.summary())
		}
	}
	page, next, awsErr := fisListPage(req, all)
	if awsErr != nil {
		return nil, awsErr
	}
	return fisJSONResponse(struct {
		Experiments []fisExperimentSummaryWire `json:"experiments"`
		NextToken   string                     `json:"nextToken,omitempty"`
	}{page, next})
}

// stopExperiment asks a running experiment to stop. It answers stopping, as the CLI's
// stop-experiment sample does, with the actions as they stood; the experiment settles to
// stopped at the next observation (fis_clock.go). An experiment that already ended cannot be
// stopped.
func (p *FISPlugin) stopExperiment(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, err := p.requireExperiment(ctx, fisPathID(req.Path, "/experiments/"))
	if err != nil {
		return nil, err
	}
	if fisTerminal(e.State.Status) {
		return nil, fisValidation("Experiment " + e.ID + " is " + e.State.Status + " and cannot be stopped.")
	}
	if e.State.Status != fisStatusStopping {
		now := p.tc.Now().UTC()
		e.State = FISActionState{Status: fisStatusStopping, Reason: fisReasonStopping}
		e.StopRequested = &now
		e.StopReason = fisReasonStopped
		if err := p.saveExperiment(e, now); err != nil {
			return nil, err
		}
	}
	return fisJSONResponse(map[string]any{"experiment": e.wire()})
}

// fisResolvedTargetWire is ResolvedTarget. targetInformation carries the resource's ARN under
// "arn"; the API reference publishes the member as a string map without naming its keys, so
// the key is substrate's.
type fisResolvedTargetWire struct {
	ResourceType      string            `json:"resourceType"`
	TargetName        string            `json:"targetName"`
	TargetInformation map[string]string `json:"targetInformation"`
}

func (p *FISPlugin) listExperimentResolvedTargets(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := fisPathID(req.Path, "/experiments/")
	id = id[:len(id)-len("/resolvedTargets")]
	e, err := p.requireExperiment(ctx, id)
	if err != nil {
		return nil, err
	}
	only := req.Params["targetName"]
	if only != "" {
		if _, ok := e.Targets[only]; !ok {
			return nil, fisNotFound("Target " + only + " not found in experiment " + e.ID + ".")
		}
	}
	all := []fisResolvedTargetWire{}
	for _, name := range fisSortedKeys(e.Targets) {
		if only != "" && name != only {
			continue
		}
		for _, r := range e.Resolved[name] {
			all = append(all, fisResolvedTargetWire{
				ResourceType: e.Targets[name].ResourceType, TargetName: name,
				TargetInformation: map[string]string{"arn": e.resolvedARN(name, r)},
			})
		}
	}
	page, next, awsErr := fisListPage(req, all)
	if awsErr != nil {
		return nil, awsErr
	}
	return fisJSONResponse(struct {
		ResolvedTargets []fisResolvedTargetWire `json:"resolvedTargets"`
		NextToken       string                  `json:"nextToken,omitempty"`
	}{page, next})
}
