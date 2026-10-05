package emulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand" // nosemgrep
	"net/http"
	"strings"
	"sync"
	"time"
)

// omicsNamespace is the state namespace for Amazon HealthOmics.
const omicsNamespace = "omics"

// OmicsPlugin emulates the Amazon HealthOmics service.
// It handles workflow run operations (StartRun, GetRun, ListRuns, CancelRun, DeleteRun)
// using the HealthOmics REST/JSON API at /run/... paths. A run's status progresses only when
// seeded; see omics_run_progression.go.
type OmicsPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController

	// rngMu guards rng, which is written by both a request and a reset: Int63n
	// advances the source in place, and ResetForRun replaces it. A *rand.Rand is
	// not safe for concurrent use, and two concurrent StartRun calls could
	// previously race on it.
	rngMu sync.Mutex

	// rng is the source run IDs are drawn from.
	rng *rand.Rand

	// rngSeed is the seed rng was built from, kept so ResetForRun can rewind the
	// source to the position it had at the start of the run rather than to an
	// unrelated one. The seed itself is still per-process wall-clock; making it
	// reproducible across processes is #856.
	rngSeed int64

	// seedMu serializes the read-modify-write of a run-status countdown; see
	// [progression.observe].
	seedMu sync.Mutex
}

// Name returns the service name "omics".
func (p *OmicsPlugin) Name() string { return omicsNamespace }

// Initialize sets up the OmicsPlugin with the provided configuration.
func (p *OmicsPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	p.rngSeed = time.Now().UnixNano()
	p.rng = rand.New(rand.NewSource(p.rngSeed)) //nolint:gosec
	return nil
}

// Shutdown is a no-op for OmicsPlugin.
func (p *OmicsPlugin) Shutdown(_ context.Context) error { return nil }

// ResetForRun rewinds the random source run IDs are drawn from to the position it
// held when the plugin was initialized, by rebuilding it from the same seed. It
// implements [ResettablePlugin]; see [ReplayEngine.resetState] for why a replay
// needs it.
//
// Rewinding to the original seed rather than to a fresh one is what makes a replay
// reproduce the run it is replaying: the recording drew from position 0 of this
// seed, so a replay must too. The seed remains per-process, so two processes still
// mint different run IDs from the same events — that is #856, and it is a separate
// question from whether one process is self-consistent.
func (p *OmicsPlugin) ResetForRun(_ context.Context) error {
	p.rngMu.Lock()
	defer p.rngMu.Unlock()
	p.rng = rand.New(rand.NewSource(p.rngSeed)) //nolint:gosec
	return nil
}

// HandleRequest dispatches a HealthOmics REST/JSON request to the appropriate handler.
func (p *OmicsPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, runID := parseOmicsOperation(requestMethod(req), req.Path)
	switch op {
	case "StartRun":
		return p.startRun(ctx, req)
	case "GetRun":
		return p.getRun(ctx, req, runID)
	case "CancelRun":
		return p.cancelRun(ctx, req, runID)
	case "DeleteRun":
		return p.deleteRun(ctx, req, runID)
	case "ListRuns":
		return p.listRuns(ctx, req)
	default:
		return nil, unknownRouteError(p.Name(), requestMethod(req), req.Path)
	}
}

// parseOmicsOperation maps an HTTP method and path to a HealthOmics operation name
// and optional run ID, matching the published URIs by whole segment.
//
// DELETE /run/{id} is DeleteRun, as API_DeleteRun publishes it. It was routed to CancelRun
// until #1165, which made a delete of a finished run "cancel" it and the published
// POST /run/{id}/cancel and the delete indistinguishable.
func parseOmicsOperation(method, path string) (op, runID string) {
	segs := strings.Split(strings.Trim(path, "/"), "/")
	if segs[0] != "run" {
		return "", ""
	}
	switch {
	case len(segs) == 1 && method == http.MethodPost:
		return "StartRun", ""
	case len(segs) == 1 && method == http.MethodGet:
		return "ListRuns", ""
	case len(segs) == 2 && segs[1] != "" && method == http.MethodGet:
		return "GetRun", segs[1]
	case len(segs) == 2 && segs[1] != "" && method == http.MethodDelete:
		return "DeleteRun", segs[1]
	case len(segs) == 3 && segs[1] != "" && segs[2] == "cancel" && method == http.MethodPost:
		return "CancelRun", segs[1]
	}
	return "", ""
}

// OmicsRun holds persisted state for a HealthOmics workflow run.
type OmicsRun struct {
	// ID is the 10-digit numeric run identifier.
	ID string `json:"id"`

	// Status is the run's recorded status: COMPLETED from StartRun, or CANCELLED once CancelRun
	// applies. What an observation reports is derived from it and any seed; see omicsRunReport.
	Status string `json:"status"`

	// WorkflowID is the workflow to run.
	WorkflowID string `json:"workflowId,omitempty"`

	// WorkflowType is the workflow type (PRIVATE, READY2RUN).
	WorkflowType string `json:"workflowType,omitempty"`

	// Name is an optional user-supplied run name.
	Name string `json:"name,omitempty"`

	// RoleArn is the IAM role ARN used by the run.
	RoleArn string `json:"roleArn,omitempty"`

	// OutputURI is the S3 URI for run outputs.
	OutputURI string `json:"outputUri,omitempty"`

	// StatusMessage is an optional human-readable status message.
	StatusMessage string `json:"statusMessage,omitempty"`

	// AccountID is the AWS account that owns the run.
	AccountID string `json:"accountID"`

	// Region is the AWS region where the run executes.
	Region string `json:"region"`
}

func (p *OmicsPlugin) startRun(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		WorkflowID   string          `json:"workflowId"`
		WorkflowType string          `json:"workflowType"`
		Name         string          `json:"name"`
		RoleArn      string          `json:"roleArn"`
		OutputURI    string          `json:"outputUri"`
		Parameters   json.RawMessage `json:"parameters"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "ValidationException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	runID := p.generateOmicsRunID()
	run := OmicsRun{
		ID:           runID,
		Status:       "COMPLETED",
		WorkflowID:   body.WorkflowID,
		WorkflowType: body.WorkflowType,
		Name:         body.Name,
		RoleArn:      body.RoleArn,
		OutputURI:    body.OutputURI,
		AccountID:    ctx.AccountID,
		Region:       ctx.Region,
	}

	goCtx := context.Background()
	runKey := "run:" + ctx.AccountID + "/" + ctx.Region + "/" + runID
	data, err := json.Marshal(run)
	if err != nil {
		return nil, fmt.Errorf("startRun: marshal: %w", err)
	}
	if err := p.state.Put(goCtx, omicsNamespace, runKey, data); err != nil {
		return nil, fmt.Errorf("startRun: put: %w", err)
	}
	idsKey := "run_ids:" + ctx.AccountID + "/" + ctx.Region
	if err := updateStringIndex(goCtx, p.state, omicsNamespace, idsKey, runID); err != nil {
		return nil, fmt.Errorf("omics startRun index: %w", err)
	}

	// API_StartRun publishes the run's status in the create response. It is what the run's first
	// observation would report, peeked rather than observed, so a "*" seed's countdown is not
	// spent by the create.
	obs, err := p.peekRun(goCtx, run)
	if err != nil {
		return nil, fmt.Errorf("startRun: status: %w", err)
	}
	return omicsJSONResponse(http.StatusCreated, map[string]string{"id": runID, "status": obs.status})
}

// loadRun reads one run, answering ResourceNotFoundException when it does not exist and the store's
// own error, wrapped, when the read fails.
func (p *OmicsPlugin) loadRun(ctx *RequestContext, runID string) (OmicsRun, string, error) {
	runKey := "run:" + ctx.AccountID + "/" + ctx.Region + "/" + runID
	data, err := p.state.Get(context.Background(), omicsNamespace, runKey)
	if err != nil {
		return OmicsRun{}, "", fmt.Errorf("omics load run %s: %w", runID, err)
	}
	if data == nil {
		return OmicsRun{}, "", &AWSError{Code: "ResourceNotFoundException", Message: "run " + runID + " not found", HTTPStatus: http.StatusNotFound}
	}
	var run OmicsRun
	if err := json.Unmarshal(data, &run); err != nil {
		return OmicsRun{}, "", fmt.Errorf("omics load run %s: unmarshal: %w", runID, err)
	}
	return run, runKey, nil
}

func (p *OmicsPlugin) getRun(ctx *RequestContext, _ *AWSRequest, runID string) (*AWSResponse, error) {
	run, _, err := p.loadRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	obs, err := p.observeRun(context.Background(), run)
	if err != nil {
		return nil, fmt.Errorf("getRun: status: %w", err)
	}
	return omicsJSONResponse(http.StatusOK, omicsRunToWire(run, obs))
}

// omicsRunStatusCancelled is the status CancelRun leaves a run in, spelled as API_GetRun and
// API_RunListItem publish it. Until #1364 it was CANCELED, one L, which neither page lists, so a wait
// loop matching the published value never saw its terminal state. A cancelled run reports STOPPING
// first, for the seeded number of observations; see omics_run_progression.go.
const omicsRunStatusCancelled = "CANCELLED"

// cancelRun handles CancelRun: POST /run/{id}/cancel, answering 202 with an empty body as
// API_CancelRun publishes. A run that has settled — COMPLETED, FAILED or CANCELLED — is in no state a
// cancel applies to, and is refused with the page's ConflictException.
func (p *OmicsPlugin) cancelRun(ctx *RequestContext, _ *AWSRequest, runID string) (*AWSResponse, error) {
	run, runKey, err := p.loadRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	goCtx := context.Background()
	obs, err := p.peekRun(goCtx, run)
	if err != nil {
		return nil, fmt.Errorf("cancelRun: status: %w", err)
	}
	if obs.settled() {
		return nil, omicsConflict("CancelRun", runID, obs.status)
	}
	run.Status = omicsRunStatusCancelled
	updated, err := json.Marshal(run)
	if err != nil {
		return nil, fmt.Errorf("cancelRun: marshal: %w", err)
	}
	if err := p.state.Put(goCtx, omicsNamespace, runKey, updated); err != nil {
		return nil, fmt.Errorf("cancelRun: put: %w", err)
	}
	// The STOPPING countdown starts from the next observation.
	if err := omicsRunProgressions.reset(goCtx, p.state, runID); err != nil {
		return nil, fmt.Errorf("cancelRun: %w", err)
	}
	return &AWSResponse{StatusCode: http.StatusAccepted, Headers: map[string]string{}, Body: nil}, nil
}

// deleteRun handles DeleteRun: DELETE /run/{id}, answering 202 with an empty body. API_DeleteRun
// allows it only for a run "that has reached a COMPLETED, FAILED, or CANCELLED stage", and publishes
// ConflictException for one that has not. The run's record, its index entry and its countdown are
// removed, so GetRun then answers ResourceNotFoundException, as the page says it will.
func (p *OmicsPlugin) deleteRun(ctx *RequestContext, _ *AWSRequest, runID string) (*AWSResponse, error) {
	run, runKey, err := p.loadRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	goCtx := context.Background()
	obs, err := p.peekRun(goCtx, run)
	if err != nil {
		return nil, fmt.Errorf("deleteRun: status: %w", err)
	}
	if !obs.settled() {
		return nil, omicsConflict("DeleteRun", runID, obs.status)
	}
	if err := p.state.Delete(goCtx, omicsNamespace, runKey); err != nil {
		return nil, fmt.Errorf("deleteRun: delete: %w", err)
	}
	if err := removeFromStringIndex(goCtx, p.state, omicsNamespace, "run_ids:"+ctx.AccountID+"/"+ctx.Region, runID); err != nil {
		return nil, fmt.Errorf("omics deleteRun index: %w", err)
	}
	if err := omicsRunProgressions.reset(goCtx, p.state, runID); err != nil {
		return nil, fmt.Errorf("deleteRun: %w", err)
	}
	return &AWSResponse{StatusCode: http.StatusAccepted, Headers: map[string]string{}, Body: nil}, nil
}

// listRuns handles ListRuns. Each run listed is observed, as GetRun observes it, so a list and a
// describe of the same run agree and either one counts as a poll.
func (p *OmicsPlugin) listRuns(ctx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	idsKey := "run_ids:" + ctx.AccountID + "/" + ctx.Region
	ids, err := loadStringIndex(goCtx, p.state, omicsNamespace, idsKey)
	if err != nil {
		return nil, fmt.Errorf("listRuns: index: %w", err)
	}

	type runItem struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Name   string `json:"name,omitempty"`
	}
	items := make([]runItem, 0, len(ids))
	for _, id := range ids {
		run, _, err := p.loadRun(ctx, id)
		var awsErr *AWSError
		if errors.As(err, &awsErr) {
			continue // an index entry whose record is gone
		}
		if err != nil {
			return nil, fmt.Errorf("listRuns: %w", err)
		}
		obs, err := p.observeRun(goCtx, run)
		if err != nil {
			return nil, fmt.Errorf("listRuns: status: %w", err)
		}
		items = append(items, runItem{ID: run.ID, Status: obs.status, Name: run.Name})
	}
	return omicsJSONResponse(http.StatusOK, map[string]interface{}{"items": items})
}

// generateOmicsRunID generates a 10-digit numeric string matching real HealthOmics run IDs.
//
// The lock is held only for the draw: rng is shared mutable state that a reset can
// replace under a concurrent request (#886), and a *rand.Rand cannot be used from
// two goroutines at once.
func (p *OmicsPlugin) generateOmicsRunID() string {
	p.rngMu.Lock()
	n := p.rng.Int63n(9000000000) + 1000000000 //nolint:gosec
	p.rngMu.Unlock()
	return fmt.Sprintf("%d", n)
}

// omicsJSONResponse serializes v to JSON and returns an AWSResponse.
func omicsJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("omics json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
