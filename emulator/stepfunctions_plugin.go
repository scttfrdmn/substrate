package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// sfnEpoch converts t to a float64 Unix epoch seconds value for use in Step
// Functions API responses. The AWS SDK v2 sfn client expects timestamps as
// JSON numbers (epoch seconds), not RFC3339 strings.
func sfnEpoch(t time.Time) float64 { return float64(t.UnixNano()) / 1e9 }

// StepFunctionsPlugin emulates the AWS Step Functions JSON-protocol API.
// It handles CreateStateMachine, DescribeStateMachine, UpdateStateMachine,
// DeleteStateMachine, ListStateMachines, StartExecution, StopExecution,
// DescribeExecution, ListExecutions, GetExecutionHistory, CreateActivity,
// DescribeActivity, ListActivities, DeleteActivity, TagResource, UntagResource,
// ListTagsForResource, and StartSyncExecution.
type StepFunctionsPlugin struct {
	state    StateManager
	logger   Logger
	tc       *TimeController
	registry *PluginRegistry // nil = Lambda Task invocation disabled
}

// Name returns the service name "states".
func (p *StepFunctionsPlugin) Name() string { return "states" }

// Initialize sets up the StepFunctionsPlugin with the provided configuration.
func (p *StepFunctionsPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
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

// Shutdown is a no-op for StepFunctionsPlugin.
func (p *StepFunctionsPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Step Functions JSON-protocol request to the
// appropriate handler. The operation is derived from the X-Amz-Target header
// (e.g. "AmazonStates.DescribeExecution" → "DescribeExecution").
func (p *StepFunctionsPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op := req.Operation
	if target := req.Headers["x-amz-target"]; target != "" {
		if dot := strings.LastIndexByte(target, '.'); dot >= 0 {
			op = target[dot+1:]
		}
	}
	switch op {
	case "CreateStateMachine":
		return p.createStateMachine(ctx, req)
	case "DescribeStateMachine":
		return p.describeStateMachine(ctx, req)
	case "UpdateStateMachine":
		return p.updateStateMachine(ctx, req)
	case "DeleteStateMachine":
		return p.deleteStateMachine(ctx, req)
	case "ListStateMachines":
		return p.listStateMachines(ctx, req)
	case "StartExecution":
		return p.startExecution(ctx, req)
	case "StartSyncExecution":
		return p.startSyncExecution(ctx, req)
	case "StopExecution":
		return p.stopExecution(ctx, req)
	case "DescribeExecution":
		return p.describeExecution(ctx, req)
	case "ListExecutions":
		return p.listExecutions(ctx, req)
	case "GetExecutionHistory":
		return p.getExecutionHistory(ctx, req)
	case "CreateActivity":
		return p.createActivity(ctx, req)
	case "DescribeActivity":
		return p.describeActivity(ctx, req)
	case "ListActivities":
		return p.listActivities(ctx, req)
	case "DeleteActivity":
		return p.deleteActivity(ctx, req)
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

// --- State key helpers ---

// smKey is [sfnStateMachineKey]. The key form lives in stepfunctions_tags.go so the tagging
// resolver builds it through the same function these operations do (#910).
func (p *StepFunctionsPlugin) smKey(accountID, region, name string) string {
	return sfnStateMachineKey(accountID, region, name)
}

func (p *StepFunctionsPlugin) smNamesKey(accountID, region string) string {
	return "statemachine_names:" + accountID + "/" + region
}

func (p *StepFunctionsPlugin) execKey(accountID, region, smName, execName string) string {
	return "execution:" + accountID + "/" + region + "/" + smName + "/" + execName
}

func (p *StepFunctionsPlugin) execIDsKey(accountID, region, smName string) string {
	return "execution_ids:" + accountID + "/" + region + "/" + smName
}

// activityKey is [sfnActivityKey]. See [StepFunctionsPlugin.smKey].
func (p *StepFunctionsPlugin) activityKey(accountID, region, name string) string {
	return sfnActivityKey(accountID, region, name)
}

func (p *StepFunctionsPlugin) activityNamesKey(accountID, region string) string {
	return "activity_names:" + accountID + "/" + region
}

// --- State helpers ---

func (p *StepFunctionsPlugin) loadStateMachine(goCtx context.Context, accountID, region, name string) (*StateMachineState, error) {
	data, err := p.state.Get(goCtx, statesNamespace, p.smKey(accountID, region, name))
	if err != nil {
		return nil, fmt.Errorf("stepfunctions loadStateMachine state.Get: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var sm StateMachineState
	if err := json.Unmarshal(data, &sm); err != nil {
		return nil, fmt.Errorf("stepfunctions loadStateMachine unmarshal: %w", err)
	}
	return &sm, nil
}

func (p *StepFunctionsPlugin) saveStateMachine(goCtx context.Context, sm *StateMachineState) error {
	data, err := json.Marshal(sm)
	if err != nil {
		return fmt.Errorf("stepfunctions saveStateMachine marshal: %w", err)
	}
	return p.state.Put(goCtx, statesNamespace, p.smKey(sm.AccountID, sm.Region, sm.Name), data)
}

func (p *StepFunctionsPlugin) loadSMNames(goCtx context.Context, accountID, region string) ([]string, error) {
	data, err := p.state.Get(goCtx, statesNamespace, p.smNamesKey(accountID, region))
	if err != nil {
		return nil, fmt.Errorf("stepfunctions loadSMNames: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return nil, fmt.Errorf("stepfunctions loadSMNames unmarshal: %w", err)
	}
	return names, nil
}

func (p *StepFunctionsPlugin) saveSMNames(goCtx context.Context, accountID, region string, names []string) error {
	sort.Strings(names)
	data, err := json.Marshal(names)
	if err != nil {
		return fmt.Errorf("stepfunctions saveSMNames marshal: %w", err)
	}
	return p.state.Put(goCtx, statesNamespace, p.smNamesKey(accountID, region), data)
}

func (p *StepFunctionsPlugin) loadExecution(goCtx context.Context, accountID, region, smName, execName string) (*ExecutionState, error) {
	data, err := p.state.Get(goCtx, statesNamespace, p.execKey(accountID, region, smName, execName))
	if err != nil {
		return nil, fmt.Errorf("stepfunctions loadExecution state.Get: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var exec ExecutionState
	if err := json.Unmarshal(data, &exec); err != nil {
		return nil, fmt.Errorf("stepfunctions loadExecution unmarshal: %w", err)
	}
	return &exec, nil
}

// saveExecution writes an execution's record under the state machine named by smName.
//
// smName is a parameter rather than being re-derived from exec.StateMachineArn, which is what this
// function used to do through extractSMNameFromARN (#912): every caller has already parsed an ARN by
// the time it gets here, so re-extracting was both redundant and the one place a write could land at
// a different key from the read that found it — the ARN is parsed once and the name is carried.
func (p *StepFunctionsPlugin) saveExecution(goCtx context.Context, smName string, exec *ExecutionState) error {
	data, err := json.Marshal(exec)
	if err != nil {
		return fmt.Errorf("stepfunctions saveExecution marshal: %w", err)
	}
	return p.state.Put(goCtx, statesNamespace, p.execKey(exec.AccountID, exec.Region, smName, exec.Name), data)
}

func (p *StepFunctionsPlugin) loadExecIDs(goCtx context.Context, accountID, region, smName string) ([]string, error) {
	data, err := p.state.Get(goCtx, statesNamespace, p.execIDsKey(accountID, region, smName))
	if err != nil {
		return nil, fmt.Errorf("stepfunctions loadExecIDs: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return nil, fmt.Errorf("stepfunctions loadExecIDs unmarshal: %w", err)
	}
	return ids, nil
}

func (p *StepFunctionsPlugin) saveExecIDs(goCtx context.Context, accountID, region, smName string, ids []string) error {
	data, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("stepfunctions saveExecIDs marshal: %w", err)
	}
	return p.state.Put(goCtx, statesNamespace, p.execIDsKey(accountID, region, smName), data)
}

func (p *StepFunctionsPlugin) loadActivity(goCtx context.Context, accountID, region, name string) (*ActivityState, error) {
	data, err := p.state.Get(goCtx, statesNamespace, p.activityKey(accountID, region, name))
	if err != nil {
		return nil, fmt.Errorf("stepfunctions loadActivity state.Get: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var act ActivityState
	if err := json.Unmarshal(data, &act); err != nil {
		return nil, fmt.Errorf("stepfunctions loadActivity unmarshal: %w", err)
	}
	return &act, nil
}

func (p *StepFunctionsPlugin) saveActivity(goCtx context.Context, act *ActivityState) error {
	data, err := json.Marshal(act)
	if err != nil {
		return fmt.Errorf("stepfunctions saveActivity marshal: %w", err)
	}
	return p.state.Put(goCtx, statesNamespace, p.activityKey(act.AccountID, act.Region, act.Name), data)
}

func (p *StepFunctionsPlugin) loadActivityNames(goCtx context.Context, accountID, region string) ([]string, error) {
	data, err := p.state.Get(goCtx, statesNamespace, p.activityNamesKey(accountID, region))
	if err != nil {
		return nil, fmt.Errorf("stepfunctions loadActivityNames: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return nil, fmt.Errorf("stepfunctions loadActivityNames unmarshal: %w", err)
	}
	return names, nil
}

func (p *StepFunctionsPlugin) saveActivityNames(goCtx context.Context, accountID, region string, names []string) error {
	sort.Strings(names)
	data, err := json.Marshal(names)
	if err != nil {
		return fmt.Errorf("stepfunctions saveActivityNames marshal: %w", err)
	}
	return p.state.Put(goCtx, statesNamespace, p.activityNamesKey(accountID, region), data)
}

// --- ARN-addressed loads ---
//
// Three helpers, one per resource kind, and every operation that takes an ARN goes through one of
// them (#912). They replaced extractSMNameFromARN and extractSMNameFromExecARN, which returned an
// ARN's last colon-separated segment and left each of eleven call sites to supply the account and
// Region from its own request context — see stepfunctions_arn.go for what that cost. Deleting the
// two functions rather than leaving them unused is deliberate: leaving them is how a twelfth call
// site acquires the defect.
//
// The not-found code differs by resource kind and not by operation, so one helper per kind is the
// right granularity: every operation naming a state machine publishes StateMachineDoesNotExist,
// every one naming an execution publishes ExecutionDoesNotExist, and each is published at HTTP 400.

// requireStateMachine parses a state-machine ARN and loads the state machine it names.
//
// The returned error is an [AWSError] — InvalidArn/400 for an ARN that is malformed, names another
// service, or names an activity or an execution, and StateMachineDoesNotExist/400 for a well-formed
// one naming nothing — or a wrapped state error. Every caller returns it unexamined.
func (p *StepFunctionsPlugin) requireStateMachine(goCtx context.Context, arn string) (*StateMachineState, sfnARNTarget, error) {
	target, arnErr := sfnParseStateMachineARN(arn)
	if arnErr != nil {
		return nil, sfnARNTarget{}, arnErr
	}
	sm, err := p.loadStateMachine(goCtx, target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, target, err
	}
	if sm == nil {
		return nil, target, sfnStateMachineDoesNotExist(arn)
	}
	return sm, target, nil
}

// requireActivity parses an activity ARN and loads the activity it names. See
// [StepFunctionsPlugin.requireStateMachine] for the error contract.
func (p *StepFunctionsPlugin) requireActivity(goCtx context.Context, arn string) (*ActivityState, sfnARNTarget, error) {
	target, arnErr := sfnParseActivityARN(arn)
	if arnErr != nil {
		return nil, sfnARNTarget{}, arnErr
	}
	act, err := p.loadActivity(goCtx, target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, target, err
	}
	if act == nil {
		return nil, target, sfnActivityDoesNotExist(arn)
	}
	return act, target, nil
}

// requireExecution parses an execution ARN and loads the execution it names. See
// [StepFunctionsPlugin.requireStateMachine] for the error contract.
//
// The execution's state key nests the execution's name under its state machine's, and both names
// come out of the ARN — the pair extractSMNameFromExecARN reconstructed by stripping one segment,
// which was right only for an ARN of exactly that arity.
func (p *StepFunctionsPlugin) requireExecution(goCtx context.Context, arn string) (*ExecutionState, sfnARNTarget, error) {
	target, arnErr := sfnParseExecutionARN(arn)
	if arnErr != nil {
		return nil, sfnARNTarget{}, arnErr
	}
	exec, err := p.loadExecution(goCtx, target.AccountID, target.Region, target.Name, target.ExecName)
	if err != nil {
		return nil, target, err
	}
	if exec == nil {
		return nil, target, sfnExecutionDoesNotExist(arn)
	}
	return exec, target, nil
}

// --- Operations ---

func (p *StepFunctionsPlugin) createStateMachine(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name       string              `json:"name"`
		Definition string              `json:"definition"`
		RoleArn    string              `json:"roleArn"`
		Type       string              `json:"type"`
		Tags       []map[string]string `json:"tags"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, sfnInvalidBody()
	}
	if input.Name == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "name is required", HTTPStatus: http.StatusBadRequest}
	}
	smType := input.Type
	if smType == "" {
		smType = "STANDARD"
	}

	goCtx := context.Background()
	existing, err := p.loadStateMachine(goCtx, ctx.AccountID, ctx.Region, input.Name)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, &AWSError{Code: "StateMachineAlreadyExists", Message: "State machine already exists: " + input.Name, HTTPStatus: http.StatusConflict}
	}

	tags := make(map[string]string)
	for _, t := range input.Tags {
		if k, ok := t["key"]; ok {
			tags[k] = t["value"]
		}
	}

	now := p.tc.Now()
	arn := fmt.Sprintf("arn:aws:states:%s:%s:stateMachine:%s", ctx.Region, ctx.AccountID, input.Name)
	sm := &StateMachineState{
		StateMachineArn: arn,
		Name:            input.Name,
		Status:          "ACTIVE",
		Definition:      input.Definition,
		RoleArn:         input.RoleArn,
		Type:            smType,
		Tags:            tags,
		CreatedDate:     now,
		AccountID:       ctx.AccountID,
		Region:          ctx.Region,
	}

	if err := p.saveStateMachine(goCtx, sm); err != nil {
		return nil, fmt.Errorf("stepfunctions createStateMachine saveStateMachine: %w", err)
	}

	names, err := p.loadSMNames(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	names = append(names, input.Name)
	if err := p.saveSMNames(goCtx, ctx.AccountID, ctx.Region, names); err != nil {
		return nil, fmt.Errorf("stepfunctions createStateMachine saveSMNames: %w", err)
	}

	out := map[string]interface{}{
		"stateMachineArn": sm.StateMachineArn,
		"creationDate":    sfnEpoch(sm.CreatedDate),
	}
	return statesJSONResponse(http.StatusOK, out)
}

func (p *StepFunctionsPlugin) describeStateMachine(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		StateMachineArn string `json:"stateMachineArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, sfnInvalidBody()
	}

	sm, _, err := p.requireStateMachine(context.Background(), input.StateMachineArn)
	if err != nil {
		return nil, err
	}

	return statesJSONResponse(http.StatusOK, smToMap(sm))
}

func (p *StepFunctionsPlugin) updateStateMachine(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		StateMachineArn string `json:"stateMachineArn"`
		Definition      string `json:"definition"`
		RoleArn         string `json:"roleArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, sfnInvalidBody()
	}

	goCtx := context.Background()
	sm, _, err := p.requireStateMachine(goCtx, input.StateMachineArn)
	if err != nil {
		return nil, err
	}

	if input.Definition != "" {
		sm.Definition = input.Definition
	}
	if input.RoleArn != "" {
		sm.RoleArn = input.RoleArn
	}

	if err := p.saveStateMachine(goCtx, sm); err != nil {
		return nil, fmt.Errorf("stepfunctions updateStateMachine saveStateMachine: %w", err)
	}

	out := map[string]interface{}{
		"updateDate": sfnEpoch(p.tc.Now()),
	}
	return statesJSONResponse(http.StatusOK, out)
}

func (p *StepFunctionsPlugin) deleteStateMachine(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		StateMachineArn string `json:"stateMachineArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, sfnInvalidBody()
	}

	goCtx := context.Background()
	_, target, err := p.requireStateMachine(goCtx, input.StateMachineArn)
	if err != nil {
		return nil, err
	}

	// The record and the name index are both removed under the ARN's own account and Region, not the
	// caller's. Reading the caller's here is what let a us-east-1 caller delete its own state machine
	// by presenting an eu-west-1 ARN, and — the worse half — leave the *real* target's name in its
	// index while removing a record that was never asked about.
	if delErr := p.state.Delete(goCtx, statesNamespace, p.smKey(target.AccountID, target.Region, target.Name)); delErr != nil {
		return nil, fmt.Errorf("stepfunctions deleteStateMachine state.Delete: %w", delErr)
	}

	names, err := p.loadSMNames(goCtx, target.AccountID, target.Region)
	if err != nil {
		return nil, err
	}
	newNames := make([]string, 0, len(names))
	for _, n := range names {
		if n != target.Name {
			newNames = append(newNames, n)
		}
	}
	if err := p.saveSMNames(goCtx, target.AccountID, target.Region, newNames); err != nil {
		return nil, fmt.Errorf("stepfunctions deleteStateMachine saveSMNames: %w", err)
	}

	return statesJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *StepFunctionsPlugin) listStateMachines(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		MaxResults int    `json:"maxResults"`
		NextToken  string `json:"nextToken"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input) //nolint:errcheck // optional body
	}
	if input.MaxResults <= 0 {
		input.MaxResults = 100
	}

	goCtx := context.Background()
	names, err := p.loadSMNames(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}

	offset := 0
	if input.NextToken != "" {
		for i, n := range names {
			if n == input.NextToken {
				offset = i
				break
			}
		}
	}
	if offset > len(names) {
		offset = len(names)
	}
	page := names[offset:]
	var nextToken string
	if len(page) > input.MaxResults {
		nextToken = page[input.MaxResults]
		page = page[:input.MaxResults]
	}

	type smEntry struct {
		StateMachineArn string  `json:"stateMachineArn"`
		Name            string  `json:"name"`
		Type            string  `json:"type"`
		CreationDate    float64 `json:"creationDate"`
	}
	entries := make([]smEntry, 0, len(page))
	for _, n := range page {
		sm, loadErr := p.loadStateMachine(goCtx, ctx.AccountID, ctx.Region, n)
		if loadErr != nil || sm == nil {
			continue
		}
		entries = append(entries, smEntry{
			StateMachineArn: sm.StateMachineArn,
			Name:            sm.Name,
			Type:            sm.Type,
			CreationDate:    sfnEpoch(sm.CreatedDate),
		})
	}

	out := map[string]interface{}{
		"stateMachines": entries,
	}
	if nextToken != "" {
		out["nextToken"] = nextToken
	}
	return statesJSONResponse(http.StatusOK, out)
}

func (p *StepFunctionsPlugin) startExecution(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		StateMachineArn string `json:"stateMachineArn"`
		Name            string `json:"name"`
		Input           string `json:"input"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, sfnInvalidBody()
	}

	goCtx := context.Background()
	sm, target, err := p.requireStateMachine(goCtx, input.StateMachineArn)
	if err != nil {
		return nil, err
	}

	execName := input.Name
	if execName == "" {
		execName = "exec-" + generateLambdaRevisionID()[:8]
	}

	// The execution belongs to the state machine, so its ARN, its record and its index entry are all
	// scoped to the state machine's account and Region. They were scoped to the caller's, which meant a
	// cross-Region StartExecution minted an ARN in the caller's Region naming a state machine that is
	// not there — an ARN DescribeExecution would then resolve to nothing.
	execArn := fmt.Sprintf("arn:aws:states:%s:%s:execution:%s:%s", target.Region, target.AccountID, target.Name, execName)
	now := p.tc.Now()
	exec := &ExecutionState{
		ExecutionArn:    execArn,
		StateMachineArn: input.StateMachineArn,
		Name:            execName,
		Status:          "RUNNING",
		Input:           input.Input,
		StartDate:       now,
		AccountID:       target.AccountID,
		Region:          target.Region,
		History:         []HistoryEvent{},
	}

	// Parse and execute the ASL definition synchronously.
	var def StateMachineDefinition
	if parseErr := json.Unmarshal([]byte(sm.Definition), &def); parseErr != nil {
		exec.Status = "FAILED"
		exec.StopDate = p.tc.Now()
		exec.ErrorDetails = "InvalidDefinition: " + parseErr.Error()
	} else {
		_, _ = p.executeASL(&def, input.Input, exec, ctx) //nolint:errcheck // status set on exec
	}

	if err := p.saveExecution(goCtx, target.Name, exec); err != nil {
		return nil, fmt.Errorf("stepfunctions startExecution saveExecution: %w", err)
	}

	ids, err := p.loadExecIDs(goCtx, target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}
	ids = append(ids, execName)
	if err := p.saveExecIDs(goCtx, target.AccountID, target.Region, target.Name, ids); err != nil {
		return nil, fmt.Errorf("stepfunctions startExecution saveExecIDs: %w", err)
	}

	out := map[string]interface{}{
		"executionArn": exec.ExecutionArn,
		"startDate":    sfnEpoch(exec.StartDate),
	}
	return statesJSONResponse(http.StatusOK, out)
}

func (p *StepFunctionsPlugin) startSyncExecution(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		StateMachineArn string `json:"stateMachineArn"`
		Name            string `json:"name"`
		Input           string `json:"input"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, sfnInvalidBody()
	}

	sm, target, err := p.requireStateMachine(context.Background(), input.StateMachineArn)
	if err != nil {
		return nil, err
	}
	if sm.Type != "EXPRESS" {
		return nil, &AWSError{
			Code:       "InvalidDefinition",
			Message:    "StartSyncExecution is only supported for EXPRESS workflows",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	var def StateMachineDefinition
	if parseErr := json.Unmarshal([]byte(sm.Definition), &def); parseErr != nil {
		return nil, &AWSError{Code: "InvalidDefinition", Message: "invalid ASL: " + parseErr.Error(), HTTPStatus: http.StatusBadRequest}
	}

	execName := input.Name
	if execName == "" {
		execName = "sync-" + generateLambdaRevisionID()[:8]
	}

	execArn := fmt.Sprintf("arn:aws:states:%s:%s:express:%s:%s", target.Region, target.AccountID, target.Name, execName)
	now := p.tc.Now()
	exec := &ExecutionState{
		ExecutionArn:    execArn,
		StateMachineArn: input.StateMachineArn,
		Name:            execName,
		Status:          "RUNNING",
		Input:           input.Input,
		StartDate:       now,
		AccountID:       target.AccountID,
		Region:          target.Region,
		History:         []HistoryEvent{},
	}

	_, _ = p.executeASL(&def, input.Input, exec, ctx) //nolint:errcheck // status set on exec

	out := map[string]interface{}{
		"executionArn": exec.ExecutionArn,
		"startDate":    sfnEpoch(exec.StartDate),
		"stopDate":     sfnEpoch(exec.StopDate),
		"status":       exec.Status,
	}
	if exec.Output != "" {
		out["output"] = exec.Output
	}
	return statesJSONResponse(http.StatusOK, out)
}

func (p *StepFunctionsPlugin) stopExecution(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ExecutionArn string `json:"executionArn"`
		Cause        string `json:"cause"`
		Error        string `json:"error"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, sfnInvalidBody()
	}

	goCtx := context.Background()
	exec, target, err := p.requireExecution(goCtx, input.ExecutionArn)
	if err != nil {
		return nil, err
	}

	now := p.tc.Now()
	exec.Status = "ABORTED"
	exec.StopDate = now

	if err := p.saveExecution(goCtx, target.Name, exec); err != nil {
		return nil, fmt.Errorf("stepfunctions stopExecution saveExecution: %w", err)
	}

	out := map[string]interface{}{
		"stopDate": sfnEpoch(now),
	}
	return statesJSONResponse(http.StatusOK, out)
}

func (p *StepFunctionsPlugin) describeExecution(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ExecutionArn string `json:"executionArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, sfnInvalidBody()
	}

	exec, _, err := p.requireExecution(context.Background(), input.ExecutionArn)
	if err != nil {
		return nil, err
	}

	return statesJSONResponse(http.StatusOK, execToMap(exec))
}

func (p *StepFunctionsPlugin) listExecutions(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		StateMachineArn string `json:"stateMachineArn"`
		MapRunArn       string `json:"mapRunArn"`
		StatusFilter    string `json:"statusFilter"`
		MaxResults      int    `json:"maxResults"`
		NextToken       string `json:"nextToken"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input) //nolint:errcheck // optional body
	}
	if input.MaxResults <= 0 {
		input.MaxResults = 100
	}

	// API_ListExecutions is the one operation in this set whose stateMachineArn is "Required: No":
	// "You can specify either a mapRunArn or a stateMachineArn, but not both." Both halves of that
	// sentence are answered — neither supplied and both supplied are each a ValidationException —
	// because reading an absent stateMachineArn is what produced the old empty-name key, and an empty
	// execution list is the one answer a caller cannot tell a refusal from.
	//
	// A Map Run is not modeled at all: nothing in the plugin mints a mapRun ARN and no distributed-map
	// state exists to produce one, so a supplied mapRunArn names a resource substrate has no record of
	// and answers ResourceNotFound — which this page publishes — rather than being silently ignored,
	// which is how it read before.
	switch {
	case input.StateMachineArn == "" && input.MapRunArn == "":
		return nil, &AWSError{
			Code:       "ValidationException",
			Message:    "Either stateMachineArn or mapRunArn must be specified",
			HTTPStatus: http.StatusBadRequest,
		}
	case input.StateMachineArn != "" && input.MapRunArn != "":
		return nil, &AWSError{
			Code:       "ValidationException",
			Message:    "You can specify either a mapRunArn or a stateMachineArn, but not both",
			HTTPStatus: http.StatusBadRequest,
		}
	case input.MapRunArn != "":
		return nil, sfnResourceNotFoundError(input.MapRunArn)
	}

	goCtx := context.Background()
	// The existence check is new. This was the one operation in the set with no check at all, so an ARN
	// naming nothing — another account's state machine, a deleted one, an activity — answered 200 with
	// an empty executions list, indistinguishable from a state machine that has never been started.
	_, target, err := p.requireStateMachine(goCtx, input.StateMachineArn)
	if err != nil {
		return nil, err
	}

	ids, err := p.loadExecIDs(goCtx, target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}

	offset := 0
	if input.NextToken != "" {
		for i, id := range ids {
			if id == input.NextToken {
				offset = i
				break
			}
		}
	}
	if offset > len(ids) {
		offset = len(ids)
	}
	page := ids[offset:]
	var nextToken string
	if len(page) > input.MaxResults {
		nextToken = page[input.MaxResults]
		page = page[:input.MaxResults]
	}

	type execEntry struct {
		ExecutionArn    string  `json:"executionArn"`
		StateMachineArn string  `json:"stateMachineArn"`
		Name            string  `json:"name"`
		Status          string  `json:"status"`
		StartDate       float64 `json:"startDate"`
		StopDate        float64 `json:"stopDate,omitempty"`
	}
	entries := make([]execEntry, 0, len(page))
	for _, execName := range page {
		exec, loadErr := p.loadExecution(goCtx, target.AccountID, target.Region, target.Name, execName)
		if loadErr != nil || exec == nil {
			continue
		}
		if input.StatusFilter != "" && exec.Status != input.StatusFilter {
			continue
		}
		e := execEntry{
			ExecutionArn:    exec.ExecutionArn,
			StateMachineArn: exec.StateMachineArn,
			Name:            exec.Name,
			Status:          exec.Status,
			StartDate:       sfnEpoch(exec.StartDate),
		}
		if !exec.StopDate.IsZero() {
			e.StopDate = sfnEpoch(exec.StopDate)
		}
		entries = append(entries, e)
	}

	out := map[string]interface{}{
		"executions": entries,
	}
	if nextToken != "" {
		out["nextToken"] = nextToken
	}
	return statesJSONResponse(http.StatusOK, out)
}

func (p *StepFunctionsPlugin) getExecutionHistory(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ExecutionArn string `json:"executionArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, sfnInvalidBody()
	}

	exec, _, err := p.requireExecution(context.Background(), input.ExecutionArn)
	if err != nil {
		return nil, err
	}

	history := exec.History
	if history == nil {
		history = []HistoryEvent{}
	}
	out := map[string]interface{}{
		"events": history,
	}
	return statesJSONResponse(http.StatusOK, out)
}

func (p *StepFunctionsPlugin) createActivity(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name string              `json:"name"`
		Tags []map[string]string `json:"tags"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, sfnInvalidBody()
	}
	if input.Name == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "name is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	existing, err := p.loadActivity(goCtx, ctx.AccountID, ctx.Region, input.Name)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, &AWSError{Code: "ActivityAlreadyExists", Message: "Activity already exists: " + input.Name, HTTPStatus: http.StatusConflict}
	}

	tags := make(map[string]string)
	for _, t := range input.Tags {
		if k, ok := t["key"]; ok {
			tags[k] = t["value"]
		}
	}

	arn := fmt.Sprintf("arn:aws:states:%s:%s:activity:%s", ctx.Region, ctx.AccountID, input.Name)
	now := p.tc.Now()
	act := &ActivityState{
		ActivityArn: arn,
		Name:        input.Name,
		Tags:        tags,
		CreatedDate: now,
		AccountID:   ctx.AccountID,
		Region:      ctx.Region,
	}

	if err := p.saveActivity(goCtx, act); err != nil {
		return nil, fmt.Errorf("stepfunctions createActivity saveActivity: %w", err)
	}

	names, err := p.loadActivityNames(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	names = append(names, input.Name)
	if err := p.saveActivityNames(goCtx, ctx.AccountID, ctx.Region, names); err != nil {
		return nil, fmt.Errorf("stepfunctions createActivity saveActivityNames: %w", err)
	}

	out := map[string]interface{}{
		"activityArn":  act.ActivityArn,
		"creationDate": sfnEpoch(act.CreatedDate),
	}
	return statesJSONResponse(http.StatusOK, out)
}

func (p *StepFunctionsPlugin) describeActivity(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ActivityArn string `json:"activityArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, sfnInvalidBody()
	}

	act, _, err := p.requireActivity(context.Background(), input.ActivityArn)
	if err != nil {
		return nil, err
	}

	out := map[string]interface{}{
		"activityArn":  act.ActivityArn,
		"name":         act.Name,
		"creationDate": sfnEpoch(act.CreatedDate),
	}
	return statesJSONResponse(http.StatusOK, out)
}

func (p *StepFunctionsPlugin) listActivities(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		MaxResults int    `json:"maxResults"`
		NextToken  string `json:"nextToken"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input) //nolint:errcheck // optional body
	}
	if input.MaxResults <= 0 {
		input.MaxResults = 100
	}

	goCtx := context.Background()
	names, err := p.loadActivityNames(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}

	offset := 0
	if input.NextToken != "" {
		for i, n := range names {
			if n == input.NextToken {
				offset = i
				break
			}
		}
	}
	if offset > len(names) {
		offset = len(names)
	}
	page := names[offset:]
	var nextToken string
	if len(page) > input.MaxResults {
		nextToken = page[input.MaxResults]
		page = page[:input.MaxResults]
	}

	type actEntry struct {
		ActivityArn  string  `json:"activityArn"`
		Name         string  `json:"name"`
		CreationDate float64 `json:"creationDate"`
	}
	entries := make([]actEntry, 0, len(page))
	for _, n := range page {
		act, loadErr := p.loadActivity(goCtx, ctx.AccountID, ctx.Region, n)
		if loadErr != nil || act == nil {
			continue
		}
		entries = append(entries, actEntry{
			ActivityArn:  act.ActivityArn,
			Name:         act.Name,
			CreationDate: sfnEpoch(act.CreatedDate),
		})
	}

	out := map[string]interface{}{
		"activities": entries,
	}
	if nextToken != "" {
		out["nextToken"] = nextToken
	}
	return statesJSONResponse(http.StatusOK, out)
}

func (p *StepFunctionsPlugin) deleteActivity(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ActivityArn string `json:"activityArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, sfnInvalidBody()
	}

	goCtx := context.Background()
	_, target, err := p.requireActivity(goCtx, input.ActivityArn)
	if err != nil {
		return nil, err
	}

	// See deleteStateMachine: the record and the name index come off under the ARN's account and
	// Region, so a foreign ARN can no longer delete the caller's own same-named activity.
	if delErr := p.state.Delete(goCtx, statesNamespace, p.activityKey(target.AccountID, target.Region, target.Name)); delErr != nil {
		return nil, fmt.Errorf("stepfunctions deleteActivity state.Delete: %w", delErr)
	}

	names, err := p.loadActivityNames(goCtx, target.AccountID, target.Region)
	if err != nil {
		return nil, err
	}
	newNames := make([]string, 0, len(names))
	for _, n := range names {
		if n != target.Name {
			newNames = append(newNames, n)
		}
	}
	if err := p.saveActivityNames(goCtx, target.AccountID, target.Region, newNames); err != nil {
		return nil, fmt.Errorf("stepfunctions deleteActivity saveActivityNames: %w", err)
	}

	return statesJSONResponse(http.StatusOK, map[string]interface{}{})
}

// TagResource, UntagResource and ListTagsForResource live in stepfunctions_tags.go, beside the
// ARN resolver they and the Resource Groups Tagging API share (#910).

// --- Response helpers ---

// smToMap converts a StateMachineState to the AWS API DescribeStateMachine shape.
func smToMap(sm *StateMachineState) map[string]interface{} {
	return map[string]interface{}{
		"stateMachineArn": sm.StateMachineArn,
		"name":            sm.Name,
		"status":          sm.Status,
		"definition":      sm.Definition,
		"roleArn":         sm.RoleArn,
		"type":            sm.Type,
		"creationDate":    sfnEpoch(sm.CreatedDate),
	}
}

// execToMap converts an ExecutionState to the AWS API DescribeExecution shape.
func execToMap(exec *ExecutionState) map[string]interface{} {
	out := map[string]interface{}{
		"executionArn":    exec.ExecutionArn,
		"stateMachineArn": exec.StateMachineArn,
		"name":            exec.Name,
		"status":          exec.Status,
		"startDate":       sfnEpoch(exec.StartDate),
	}
	if exec.Input != "" {
		out["input"] = exec.Input
	}
	if exec.Output != "" {
		out["output"] = exec.Output
	}
	if !exec.StopDate.IsZero() {
		out["stopDate"] = sfnEpoch(exec.StopDate)
	}
	return out
}

// statesJSONResponse builds an AWSResponse with a JSON body for Step Functions
// using Content-Type: application/x-amz-json-1.0.
func statesJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("stepfunctions marshal response: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.0"},
		Body:       body,
	}, nil
}
