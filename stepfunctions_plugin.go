package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

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
		return nil, &AWSError{
			Code:       "UnsupportedOperationException",
			Message:    fmt.Sprintf("StepFunctionsPlugin: unknown operation %q", op),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- State key helpers ---

func (p *StepFunctionsPlugin) smKey(accountID, region, name string) string {
	return "statemachine:" + accountID + "/" + region + "/" + name
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

func (p *StepFunctionsPlugin) activityKey(accountID, region, name string) string {
	return "activity:" + accountID + "/" + region + "/" + name
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

func (p *StepFunctionsPlugin) saveExecution(goCtx context.Context, exec *ExecutionState) error {
	smName := extractSMNameFromARN(exec.StateMachineArn)
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

// --- ARN helpers ---

// extractSMNameFromARN returns the state machine name from its ARN.
// Format: arn:aws:states:{region}:{acct}:stateMachine:{name}.
func extractSMNameFromARN(arn string) string {
	if idx := strings.LastIndexByte(arn, ':'); idx >= 0 {
		return arn[idx+1:]
	}
	return arn
}

// extractSMNameFromExecARN returns the state machine name embedded in an
// execution ARN. Format: arn:aws:states:{region}:{acct}:execution:{smName}:{execName}.
func extractSMNameFromExecARN(execArn string) string {
	// Strip the last segment (execName) then extract the smName.
	if idx := strings.LastIndexByte(execArn, ':'); idx >= 0 {
		rest := execArn[:idx]
		return extractSMNameFromARN(rest)
	}
	return ""
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
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
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
		"creationDate":    sm.CreatedDate.Format(time.RFC3339),
	}
	return statesJSONResponse(http.StatusOK, out)
}

func (p *StepFunctionsPlugin) describeStateMachine(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		StateMachineArn string `json:"stateMachineArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	name := extractSMNameFromARN(input.StateMachineArn)
	goCtx := context.Background()
	sm, err := p.loadStateMachine(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if sm == nil {
		return nil, &AWSError{Code: "StateMachineDoesNotExist", Message: "State machine does not exist: " + input.StateMachineArn, HTTPStatus: http.StatusNotFound}
	}

	return statesJSONResponse(http.StatusOK, smToMap(sm))
}

func (p *StepFunctionsPlugin) updateStateMachine(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		StateMachineArn string `json:"stateMachineArn"`
		Definition      string `json:"definition"`
		RoleArn         string `json:"roleArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	name := extractSMNameFromARN(input.StateMachineArn)
	goCtx := context.Background()
	sm, err := p.loadStateMachine(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if sm == nil {
		return nil, &AWSError{Code: "StateMachineDoesNotExist", Message: "State machine does not exist: " + input.StateMachineArn, HTTPStatus: http.StatusNotFound}
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
		"updateDate": p.tc.Now().Format(time.RFC3339),
	}
	return statesJSONResponse(http.StatusOK, out)
}

func (p *StepFunctionsPlugin) deleteStateMachine(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		StateMachineArn string `json:"stateMachineArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	name := extractSMNameFromARN(input.StateMachineArn)
	goCtx := context.Background()
	sm, err := p.loadStateMachine(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if sm == nil {
		return nil, &AWSError{Code: "StateMachineDoesNotExist", Message: "State machine does not exist: " + input.StateMachineArn, HTTPStatus: http.StatusNotFound}
	}

	if delErr := p.state.Delete(goCtx, statesNamespace, p.smKey(ctx.AccountID, ctx.Region, name)); delErr != nil {
		return nil, fmt.Errorf("stepfunctions deleteStateMachine state.Delete: %w", delErr)
	}

	names, err := p.loadSMNames(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	newNames := make([]string, 0, len(names))
	for _, n := range names {
		if n != name {
			newNames = append(newNames, n)
		}
	}
	if err := p.saveSMNames(goCtx, ctx.AccountID, ctx.Region, newNames); err != nil {
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
		StateMachineArn string `json:"stateMachineArn"`
		Name            string `json:"name"`
		Type            string `json:"type"`
		CreationDate    string `json:"creationDate"`
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
			CreationDate:    sm.CreatedDate.Format(time.RFC3339),
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
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	smName := extractSMNameFromARN(input.StateMachineArn)
	goCtx := context.Background()
	sm, err := p.loadStateMachine(goCtx, ctx.AccountID, ctx.Region, smName)
	if err != nil {
		return nil, err
	}
	if sm == nil {
		return nil, &AWSError{Code: "StateMachineDoesNotExist", Message: "State machine does not exist: " + input.StateMachineArn, HTTPStatus: http.StatusNotFound}
	}

	execName := input.Name
	if execName == "" {
		execName = "exec-" + generateLambdaRevisionID()[:8]
	}

	execArn := fmt.Sprintf("arn:aws:states:%s:%s:execution:%s:%s", ctx.Region, ctx.AccountID, smName, execName)
	now := p.tc.Now()
	exec := &ExecutionState{
		ExecutionArn:    execArn,
		StateMachineArn: input.StateMachineArn,
		Name:            execName,
		Status:          "RUNNING",
		Input:           input.Input,
		StartDate:       now,
		AccountID:       ctx.AccountID,
		Region:          ctx.Region,
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

	if err := p.saveExecution(goCtx, exec); err != nil {
		return nil, fmt.Errorf("stepfunctions startExecution saveExecution: %w", err)
	}

	ids, err := p.loadExecIDs(goCtx, ctx.AccountID, ctx.Region, smName)
	if err != nil {
		return nil, err
	}
	ids = append(ids, execName)
	if err := p.saveExecIDs(goCtx, ctx.AccountID, ctx.Region, smName, ids); err != nil {
		return nil, fmt.Errorf("stepfunctions startExecution saveExecIDs: %w", err)
	}

	out := map[string]interface{}{
		"executionArn": exec.ExecutionArn,
		"startDate":    exec.StartDate.Format(time.RFC3339),
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
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	smName := extractSMNameFromARN(input.StateMachineArn)
	goCtx := context.Background()
	sm, err := p.loadStateMachine(goCtx, ctx.AccountID, ctx.Region, smName)
	if err != nil {
		return nil, err
	}
	if sm == nil {
		return nil, &AWSError{Code: "StateMachineDoesNotExist", Message: "State machine does not exist: " + input.StateMachineArn, HTTPStatus: http.StatusNotFound}
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

	execArn := fmt.Sprintf("arn:aws:states:%s:%s:express:%s:%s", ctx.Region, ctx.AccountID, smName, execName)
	now := p.tc.Now()
	exec := &ExecutionState{
		ExecutionArn:    execArn,
		StateMachineArn: input.StateMachineArn,
		Name:            execName,
		Status:          "RUNNING",
		Input:           input.Input,
		StartDate:       now,
		AccountID:       ctx.AccountID,
		Region:          ctx.Region,
		History:         []HistoryEvent{},
	}

	_, _ = p.executeASL(&def, input.Input, exec, ctx) //nolint:errcheck // status set on exec

	out := map[string]interface{}{
		"executionArn": exec.ExecutionArn,
		"startDate":    exec.StartDate.Format(time.RFC3339),
		"stopDate":     exec.StopDate.Format(time.RFC3339),
		"status":       exec.Status,
	}
	if exec.Output != "" {
		out["output"] = exec.Output
	}
	return statesJSONResponse(http.StatusOK, out)
}

func (p *StepFunctionsPlugin) stopExecution(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ExecutionArn string `json:"executionArn"`
		Cause        string `json:"cause"`
		Error        string `json:"error"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	smName := extractSMNameFromExecARN(input.ExecutionArn)
	execName := extractSMNameFromARN(input.ExecutionArn)
	goCtx := context.Background()
	exec, err := p.loadExecution(goCtx, ctx.AccountID, ctx.Region, smName, execName)
	if err != nil {
		return nil, err
	}
	if exec == nil {
		return nil, &AWSError{Code: "ExecutionDoesNotExist", Message: "Execution does not exist: " + input.ExecutionArn, HTTPStatus: http.StatusNotFound}
	}

	now := p.tc.Now()
	exec.Status = "ABORTED"
	exec.StopDate = now

	if err := p.saveExecution(goCtx, exec); err != nil {
		return nil, fmt.Errorf("stepfunctions stopExecution saveExecution: %w", err)
	}

	out := map[string]interface{}{
		"stopDate": now.Format(time.RFC3339),
	}
	return statesJSONResponse(http.StatusOK, out)
}

func (p *StepFunctionsPlugin) describeExecution(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ExecutionArn string `json:"executionArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	smName := extractSMNameFromExecARN(input.ExecutionArn)
	execName := extractSMNameFromARN(input.ExecutionArn)
	goCtx := context.Background()
	exec, err := p.loadExecution(goCtx, ctx.AccountID, ctx.Region, smName, execName)
	if err != nil {
		return nil, err
	}
	if exec == nil {
		return nil, &AWSError{Code: "ExecutionDoesNotExist", Message: "Execution does not exist: " + input.ExecutionArn, HTTPStatus: http.StatusNotFound}
	}

	return statesJSONResponse(http.StatusOK, execToMap(exec))
}

func (p *StepFunctionsPlugin) listExecutions(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		StateMachineArn string `json:"stateMachineArn"`
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

	smName := extractSMNameFromARN(input.StateMachineArn)
	goCtx := context.Background()
	ids, err := p.loadExecIDs(goCtx, ctx.AccountID, ctx.Region, smName)
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
		ExecutionArn    string `json:"executionArn"`
		StateMachineArn string `json:"stateMachineArn"`
		Name            string `json:"name"`
		Status          string `json:"status"`
		StartDate       string `json:"startDate"`
		StopDate        string `json:"stopDate,omitempty"`
	}
	entries := make([]execEntry, 0, len(page))
	for _, execName := range page {
		exec, loadErr := p.loadExecution(goCtx, ctx.AccountID, ctx.Region, smName, execName)
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
			StartDate:       exec.StartDate.Format(time.RFC3339),
		}
		if !exec.StopDate.IsZero() {
			e.StopDate = exec.StopDate.Format(time.RFC3339)
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

func (p *StepFunctionsPlugin) getExecutionHistory(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ExecutionArn string `json:"executionArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	smName := extractSMNameFromExecARN(input.ExecutionArn)
	execName := extractSMNameFromARN(input.ExecutionArn)
	goCtx := context.Background()
	exec, err := p.loadExecution(goCtx, ctx.AccountID, ctx.Region, smName, execName)
	if err != nil {
		return nil, err
	}
	if exec == nil {
		return nil, &AWSError{Code: "ExecutionDoesNotExist", Message: "Execution does not exist: " + input.ExecutionArn, HTTPStatus: http.StatusNotFound}
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
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
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
		"creationDate": act.CreatedDate.Format(time.RFC3339),
	}
	return statesJSONResponse(http.StatusOK, out)
}

func (p *StepFunctionsPlugin) describeActivity(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ActivityArn string `json:"activityArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	name := extractSMNameFromARN(input.ActivityArn)
	goCtx := context.Background()
	act, err := p.loadActivity(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if act == nil {
		return nil, &AWSError{Code: "ActivityDoesNotExist", Message: "Activity does not exist: " + input.ActivityArn, HTTPStatus: http.StatusNotFound}
	}

	out := map[string]interface{}{
		"activityArn":  act.ActivityArn,
		"name":         act.Name,
		"creationDate": act.CreatedDate.Format(time.RFC3339),
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
		ActivityArn  string `json:"activityArn"`
		Name         string `json:"name"`
		CreationDate string `json:"creationDate"`
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
			CreationDate: act.CreatedDate.Format(time.RFC3339),
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

func (p *StepFunctionsPlugin) deleteActivity(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ActivityArn string `json:"activityArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	name := extractSMNameFromARN(input.ActivityArn)
	goCtx := context.Background()
	act, err := p.loadActivity(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if act == nil {
		return nil, &AWSError{Code: "ActivityDoesNotExist", Message: "Activity does not exist: " + input.ActivityArn, HTTPStatus: http.StatusNotFound}
	}

	if delErr := p.state.Delete(goCtx, statesNamespace, p.activityKey(ctx.AccountID, ctx.Region, name)); delErr != nil {
		return nil, fmt.Errorf("stepfunctions deleteActivity state.Delete: %w", delErr)
	}

	names, err := p.loadActivityNames(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	newNames := make([]string, 0, len(names))
	for _, n := range names {
		if n != name {
			newNames = append(newNames, n)
		}
	}
	if err := p.saveActivityNames(goCtx, ctx.AccountID, ctx.Region, newNames); err != nil {
		return nil, fmt.Errorf("stepfunctions deleteActivity saveActivityNames: %w", err)
	}

	return statesJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *StepFunctionsPlugin) tagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceArn string            `json:"resourceArn"`
		Tags        map[string]string `json:"tags"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	name := extractSMNameFromARN(input.ResourceArn)

	if strings.Contains(input.ResourceArn, ":stateMachine:") {
		sm, err := p.loadStateMachine(goCtx, ctx.AccountID, ctx.Region, name)
		if err != nil {
			return nil, err
		}
		if sm == nil {
			return nil, &AWSError{Code: "ResourceNotFound", Message: "Resource not found: " + input.ResourceArn, HTTPStatus: http.StatusNotFound}
		}
		if sm.Tags == nil {
			sm.Tags = make(map[string]string)
		}
		for k, v := range input.Tags {
			sm.Tags[k] = v
		}
		if err := p.saveStateMachine(goCtx, sm); err != nil {
			return nil, fmt.Errorf("stepfunctions tagResource saveStateMachine: %w", err)
		}
	} else if strings.Contains(input.ResourceArn, ":activity:") {
		act, err := p.loadActivity(goCtx, ctx.AccountID, ctx.Region, name)
		if err != nil {
			return nil, err
		}
		if act == nil {
			return nil, &AWSError{Code: "ResourceNotFound", Message: "Resource not found: " + input.ResourceArn, HTTPStatus: http.StatusNotFound}
		}
		if act.Tags == nil {
			act.Tags = make(map[string]string)
		}
		for k, v := range input.Tags {
			act.Tags[k] = v
		}
		if err := p.saveActivity(goCtx, act); err != nil {
			return nil, fmt.Errorf("stepfunctions tagResource saveActivity: %w", err)
		}
	} else {
		return nil, &AWSError{Code: "InvalidArn", Message: "unsupported resource ARN: " + input.ResourceArn, HTTPStatus: http.StatusBadRequest}
	}

	return statesJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *StepFunctionsPlugin) untagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceArn string   `json:"resourceArn"`
		TagKeys     []string `json:"tagKeys"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	removeSet := make(map[string]bool, len(input.TagKeys))
	for _, k := range input.TagKeys {
		removeSet[k] = true
	}

	goCtx := context.Background()
	name := extractSMNameFromARN(input.ResourceArn)

	if strings.Contains(input.ResourceArn, ":stateMachine:") {
		sm, err := p.loadStateMachine(goCtx, ctx.AccountID, ctx.Region, name)
		if err != nil {
			return nil, err
		}
		if sm == nil {
			return nil, &AWSError{Code: "ResourceNotFound", Message: "Resource not found: " + input.ResourceArn, HTTPStatus: http.StatusNotFound}
		}
		for k := range removeSet {
			delete(sm.Tags, k)
		}
		if err := p.saveStateMachine(goCtx, sm); err != nil {
			return nil, fmt.Errorf("stepfunctions untagResource saveStateMachine: %w", err)
		}
	} else if strings.Contains(input.ResourceArn, ":activity:") {
		act, err := p.loadActivity(goCtx, ctx.AccountID, ctx.Region, name)
		if err != nil {
			return nil, err
		}
		if act == nil {
			return nil, &AWSError{Code: "ResourceNotFound", Message: "Resource not found: " + input.ResourceArn, HTTPStatus: http.StatusNotFound}
		}
		for k := range removeSet {
			delete(act.Tags, k)
		}
		if err := p.saveActivity(goCtx, act); err != nil {
			return nil, fmt.Errorf("stepfunctions untagResource saveActivity: %w", err)
		}
	} else {
		return nil, &AWSError{Code: "InvalidArn", Message: "unsupported resource ARN: " + input.ResourceArn, HTTPStatus: http.StatusBadRequest}
	}

	return statesJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *StepFunctionsPlugin) listTagsForResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceArn string `json:"resourceArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	name := extractSMNameFromARN(input.ResourceArn)
	var tags map[string]string

	if strings.Contains(input.ResourceArn, ":stateMachine:") {
		sm, err := p.loadStateMachine(goCtx, ctx.AccountID, ctx.Region, name)
		if err != nil {
			return nil, err
		}
		if sm == nil {
			return nil, &AWSError{Code: "ResourceNotFound", Message: "Resource not found: " + input.ResourceArn, HTTPStatus: http.StatusNotFound}
		}
		tags = sm.Tags
	} else if strings.Contains(input.ResourceArn, ":activity:") {
		act, err := p.loadActivity(goCtx, ctx.AccountID, ctx.Region, name)
		if err != nil {
			return nil, err
		}
		if act == nil {
			return nil, &AWSError{Code: "ResourceNotFound", Message: "Resource not found: " + input.ResourceArn, HTTPStatus: http.StatusNotFound}
		}
		tags = act.Tags
	} else {
		return nil, &AWSError{Code: "InvalidArn", Message: "unsupported resource ARN: " + input.ResourceArn, HTTPStatus: http.StatusBadRequest}
	}

	if tags == nil {
		tags = make(map[string]string)
	}
	out := map[string]interface{}{
		"tags": tags,
	}
	return statesJSONResponse(http.StatusOK, out)
}

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
		"creationDate":    sm.CreatedDate.Format(time.RFC3339),
	}
}

// execToMap converts an ExecutionState to the AWS API DescribeExecution shape.
func execToMap(exec *ExecutionState) map[string]interface{} {
	out := map[string]interface{}{
		"executionArn":    exec.ExecutionArn,
		"stateMachineArn": exec.StateMachineArn,
		"name":            exec.Name,
		"status":          exec.Status,
		"startDate":       exec.StartDate.Format(time.RFC3339),
	}
	if exec.Input != "" {
		out["input"] = exec.Input
	}
	if exec.Output != "" {
		out["output"] = exec.Output
	}
	if !exec.StopDate.IsZero() {
		out["stopDate"] = exec.StopDate.Format(time.RFC3339)
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
