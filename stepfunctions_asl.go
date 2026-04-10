package substrate

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// aslStateError carries error code and cause for a failed ASL state.
type aslStateError struct {
	// Error is the error code (e.g. "States.TaskFailed").
	Error string
	// Cause is a human-readable description of the failure.
	Cause string
}

// executeASL runs the state machine defined by def with the given inputJSON,
// recording events into exec.History. On success it returns the final output JSON
// and sets exec.Status = "SUCCEEDED". On failure it sets exec.Status = "FAILED"
// and returns a non-nil error.
func (p *StepFunctionsPlugin) executeASL(
	def *StateMachineDefinition,
	inputJSON string,
	exec *ExecutionState,
	reqCtx *RequestContext,
) (string, error) {
	// Parse execution input.
	var currentData interface{}
	if inputJSON != "" && inputJSON != "null" {
		if err := json.Unmarshal([]byte(inputJSON), &currentData); err != nil {
			currentData = map[string]interface{}{}
		}
	}
	if currentData == nil {
		currentData = map[string]interface{}{}
	}

	histID := int64(len(exec.History))
	nextID := func() int64 {
		histID++
		return histID
	}

	appendEvent := func(ev HistoryEvent) {
		exec.History = append(exec.History, ev)
	}

	// Record ExecutionStarted.
	{
		ev := HistoryEvent{ID: nextID(), Type: "ExecutionStarted", Timestamp: p.tc.Now()}
		d := map[string]interface{}{"input": inputJSON}
		ev.ExecutionStartedEventDetails = &d
		appendEvent(ev)
	}

	const maxSteps = 1000
	stateName := def.StartAt

	for step := 0; step < maxSteps; step++ {
		state, ok := def.States[stateName]
		if !ok {
			return p.aslFail(exec, &histID, "States.Runtime", "state not found: "+stateName)
		}

		// Apply InputPath to derive the effective input.
		effectiveInput := aslApplyPath(currentData, state.InputPath)

		// Record StateEntered.
		{
			ev := HistoryEvent{ID: nextID(), Type: "StateEntered", Timestamp: p.tc.Now()}
			d := map[string]interface{}{"name": stateName, "input": aslMarshalStr(effectiveInput)}
			ev.StateEnteredEventDetails = &d
			appendEvent(ev)
		}

		// Handle terminal Fail state inline.
		if state.Type == "Fail" {
			return p.aslFail(exec, &histID, state.Error, state.Cause)
		}

		// Apply Parameters to transform input before execution.
		if state.Parameters != nil {
			effectiveInput = aslApplyParameters(state.Parameters, effectiveInput)
		}

		// Execute the state.
		var result interface{}
		var nextState string
		var stateErr *aslStateError

		switch state.Type {
		case "Pass":
			result, nextState, stateErr = p.aslRunPass(state, effectiveInput)
		case "Task":
			result, nextState, stateErr = p.aslRunTask(state, effectiveInput, exec, reqCtx, &histID)
		case "Wait":
			result, nextState, stateErr = p.aslRunWait(state, effectiveInput)
		case "Choice":
			result, nextState, stateErr = p.aslRunChoice(state, effectiveInput)
		case "Succeed":
			result, nextState = effectiveInput, ""
		case "Parallel":
			result, nextState, stateErr = p.aslRunParallel(state, effectiveInput, exec, reqCtx)
		case "Map":
			result, nextState, stateErr = p.aslRunMap(state, effectiveInput, exec, reqCtx)
		default:
			return p.aslFail(exec, &histID, "States.Runtime", "unknown state type: "+state.Type)
		}

		// Handle state-level errors with Catch/Retry already applied by aslRunTask;
		// for other state types apply Catch here.
		if stateErr != nil {
			caught := false
			for _, cc := range state.Catch {
				if aslErrorMatches(stateErr.Error, cc.ErrorEquals) {
					errOutput := map[string]interface{}{
						"Error": stateErr.Error,
						"Cause": stateErr.Cause,
					}
					merged := aslSetPath(currentData, cc.ResultPath, errOutput)
					// StateExited for the caught state.
					{
						ev := HistoryEvent{ID: nextID(), Type: "StateExited", Timestamp: p.tc.Now()}
						d := map[string]interface{}{"name": stateName, "output": aslMarshalStr(merged)}
						ev.StateExitedEventDetails = &d
						appendEvent(ev)
					}
					currentData = merged
					stateName = cc.Next
					caught = true
					break
				}
			}
			if !caught {
				return p.aslFail(exec, &histID, stateErr.Error, stateErr.Cause)
			}
			continue
		}

		// For Choice state, data passes through unchanged.
		if state.Type == "Choice" {
			ev := HistoryEvent{ID: nextID(), Type: "StateExited", Timestamp: p.tc.Now()}
			d := map[string]interface{}{"name": stateName, "output": aslMarshalStr(currentData)}
			ev.StateExitedEventDetails = &d
			appendEvent(ev)
			stateName = nextState
			continue
		}

		// Apply ResultSelector to filter task result before merging.
		if state.ResultSelector != nil && result != nil {
			result = aslApplyParameters(state.ResultSelector, result)
		}

		// Apply ResultPath: merge result into current input.
		currentData = aslSetPath(currentData, state.ResultPath, result)

		// Apply OutputPath: select sub-value of current data.
		currentData = aslApplyPath(currentData, state.OutputPath)

		// Record StateExited.
		{
			ev := HistoryEvent{ID: nextID(), Type: "StateExited", Timestamp: p.tc.Now()}
			d := map[string]interface{}{"name": stateName, "output": aslMarshalStr(currentData)}
			ev.StateExitedEventDetails = &d
			appendEvent(ev)
		}

		// Terminal state?
		if state.End || state.Type == "Succeed" {
			outputJSON := aslMarshalStr(currentData)
			ev := HistoryEvent{ID: nextID(), Type: "ExecutionSucceeded", Timestamp: p.tc.Now()}
			d := map[string]interface{}{"output": outputJSON}
			ev.ExecutionSucceededEventDetails = &d
			appendEvent(ev)
			exec.Status = "SUCCEEDED"
			exec.Output = outputJSON
			exec.StopDate = p.tc.Now()
			return outputJSON, nil
		}

		stateName = nextState
	}

	return p.aslFail(exec, &histID, "States.Runtime", "state machine exceeded maximum steps")
}

// aslFail records an ExecutionFailed event, sets exec to FAILED, and returns an error.
func (p *StepFunctionsPlugin) aslFail(exec *ExecutionState, histID *int64, errCode, cause string) (string, error) {
	*histID++
	ev := HistoryEvent{ID: *histID, Type: "ExecutionFailed", Timestamp: p.tc.Now()}
	d := map[string]interface{}{"error": errCode, "cause": cause}
	ev.ExecutionFailedEventDetails = &d
	exec.History = append(exec.History, ev)
	exec.Status = "FAILED"
	exec.StopDate = p.tc.Now()
	exec.ErrorDetails = errCode + ": " + cause
	return "", fmt.Errorf("%s: %s", errCode, cause)
}

// --- State handlers ---

func (p *StepFunctionsPlugin) aslRunPass(state *ASLState, input interface{}) (interface{}, string, *aslStateError) {
	if state.Result != nil {
		return state.Result, state.Next, nil
	}
	return input, state.Next, nil
}

func (p *StepFunctionsPlugin) aslRunWait(state *ASLState, input interface{}) (interface{}, string, *aslStateError) {
	seconds := state.Seconds
	if state.SecondsPath != "" {
		if val, err := aslGetPath(input, state.SecondsPath); err == nil {
			if n, ok := val.(float64); ok {
				seconds = int(n)
			}
		}
	}
	if seconds > 0 {
		p.tc.SetTime(p.tc.Now().Add(time.Duration(seconds) * time.Second))
	}
	return input, state.Next, nil
}

func (p *StepFunctionsPlugin) aslRunChoice(state *ASLState, input interface{}) (interface{}, string, *aslStateError) {
	for i := range state.Choices {
		if aslEvalRule(&state.Choices[i], input) {
			return input, state.Choices[i].Next, nil
		}
	}
	if state.Default != "" {
		return input, state.Default, nil
	}
	return nil, "", &aslStateError{Error: "States.NoChoiceMatched", Cause: "no choice rule matched and no Default"}
}

func (p *StepFunctionsPlugin) aslRunTask(
	state *ASLState,
	input interface{},
	exec *ExecutionState,
	reqCtx *RequestContext,
	histID *int64,
) (interface{}, string, *aslStateError) {
	nextID := func() int64 {
		*histID++
		return *histID
	}

	// TaskScheduled event.
	{
		ev := HistoryEvent{ID: nextID(), Type: "TaskScheduled", Timestamp: p.tc.Now()}
		d := map[string]interface{}{"resource": state.Resource}
		ev.TaskScheduledEventDetails = &d
		exec.History = append(exec.History, ev)
	}

	attempt := 0
	var lastErr *aslStateError

	for {
		startTime := p.tc.Now()
		result, invokeErr := p.aslInvokeResource(state.Resource, input, reqCtx)

		// Check TimeoutSeconds.
		if invokeErr == nil && state.TimeoutSeconds > 0 {
			elapsed := p.tc.Now().Sub(startTime)
			if elapsed > time.Duration(state.TimeoutSeconds)*time.Second {
				invokeErr = fmt.Errorf("task timed out after %v", elapsed)
			}
		}

		if invokeErr == nil {
			ev := HistoryEvent{ID: nextID(), Type: "TaskSucceeded", Timestamp: p.tc.Now()}
			d := map[string]interface{}{"output": aslMarshalStr(result)}
			ev.TaskSucceededEventDetails = &d
			exec.History = append(exec.History, ev)
			return result, state.Next, nil
		}

		errCode := "States.TaskFailed"
		if strings.Contains(invokeErr.Error(), "timed out") {
			errCode = "States.Timeout"
		}
		lastErr = &aslStateError{Error: errCode, Cause: invokeErr.Error()}

		// Find matching retry config.
		maxAttempts := 0
		intervalSecs := 1
		backoffRate := 2.0
		for _, rc := range state.Retry {
			if aslErrorMatches(lastErr.Error, rc.ErrorEquals) {
				maxAttempts = rc.MaxAttempts
				if rc.IntervalSeconds > 0 {
					intervalSecs = rc.IntervalSeconds
				}
				if rc.BackoffRate > 0 {
					backoffRate = rc.BackoffRate
				}
				break
			}
		}

		if attempt >= maxAttempts {
			break
		}

		// Advance time for backoff.
		wait := float64(intervalSecs)
		for i := 1; i < attempt+1; i++ {
			wait *= backoffRate
		}
		p.tc.SetTime(p.tc.Now().Add(time.Duration(wait) * time.Second))
		attempt++
	}

	// All retries exhausted.
	{
		ev := HistoryEvent{ID: nextID(), Type: "TaskFailed", Timestamp: p.tc.Now()}
		d := map[string]interface{}{"error": lastErr.Error, "cause": lastErr.Cause}
		ev.TaskFailedEventDetails = &d
		exec.History = append(exec.History, ev)
	}
	return nil, "", lastErr
}

func (p *StepFunctionsPlugin) aslRunParallel(
	state *ASLState,
	input interface{},
	exec *ExecutionState,
	reqCtx *RequestContext,
) (interface{}, string, *aslStateError) {
	results := make([]interface{}, 0, len(state.Branches))
	for i := range state.Branches {
		branch := state.Branches[i]
		branchExec := &ExecutionState{History: []HistoryEvent{}}
		inputStr := aslMarshalStr(input)
		output, err := p.executeASL(&branch, inputStr, branchExec, reqCtx)
		if err != nil {
			return nil, "", &aslStateError{Error: "States.BranchFailed", Cause: err.Error()}
		}
		var result interface{}
		_ = json.Unmarshal([]byte(output), &result) //nolint:errcheck // best-effort
		results = append(results, result)
	}
	return results, state.Next, nil
}

func (p *StepFunctionsPlugin) aslRunMap(
	state *ASLState,
	input interface{},
	exec *ExecutionState,
	reqCtx *RequestContext,
) (interface{}, string, *aslStateError) {
	items := input
	if state.ItemsPath != "" {
		val, err := aslGetPath(input, state.ItemsPath)
		if err != nil {
			return nil, "", &aslStateError{
				Error: "States.Runtime",
				Cause: "ItemsPath not found: " + state.ItemsPath,
			}
		}
		items = val
	}

	arr, ok := items.([]interface{})
	if !ok {
		return nil, "", &aslStateError{
			Error: "States.Runtime",
			Cause: "Map state input is not an array",
		}
	}

	if state.Iterator == nil {
		return []interface{}{}, state.Next, nil
	}

	results := make([]interface{}, 0, len(arr))
	for _, item := range arr {
		itemExec := &ExecutionState{History: []HistoryEvent{}}
		output, err := p.executeASL(state.Iterator, aslMarshalStr(item), itemExec, reqCtx)
		if err != nil {
			return nil, "", &aslStateError{Error: "States.Runtime", Cause: err.Error()}
		}
		var result interface{}
		_ = json.Unmarshal([]byte(output), &result) //nolint:errcheck // best-effort
		results = append(results, result)
	}
	return results, state.Next, nil
}

// aslInvokeResource invokes a Task resource and returns its output.
// Lambda ARNs are dispatched via the plugin registry; all other resources
// return a stub empty object.
func (p *StepFunctionsPlugin) aslInvokeResource(resource string, input interface{}, reqCtx *RequestContext) (interface{}, error) {
	if strings.Contains(resource, ":lambda:") && p.registry != nil {
		return p.aslInvokeLambda(resource, input, reqCtx)
	}
	return map[string]interface{}{}, nil
}

// aslInvokeLambda invokes a Lambda function via the plugin registry.
func (p *StepFunctionsPlugin) aslInvokeLambda(resource string, input interface{}, reqCtx *RequestContext) (interface{}, error) {
	funcName := extractSMNameFromARN(resource)
	payloadBytes, err := json.Marshal(input)
	if err != nil {
		payloadBytes = []byte("{}")
	}

	lambdaReq := &AWSRequest{
		Service:   "lambda",
		Operation: "POST",
		Path:      "/2015-03-31/functions/" + funcName + "/invocations",
		Headers:   map[string]string{},
		Params:    map[string]string{},
		Body:      payloadBytes,
	}

	resp, routeErr := p.registry.RouteRequest(reqCtx, lambdaReq)
	if routeErr != nil {
		return nil, routeErr
	}

	var result interface{}
	if unmarshalErr := json.Unmarshal(resp.Body, &result); unmarshalErr != nil {
		return map[string]interface{}{}, nil //nolint:nilerr // Non-JSON response: return empty object.
	}
	return result, nil
}

// --- Choice rule evaluator ---

// aslEvalRule evaluates a single ChoiceRule against the given input data.
func aslEvalRule(rule *ChoiceRule, input interface{}) bool {
	// Logical combinators.
	if len(rule.And) > 0 {
		for i := range rule.And {
			if !aslEvalRule(&rule.And[i], input) {
				return false
			}
		}
		return true
	}
	if len(rule.Or) > 0 {
		for i := range rule.Or {
			if aslEvalRule(&rule.Or[i], input) {
				return true
			}
		}
		return false
	}
	if rule.Not != nil {
		return !aslEvalRule(rule.Not, input)
	}

	if rule.Variable == "" {
		return false
	}
	varVal, err := aslGetPath(input, rule.Variable)
	if err != nil {
		return false
	}

	switch {
	case rule.StringEquals != nil:
		s, ok := varVal.(string)
		return ok && s == *rule.StringEquals
	case rule.NumericEquals != nil:
		n, ok := varVal.(float64)
		return ok && n == *rule.NumericEquals
	case rule.BooleanEquals != nil:
		b, ok := varVal.(bool)
		return ok && b == *rule.BooleanEquals
	case rule.StringGreaterThan != nil:
		s, ok := varVal.(string)
		return ok && s > *rule.StringGreaterThan
	case rule.StringLessThan != nil:
		s, ok := varVal.(string)
		return ok && s < *rule.StringLessThan
	case rule.NumericGreaterThan != nil:
		n, ok := varVal.(float64)
		return ok && n > *rule.NumericGreaterThan
	case rule.NumericLessThan != nil:
		n, ok := varVal.(float64)
		return ok && n < *rule.NumericLessThan
	case rule.StringGreaterThanOrEquals != nil:
		s, ok := varVal.(string)
		return ok && s >= *rule.StringGreaterThanOrEquals
	case rule.StringLessThanOrEquals != nil:
		s, ok := varVal.(string)
		return ok && s <= *rule.StringLessThanOrEquals
	case rule.NumericGreaterThanOrEquals != nil:
		n, ok := varVal.(float64)
		return ok && n >= *rule.NumericGreaterThanOrEquals
	case rule.NumericLessThanOrEquals != nil:
		n, ok := varVal.(float64)
		return ok && n <= *rule.NumericLessThanOrEquals
	case rule.IsNull != nil:
		isNull := varVal == nil
		return isNull == *rule.IsNull
	case rule.IsPresent != nil:
		// IsPresent checks whether the variable path resolves to any value.
		// Since we already resolved it above (and returned false on error),
		// reaching here means the variable is present.
		return *rule.IsPresent
	}
	return false
}

// aslErrorMatches returns true if errCode matches any entry in the list,
// or if the list contains "States.ALL".
func aslErrorMatches(errCode string, errorEquals []string) bool {
	for _, e := range errorEquals {
		if e == errCode || e == "States.ALL" {
			return true
		}
	}
	return false
}

// aslApplyParameters constructs a new JSON object from params, resolving
// any keys ending with ".$" as JSONPath references into input.
func aslApplyParameters(params map[string]interface{}, input interface{}) interface{} {
	result := make(map[string]interface{})
	for key, val := range params {
		if strings.HasSuffix(key, ".$") {
			cleanKey := strings.TrimSuffix(key, ".$")
			path, _ := val.(string)
			resolved, err := aslGetPath(input, path)
			if err == nil {
				result[cleanKey] = resolved
			}
		} else {
			result[key] = val
		}
	}
	return result
}

// --- JSONPath-style helpers ---

// aslGetPath extracts the value at path from data.
// Supports "$" (entire document) and "$.field[.nested]" references.
func aslGetPath(data interface{}, path string) (interface{}, error) {
	if path == "" || path == "$" {
		return data, nil
	}
	if !strings.HasPrefix(path, "$.") {
		return nil, fmt.Errorf("unsupported path %q: must begin with $", path)
	}
	parts := strings.Split(path[2:], ".")
	current := data
	for _, part := range parts {
		m, ok := current.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("path %q: element %q: not an object", path, part)
		}
		val, exists := m[part]
		if !exists {
			return nil, fmt.Errorf("path %q: key %q not found", path, part)
		}
		current = val
	}
	return current, nil
}

// aslApplyPath applies InputPath or OutputPath to data. Empty string or "$"
// returns data unchanged; otherwise the referenced sub-value is returned.
func aslApplyPath(data interface{}, path string) interface{} {
	if path == "" || path == "$" {
		return data
	}
	val, err := aslGetPath(data, path)
	if err != nil {
		return data // fallback: return unchanged on error
	}
	return val
}

// aslSetPath applies ResultPath, merging result into the current input at path.
// Empty string or "$" replaces the entire current value with result.
// "$.field" sets that field on a copy of the current map.
func aslSetPath(current interface{}, path string, result interface{}) interface{} {
	if path == "" || path == "$" {
		return result
	}
	if !strings.HasPrefix(path, "$.") {
		return current
	}
	parts := strings.Split(path[2:], ".")
	currentMap, ok := current.(map[string]interface{})
	if !ok {
		return current
	}
	return aslSetPathInMap(currentMap, parts, result)
}

// aslSetPathInMap recursively sets a value at the given key path within a map.
func aslSetPathInMap(m map[string]interface{}, parts []string, value interface{}) map[string]interface{} {
	// Shallow copy.
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = v
	}
	if len(parts) == 1 {
		out[parts[0]] = value
		return out
	}
	// Recurse into nested map.
	sub, _ := m[parts[0]].(map[string]interface{})
	if sub == nil {
		sub = map[string]interface{}{}
	}
	out[parts[0]] = aslSetPathInMap(sub, parts[1:], value)
	return out
}

// aslMarshalStr marshals v to a JSON string, returning "null" on error.
func aslMarshalStr(v interface{}) string {
	if v == nil {
		return "null"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "null"
	}
	return string(b)
}
