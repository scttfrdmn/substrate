package substrate

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// maxEBEventRingBuffer is the maximum number of PutEvents events retained in state.
const maxEBEventRingBuffer = 100

// EventBridgePlugin emulates the Amazon EventBridge JSON-protocol API.
// It handles PutRule, DeleteRule, ListRules, DescribeRule, PutTargets,
// RemoveTargets, ListTargetsByRule, PutEvents, and ListEventBuses.
type EventBridgePlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "eventbridge".
func (p *EventBridgePlugin) Name() string { return "eventbridge" }

// Initialize sets up the EventBridgePlugin with the provided configuration.
func (p *EventBridgePlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for EventBridgePlugin.
func (p *EventBridgePlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an EventBridge JSON-protocol request to the
// appropriate handler.
func (p *EventBridgePlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "PutRule":
		return p.putRule(ctx, req)
	case "DeleteRule":
		return p.deleteRule(ctx, req)
	case "ListRules":
		return p.listRules(ctx, req)
	case "DescribeRule":
		return p.describeRule(ctx, req)
	case "PutTargets":
		return p.putTargets(ctx, req)
	case "RemoveTargets":
		return p.removeTargets(ctx, req)
	case "ListTargetsByRule":
		return p.listTargetsByRule(ctx, req)
	case "PutEvents":
		return p.putEvents(ctx, req)
	case "ListEventBuses":
		return p.listEventBuses(ctx)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    fmt.Sprintf("EventBridgePlugin: unknown operation %q", req.Operation),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- Rule operations --------------------------------------------------------

func (p *EventBridgePlugin) putRule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Name               string `json:"Name"`
		EventPattern       string `json:"EventPattern"`
		ScheduleExpression string `json:"ScheduleExpression"`
		State              string `json:"State"`
		Description        string `json:"Description"`
		EventBusName       string `json:"EventBusName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.Name == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}

	state := body.State
	if state == "" {
		state = "ENABLED"
	}
	busName := body.EventBusName
	if busName == "" {
		busName = "default"
	}

	goCtx := context.Background()
	rule := EBRule{
		Name:               body.Name,
		ARN:                ebRuleARN(ctx.Region, ctx.AccountID, body.Name),
		EventPattern:       body.EventPattern,
		ScheduleExpression: body.ScheduleExpression,
		State:              state,
		Description:        body.Description,
		EventBusName:       busName,
	}
	data, err := json.Marshal(rule)
	if err != nil {
		return nil, fmt.Errorf("eventbridge putRule marshal: %w", err)
	}
	if err := p.state.Put(goCtx, eventbridgeNamespace, ebRuleKey(ctx.AccountID, ctx.Region, body.Name), data); err != nil {
		return nil, fmt.Errorf("eventbridge putRule state.Put: %w", err)
	}

	idxKey := ebRuleNamesKey(ctx.AccountID, ctx.Region)
	updateStringIndex(goCtx, p.state, eventbridgeNamespace, idxKey, body.Name)

	type response struct {
		RuleArn string `json:"RuleArn"`
	}
	return ebJSONResponse(http.StatusOK, response{RuleArn: rule.ARN})
}

func (p *EventBridgePlugin) deleteRule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Name         string `json:"Name"`
		EventBusName string `json:"EventBusName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.Name == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	ruleKey := ebRuleKey(ctx.AccountID, ctx.Region, body.Name)
	existing, err := p.state.Get(goCtx, eventbridgeNamespace, ruleKey)
	if err != nil {
		return nil, fmt.Errorf("eventbridge deleteRule state.Get: %w", err)
	}
	if existing == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Rule not found: " + body.Name, HTTPStatus: http.StatusNotFound}
	}

	// Check if targets are still attached.
	targetsKey := ebTargetsKey(ctx.AccountID, ctx.Region, body.Name)
	targetsData, _ := p.state.Get(goCtx, eventbridgeNamespace, targetsKey)
	if targetsData != nil {
		var targets []EBTarget
		if json.Unmarshal(targetsData, &targets) == nil && len(targets) > 0 {
			return nil, &AWSError{
				Code:       "ValidationException",
				Message:    "Rule " + body.Name + " still has targets. Remove targets before deleting the rule.",
				HTTPStatus: http.StatusBadRequest,
			}
		}
	}

	if err := p.state.Delete(goCtx, eventbridgeNamespace, ruleKey); err != nil {
		return nil, fmt.Errorf("eventbridge deleteRule state.Delete: %w", err)
	}
	_ = p.state.Delete(goCtx, eventbridgeNamespace, targetsKey)

	idxKey := ebRuleNamesKey(ctx.AccountID, ctx.Region)
	removeFromStringIndex(goCtx, p.state, eventbridgeNamespace, idxKey, body.Name)

	return ebJSONResponse(http.StatusOK, struct{}{})
}

func (p *EventBridgePlugin) listRules(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		NamePrefix   string `json:"NamePrefix"`
		EventBusName string `json:"EventBusName"`
		NextToken    string `json:"NextToken"`
		Limit        int    `json:"Limit"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
	}

	goCtx := context.Background()
	idxKey := ebRuleNamesKey(ctx.AccountID, ctx.Region)
	allNames, err := loadStringIndex(goCtx, p.state, eventbridgeNamespace, idxKey)
	if err != nil {
		return nil, fmt.Errorf("eventbridge listRules loadIndex: %w", err)
	}

	names := allNames
	if body.NamePrefix != "" {
		filtered := make([]string, 0, len(allNames))
		for _, n := range allNames {
			if strings.HasPrefix(n, body.NamePrefix) {
				filtered = append(filtered, n)
			}
		}
		names = filtered
	}

	limit := body.Limit
	if limit <= 0 {
		limit = 100
	}
	offset := 0
	if body.NextToken != "" {
		if decoded, decErr := base64.StdEncoding.DecodeString(body.NextToken); decErr == nil {
			if n, atoiErr := strconv.Atoi(string(decoded)); atoiErr == nil && n > 0 {
				offset = n
			}
		}
	}
	if offset > len(names) {
		offset = len(names)
	}

	end := offset + limit
	var nextToken string
	if end < len(names) {
		nextToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	} else {
		end = len(names)
	}

	rules := make([]EBRule, 0, end-offset)
	for _, name := range names[offset:end] {
		data, getErr := p.state.Get(goCtx, eventbridgeNamespace, ebRuleKey(ctx.AccountID, ctx.Region, name))
		if getErr != nil || data == nil {
			continue
		}
		var rule EBRule
		if unmarshalErr := json.Unmarshal(data, &rule); unmarshalErr != nil {
			continue
		}
		rules = append(rules, rule)
	}

	type response struct {
		Rules     []EBRule `json:"Rules"`
		NextToken string   `json:"NextToken,omitempty"`
	}
	return ebJSONResponse(http.StatusOK, response{Rules: rules, NextToken: nextToken})
}

func (p *EventBridgePlugin) describeRule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Name         string `json:"Name"`
		EventBusName string `json:"EventBusName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.Name == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	data, err := p.state.Get(goCtx, eventbridgeNamespace, ebRuleKey(ctx.AccountID, ctx.Region, body.Name))
	if err != nil {
		return nil, fmt.Errorf("eventbridge describeRule state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Rule not found: " + body.Name, HTTPStatus: http.StatusNotFound}
	}

	var rule EBRule
	if err := json.Unmarshal(data, &rule); err != nil {
		return nil, fmt.Errorf("eventbridge describeRule unmarshal: %w", err)
	}
	return ebJSONResponse(http.StatusOK, rule)
}

// --- Target operations ------------------------------------------------------

func (p *EventBridgePlugin) putTargets(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Rule         string     `json:"Rule"`
		EventBusName string     `json:"EventBusName"`
		Targets      []EBTarget `json:"Targets"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.Rule == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "Rule is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()

	// Verify the rule exists.
	ruleData, err := p.state.Get(goCtx, eventbridgeNamespace, ebRuleKey(ctx.AccountID, ctx.Region, body.Rule))
	if err != nil {
		return nil, fmt.Errorf("eventbridge putTargets rule.Get: %w", err)
	}
	if ruleData == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Rule not found: " + body.Rule, HTTPStatus: http.StatusNotFound}
	}

	targetsKey := ebTargetsKey(ctx.AccountID, ctx.Region, body.Rule)
	var existing []EBTarget
	existingData, _ := p.state.Get(goCtx, eventbridgeNamespace, targetsKey)
	if existingData != nil {
		_ = json.Unmarshal(existingData, &existing)
	}

	// Merge targets: overwrite by ID.
	idMap := make(map[string]int, len(existing))
	for i, t := range existing {
		idMap[t.Id] = i
	}
	for _, t := range body.Targets {
		if idx, ok := idMap[t.Id]; ok {
			existing[idx] = t
		} else {
			existing = append(existing, t)
			idMap[t.Id] = len(existing) - 1
		}
	}

	data, err := json.Marshal(existing)
	if err != nil {
		return nil, fmt.Errorf("eventbridge putTargets marshal: %w", err)
	}
	if err := p.state.Put(goCtx, eventbridgeNamespace, targetsKey, data); err != nil {
		return nil, fmt.Errorf("eventbridge putTargets state.Put: %w", err)
	}

	type response struct {
		FailedEntryCount int           `json:"FailedEntryCount"`
		FailedEntries    []interface{} `json:"FailedEntries"`
	}
	return ebJSONResponse(http.StatusOK, response{FailedEntryCount: 0, FailedEntries: []interface{}{}})
}

func (p *EventBridgePlugin) removeTargets(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Rule         string   `json:"Rule"`
		EventBusName string   `json:"EventBusName"`
		IDs          []string `json:"Ids"` //nolint:revive
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.Rule == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "Rule is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	targetsKey := ebTargetsKey(ctx.AccountID, ctx.Region, body.Rule)
	existingData, _ := p.state.Get(goCtx, eventbridgeNamespace, targetsKey)
	var existing []EBTarget
	if existingData != nil {
		_ = json.Unmarshal(existingData, &existing)
	}

	removeIDs := make(map[string]bool, len(body.IDs))
	for _, id := range body.IDs {
		removeIDs[id] = true
	}

	remaining := make([]EBTarget, 0, len(existing))
	for _, t := range existing {
		if !removeIDs[t.Id] {
			remaining = append(remaining, t)
		}
	}

	data, err := json.Marshal(remaining)
	if err != nil {
		return nil, fmt.Errorf("eventbridge removeTargets marshal: %w", err)
	}
	if err := p.state.Put(goCtx, eventbridgeNamespace, targetsKey, data); err != nil {
		return nil, fmt.Errorf("eventbridge removeTargets state.Put: %w", err)
	}

	type response struct {
		FailedEntryCount int           `json:"FailedEntryCount"`
		FailedEntries    []interface{} `json:"FailedEntries"`
	}
	return ebJSONResponse(http.StatusOK, response{FailedEntryCount: 0, FailedEntries: []interface{}{}})
}

func (p *EventBridgePlugin) listTargetsByRule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Rule         string `json:"Rule"`
		EventBusName string `json:"EventBusName"`
		NextToken    string `json:"NextToken"`
		Limit        int    `json:"Limit"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.Rule == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "Rule is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()

	// Verify the rule exists.
	ruleData, err := p.state.Get(goCtx, eventbridgeNamespace, ebRuleKey(ctx.AccountID, ctx.Region, body.Rule))
	if err != nil {
		return nil, fmt.Errorf("eventbridge listTargetsByRule rule.Get: %w", err)
	}
	if ruleData == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Rule not found: " + body.Rule, HTTPStatus: http.StatusNotFound}
	}

	targetsData, _ := p.state.Get(goCtx, eventbridgeNamespace, ebTargetsKey(ctx.AccountID, ctx.Region, body.Rule))
	var targets []EBTarget
	if targetsData != nil {
		_ = json.Unmarshal(targetsData, &targets)
	}
	if targets == nil {
		targets = []EBTarget{}
	}

	type response struct {
		Targets   []EBTarget `json:"Targets"`
		NextToken string     `json:"NextToken,omitempty"`
	}
	return ebJSONResponse(http.StatusOK, response{Targets: targets})
}

// --- Event operations -------------------------------------------------------

func (p *EventBridgePlugin) putEvents(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Entries []struct {
			Source       string `json:"Source"`
			DetailType   string `json:"DetailType"`
			Detail       string `json:"Detail"`
			EventBusName string `json:"EventBusName"`
		} `json:"Entries"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	eventsKey := ebEventsKey(ctx.AccountID, ctx.Region)

	var existing []EBEvent
	existingData, _ := p.state.Get(goCtx, eventbridgeNamespace, eventsKey)
	if existingData != nil {
		_ = json.Unmarshal(existingData, &existing)
	}

	now := p.tc.Now().UnixMilli()
	type resultEntry struct {
		EventID      string `json:"EventId,omitempty"`
		ErrorCode    string `json:"ErrorCode,omitempty"`
		ErrorMessage string `json:"ErrorMessage,omitempty"`
	}
	results := make([]resultEntry, 0, len(body.Entries))

	for _, entry := range body.Entries {
		if entry.Source == "" || entry.DetailType == "" || entry.Detail == "" {
			results = append(results, resultEntry{
				ErrorCode:    "InvalidEventPatternException",
				ErrorMessage: "Source, DetailType, and Detail are required",
			})
			continue
		}
		busName := entry.EventBusName
		if busName == "" {
			busName = "default"
		}
		ev := EBEvent{
			Source:       entry.Source,
			DetailType:   entry.DetailType,
			Detail:       entry.Detail,
			EventBusName: busName,
			Time:         now,
			EventID:      generateLambdaRevisionID(),
		}
		existing = append(existing, ev)
		results = append(results, resultEntry{EventID: ev.EventID})
	}

	// Keep only the last maxEBEventRingBuffer events.
	if len(existing) > maxEBEventRingBuffer {
		existing = existing[len(existing)-maxEBEventRingBuffer:]
	}

	data, err := json.Marshal(existing)
	if err != nil {
		return nil, fmt.Errorf("eventbridge putEvents marshal: %w", err)
	}
	if err := p.state.Put(goCtx, eventbridgeNamespace, eventsKey, data); err != nil {
		return nil, fmt.Errorf("eventbridge putEvents state.Put: %w", err)
	}

	failedCount := 0
	for _, r := range results {
		if r.ErrorCode != "" {
			failedCount++
		}
	}

	type response struct {
		FailedEntryCount int           `json:"FailedEntryCount"`
		Entries          []resultEntry `json:"Entries"`
	}
	return ebJSONResponse(http.StatusOK, response{FailedEntryCount: failedCount, Entries: results})
}

func (p *EventBridgePlugin) listEventBuses(_ *RequestContext) (*AWSResponse, error) {
	type eventBus struct {
		Name   string `json:"Name"`
		ARN    string `json:"Arn"`
		Policy string `json:"Policy,omitempty"`
	}
	type response struct {
		EventBuses []eventBus `json:"EventBuses"`
	}
	return ebJSONResponse(http.StatusOK, response{
		EventBuses: []eventBus{
			{Name: "default", ARN: "arn:aws:events:us-east-1:000000000000:event-bus/default"},
		},
	})
}

// --- State key helpers -------------------------------------------------------

func ebRuleKey(accountID, region, name string) string {
	return "ebrule:" + accountID + "/" + region + "/" + name
}

func ebRuleNamesKey(accountID, region string) string {
	return "ebrule_names:" + accountID + "/" + region
}

func ebTargetsKey(accountID, region, ruleName string) string {
	return "ebtargets:" + accountID + "/" + region + "/" + ruleName
}

func ebEventsKey(accountID, region string) string {
	return "ebevents:" + accountID + "/" + region
}

// --- Response helper ---------------------------------------------------------

func ebJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("ebJSONResponse marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
