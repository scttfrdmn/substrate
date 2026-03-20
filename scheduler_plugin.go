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

// schedulerNamespace is the state namespace used by SchedulerPlugin.
const schedulerNamespace = "scheduler"

// SchedulerRecord represents a stored EventBridge Scheduler schedule.
type SchedulerRecord struct {
	// Name is the name of the schedule.
	Name string `json:"name"`
	// GroupName is the schedule group the schedule belongs to.
	GroupName string `json:"group_name"`
	// ScheduleExpression is the schedule expression (rate, cron, or one-time).
	ScheduleExpression string `json:"schedule_expression"`
	// ScheduleExpressionTimezone is the timezone for the schedule expression.
	ScheduleExpressionTimezone string `json:"schedule_expression_timezone"`
	// State is the schedule state: ENABLED or DISABLED.
	State string `json:"state"`
	// Target is the target configuration for the schedule.
	Target SchedulerTarget `json:"target"`
	// FlexibleTimeWindow controls how the scheduler handles flexible timing.
	FlexibleTimeWindow SchedulerFlexibleTimeWindow `json:"flexible_time_window"`
	// Description is a human-readable description of the schedule.
	Description string `json:"description"`
	// ARN is the Amazon Resource Name of the schedule.
	ARN string `json:"arn"`
	// CreationDate is the ISO-8601 timestamp when the schedule was created.
	CreationDate string `json:"creation_date"`
	// LastModificationDate is the ISO-8601 timestamp of the last update.
	LastModificationDate string `json:"last_modification_date"`
	// ClientToken is the idempotency token for the create operation.
	ClientToken string `json:"client_token,omitempty"`
	// AccountID is the AWS account ID that owns this schedule.
	AccountID string `json:"account_id"`
	// Region is the AWS region where this schedule was created.
	Region string `json:"region"`
}

// SchedulerTarget holds the target configuration for a schedule.
type SchedulerTarget struct {
	// ARN is the Amazon Resource Name of the target resource.
	ARN string `json:"arn"`
	// RoleARN is the ARN of the IAM role used to invoke the target.
	RoleARN string `json:"role_arn"`
	// Input is the JSON text that is passed to the target when the schedule runs.
	Input string `json:"input,omitempty"`
	// RetryPolicy configures retries on invocation failure.
	RetryPolicy *SchedulerRetryPolicy `json:"retry_policy,omitempty"`
}

// SchedulerRetryPolicy configures retry behavior for a schedule target.
type SchedulerRetryPolicy struct {
	// MaximumEventAgeInSeconds is the maximum age in seconds of an event
	// before it is discarded.
	MaximumEventAgeInSeconds int32 `json:"maximum_event_age_in_seconds"`
	// MaximumRetryAttempts is the maximum number of retry attempts.
	MaximumRetryAttempts int32 `json:"maximum_retry_attempts"`
}

// SchedulerFlexibleTimeWindow configures the flexible time window for a schedule.
type SchedulerFlexibleTimeWindow struct {
	// Mode is the flexible time window mode: OFF or FLEXIBLE.
	Mode string `json:"mode"`
	// MaximumWindowInMinutes is the maximum time window in minutes when Mode is FLEXIBLE.
	MaximumWindowInMinutes int32 `json:"maximum_window_in_minutes,omitempty"`
}

// SchedulerPlugin emulates the Amazon EventBridge Scheduler REST/JSON API.
// It handles CreateSchedule, GetSchedule, UpdateSchedule, DeleteSchedule, and
// ListSchedules using path-based HTTP method routing.
type SchedulerPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "scheduler".
func (p *SchedulerPlugin) Name() string { return schedulerNamespace }

// Initialize sets up the SchedulerPlugin with the provided configuration.
func (p *SchedulerPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for SchedulerPlugin.
func (p *SchedulerPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an EventBridge Scheduler REST/JSON request to the
// appropriate handler using path-based operation routing.
func (p *SchedulerPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op := parseSchedulerOperation(req.Operation, req.Path)
	switch op {
	case "CreateSchedule":
		return p.createSchedule(ctx, req)
	case "GetSchedule":
		return p.getSchedule(ctx, req)
	case "UpdateSchedule":
		return p.updateSchedule(ctx, req)
	case "DeleteSchedule":
		return p.deleteSchedule(ctx, req)
	case "ListSchedules":
		return p.listSchedules(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    fmt.Sprintf("SchedulerPlugin: unknown operation %q", op),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// parseSchedulerOperation maps an HTTP method and path to an EventBridge
// Scheduler operation name. Returns "" when the path does not match a known
// Scheduler route.
func parseSchedulerOperation(method, path string) string {
	const schedulesPrefix = "/schedules/"
	const schedulesPath = "/schedules"

	if path == schedulesPath || path == schedulesPath+"/" {
		if method == http.MethodGet {
			return "ListSchedules"
		}
		return ""
	}

	if strings.HasPrefix(path, schedulesPrefix) {
		name := strings.TrimPrefix(path, schedulesPrefix)
		// Only match single-segment names (no further slashes).
		if name != "" && !strings.Contains(name, "/") {
			switch method {
			case http.MethodPost:
				return "CreateSchedule"
			case http.MethodPut:
				return "UpdateSchedule"
			case http.MethodGet:
				return "GetSchedule"
			case http.MethodDelete:
				return "DeleteSchedule"
			}
		}
	}

	return ""
}

// schedNameFromPath extracts the schedule name from a path of the form
// /schedules/{name}.
func schedNameFromPath(path string) string {
	const prefix = "/schedules/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	return strings.TrimPrefix(path, prefix)
}

// --- Operations --------------------------------------------------------------

func (p *SchedulerPlugin) createSchedule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := schedNameFromPath(req.Path)
	if name == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}

	var body struct {
		GroupName                  string                      `json:"GroupName"`
		ScheduleExpression         string                      `json:"ScheduleExpression"`
		ScheduleExpressionTimezone string                      `json:"ScheduleExpressionTimezone"`
		State                      string                      `json:"State"`
		Target                     SchedulerTarget             `json:"Target"`
		FlexibleTimeWindow         SchedulerFlexibleTimeWindow `json:"FlexibleTimeWindow"`
		Description                string                      `json:"Description"`
		ClientToken                string                      `json:"ClientToken"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, &AWSError{Code: "ValidationException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
		}
	}

	groupName := body.GroupName
	if groupName == "" {
		groupName = "default"
	}
	state := body.State
	if state == "" {
		state = "ENABLED"
	}

	goCtx := context.Background()
	recKey := schedKey(ctx.AccountID, ctx.Region, groupName, name)
	existing, err := p.state.Get(goCtx, schedulerNamespace, recKey)
	if err != nil {
		return nil, fmt.Errorf("scheduler createSchedule state.Get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{
			Code:       "ConflictException",
			Message:    fmt.Sprintf("Schedule %s already exists in group %s", name, groupName),
			HTTPStatus: http.StatusConflict,
		}
	}

	now := p.tc.Now().UTC().Format(time.RFC3339)
	arn := schedARN(ctx.Region, ctx.AccountID, groupName, name)

	rec := SchedulerRecord{
		Name:                       name,
		GroupName:                  groupName,
		ScheduleExpression:         body.ScheduleExpression,
		ScheduleExpressionTimezone: body.ScheduleExpressionTimezone,
		State:                      state,
		Target:                     body.Target,
		FlexibleTimeWindow:         body.FlexibleTimeWindow,
		Description:                body.Description,
		ARN:                        arn,
		CreationDate:               now,
		LastModificationDate:       now,
		ClientToken:                body.ClientToken,
		AccountID:                  ctx.AccountID,
		Region:                     ctx.Region,
	}

	data, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("scheduler createSchedule marshal: %w", err)
	}
	if err := p.state.Put(goCtx, schedulerNamespace, recKey, data); err != nil {
		return nil, fmt.Errorf("scheduler createSchedule state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, schedulerNamespace, schedNamesKey(ctx.AccountID, ctx.Region, groupName), name)

	type response struct {
		ScheduleArn string `json:"ScheduleArn"`
	}
	return schedulerJSONResponse(http.StatusCreated, response{ScheduleArn: arn})
}

func (p *SchedulerPlugin) getSchedule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := schedNameFromPath(req.Path)
	if name == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}

	groupName := req.Params["groupName"]
	if groupName == "" {
		groupName = "default"
	}

	goCtx := context.Background()
	data, err := p.state.Get(goCtx, schedulerNamespace, schedKey(ctx.AccountID, ctx.Region, groupName, name))
	if err != nil {
		return nil, fmt.Errorf("scheduler getSchedule state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{
			Code:       "ResourceNotFoundException",
			Message:    fmt.Sprintf("Schedule %s not found in group %s", name, groupName),
			HTTPStatus: http.StatusNotFound,
		}
	}

	var rec SchedulerRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("scheduler getSchedule unmarshal: %w", err)
	}

	return schedulerJSONResponse(http.StatusOK, schedRecordToWire(rec))
}

func (p *SchedulerPlugin) updateSchedule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := schedNameFromPath(req.Path)
	if name == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}

	var body struct {
		GroupName                  string                       `json:"GroupName"`
		ScheduleExpression         string                       `json:"ScheduleExpression"`
		ScheduleExpressionTimezone string                       `json:"ScheduleExpressionTimezone"`
		State                      string                       `json:"State"`
		Target                     *SchedulerTarget             `json:"Target"`
		FlexibleTimeWindow         *SchedulerFlexibleTimeWindow `json:"FlexibleTimeWindow"`
		Description                string                       `json:"Description"`
		ClientToken                string                       `json:"ClientToken"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, &AWSError{Code: "ValidationException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
		}
	}

	groupName := body.GroupName
	if groupName == "" {
		groupName = "default"
	}

	goCtx := context.Background()
	recKey := schedKey(ctx.AccountID, ctx.Region, groupName, name)
	data, err := p.state.Get(goCtx, schedulerNamespace, recKey)
	if err != nil {
		return nil, fmt.Errorf("scheduler updateSchedule state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{
			Code:       "ResourceNotFoundException",
			Message:    fmt.Sprintf("Schedule %s not found in group %s", name, groupName),
			HTTPStatus: http.StatusNotFound,
		}
	}

	var rec SchedulerRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("scheduler updateSchedule unmarshal: %w", err)
	}

	if body.ScheduleExpression != "" {
		rec.ScheduleExpression = body.ScheduleExpression
	}
	if body.ScheduleExpressionTimezone != "" {
		rec.ScheduleExpressionTimezone = body.ScheduleExpressionTimezone
	}
	if body.State != "" {
		rec.State = body.State
	}
	if body.Target != nil {
		rec.Target = *body.Target
	}
	if body.FlexibleTimeWindow != nil {
		rec.FlexibleTimeWindow = *body.FlexibleTimeWindow
	}
	if body.Description != "" {
		rec.Description = body.Description
	}
	rec.LastModificationDate = p.tc.Now().UTC().Format(time.RFC3339)

	updated, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("scheduler updateSchedule marshal: %w", err)
	}
	if err := p.state.Put(goCtx, schedulerNamespace, recKey, updated); err != nil {
		return nil, fmt.Errorf("scheduler updateSchedule state.Put: %w", err)
	}

	type response struct {
		ScheduleArn string `json:"ScheduleArn"`
	}
	return schedulerJSONResponse(http.StatusOK, response{ScheduleArn: rec.ARN})
}

func (p *SchedulerPlugin) deleteSchedule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := schedNameFromPath(req.Path)
	if name == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}

	groupName := req.Params["groupName"]
	if groupName == "" {
		groupName = "default"
	}

	goCtx := context.Background()
	recKey := schedKey(ctx.AccountID, ctx.Region, groupName, name)
	if err := p.state.Delete(goCtx, schedulerNamespace, recKey); err != nil {
		return nil, fmt.Errorf("scheduler deleteSchedule state.Delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, schedulerNamespace, schedNamesKey(ctx.AccountID, ctx.Region, groupName), name)

	return &AWSResponse{StatusCode: http.StatusOK, Headers: map[string]string{"Content-Type": "application/json"}, Body: []byte("{}")}, nil
}

func (p *SchedulerPlugin) listSchedules(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	groupName := req.Params["groupName"]
	if groupName == "" {
		groupName = "default"
	}
	namePrefix := req.Params["namePrefix"]
	stateFilter := req.Params["state"]
	nextTokenParam := req.Params["nextToken"]

	maxResults := 20
	if mr := req.Params["maxResults"]; mr != "" {
		if n, err := strconv.Atoi(mr); err == nil && n > 0 {
			maxResults = n
			if maxResults > 100 {
				maxResults = 100
			}
		}
	}

	goCtx := context.Background()
	allNames, err := loadStringIndex(goCtx, p.state, schedulerNamespace, schedNamesKey(ctx.AccountID, ctx.Region, groupName))
	if err != nil {
		return nil, fmt.Errorf("scheduler listSchedules loadIndex: %w", err)
	}

	// Apply namePrefix filter.
	names := allNames
	if namePrefix != "" {
		filtered := make([]string, 0, len(allNames))
		for _, n := range allNames {
			if strings.HasPrefix(n, namePrefix) {
				filtered = append(filtered, n)
			}
		}
		names = filtered
	}

	// Parse offset from nextToken.
	offset := 0
	if nextTokenParam != "" {
		if decoded, decErr := base64.StdEncoding.DecodeString(nextTokenParam); decErr == nil {
			if n, atoiErr := strconv.Atoi(string(decoded)); atoiErr == nil && n > 0 {
				offset = n
			}
		}
	}
	if offset > len(names) {
		offset = len(names)
	}

	end := offset + maxResults
	var nextToken string
	if end < len(names) {
		nextToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	} else {
		end = len(names)
	}

	type targetSummary struct {
		Arn string `json:"Arn"`
	}
	// CreationDate and LastModificationDate are Unix epoch seconds (float64).
	type schedSummary struct {
		Arn                  string        `json:"Arn"`
		Name                 string        `json:"Name"`
		GroupName            string        `json:"GroupName"`
		State                string        `json:"State"`
		CreationDate         float64       `json:"CreationDate"`
		LastModificationDate float64       `json:"LastModificationDate"`
		Target               targetSummary `json:"Target"`
	}

	schedules := make([]schedSummary, 0, end-offset)
	for _, n := range names[offset:end] {
		data, getErr := p.state.Get(goCtx, schedulerNamespace, schedKey(ctx.AccountID, ctx.Region, groupName, n))
		if getErr != nil || data == nil {
			continue
		}
		var rec SchedulerRecord
		if unmarshalErr := json.Unmarshal(data, &rec); unmarshalErr != nil {
			continue
		}
		if stateFilter != "" && rec.State != stateFilter {
			continue
		}
		ct, _ := time.Parse(time.RFC3339, rec.CreationDate)
		mt, _ := time.Parse(time.RFC3339, rec.LastModificationDate)
		schedules = append(schedules, schedSummary{
			Arn:                  rec.ARN,
			Name:                 rec.Name,
			GroupName:            rec.GroupName,
			State:                rec.State,
			CreationDate:         float64(ct.Unix()),
			LastModificationDate: float64(mt.Unix()),
			Target:               targetSummary{Arn: rec.Target.ARN},
		})
	}

	type response struct {
		Schedules []schedSummary `json:"Schedules"`
		NextToken string         `json:"NextToken,omitempty"`
	}
	return schedulerJSONResponse(http.StatusOK, response{Schedules: schedules, NextToken: nextToken})
}

// --- Wire format -------------------------------------------------------------

// schedWireRecord is the full wire-format representation of a schedule as
// returned by GetSchedule. CreationDate and LastModificationDate are Unix epoch
// seconds (float64) as required by the AWS SDK.
type schedWireRecord struct {
	Arn                        string                      `json:"Arn"`
	Name                       string                      `json:"Name"`
	GroupName                  string                      `json:"GroupName"`
	ScheduleExpression         string                      `json:"ScheduleExpression"`
	ScheduleExpressionTimezone string                      `json:"ScheduleExpressionTimezone,omitempty"`
	State                      string                      `json:"State"`
	Target                     schedWireTarget             `json:"Target"`
	FlexibleTimeWindow         schedWireFlexibleTimeWindow `json:"FlexibleTimeWindow"`
	Description                string                      `json:"Description,omitempty"`
	CreationDate               float64                     `json:"CreationDate"`
	LastModificationDate       float64                     `json:"LastModificationDate"`
	ClientToken                string                      `json:"ClientToken,omitempty"`
}

// schedWireTarget is the wire-format representation of a schedule target.
type schedWireTarget struct {
	Arn         string                `json:"Arn"`
	RoleArn     string                `json:"RoleArn"`
	Input       string                `json:"Input,omitempty"`
	RetryPolicy *schedWireRetryPolicy `json:"RetryPolicy,omitempty"`
}

// schedWireRetryPolicy is the wire-format representation of a retry policy.
type schedWireRetryPolicy struct {
	MaximumEventAgeInSeconds int32 `json:"MaximumEventAgeInSeconds"`
	MaximumRetryAttempts     int32 `json:"MaximumRetryAttempts"`
}

// schedWireFlexibleTimeWindow is the wire-format flexible time window.
type schedWireFlexibleTimeWindow struct {
	Mode                   string `json:"Mode"`
	MaximumWindowInMinutes int32  `json:"MaximumWindowInMinutes,omitempty"`
}

// schedRecordToWire converts a SchedulerRecord to the AWS wire format for GetSchedule.
// Timestamps are converted from RFC3339 strings to Unix epoch seconds (float64).
func schedRecordToWire(rec SchedulerRecord) schedWireRecord {
	ct, _ := time.Parse(time.RFC3339, rec.CreationDate)
	mt, _ := time.Parse(time.RFC3339, rec.LastModificationDate)
	w := schedWireRecord{
		Arn:                        rec.ARN,
		Name:                       rec.Name,
		GroupName:                  rec.GroupName,
		ScheduleExpression:         rec.ScheduleExpression,
		ScheduleExpressionTimezone: rec.ScheduleExpressionTimezone,
		State:                      rec.State,
		Description:                rec.Description,
		CreationDate:               float64(ct.Unix()),
		LastModificationDate:       float64(mt.Unix()),
		ClientToken:                rec.ClientToken,
		Target: schedWireTarget{
			Arn:     rec.Target.ARN,
			RoleArn: rec.Target.RoleARN,
			Input:   rec.Target.Input,
		},
		FlexibleTimeWindow: schedWireFlexibleTimeWindow{
			Mode:                   rec.FlexibleTimeWindow.Mode,
			MaximumWindowInMinutes: rec.FlexibleTimeWindow.MaximumWindowInMinutes,
		},
	}
	if rec.Target.RetryPolicy != nil {
		w.Target.RetryPolicy = &schedWireRetryPolicy{
			MaximumEventAgeInSeconds: rec.Target.RetryPolicy.MaximumEventAgeInSeconds,
			MaximumRetryAttempts:     rec.Target.RetryPolicy.MaximumRetryAttempts,
		}
	}
	return w
}

// --- State key helpers -------------------------------------------------------

// schedKey returns the state key for a single schedule record.
func schedKey(accountID, region, groupName, name string) string {
	return "sched:" + accountID + "/" + region + "/" + groupName + "/" + name
}

// schedNamesKey returns the state key for the name index of schedules in a group.
func schedNamesKey(accountID, region, groupName string) string {
	return "sched_names:" + accountID + "/" + region + "/" + groupName
}

// schedARN returns the ARN for a schedule.
func schedARN(region, accountID, groupName, name string) string {
	return fmt.Sprintf("arn:aws:scheduler:%s:%s:schedule/%s/%s", region, accountID, groupName, name)
}

// --- Response helper ---------------------------------------------------------

// schedulerJSONResponse serializes v to JSON and returns an AWSResponse with
// the given HTTP status code.
func schedulerJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("schedulerJSONResponse marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
