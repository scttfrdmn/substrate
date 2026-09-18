package emulator

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

// schedulerScheduleInput is a CreateSchedule or UpdateSchedule request body.
//
// It exists because the handlers decoded a caller's body straight into the storage types above, whose
// JSON tags are substrate's own snake_case: `{"Target":{"RoleArn":…}}` does not match `role_arn` under
// Go's case-insensitive field matching, so `Target.RoleArn` and `Target.RetryPolicy` were **silently
// dropped**, as was `FlexibleTimeWindow.MaximumWindowInMinutes` (`maximum_window_in_minutes`). Only
// `Target.Arn` and `FlexibleTimeWindow.Mode` happened to match. That is #1013's rule read backwards: a
// request is decoded through a wire struct, never through the record it will be stored in.
//
// Target and FlexibleTimeWindow are pointers so an absent object is distinguishable from an empty one,
// which is what makes their `Required: Yes` assertable at all.
type schedulerScheduleInput struct {
	GroupName                  string                            `json:"GroupName"`
	ScheduleExpression         string                            `json:"ScheduleExpression"`
	ScheduleExpressionTimezone string                            `json:"ScheduleExpressionTimezone"`
	State                      string                            `json:"State"`
	Target                     *schedulerTargetInput             `json:"Target"`
	FlexibleTimeWindow         *schedulerFlexibleTimeWindowInput `json:"FlexibleTimeWindow"`
	Description                string                            `json:"Description"`
	ClientToken                string                            `json:"ClientToken"`
}

// schedulerTargetInput is the `Target` object as AWS publishes it.
//
// The templated-target objects (EcsParameters, EventBridgeParameters, KinesisParameters,
// SageMakerPipelineParameters, SqsParameters, DeadLetterConfig) are published and unmodelled, so they
// are not decoded and not reported — #1013's honest-empty rule rather than a silent round trip.
type schedulerTargetInput struct {
	Arn         string                     `json:"Arn"`
	RoleArn     string                     `json:"RoleArn"`
	Input       string                     `json:"Input"`
	RetryPolicy *schedulerRetryPolicyInput `json:"RetryPolicy"`
}

// schedulerRetryPolicyInput is the `RetryPolicy` object as AWS publishes it.
type schedulerRetryPolicyInput struct {
	MaximumEventAgeInSeconds int32 `json:"MaximumEventAgeInSeconds"`
	MaximumRetryAttempts     int32 `json:"MaximumRetryAttempts"`
}

// schedulerFlexibleTimeWindowInput is the `FlexibleTimeWindow` object as AWS publishes it.
//
// MaximumWindowInMinutes is a pointer because its published Valid Range starts at 1: an explicit zero
// is outside the range and refused, where an absent member is accepted.
type schedulerFlexibleTimeWindowInput struct {
	Mode                   string `json:"Mode"`
	MaximumWindowInMinutes *int32 `json:"MaximumWindowInMinutes"`
}

// record folds a decoded target into the stored shape. A nil receiver folds to the zero target, which
// only a caller that bypassed [schedulerValidateTarget] can produce.
func (t *schedulerTargetInput) record() SchedulerTarget {
	if t == nil {
		return SchedulerTarget{}
	}
	out := SchedulerTarget{ARN: t.Arn, RoleARN: t.RoleArn, Input: t.Input}
	if t.RetryPolicy != nil {
		out.RetryPolicy = &SchedulerRetryPolicy{
			MaximumEventAgeInSeconds: t.RetryPolicy.MaximumEventAgeInSeconds,
			MaximumRetryAttempts:     t.RetryPolicy.MaximumRetryAttempts,
		}
	}
	return out
}

// record folds a decoded flexible time window into the stored shape, where an absent maximum is stored
// as zero and rendered under omitempty — the same "unset" the wire uses.
func (w *schedulerFlexibleTimeWindowInput) record() SchedulerFlexibleTimeWindow {
	if w == nil {
		return SchedulerFlexibleTimeWindow{}
	}
	out := SchedulerFlexibleTimeWindow{Mode: w.Mode}
	if w.MaximumWindowInMinutes != nil {
		out.MaximumWindowInMinutes = *w.MaximumWindowInMinutes
	}
	return out
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
	op := parseSchedulerOperation(requestMethod(req), req.Path)
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
		// parseSchedulerOperation answers "" for a path it does not recognize, so there
		// is no operation name to name and the refusal reports the verb and path.
		return nil, unknownRouteError(p.Name(), requestMethod(req), req.Path)
	}
}

// parseSchedulerOperation maps an HTTP method and path to an EventBridge
// Scheduler operation name. Returns "" when the path does not match a known
// Scheduler route.
//
// An empty name is routed to the single-schedule operation rather than folded onto the collection:
// "/schedules/" names a schedule whose name is empty, and `Name` is `Required: Yes`, so the answer is
// the handler's ValidationException and not a list of every schedule in the group. Folding it onto
// ListSchedules is what made four empty-name guards unreachable, and what let a caller who built the
// URL from an empty variable read a full listing as success (#1009). AWS publishes nothing about a
// trailing slash on either route, so this is substrate's reading; it follows [parseEFSOperation],
// which never trimmed, over the trimming routers, because a refusal is recoverable and a wrong
// operation is not.
func parseSchedulerOperation(method, path string) string {
	const schedulesPrefix = "/schedules/"
	const schedulesPath = "/schedules"

	if path == schedulesPath {
		if method == http.MethodGet {
			return "ListSchedules"
		}
		return ""
	}

	if strings.HasPrefix(path, schedulesPrefix) {
		name := strings.TrimPrefix(path, schedulesPrefix)
		// Only match single-segment names (no further slashes). An empty name matches, so the
		// operation's own guard decides it.
		if !strings.Contains(name, "/") {
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

	var body schedulerScheduleInput
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, schedulerInvalidBody()
		}
	}
	// The whole request is checked before any state is read, so the same body is refused whether or
	// not the name is free: the shape of a request does not depend on what is stored.
	if awsErr := schedulerValidateScheduleInput(name, &body); awsErr != nil {
		return nil, awsErr
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
		return nil, schedulerConflict(name, groupName)
	}

	now := p.tc.Now().UTC().Format(time.RFC3339)
	arn := schedARN(ctx.Region, ctx.AccountID, groupName, name)

	rec := SchedulerRecord{
		Name:                       name,
		GroupName:                  groupName,
		ScheduleExpression:         body.ScheduleExpression,
		ScheduleExpressionTimezone: body.ScheduleExpressionTimezone,
		State:                      state,
		Target:                     body.Target.record(),
		FlexibleTimeWindow:         body.FlexibleTimeWindow.record(),
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
	// 200, not 201: `API_CreateSchedule`'s Response Syntax opens "HTTP/1.1 200" and its Response
	// Elements say "the service sends back an HTTP 200 response". Substrate answered 201 because a
	// create conventionally does, which is a convention this page does not follow (#1008).
	return schedulerJSONResponse(http.StatusOK, response{ScheduleArn: arn})
}

func (p *SchedulerPlugin) getSchedule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// Reachable since #1009: "/schedules/" routes here rather than to ListSchedules, so a caller who
	// built the path from an empty variable reads the refusal `Name`'s Required: Yes implies.
	name := schedNameFromPath(req.Path)
	if awsErr := schedulerValidateName("name", name); awsErr != nil {
		return nil, awsErr
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
		return nil, schedulerNotFound(name, groupName)
	}

	var rec SchedulerRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("scheduler getSchedule unmarshal: %w", err)
	}

	return schedulerJSONResponse(http.StatusOK, schedRecordToWire(rec))
}

func (p *SchedulerPlugin) updateSchedule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := schedNameFromPath(req.Path)

	var body schedulerScheduleInput
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, schedulerInvalidBody()
		}
	}
	// `API_UpdateSchedule` publishes the same three Required: Yes members as the create, so the same
	// validator serves both and an update that names only the expression is now refused. What this
	// does not settle is the page's full-replace statement for the *optional* members, which substrate
	// still merges — a separate divergence, split out of #1008 rather than folded in here.
	if awsErr := schedulerValidateScheduleInput(name, &body); awsErr != nil {
		return nil, awsErr
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
		return nil, schedulerNotFound(name, groupName)
	}

	var rec SchedulerRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("scheduler updateSchedule unmarshal: %w", err)
	}

	rec.ScheduleExpression = body.ScheduleExpression
	rec.Target = body.Target.record()
	rec.FlexibleTimeWindow = body.FlexibleTimeWindow.record()
	if body.ScheduleExpressionTimezone != "" {
		rec.ScheduleExpressionTimezone = body.ScheduleExpressionTimezone
	}
	if body.State != "" {
		rec.State = body.State
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
	// Reachable since #1009, as in getSchedule: a DELETE naming no schedule is refused rather than
	// answering the success of a delete that removed nothing.
	name := schedNameFromPath(req.Path)
	if awsErr := schedulerValidateName("name", name); awsErr != nil {
		return nil, awsErr
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
