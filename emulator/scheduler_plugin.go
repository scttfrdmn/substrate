package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
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
	// ActionAfterCompletion is NONE or DELETE, what Scheduler does to the schedule once it has
	// finished invoking its target. Empty when the request named none (#1442).
	ActionAfterCompletion string `json:"action_after_completion,omitempty"`
	// StartDate is the epoch-seconds time after which the schedule may begin invoking its target,
	// as the request sent it; nil when it named none (#1442).
	StartDate *float64 `json:"start_date,omitempty"`
	// EndDate is the epoch-seconds time before which the schedule may invoke its target, as the
	// request sent it; nil when it named none (#1442).
	EndDate *float64 `json:"end_date,omitempty"`
	// KmsKeyArn is the customer managed KMS key the request named. Recorded and reported; no
	// encryption is performed. Empty when the request named none (#1442).
	KmsKeyArn string `json:"kms_key_arn,omitempty"`
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
	// DeadLetterConfig names the SQS queue a failed invocation's dead-letter event is sent to (#1501).
	DeadLetterConfig *SchedulerDeadLetterConfig `json:"dead_letter_config,omitempty"`
	// SqsParameters carries the message group of an SQS FIFO target (#1501).
	SqsParameters *SchedulerSqsParameters `json:"sqs_parameters,omitempty"`
	// EventBridgeParameters carries the detail type and source of an EventBridge target (#1501).
	EventBridgeParameters *SchedulerEventBridgeParameters `json:"eventbridge_parameters,omitempty"`
}

// SchedulerDeadLetterConfig is a target's dead-letter queue.
type SchedulerDeadLetterConfig struct {
	// ARN is the SQS queue's ARN.
	ARN string `json:"arn,omitempty"`
}

// SchedulerSqsParameters is the templated SQS target's parameter set.
type SchedulerSqsParameters struct {
	// MessageGroupID is the FIFO message group ID.
	MessageGroupID string `json:"message_group_id,omitempty"`
}

// SchedulerEventBridgeParameters is the templated EventBridge target's parameter set.
type SchedulerEventBridgeParameters struct {
	// DetailType is the event's detail type.
	DetailType string `json:"detail_type"`
	// Source is the event's source.
	Source string `json:"source"`
}

// SchedulerRetryPolicy configures retry behavior for a schedule target.
type SchedulerRetryPolicy struct {
	// MaximumEventAgeInSeconds is the maximum age in seconds of an event before it is discarded;
	// nil when the request named none, so the default applies (an explicit value is kept as sent).
	MaximumEventAgeInSeconds *int32 `json:"maximum_event_age_in_seconds,omitempty"`
	// MaximumRetryAttempts is the maximum number of retry attempts; nil when the request named
	// none, so the default applies, where an explicit 0 means no retries.
	MaximumRetryAttempts *int32 `json:"maximum_retry_attempts,omitempty"`
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
	ActionAfterCompletion      string                            `json:"ActionAfterCompletion"`
	StartDate                  *float64                          `json:"StartDate"`
	EndDate                    *float64                          `json:"EndDate"`
	KmsKeyArn                  string                            `json:"KmsKeyArn"`
}

// schedulerTargetInput is the `Target` object as AWS publishes it.
//
// DeadLetterConfig, SqsParameters and EventBridgeParameters are decoded and reported, because a
// due schedule acts on them (#1501). The other templated-target objects (EcsParameters,
// EventBridgePutEventsV2Parameters, EventBridgePutRawEventsParameters, KinesisParameters,
// SageMakerPipelineParameters) belong to targets substrate records rather than dispatches, so they
// are not decoded and not reported — #1013's honest-empty rule rather than a silent round trip.
type schedulerTargetInput struct {
	Arn                   string                       `json:"Arn"`
	RoleArn               string                       `json:"RoleArn"`
	Input                 string                       `json:"Input"`
	RetryPolicy           *schedulerRetryPolicyInput   `json:"RetryPolicy"`
	DeadLetterConfig      *schedulerDeadLetterInput    `json:"DeadLetterConfig"`
	SqsParameters         *schedulerSqsParametersInput `json:"SqsParameters"`
	EventBridgeParameters *schedulerEventBridgeInput   `json:"EventBridgeParameters"`
}

// schedulerDeadLetterInput is the `DeadLetterConfig` object as AWS publishes it.
type schedulerDeadLetterInput struct {
	Arn string `json:"Arn"`
}

// schedulerSqsParametersInput is the `SqsParameters` object as AWS publishes it.
type schedulerSqsParametersInput struct {
	MessageGroupID string `json:"MessageGroupId,omitempty"`
}

// schedulerEventBridgeInput is the `EventBridgeParameters` object as AWS publishes it.
type schedulerEventBridgeInput struct {
	DetailType string `json:"DetailType"`
	Source     string `json:"Source"`
}

// schedulerRetryPolicyInput is the `RetryPolicy` object as AWS publishes it. Both members are
// `Required: No`, so they are pointers: an absent member takes the default (see
// scheduler_clock.go), and an explicit 0 retry attempts means none.
type schedulerRetryPolicyInput struct {
	MaximumEventAgeInSeconds *int32 `json:"MaximumEventAgeInSeconds"`
	MaximumRetryAttempts     *int32 `json:"MaximumRetryAttempts"`
}

// schedulerFlexibleTimeWindowInput is the `FlexibleTimeWindow` object as AWS publishes it.
//
// MaximumWindowInMinutes is a pointer because its published Valid Range starts at 1: an explicit zero
// is outside the range and refused, where an absent member is accepted.
type schedulerFlexibleTimeWindowInput struct {
	Mode                   string `json:"Mode"`
	MaximumWindowInMinutes *int32 `json:"MaximumWindowInMinutes"`
}

// schedulerStateEnabled is the value a schedule carries when a request names no State (#1089).
//
// The API Reference publishes no default. The `State` entry is byte-identical on all four pages that
// carry it — `API_UpdateSchedule`, `API_CreateSchedule`, `API_GetSchedule`, `API_ScheduleSummary` —
// and each is three lines with no Default: "Specifies whether the schedule is enabled or disabled.
// Type: String. Valid Values: ENABLED | DISABLED". The CLI reference for `create-schedule --state`
// and CloudFormation's `AWS::Scheduler::Schedule` are equally silent, and `API_GetSchedule` has no
// Examples section, so no published sample shows a rendered value either.
//
// ENABLED is nonetheless documented, in the User Guide rather than the reference: "By default, the
// EventBridge Scheduler enables your schedule" (`scheduler/latest/UserGuide/getting-started.html`).
// That is the citation, and it is the only one — an earlier reading of this defect cited
// `API_Schedule.html`, which does not exist (it answers 302 to the landing page, and `API_Types.html`
// publishes `ScheduleSummary` with no `Schedule` type at all).
const schedulerStateEnabled = "ENABLED"

// applyTo writes every member the operation publishes onto rec, so an omitted optional member reverts
// to its system default instead of keeping whatever a previous call left there (#1089).
//
// `API_UpdateSchedule`'s lede is as explicit as AWS gets: "When you call UpdateSchedule, EventBridge
// Scheduler uses all values, including empty values, specified in the request and overrides the
// existing schedule. This is by design. This means that if you do not set an optional field in your
// request, that field will be set to its system-default value after the update." The User Guide puts
// the same rule in one sentence: "If you do not specify a parameter that you've previously set, it
// defaults to null" (`managing-schedule-state.html`). Substrate guarded each optional member on a
// non-empty value, so an update omitting Description kept the old description and one omitting State
// kept DISABLED where AWS restores the default — a divergence invisible until a caller omits
// something, which is precisely the read-modify-write AWS recommends getting wrong.
//
// Create and update share this, which is the point: the merge drifted in the first place because two
// doors each decided independently what a schedule's configuration is. Only the members that are not
// configuration stay outside — Name, GroupName and ARN are identity, the timestamps are bookkeeping,
// and none of the four is settable by a request body.
//
// `null` is not among State's Valid Values, so an omitted State resolves to [schedulerStateEnabled]
// rather than to the empty string: reporting a value the page does not publish is what #1013 forbids.
// Description and ScheduleExpressionTimezone have no published default, so for them the system
// default *is* absence, which omitempty on the wire already expresses.
func (in *schedulerScheduleInput) applyTo(rec *SchedulerRecord) {
	rec.ScheduleExpression = in.ScheduleExpression
	rec.ScheduleExpressionTimezone = in.ScheduleExpressionTimezone
	rec.State = in.state()
	rec.Target = in.Target.record()
	rec.FlexibleTimeWindow = in.FlexibleTimeWindow.record()
	rec.Description = in.Description
	rec.ClientToken = in.ClientToken
	// The four members #1442 found dropped. Each is assigned unconditionally like the rest, so an
	// update omitting one clears it — the replace rule above, with no published default to revert
	// to: none of the three operation pages states one for ActionAfterCompletion, and the two
	// dates and the key have none by nature.
	rec.ActionAfterCompletion = in.ActionAfterCompletion
	rec.StartDate = in.StartDate
	rec.EndDate = in.EndDate
	rec.KmsKeyArn = in.KmsKeyArn
}

// state resolves the request's State to the value the schedule carries, applying the documented
// default when the member is absent. See [schedulerStateEnabled] for where that default is published.
func (in *schedulerScheduleInput) state() string {
	if in.State == "" {
		return schedulerStateEnabled
	}
	return in.State
}

// record folds a decoded target into the stored shape. A nil receiver folds to the zero target, which
// only a caller that bypassed [schedulerValidateTarget] can produce.
//
// It composes from the receiver alone and never reads the stored record, so a member the request
// omits cannot survive an update — the replace-not-merge rule [schedulerScheduleInput.applyTo]
// states, holding one level down for RoleArn, Input and RetryPolicy without a second mechanism.
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
	if t.DeadLetterConfig != nil {
		out.DeadLetterConfig = &SchedulerDeadLetterConfig{ARN: t.DeadLetterConfig.Arn}
	}
	if t.SqsParameters != nil {
		out.SqsParameters = &SchedulerSqsParameters{MessageGroupID: t.SqsParameters.MessageGroupID}
	}
	if t.EventBridgeParameters != nil {
		out.EventBridgeParameters = &SchedulerEventBridgeParameters{
			DetailType: t.EventBridgeParameters.DetailType,
			Source:     t.EventBridgeParameters.Source,
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

// SchedulerPlugin emulates the Amazon EventBridge Scheduler REST/JSON API: schedules, schedule
// groups and their tags, using path-based HTTP method routing. It is a [ClockDrivenPlugin]: a due
// schedule invokes its target on the simulated clock (see scheduler_clock.go).
type SchedulerPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
	// auth authorizes the requests a due schedule makes as its target's RoleArn; nil authorizes
	// nothing, as for the CloudFormation deployer.
	auth *AuthController
	// mu serializes the clock-driven work with the operations that arm, re-arm or remove a timer,
	// so an occurrence is never fired twice or fired against a configuration being replaced.
	mu sync.Mutex
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
	p.auth, _ = cfg.Options["auth_controller"].(*AuthController)
	return nil
}

// Shutdown is a no-op for SchedulerPlugin.
func (p *SchedulerPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an EventBridge Scheduler REST/JSON request to the
// appropriate handler using path-based operation routing.
func (p *SchedulerPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op := parseSchedulerOperation(requestMethod(req), req.Path)
	switch op {
	case "CreateScheduleGroup":
		return p.createScheduleGroup(ctx, req)
	case "GetScheduleGroup":
		return p.getScheduleGroup(ctx, req)
	case "ListScheduleGroups":
		return p.listScheduleGroups(ctx, req)
	case "DeleteScheduleGroup":
		return p.deleteScheduleGroup(ctx, req)
	case "TagResource":
		return p.tagResource(ctx, req)
	case "UntagResource":
		return p.untagResource(ctx, req)
	case "ListTagsForResource":
		return p.listTagsForResource(ctx, req)
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

	// Schedule groups and tags (#1501). A group name is one segment, as a schedule's is; a tag
	// route's ResourceArn carries "/" once decoded, so everything after /tags/ is the ARN.
	switch {
	case path == "/schedule-groups":
		if method == http.MethodGet {
			return "ListScheduleGroups"
		}
		return ""
	case strings.HasPrefix(path, "/schedule-groups/"):
		if strings.Contains(strings.TrimPrefix(path, "/schedule-groups/"), "/") {
			return ""
		}
		switch method {
		case http.MethodPost:
			return "CreateScheduleGroup"
		case http.MethodGet:
			return "GetScheduleGroup"
		case http.MethodDelete:
			return "DeleteScheduleGroup"
		}
		return ""
	case strings.HasPrefix(path, "/tags/"):
		switch method {
		case http.MethodPost:
			return "TagResource"
		case http.MethodGet:
			return "ListTagsForResource"
		case http.MethodDelete:
			return "UntagResource"
		}
		return ""
	}

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

	goCtx := context.Background()
	if err := p.ensureDefaultGroup(goCtx, ctx.AccountID, ctx.Region); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// `API_CreateSchedule` publishes ResourceNotFoundException, and the group is the one resource
	// the request references that it does not create (#1501).
	group, err := p.loadGroup(goCtx, ctx.AccountID, ctx.Region, groupName)
	if err != nil {
		return nil, err
	}
	if group == nil {
		return nil, schedulerGroupNotFound(groupName)
	}
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

	// Identity and bookkeeping here, configuration through applyTo, which the update shares so the
	// two doors cannot disagree about what a body means.
	rec := SchedulerRecord{
		Name:                 name,
		GroupName:            groupName,
		ARN:                  arn,
		CreationDate:         now,
		LastModificationDate: now,
		AccountID:            ctx.AccountID,
		Region:               ctx.Region,
	}
	body.applyTo(&rec)

	data, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("scheduler createSchedule marshal: %w", err)
	}
	if err := p.state.Put(goCtx, schedulerNamespace, recKey, data); err != nil {
		return nil, fmt.Errorf("scheduler createSchedule state.Put: %w", err)
	}
	if err := updateStringIndex(goCtx, p.state, schedulerNamespace, schedNamesKey(ctx.AccountID, ctx.Region, groupName), name); err != nil {
		return nil, fmt.Errorf("scheduler createSchedule index: %w", err)
	}
	if err := p.armTimer(goCtx, &rec); err != nil {
		return nil, fmt.Errorf("scheduler createSchedule timer: %w", err)
	}

	type response struct {
		ScheduleArn string `json:"ScheduleArn"`
	}
	// 200, not 201: `API_CreateSchedule`'s Response Syntax opens "HTTP/1.1 200" and its Response
	// Elements say "the service sends back an HTTP 200 response". Substrate answered 201 because a
	// create conventionally does, which is a convention this page does not follow (#1008).
	return schedulerJSONResponse(http.StatusOK, response{ScheduleArn: arn})
}

func (p *SchedulerPlugin) getSchedule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// Reachable: "/schedules/" routes here rather than to ListSchedules, so a caller who built the
	// path from an empty variable reads the refusal `Name`'s Required: Yes implies (#1009).
	name := schedNameFromPath(req.Path)
	if awsErr := schedulerValidateName("name", name); awsErr != nil {
		return nil, awsErr
	}

	// lowerCamel here, and PascalCase at listSchedules, because the two pages publish different query
	// strings — see scheduler_query_keys.go (#1226).
	groupName := req.Params[schedGroupNameKey]
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
	// validator serves both and an update that names only the expression is refused (#1008). The
	// page's full-replace statement for the *optional* members holds too (#1089): applyTo below
	// assigns every one of them, so an omitted member reverts rather than persisting.
	if awsErr := schedulerValidateScheduleInput(name, &body); awsErr != nil {
		return nil, awsErr
	}

	groupName := body.GroupName
	if groupName == "" {
		groupName = "default"
	}

	p.mu.Lock()
	defer p.mu.Unlock()
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

	body.applyTo(&rec)
	rec.LastModificationDate = p.tc.Now().UTC().Format(time.RFC3339)

	updated, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("scheduler updateSchedule marshal: %w", err)
	}
	if err := p.state.Put(goCtx, schedulerNamespace, recKey, updated); err != nil {
		return nil, fmt.Errorf("scheduler updateSchedule state.Put: %w", err)
	}
	if err := p.armTimer(goCtx, &rec); err != nil {
		return nil, fmt.Errorf("scheduler updateSchedule timer: %w", err)
	}

	type response struct {
		ScheduleArn string `json:"ScheduleArn"`
	}
	return schedulerJSONResponse(http.StatusOK, response{ScheduleArn: rec.ARN})
}

func (p *SchedulerPlugin) deleteSchedule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// Reachable, as in getSchedule (#1009): a DELETE naming no schedule is refused rather than
	// answering the success of a delete that removed nothing.
	name := schedNameFromPath(req.Path)
	if awsErr := schedulerValidateName("name", name); awsErr != nil {
		return nil, awsErr
	}

	// lowerCamel, like getSchedule and unlike listSchedules, because this page publishes it that way
	// (#1226).
	groupName := req.Params[schedGroupNameKey]
	if groupName == "" {
		groupName = "default"
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	goCtx := context.Background()
	// `API_DeleteSchedule` publishes ResourceNotFoundException/404 for a delete of a schedule that
	// does not exist (#1501).
	existing, err := p.state.Get(goCtx, schedulerNamespace, schedKey(ctx.AccountID, ctx.Region, groupName, name))
	if err != nil {
		return nil, fmt.Errorf("scheduler deleteSchedule state.Get: %w", err)
	}
	if existing == nil {
		return nil, schedulerNotFound(name, groupName)
	}
	if err := p.removeSchedule(goCtx, ctx.AccountID, ctx.Region, groupName, name); err != nil {
		return nil, fmt.Errorf("scheduler deleteSchedule: %w", err)
	}

	return &AWSResponse{StatusCode: http.StatusOK, Headers: map[string]string{"Content-Type": "application/json"}, Body: []byte("{}")}, nil
}

func (p *SchedulerPlugin) listSchedules(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// PascalCase, and ScheduleGroup rather than groupName, because this page publishes its query
	// string that way where the single-schedule pages do not — see scheduler_query_keys.go (#1226).
	//
	// GroupName is a filter: "If specified, only lists the schedules whose associated schedule group
	// matches the given filter." Absent, every group's schedules are listed, ordered by group and
	// then by name (#1501). A group that does not exist matches nothing, so it lists nothing.
	groupName := req.Params[schedListGroupKey]
	namePrefix := req.Params[schedListNamePrefixKey]
	stateFilter := req.Params[schedListStateKey]

	// Above the index load, so the refusal does not depend on what the store holds — see
	// scheduler_pagination.go for the code's provenance and the ordering argument (#1086).
	offset, tokenOK := decodeOffsetPaginationToken(req.Params[schedListNextTokenKey])
	if !tokenOK {
		return nil, schedInvalidPaginationToken()
	}

	maxResults := schedListDefaultMaxResults
	if mr := req.Params[schedListMaxResultsKey]; mr != "" {
		if n, err := strconv.Atoi(mr); err == nil && n > 0 {
			maxResults = n
			if maxResults > schedListMaxMaxResults {
				maxResults = schedListMaxMaxResults
			}
		}
	}

	goCtx := context.Background()
	groups := []string{groupName}
	if groupName == "" {
		// The default group is materialized first, so a default-group schedule stored before
		// groups were indexed (#1501) still lists: the group index is what this listing walks.
		if err := p.ensureDefaultGroup(goCtx, ctx.AccountID, ctx.Region); err != nil {
			return nil, err
		}
		var err error
		if groups, err = p.groupNames(goCtx, ctx.AccountID, ctx.Region); err != nil {
			return nil, fmt.Errorf("scheduler listSchedules groups: %w", err)
		}
	}
	type groupedName struct{ group, name string }
	var names []groupedName
	for _, g := range groups {
		groupNames, err := loadStringIndex(goCtx, p.state, schedulerNamespace, schedNamesKey(ctx.AccountID, ctx.Region, g))
		if err != nil {
			return nil, fmt.Errorf("scheduler listSchedules loadIndex: %w", err)
		}
		for _, n := range groupNames {
			if namePrefix == "" || strings.HasPrefix(n, namePrefix) {
				names = append(names, groupedName{group: g, name: n})
			}
		}
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

	// Every published filter is applied before the cut, which is why the records are loaded for the
	// whole group rather than for one page: State lives in the record and not in the name index, so a
	// state-filtered listing cannot be assembled from names alone — see scheduler_list_filters.go
	// (#1229).
	matching := make([]schedSummary, 0, len(names))
	for _, n := range names {
		data, getErr := p.state.Get(goCtx, schedulerNamespace, schedKey(ctx.AccountID, ctx.Region, n.group, n.name))
		if getErr != nil || data == nil {
			// A name the index holds with no record behind it, and a record that will not decode, are
			// both store inconsistencies rather than filters. They still shorten the *listing*, but no
			// longer a page: the cut is below, so a page is short only when the listing has run out.
			// See scheduler_list_filters.go for why skipping rather than refusing is the reading
			// (#1229).
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
		matching = append(matching, schedSummary{
			Arn:                  rec.ARN,
			Name:                 rec.Name,
			GroupName:            rec.GroupName,
			State:                rec.State,
			CreationDate:         float64(ct.Unix()),
			LastModificationDate: float64(mt.Unix()),
			Target:               targetSummary{Arn: rec.Target.ARN},
		})
	}

	// The index is ASCII-sorted by schedule name, which is stable across calls — what
	// [pageByOffsetToken]'s offset relies on. Both filters above preserve that order, so the offset
	// counts the schedules this request can see rather than every schedule in the group. A
	// past-the-end offset clamps to a final empty page rather than being refused, because a token
	// substrate issued over a listing that has since shrunk is still a token it issued.
	schedules, nextToken := pageByOffsetToken(matching, offset, maxResults)

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
//
// `ClientToken` is not rendered: it is not one of `API_GetSchedule`'s fifteen published response
// elements, but a request-only idempotency token, published on the create and the update and on no
// read. Because an omitted member reverts instead of persisting, rendering it would make an
// unpublished field change under callers who never named it (#1089). The record keeps it as
// recorded intent.
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
	ActionAfterCompletion      string                      `json:"ActionAfterCompletion,omitempty"`
	StartDate                  *float64                    `json:"StartDate,omitempty"`
	EndDate                    *float64                    `json:"EndDate,omitempty"`
	KmsKeyArn                  string                      `json:"KmsKeyArn,omitempty"`
}

// schedWireTarget is the wire-format representation of a schedule target.
type schedWireTarget struct {
	Arn                   string                       `json:"Arn"`
	RoleArn               string                       `json:"RoleArn"`
	Input                 string                       `json:"Input,omitempty"`
	RetryPolicy           *schedWireRetryPolicy        `json:"RetryPolicy,omitempty"`
	DeadLetterConfig      *schedulerDeadLetterInput    `json:"DeadLetterConfig,omitempty"`
	SqsParameters         *schedulerSqsParametersInput `json:"SqsParameters,omitempty"`
	EventBridgeParameters *schedulerEventBridgeInput   `json:"EventBridgeParameters,omitempty"`
}

// schedWireRetryPolicy is the wire-format representation of a retry policy.
type schedWireRetryPolicy struct {
	MaximumEventAgeInSeconds *int32 `json:"MaximumEventAgeInSeconds,omitempty"`
	MaximumRetryAttempts     *int32 `json:"MaximumRetryAttempts,omitempty"`
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
		ActionAfterCompletion:      rec.ActionAfterCompletion,
		StartDate:                  rec.StartDate,
		EndDate:                    rec.EndDate,
		KmsKeyArn:                  rec.KmsKeyArn,
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
	if d := rec.Target.DeadLetterConfig; d != nil {
		w.Target.DeadLetterConfig = &schedulerDeadLetterInput{Arn: d.ARN}
	}
	if s := rec.Target.SqsParameters; s != nil {
		w.Target.SqsParameters = &schedulerSqsParametersInput{MessageGroupID: s.MessageGroupID}
	}
	if e := rec.Target.EventBridgeParameters; e != nil {
		w.Target.EventBridgeParameters = &schedulerEventBridgeInput{DetailType: e.DetailType, Source: e.Source}
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
