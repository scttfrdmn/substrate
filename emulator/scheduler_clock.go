package emulator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// A schedule invokes its target on the simulated clock (#1501), and a finished schedule with
// ActionAfterCompletion DELETE deletes itself (#1475). See clock_driven.go for the model this
// implements; Lambda's event-source mappings (lambda_esm_clock.go) are the other user.
//
// # The timer
//
// Every schedule has a timer beside its record, at sched_timer:{account}/{region}/{group}/{name}:
// when its next occurrence is due, how many it has fired, and the invocations still being retried.
// CreateSchedule and UpdateSchedule arm it, DeleteSchedule and DeleteScheduleGroup remove it. Before
// every top-level request, [SchedulerPlugin.RunDue] reads each timer and does what is due at the
// clock's current reading, so a frozen clock makes nothing due and a replay, which pins the clock
// to each recorded event, makes the same occurrences due at the same request.
//
// An UpdateSchedule re-arms the timer from the update, as a fresh configuration: "EventBridge
// Scheduler uses all values ... specified in the request and overrides the existing schedule". The
// one thing it keeps is a one-time schedule's firing, so an update that leaves its at() expression
// unchanged does not invoke it twice.
//
// # A State of DISABLED
//
// A disabled schedule fires nothing and its occurrences are not made up when it is re-enabled,
// because re-enabling is an UpdateSchedule and re-arms the timer. A one-time schedule re-enabled
// after its instant fires at once: "When re-enabling a one-time schedule after its original
// scheduled time has passed, the schedule may immediately invoke its target. This can occur even if
// the schedule was disabled before its original execution time" (troubleshooting.html).
//
// # Retries and the dead-letter queue
//
// `API_RetryPolicy` publishes MaximumRetryAttempts (0–185) and MaximumEventAgeInSeconds
// (60–86400): "Retry attempts with exponential backoff continue until either the maximum number of
// attempts is made or until the duration of the MaximumEventAgeInSeconds is reached." AWS publishes
// neither the backoff's intervals nor a default policy; substrate's readings are that a schedule
// with no RetryPolicy has the published maxima (185 attempts, 86400 seconds), and that retry n is
// due 60·2^(n−1) seconds after the attempt before it, capped at 15 minutes. Event age is measured
// from the occurrence's scheduled time. A permanent error is not retried: the dead-letter event's
// EXHAUSTED_RETRY_CONDITION "will be present if the error from the target API is a retryable
// error, and not a permanent error" (configuring-schedule-dlq.html).
//
// An invocation that fails for good is sent to Target.DeadLetterConfig's queue, as the role, with
// the published dead-letter event: the Input as the body, and ERROR_CODE, ERROR_MESSAGE,
// EXECUTION_ID, EXHAUSTED_RETRY_CONDITION (retryable errors only), IS_PAYLOAD_TRUNCATED,
// RETRY_ATTEMPTS, SCHEDULED_TIME, SCHEDULE_ARN and TARGET_ARN as String message attributes. With no
// dead-letter queue the event is dropped.
//
// # Authorization
//
// "The execution role you specify must have the permissions to call the API operation you want your
// schedule to target." Each target request and each dead-letter send is authorized as
// Target.RoleArn, through the same [AuthController.CheckAccess] the CloudFormation deployer uses for
// its stack role — so a role substrate's IAM holds has its policies evaluated, and one it does not
// is not enforced, as for every other caller. A denial is a permanent failure. The role's trust
// policy is not consulted.
//
// # Completion
//
// "For one-time schedules, this occurs after the schedule has invoked its target once. For
// recurring schedules ... your schedule is deleted after its last invocation. A recurring
// schedule's last invocation is the invocation that occurs closest to the EndDate you specify. If
// you configure a schedule with automatic deletion but do not specify a value for EndDate,
// EventBridge Scheduler does not automatically delete the schedule" (managing-schedule-delete.html).
// A schedule is finished once it has no next occurrence and no invocation being retried, and, for a
// recurring schedule that never fired, once the clock has passed its EndDate. A finished schedule
// with ActionAfterCompletion DELETE is deleted; with NONE, or none, it stays, State unchanged.

// schedTimerPrefix begins every schedule's timer key.
const schedTimerPrefix = "sched_timer:"

// schedMaxStepsPerRun bounds the occurrences and attempts one schedule can run in one request;
// the rest stay due for the next request, as an event-source mapping's polls do.
const schedMaxStepsPerRun = 100

// Retry policy defaults and the backoff, as described above.
const (
	schedDefaultMaxRetryAttempts = 185
	schedDefaultMaxEventAge      = 86400 * time.Second
	schedRetryBaseDelay          = 60 * time.Second
	schedRetryMaxDelay           = 15 * time.Minute
)

// schedTimer is a schedule's clock state.
type schedTimer struct {
	// Expression and Timezone are the configuration the timer was armed for.
	Expression string `json:"expression"`
	Timezone   string `json:"timezone,omitempty"`
	// Next is the scheduled time of the next occurrence, nil when there is none.
	Next *time.Time `json:"next,omitempty"`
	// Fired counts the occurrences fired under this configuration.
	Fired int `json:"fired"`
	// Completed records that the schedule finished and was kept.
	Completed bool `json:"completed,omitempty"`
	// Pending are the invocations awaiting a retry, oldest first.
	Pending []schedPending `json:"pending,omitempty"`
}

// schedPending is an invocation awaiting a retry.
type schedPending struct {
	// Scheduled is the occurrence's scheduled time.
	Scheduled time.Time `json:"scheduled"`
	// Attempts counts the attempts made so far.
	Attempts int `json:"attempts"`
	// NextAttempt is when the next attempt is due.
	NextAttempt time.Time `json:"next_attempt"`
}

// schedTimerKey is the state key of a schedule's timer.
func schedTimerKey(accountID, region, groupName, name string) string {
	return schedTimerPrefix + accountID + "/" + region + "/" + groupName + "/" + name
}

func (p *SchedulerPlugin) loadTimer(ctx context.Context, key string) (*schedTimer, error) {
	data, err := p.state.Get(ctx, schedulerNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("scheduler load timer: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var t schedTimer
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("scheduler decode timer: %w", err)
	}
	return &t, nil
}

func (p *SchedulerPlugin) saveTimer(ctx context.Context, key string, t *schedTimer) error {
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("scheduler marshal timer: %w", err)
	}
	if err := p.state.Put(ctx, schedulerNamespace, key, data); err != nil {
		return fmt.Errorf("scheduler save timer: %w", err)
	}
	return nil
}

// armTimer arms a schedule's timer for its current configuration, at the simulated now. The caller
// holds p.mu.
func (p *SchedulerPlugin) armTimer(ctx context.Context, rec *SchedulerRecord) error {
	key := schedTimerKey(rec.AccountID, rec.Region, rec.GroupName, rec.Name)
	old, err := p.loadTimer(ctx, key)
	if err != nil {
		return err
	}
	t := &schedTimer{Expression: rec.ScheduleExpression, Timezone: rec.ScheduleExpressionTimezone}
	expr, err := parseSchedExpression(rec.ScheduleExpression, rec.ScheduleExpressionTimezone)
	if err != nil {
		// Stored and reported as sent, and never fired: substrate does not invent the refusal
		// CreateSchedule would answer for an expression it cannot evaluate.
		p.logger.Warn("scheduler: schedule expression is not evaluated", "schedule", rec.ARN, "err", err)
		return p.saveTimer(ctx, key, t)
	}
	if expr.kind == schedExprAt && old != nil && old.Fired > 0 &&
		old.Expression == t.Expression && old.Timezone == t.Timezone {
		t.Fired, t.Completed = old.Fired, old.Completed
		return p.saveTimer(ctx, key, t)
	}
	t.Next = expr.first(p.tc.Now(), schedEpochTime(rec.StartDate), schedEpochTime(rec.EndDate))
	return p.saveTimer(ctx, key, t)
}

// removeSchedule deletes a schedule's record, timer and index entry. The caller holds p.mu.
func (p *SchedulerPlugin) removeSchedule(ctx context.Context, accountID, region, groupName, name string) error {
	if err := p.state.Delete(ctx, schedulerNamespace, schedKey(accountID, region, groupName, name)); err != nil {
		return fmt.Errorf("scheduler remove schedule: %w", err)
	}
	if err := p.state.Delete(ctx, schedulerNamespace, schedTimerKey(accountID, region, groupName, name)); err != nil {
		return fmt.Errorf("scheduler remove timer: %w", err)
	}
	if err := removeFromStringIndex(ctx, p.state, schedulerNamespace, schedNamesKey(accountID, region, groupName), name); err != nil {
		return fmt.Errorf("scheduler remove index: %w", err)
	}
	return nil
}

// RunDue fires every occurrence and retry due at the simulated clock's current reading, for every
// schedule, in state-key order, and deletes the finished schedules whose ActionAfterCompletion is
// DELETE. It implements [ClockDrivenPlugin].
func (p *SchedulerPlugin) RunDue(trigger *RequestContext, dispatch InternalDispatch) error {
	ctx := context.Background()
	p.mu.Lock()
	defer p.mu.Unlock()
	keys, err := p.state.List(ctx, schedulerNamespace, schedTimerPrefix)
	if err != nil {
		return fmt.Errorf("scheduler list timers: %w", err)
	}
	if len(keys) == 0 {
		return nil
	}
	now := p.tc.Now()
	for _, key := range keys {
		if err := p.runTimer(ctx, trigger, dispatch, key, now); err != nil {
			return err
		}
	}
	return nil
}

// schedRun is one schedule's work in one RunDue.
type schedRun struct {
	p        *SchedulerPlugin
	trigger  *RequestContext
	dispatch InternalDispatch
	rec      *SchedulerRecord
	now      time.Time
	steps    int
}

func (p *SchedulerPlugin) runTimer(ctx context.Context, trigger *RequestContext, dispatch InternalDispatch, key string, now time.Time) error {
	t, err := p.loadTimer(ctx, key)
	if err != nil || t == nil {
		return err
	}
	if t.Completed {
		return nil
	}
	data, err := p.state.Get(ctx, schedulerNamespace, "sched:"+strings.TrimPrefix(key, schedTimerPrefix))
	if err != nil {
		return fmt.Errorf("scheduler load schedule: %w", err)
	}
	if data == nil {
		// A timer whose schedule is gone is the remnant of a store fault; nothing can fire it.
		if err := p.state.Delete(ctx, schedulerNamespace, key); err != nil {
			return fmt.Errorf("scheduler delete orphan timer: %w", err)
		}
		return nil
	}
	var rec SchedulerRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return fmt.Errorf("scheduler decode schedule: %w", err)
	}
	if rec.State != schedulerStateEnabled {
		return nil
	}
	expr, exprErr := parseSchedExpression(rec.ScheduleExpression, rec.ScheduleExpressionTimezone)

	run := &schedRun{p: p, trigger: trigger, dispatch: dispatch, rec: &rec, now: now}
	before, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("scheduler marshal timer: %w", err)
	}

	// Retries first, oldest occurrence first, then the occurrences that came due.
	var still []schedPending
	for _, pend := range t.Pending {
		if kept, ok := run.retry(ctx, pend); ok {
			still = append(still, kept)
		}
	}
	t.Pending = still
	for exprErr == nil && t.Next != nil && !t.Next.After(now) && run.steps < schedMaxStepsPerRun {
		scheduled := *t.Next
		if expr.kind == schedExprAt {
			t.Next = nil
		} else {
			t.Next = expr.next(scheduled, schedEpochTime(rec.EndDate))
		}
		t.Fired++
		if kept, ok := run.retry(ctx, schedPending{Scheduled: scheduled, NextAttempt: scheduled}); ok {
			t.Pending = append(t.Pending, kept)
		}
	}
	sort.SliceStable(t.Pending, func(i, j int) bool { return t.Pending[i].Scheduled.Before(t.Pending[j].Scheduled) })

	if exprErr == nil && t.Next == nil && len(t.Pending) == 0 && schedFinished(expr, t, &rec, now) {
		if rec.ActionAfterCompletion == "DELETE" {
			return p.removeSchedule(ctx, rec.AccountID, rec.Region, rec.GroupName, rec.Name)
		}
		t.Completed = true
	}

	after, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("scheduler marshal timer: %w", err)
	}
	if string(after) == string(before) {
		// Nothing was due: observing a schedule writes no state.
		return nil
	}
	return p.saveTimer(ctx, key, t)
}

// schedFinished reports whether a schedule with no next occurrence and nothing pending has
// finished: a one-time schedule once it fired, a recurring one once it fired its last occurrence
// or, never having fired, once the clock passed its EndDate.
func schedFinished(expr *schedExpression, t *schedTimer, rec *SchedulerRecord, now time.Time) bool {
	if expr.kind == schedExprAt {
		return t.Fired > 0
	}
	end := schedEpochTime(rec.EndDate)
	if end == nil {
		return false
	}
	return t.Fired > 0 || !now.Before(*end)
}

// retry makes every attempt of one invocation that is due, and reports the invocation to keep
// pending, if any.
func (r *schedRun) retry(ctx context.Context, pend schedPending) (schedPending, bool) {
	maxRetries, maxAge := schedDefaultMaxRetryAttempts, schedDefaultMaxEventAge
	if rp := r.rec.Target.RetryPolicy; rp != nil {
		maxRetries = int(rp.MaximumRetryAttempts)
		if rp.MaximumEventAgeInSeconds > 0 {
			maxAge = time.Duration(rp.MaximumEventAgeInSeconds) * time.Second
		}
	}
	for !pend.NextAttempt.After(r.now) && r.steps < schedMaxStepsPerRun {
		r.steps++
		pend.Attempts++
		failure := r.attempt(ctx, pend)
		if failure == nil {
			return schedPending{}, false
		}
		condition := ""
		switch {
		case !failure.retryable():
		case pend.Attempts-1 >= maxRetries:
			condition = "MaximumRetryAttempts"
		default:
			delay := schedRetryBaseDelay << min(pend.Attempts-1, 10)
			delay = min(delay, schedRetryMaxDelay)
			next := pend.NextAttempt.Add(delay)
			if next.Sub(pend.Scheduled) > maxAge {
				condition = "MaximumEventAgeInSeconds"
			} else {
				pend.NextAttempt = next
				continue
			}
		}
		r.deadLetter(ctx, pend, failure, condition)
		return schedPending{}, false
	}
	return pend, true
}

// executionID is the deterministic ID of one attempt, in the 16-hex-digit form the guide shows.
func (r *schedRun) executionID(pend schedPending) string {
	sum := sha256.Sum256([]byte(r.rec.ARN + "\x00" + pend.Scheduled.UTC().Format(time.RFC3339Nano) +
		"\x00" + strconv.Itoa(pend.Attempts)))
	return hex.EncodeToString(sum[:8])
}

// reqCtx derives the context of one dispatch from the triggering request, the schedule, the
// occurrence and the attempt, so a replay derives the same request ID and the same minted IDs.
func (r *schedRun) reqCtx(pend schedPending, step, accountID, region string) *RequestContext {
	label := "scheduler/" + r.rec.ARN + "/" + pend.Scheduled.UTC().Format(time.RFC3339Nano) +
		"/" + strconv.Itoa(pend.Attempts) + "/" + step
	id := clockDrivenRequestID(r.trigger.RequestID, label)
	if accountID == "" {
		accountID = r.rec.AccountID
	}
	if region == "" {
		region = r.rec.Region
	}
	return &RequestContext{
		RequestID: id,
		AccountID: accountID,
		Region:    region,
		Timestamp: r.p.tc.Now(),
		IDs:       NewIDMint(id),
		Metadata:  map[string]interface{}{"stream_id": streamIDFromContext(r.trigger)},
	}
}

// send authorizes req as the schedule's role and dispatches it.
func (r *schedRun) send(reqCtx *RequestContext, req *AWSRequest) *schedFailure {
	if role := r.rec.Target.RoleARN; role != "" {
		reqCtx.Principal = &Principal{ARN: role, Type: "Role"}
	}
	if r.p.auth != nil && reqCtx.Principal != nil {
		if err := r.p.auth.CheckAccess(reqCtx, req); err != nil {
			return schedTargetFailure(nil, err)
		}
	}
	return schedTargetFailure(r.dispatch(reqCtx, req))
}

// input is the target's Input for one attempt, with the context attributes replaced.
func (r *schedRun) input(pend schedPending) string {
	return schedContextInput(r.rec.Target.Input, r.rec.ARN, pend.Scheduled.UTC().Format(time.RFC3339),
		r.executionID(pend), pend.Attempts)
}

// attempt makes one attempt of an invocation, nil when it succeeded.
func (r *schedRun) attempt(ctx context.Context, pend schedPending) *schedFailure {
	seed, err := r.p.resolveSeededOutcome(ctx, r.rec.GroupName, r.rec.Name)
	if err != nil {
		r.p.logger.Warn("scheduler: invocation-outcome seed unreadable", "schedule", r.rec.ARN, "err", err)
	}
	if seed != nil && (seed.FailAttempts <= 0 || pend.Attempts <= seed.FailAttempts) {
		return seed.failure()
	}
	call := buildTargetCall(r.rec, r.input(pend))
	switch {
	case call.invalid != nil:
		return call.invalid
	case call.intent != "":
		r.p.logger.Info("scheduler: target recorded, not dispatched", "schedule", r.rec.ARN,
			"target", r.rec.Target.ARN, "reason", call.intent)
		return nil
	}
	return r.send(r.reqCtx(pend, "target", call.accountID, call.region), call.req)
}

// deadLetter sends a failed invocation's dead-letter event to the schedule's queue, or drops it.
func (r *schedRun) deadLetter(_ context.Context, pend schedPending, failure *schedFailure, condition string) {
	dlq := r.rec.Target.DeadLetterConfig
	if dlq == nil || dlq.ARN == "" {
		r.p.logger.Info("scheduler: invocation dropped", "schedule", r.rec.ARN, "code", failure.Code)
		return
	}
	parts := strings.Split(dlq.ARN, ":")
	if len(parts) != 6 {
		r.p.logger.Warn("scheduler: dead-letter queue ARN is not an SQS ARN", "arn", dlq.ARN)
		return
	}
	region, account, queue := parts[3], parts[4], parts[5]
	attr := func(v string) SQSMessageAttribute { return SQSMessageAttribute{DataType: "String", StringValue: v} }
	attrs := map[string]SQSMessageAttribute{
		"ERROR_CODE":           attr(failure.Code),
		"ERROR_MESSAGE":        attr(failure.Message),
		"EXECUTION_ID":         attr(r.executionID(pend)),
		"IS_PAYLOAD_TRUNCATED": attr("false"),
		"RETRY_ATTEMPTS":       attr(strconv.Itoa(pend.Attempts - 1)),
		"SCHEDULED_TIME":       attr(pend.Scheduled.UTC().Format(time.RFC3339)),
		"SCHEDULE_ARN":         attr(r.rec.ARN),
		"TARGET_ARN":           attr(r.rec.Target.ARN),
	}
	if attrs["ERROR_MESSAGE"].StringValue == "" {
		delete(attrs, "ERROR_MESSAGE")
	}
	if condition != "" {
		attrs["EXHAUSTED_RETRY_CONDITION"] = attr(condition)
	}
	body := r.input(pend)
	if body == "" {
		body = "{}"
	}
	req := schedJSONRequest("sqs", pluginRouting["sqs"].TargetPrefix, "SendMessage", schedMustJSON(map[string]any{
		"QueueUrl":          sqsQueueURL(region, account, queue),
		"MessageBody":       body,
		"MessageAttributes": attrs,
	}))
	if f := r.send(r.reqCtx(pend, "dead-letter", account, region), req); f != nil {
		r.p.logger.Warn("scheduler: dead-letter event not delivered", "schedule", r.rec.ARN,
			"queue", dlq.ARN, "code", f.Code, "status", http.StatusText(f.Status))
	}
}
