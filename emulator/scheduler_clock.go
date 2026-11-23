package emulator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
// every top-level request, [SchedulerPlugin.RunDue] does what is due at the clock's current reading,
// so a frozen clock makes nothing due and a replay, which pins the clock to each recorded event,
// makes the same occurrences due at the same request. Each timer carries its own Due time and the
// earliest over all of them is kept at [schedWakeKey], so a request with nothing due reads one key.
//
// An UpdateSchedule re-arms the timer; see [schedTimer.arm] for what an update keeps.
//
// # A State of DISABLED
//
// A disabled schedule fires nothing and its occurrences are not made up when it is re-enabled: the
// re-enabling update resumes the series after the update. A one-time schedule re-enabled
// after its instant fires at once: "When re-enabling a one-time schedule after its original
// scheduled time has passed, the schedule may immediately invoke its target. This can occur even if
// the schedule was disabled before its original execution time" (troubleshooting.html).
//
// # Retries and the dead-letter queue
//
// `API_RetryPolicy` publishes MaximumRetryAttempts (0–185) and MaximumEventAgeInSeconds
// (60–86400): "Retry attempts with exponential backoff continue until either the maximum number of
// attempts is made or until the duration of the MaximumEventAgeInSeconds is reached." AWS publishes
// neither the backoff's intervals nor a default — not on `API_RetryPolicy`, `API_Target`, the CLI
// reference or `AWS::Scheduler::Schedule RetryPolicy`, all of which list both members as optional
// with a range and nothing more. Substrate's readings are that each member a target omits, whether
// it omits the whole RetryPolicy or one member of it, takes its published maximum (185 attempts,
// 86400 seconds), with an explicit 0 attempts meaning no retries; and that retry n is
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

// schedTimer is a schedule's clock state. It carries everything needed to decide whether the
// schedule needs attention — its Due time — so a request with nothing due reads no schedule record.
type schedTimer struct {
	// Expression, Timezone and Start are the configuration the timer was armed for; an update
	// that keeps all three keeps the series' anchor and progress.
	Expression string     `json:"expression"`
	Timezone   string     `json:"timezone,omitempty"`
	Start      *time.Time `json:"start,omitempty"`
	// End is the schedule's EndDate, nil when it has none.
	End *time.Time `json:"end,omitempty"`
	// OneTime records an at() expression.
	OneTime bool `json:"one_time,omitempty"`
	// Disabled records a State of DISABLED, which fires nothing.
	Disabled bool `json:"disabled,omitempty"`
	// Unevaluable records an expression substrate cannot evaluate, which never fires.
	Unevaluable bool `json:"unevaluable,omitempty"`
	// ArmedAt anchors a rate() series that has no StartDate: the moment it was first armed.
	ArmedAt time.Time `json:"armed_at"`
	// Next is the scheduled time of the next occurrence, nil when there is none.
	Next *time.Time `json:"next,omitempty"`
	// Last is the scheduled time of the last occurrence fired, nil when none has.
	Last *time.Time `json:"last,omitempty"`
	// Fired counts the occurrences fired.
	Fired int `json:"fired"`
	// Completed records that the schedule finished and was kept.
	Completed bool `json:"completed,omitempty"`
	// Pending are the invocations awaiting a retry, oldest first.
	Pending []schedPending `json:"pending,omitempty"`
	// Due is when the timer next needs attention, nil when it never will on its own (completed,
	// disabled, unevaluable, or recurring with nothing left and no EndDate).
	Due *time.Time `json:"due,omitempty"`
}

// due computes when the timer next needs attention, at the simulated now.
func (t *schedTimer) due(now time.Time) *time.Time {
	if t.Disabled || t.Completed || t.Unevaluable {
		return nil
	}
	var earliest *time.Time
	consider := func(c time.Time) {
		if earliest == nil || c.Before(*earliest) {
			c := c
			earliest = &c
		}
	}
	if t.Next != nil {
		consider(*t.Next)
	}
	for _, p := range t.Pending {
		consider(p.NextAttempt)
	}
	if earliest != nil {
		return earliest
	}
	// Nothing left to fire: the schedule is finished now if it fired, or, recurring with an
	// EndDate and never fired, once the clock passes EndDate; see [schedFinished].
	switch {
	case t.Fired > 0 && (t.OneTime || t.End != nil):
		return &now
	case !t.OneTime && t.End != nil:
		end := *t.End
		return &end
	}
	return nil
}

// schedWakeKey holds the earliest Due over every timer, so a request with nothing due costs one
// read. It is kept conservative — never later than the true earliest — by lowering it whenever a
// timer is armed and recomputing it whenever the timers are scanned. It lives in the state
// manager rather than on the plugin so a reset, a snapshot restore and a replay all see the value
// the timers they hold imply, with no in-memory copy to fall out of step.
const schedWakeKey = "sched_wake"

// schedWake is the stored earliest Due.
type schedWake struct {
	At time.Time `json:"at"`
}

func (p *SchedulerPlugin) loadWake(ctx context.Context) (*time.Time, error) {
	data, err := p.state.Get(ctx, schedulerNamespace, schedWakeKey)
	if err != nil {
		return nil, fmt.Errorf("scheduler load wake: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var w schedWake
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("scheduler decode wake: %w", err)
	}
	return &w.At, nil
}

func (p *SchedulerPlugin) saveWake(ctx context.Context, at *time.Time) error {
	if at == nil {
		if err := p.state.Delete(ctx, schedulerNamespace, schedWakeKey); err != nil {
			return fmt.Errorf("scheduler clear wake: %w", err)
		}
		return nil
	}
	data, err := json.Marshal(schedWake{At: *at})
	if err != nil {
		return fmt.Errorf("scheduler marshal wake: %w", err)
	}
	if err := p.state.Put(ctx, schedulerNamespace, schedWakeKey, data); err != nil {
		return fmt.Errorf("scheduler save wake: %w", err)
	}
	return nil
}

// lowerWake makes the wake no later than at. The caller holds p.mu.
func (p *SchedulerPlugin) lowerWake(ctx context.Context, at *time.Time) error {
	if at == nil {
		return nil
	}
	cur, err := p.loadWake(ctx)
	if err != nil {
		return err
	}
	if cur != nil && !at.Before(*cur) {
		return nil
	}
	return p.saveWake(ctx, at)
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
	now := p.tc.Now()
	start, end := schedEpochTime(rec.StartDate), schedEpochTime(rec.EndDate)
	t := &schedTimer{
		Expression: rec.ScheduleExpression, Timezone: rec.ScheduleExpressionTimezone,
		Start: start, End: end, Disabled: rec.State != schedulerStateEnabled, ArmedAt: now,
	}
	if old != nil {
		// An invocation already being retried is still in flight; an update does not cancel it.
		t.Pending = old.Pending
	}
	t.arm(old, rec, now)
	if t.Unevaluable {
		// CreateSchedule and UpdateSchedule refuse an expression or a time zone the parser cannot
		// evaluate (#1514), so only a record written before that refusal existed can reach here. It
		// is kept, reported as sent, and never fired.
		p.logger.Warn("scheduler: schedule expression is not evaluated", "schedule", rec.ARN)
	}
	t.Due = t.due(now)
	if err := p.saveTimer(ctx, key, t); err != nil {
		return err
	}
	return p.lowerWake(ctx, t.Due)
}

// arm sets the timer's series for rec's configuration, keeping old's progress where the
// configuration that decides the series is unchanged.
//
// An update that keeps the expression, zone and StartDate keeps the series: a rate() schedule with
// no StartDate keeps its anchor rather than restarting — and firing — at every update, and a
// one-time schedule that has fired is not fired again. `API_UpdateSchedule` replaces the
// configuration and publishes nothing about the series, so a changed expression, zone or StartDate
// starts a new series from the update. A schedule re-enabled with its series kept resumes at the
// first occurrence after the update: the occurrences missed while it was disabled are not made up.
// An update that sets ActionAfterCompletion DELETE on a finished schedule lets the next run finish
// it again, which deletes it.
func (t *schedTimer) arm(old *schedTimer, rec *SchedulerRecord, now time.Time) {
	expr, err := parseSchedExpression(rec.ScheduleExpression, rec.ScheduleExpressionTimezone)
	if err != nil {
		t.Unevaluable = true
		return
	}
	t.OneTime = expr.kind == schedExprAt
	same := old != nil && old.Expression == t.Expression && old.Timezone == t.Timezone &&
		schedTimeEqual(old.Start, t.Start)
	if !same {
		t.Next = expr.firstFrom(now, now, t.Start, t.End)
		return
	}
	t.Fired, t.Last = old.Fired, old.Last
	t.Completed = old.Completed && rec.ActionAfterCompletion != "DELETE"
	if t.OneTime {
		if t.Fired == 0 {
			t.Next = expr.firstFrom(now, now, nil, nil)
		}
		return
	}
	t.ArmedAt = old.ArmedAt
	from := t.ArmedAt
	if old.Disabled && !t.Disabled {
		from = now
	}
	if t.Last != nil {
		t.Next = expr.next(*t.Last, t.End)
		if t.Next != nil && t.Next.Before(from) {
			t.Next = expr.firstFrom(t.ArmedAt, from, t.Start, t.End)
		}
	} else {
		t.Next = expr.firstFrom(t.ArmedAt, from, t.Start, t.End)
	}
	if t.Next != nil {
		t.Completed = false
	}
}

func schedTimeEqual(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
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
//
// A request with nothing due reads one key, the wake ([schedWakeKey]). When something is due, each
// timer is read and only a timer whose own Due has come loads its schedule record. One schedule's
// failure is logged and joined into the returned error, and does not stop the others, as
// [PluginRegistry.RunClockDriven] treats plugins.
func (p *SchedulerPlugin) RunDue(trigger *RequestContext, dispatch InternalDispatch) error {
	ctx := context.Background()
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.tc.Now()
	wake, err := p.loadWake(ctx)
	if err != nil {
		return err
	}
	if wake == nil || wake.After(now) {
		return nil
	}
	keys, err := p.state.List(ctx, schedulerNamespace, schedTimerPrefix)
	if err != nil {
		return fmt.Errorf("scheduler list timers: %w", err)
	}
	var errs []error
	var earliest *time.Time
	for _, key := range keys {
		t, err := p.loadTimer(ctx, key)
		if err != nil {
			p.logger.Warn("scheduler: timer unreadable", "timer", key, "err", err)
			errs = append(errs, err)
			continue
		}
		if t == nil {
			continue
		}
		if t.Due != nil && !t.Due.After(now) {
			if err := p.runTimer(ctx, trigger, dispatch, key, t, now); err != nil {
				p.logger.Warn("scheduler: schedule's due work failed", "timer", key, "err", err)
				errs = append(errs, err)
			}
		}
		if t.Due != nil && (earliest == nil || t.Due.Before(*earliest)) {
			earliest = t.Due
		}
	}
	if err := p.saveWake(ctx, earliest); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
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

// runTimer does one due timer's work and updates t, which the caller reads Due from afterwards.
func (p *SchedulerPlugin) runTimer(ctx context.Context, trigger *RequestContext, dispatch InternalDispatch, key string, t *schedTimer, now time.Time) error {
	data, err := p.state.Get(ctx, schedulerNamespace, "sched:"+strings.TrimPrefix(key, schedTimerPrefix))
	if err != nil {
		// Transient: the timer stays due and is tried again on the next request.
		return fmt.Errorf("scheduler load schedule: %w", err)
	}
	if data == nil {
		// A timer whose schedule is gone is the remnant of a store fault; nothing can fire it.
		t.Due = nil
		if err := p.state.Delete(ctx, schedulerNamespace, key); err != nil {
			return fmt.Errorf("scheduler delete orphan timer: %w", err)
		}
		return nil
	}
	var rec SchedulerRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		// A record that will not decode will not decode next time either: the timer is set aside
		// (no Due) so it does not hold every later request to a rescan, and an UpdateSchedule
		// that rewrites the record re-arms it.
		t.Due = nil
		if saveErr := p.saveTimer(ctx, key, t); saveErr != nil {
			return errors.Join(fmt.Errorf("scheduler decode schedule: %w", err), saveErr)
		}
		return fmt.Errorf("scheduler decode schedule: %w", err)
	}
	expr, exprErr := parseSchedExpression(rec.ScheduleExpression, rec.ScheduleExpressionTimezone)
	if exprErr != nil || rec.State != schedulerStateEnabled {
		// The timer was armed with this configuration and would not be due; it is only reached
		// through a record written without a re-arm, which leaves nothing to fire.
		t.Due = nil
		return p.saveTimer(ctx, key, t)
	}

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
	for t.Next != nil && !t.Next.After(now) && run.steps < schedMaxStepsPerRun {
		scheduled := *t.Next
		if expr.kind == schedExprAt {
			t.Next = nil
		} else {
			t.Next = expr.next(scheduled, schedEpochTime(rec.EndDate))
		}
		t.Fired++
		t.Last = &scheduled
		if kept, ok := run.retry(ctx, schedPending{Scheduled: scheduled, NextAttempt: scheduled}); ok {
			t.Pending = append(t.Pending, kept)
		}
	}
	sort.SliceStable(t.Pending, func(i, j int) bool { return t.Pending[i].Scheduled.Before(t.Pending[j].Scheduled) })

	if t.Next == nil && len(t.Pending) == 0 && schedFinished(expr, t, &rec, now) {
		if rec.ActionAfterCompletion == "DELETE" {
			t.Due = nil
			return p.removeSchedule(ctx, rec.AccountID, rec.Region, rec.GroupName, rec.Name)
		}
		t.Completed = true
	}
	t.Due = t.due(now)

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
		// Each member defaults on its own, so a partial policy keeps the default for the member it
		// omits, and an explicit 0 retry attempts is no retries.
		if rp.MaximumRetryAttempts != nil {
			maxRetries = int(*rp.MaximumRetryAttempts)
		}
		if rp.MaximumEventAgeInSeconds != nil {
			maxAge = time.Duration(*rp.MaximumEventAgeInSeconds) * time.Second
		}
	}
	for !pend.NextAttempt.After(r.now) && r.steps < schedMaxStepsPerRun {
		r.steps++
		pend.Attempts++
		attemptAt := pend.NextAttempt
		failure := r.attempt(ctx, pend)
		r.count(ctx, attemptAt, schedAttemptMetrics(failure)...)
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
		r.deadLetter(ctx, pend, failure, condition, attemptAt)
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

// schedAttemptMetrics are the AWS/Scheduler metrics one attempt counts; see scheduler_metrics.go.
func schedAttemptMetrics(failure *schedFailure) []string {
	metrics := []string{schedMetricAttempt}
	if failure == nil {
		return metrics
	}
	metrics = append(metrics, schedMetricTargetError)
	if failure.throttled() {
		metrics = append(metrics, schedMetricTargetThrottled)
	}
	return metrics
}

// count adds one event to each named AWS/Scheduler metric, in the minute holding at. A counter
// that cannot be written is logged rather than failing the invocation it describes: what the
// invocation did does not depend on whether it was counted.
func (r *schedRun) count(ctx context.Context, at time.Time, metrics ...string) {
	if err := r.p.countMetrics(ctx, r.rec, at, metrics...); err != nil {
		r.p.logger.Warn("scheduler: invocation metrics not counted", "schedule", r.rec.ARN, "err", err)
	}
}

// deadLetter sends a failed invocation's dead-letter event to the schedule's queue, or drops it.
// at is when the last attempt was due, the minute its metrics count in.
func (r *schedRun) deadLetter(ctx context.Context, pend schedPending, failure *schedFailure, condition string, at time.Time) {
	r.count(ctx, at, schedMetricDropped)
	dlq := r.rec.Target.DeadLetterConfig
	if dlq == nil || dlq.ARN == "" {
		r.p.logger.Info("scheduler: invocation dropped", "schedule", r.rec.ARN, "code", failure.Code)
		return
	}
	parts := strings.Split(dlq.ARN, ":")
	if len(parts) != 6 {
		r.p.logger.Warn("scheduler: dead-letter queue ARN is not an SQS ARN", "arn", dlq.ARN)
		r.count(ctx, at, schedMetricFailedToSendDLQ)
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
		metrics := []string{schedMetricFailedToSendDLQ}
		if f.Code != "" {
			metrics = append(metrics, schedMetricFailedCodePrefix+f.Code)
		}
		r.count(ctx, at, metrics...)
		return
	}
	r.count(ctx, at, schedMetricSentToDLQ)
}
