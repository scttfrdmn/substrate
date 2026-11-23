package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// AWS/Scheduler invocation metrics (#1513): the counters the Scheduler keeps, and how CloudWatch
// reads them.
//
// EventBridge Scheduler publishes, in the AWS/Scheduler namespace with a ScheduleGroup dimension
// and as account metrics with none (monitoring-cloudwatch.html):
//
//   - InvocationAttemptCount — "Emitted for every invocation attempt."
//   - TargetErrorCount — "Emitted when the target returns an exception after EventBridge Scheduler
//     calls the target API."
//   - TargetErrorThrottledCount — "Emitted when target invocation fails due to API throttling by
//     the target."
//   - InvocationDroppedCount — "Emitted when EventBridge Scheduler stops attempting to invoke the
//     target after a schedule's retry policy has been exhausted."
//   - InvocationsSentToDeadLetterCount — "Emitted for every successful delivery to a schedule's
//     DLQ."
//   - InvocationsFailedToBeSentToDeadLetterCount, and the same name suffixed _<error_code> —
//     "Emitted when EventBridge Scheduler cannot deliver an event to the DLQ."
//
// # Derived on read, not published
//
// Every one of these is a fact about what the Scheduler's clock-driven invocations did, which
// substrate already decides on the simulated clock and records in state (scheduler_clock.go). So
// the Scheduler keeps a counter per metric per minute per schedule group, beside its timers, and
// CloudWatch reads those counters at request time — the precedent of the EC2 vCPU usage metrics
// (cloudwatch_usage.go), which CloudWatch derives from EC2's stored instances. Nothing is
// dispatched from the Scheduler to CloudWatch, so no extra event is recorded, and a replay, which
// re-runs the same invocations at the same requests, rebuilds the same counters.
//
// # What substrate's readings are
//
//   - An attempt counts at the time it was due, its minute, even when the clock reached that time
//     in one jump: a schedule's invocations happen at their scheduled times on the simulated clock.
//   - TargetErrorCount counts every failed attempt — a target's error answer, a seeded failure
//     (which stands for one), a denial of the schedule's role, and a target input substrate could
//     not build. TargetErrorThrottledCount also counts a failure that is a throttle, read as the
//     retry classification reads one: HTTP 429, or a code naming a throttle.
//   - InvocationDroppedCount counts every invocation that fails for good — a permanent failure, or
//     a retryable one whose retry policy ran out — whether or not a dead-letter queue then receives
//     it, since the page says only that Scheduler "stops attempting to invoke the target".
//   - A dead-letter delivery the DLQ refuses counts InvocationsFailedToBeSentToDeadLetterCount and
//     the variant suffixed with the refusal's code; a DLQ ARN that is not an SQS queue ARN counts
//     the unsuffixed one only, since no service answered a code.
//   - InvocationThrottleCount is never counted: substrate models no Scheduler invocation quota to
//     exceed. InvocationsSentToDeadLetterCount_Truncated_MessageSizeExceeded is never counted:
//     substrate does not truncate a dead-letter payload.
//
// Each counted event is one sample of value 1, unit Count, so a period's Sum and SampleCount are
// the number of events and its Average, Minimum and Maximum are 1.

// schedMetricsNamespace is the CloudWatch namespace the Scheduler's metrics are reported in.
const schedMetricsNamespace = "AWS/Scheduler"

// schedMetricsDimension is the one dimension the Scheduler's metrics carry.
const schedMetricsDimension = "ScheduleGroup"

// The AWS/Scheduler metric names substrate counts.
const (
	schedMetricAttempt          = "InvocationAttemptCount"
	schedMetricTargetError      = "TargetErrorCount"
	schedMetricTargetThrottled  = "TargetErrorThrottledCount"
	schedMetricDropped          = "InvocationDroppedCount"
	schedMetricSentToDLQ        = "InvocationsSentToDeadLetterCount"
	schedMetricFailedToSendDLQ  = "InvocationsFailedToBeSentToDeadLetterCount"
	schedMetricsKeyPrefix       = "sched_metrics:"
	schedMetricsMaxHoursKept    = 7 * 24
	schedMetricFailedCodePrefix = schedMetricFailedToSendDLQ + "_"
)

// schedMetricMinute is one minute's counters for one schedule group.
type schedMetricMinute struct {
	// T is the minute, in Unix seconds.
	T int64 `json:"t"`
	// Counts maps a metric name to the events counted in the minute.
	Counts map[string]int `json:"c"`
}

// schedMetricRecord is one hour of one schedule group's counters, oldest minute first. A group
// keeps at most [schedMetricsMaxHoursKept] hours, dropping the oldest, as a CloudWatch series keeps
// a fixed number of points; an hour per record means a run rewrites the hour it counted in rather
// than the group's whole history.
type schedMetricRecord struct {
	Minutes []schedMetricMinute `json:"minutes"`
}

// schedMetricsGroupPrefix is the key prefix of one schedule group's hour records. A group name
// cannot hold a slash — its pattern is [0-9a-zA-Z-_.]+ — so the prefix names one group.
func schedMetricsGroupPrefix(accountID, region, groupName string) string {
	return schedMetricsKeyPrefix + accountID + "/" + region + "/" + groupName + "/"
}

// schedMetricsKey is the state key of one hour of a schedule group's counters.
func schedMetricsKey(accountID, region, groupName string, hour int64) string {
	return schedMetricsGroupPrefix(accountID, region, groupName) + fmt.Sprintf("%012d", hour)
}

// schedMetricsHour reads the hour a record key names.
func schedMetricsHour(key string) (int64, bool) {
	i := strings.LastIndexByte(key, '/')
	if i < 0 {
		return 0, false
	}
	h, err := strconv.ParseInt(key[i+1:], 10, 64)
	return h, err == nil
}

// loadSchedMetrics reads one hour record from the Scheduler's namespace.
func loadSchedMetrics(ctx context.Context, state StateManager, key string) (schedMetricRecord, error) {
	data, err := state.Get(ctx, schedulerNamespace, key)
	if err != nil {
		return schedMetricRecord{}, fmt.Errorf("scheduler load metrics: %w", err)
	}
	var rec schedMetricRecord
	if data == nil {
		return rec, nil
	}
	if err := json.Unmarshal(data, &rec); err != nil {
		return schedMetricRecord{}, fmt.Errorf("scheduler decode metrics %s: %w", key, err)
	}
	return rec, nil
}

// schedMetricCounts are the events one run counted: minute (Unix seconds) to metric to count.
type schedMetricCounts map[int64]map[string]int

// add counts one event of each named metric in the minute holding at.
func (c schedMetricCounts) add(at time.Time, metrics ...string) {
	minute := at.UTC().Truncate(time.Minute).Unix()
	if c[minute] == nil {
		c[minute] = map[string]int{}
	}
	for _, name := range metrics {
		c[minute][name]++
	}
}

// countMetrics adds a run's counts to rec's schedule group, writing each hour it touched once,
// then drops the group's oldest hours past the retention cap. The caller holds p.mu.
func (p *SchedulerPlugin) countMetrics(ctx context.Context, rec *SchedulerRecord, counts schedMetricCounts) error {
	if len(counts) == 0 {
		return nil
	}
	byHour := map[int64][]int64{}
	for minute := range counts {
		hour := minute - minute%3600
		byHour[hour] = append(byHour[hour], minute)
	}
	hours := make([]int64, 0, len(byHour))
	for h := range byHour {
		hours = append(hours, h)
	}
	sort.Slice(hours, func(i, j int) bool { return hours[i] < hours[j] })
	for _, hour := range hours {
		key := schedMetricsKey(rec.AccountID, rec.Region, rec.GroupName, hour)
		m, err := loadSchedMetrics(ctx, p.state, key)
		if err != nil {
			return err
		}
		for _, minute := range byHour[hour] {
			i := sort.Search(len(m.Minutes), func(i int) bool { return m.Minutes[i].T >= minute })
			if i == len(m.Minutes) || m.Minutes[i].T != minute {
				m.Minutes = append(m.Minutes, schedMetricMinute{})
				copy(m.Minutes[i+1:], m.Minutes[i:])
				m.Minutes[i] = schedMetricMinute{T: minute, Counts: map[string]int{}}
			}
			for name, n := range counts[minute] {
				m.Minutes[i].Counts[name] += n
			}
		}
		data, err := json.Marshal(m)
		if err != nil {
			return fmt.Errorf("scheduler marshal metrics: %w", err)
		}
		if err := p.state.Put(ctx, schedulerNamespace, key, data); err != nil {
			return fmt.Errorf("scheduler store metrics: %w", err)
		}
	}
	keys, err := p.state.List(ctx, schedulerNamespace, schedMetricsGroupPrefix(rec.AccountID, rec.Region, rec.GroupName))
	if err != nil {
		return fmt.Errorf("scheduler list metrics: %w", err)
	}
	for len(keys) > schedMetricsMaxHoursKept {
		if err := p.state.Delete(ctx, schedulerNamespace, keys[0]); err != nil {
			return fmt.Errorf("scheduler drop metrics: %w", err)
		}
		keys = keys[1:]
	}
	return nil
}

// schedGroupMetricKeys lists one group's hour records, oldest first.
func schedGroupMetricKeys(ctx context.Context, state StateManager, accountID, region, group string) ([]string, error) {
	keys, err := state.List(ctx, schedulerNamespace, schedMetricsGroupPrefix(accountID, region, group))
	if err != nil {
		return nil, fmt.Errorf("cloudwatch list scheduler metrics: %w", err)
	}
	return keys, nil
}

// cwSchedulerGroups returns the schedule groups the dimensions select, and false when they select
// none: no dimensions select the account metric, which sums every group, and ScheduleGroup alone
// selects one group.
func cwSchedulerGroups(ctx context.Context, state StateManager, accountID, region string, dims []cwDimension) ([]string, bool, error) {
	switch {
	case len(dims) == 0:
		prefix := schedMetricsKeyPrefix + accountID + "/" + region + "/"
		keys, err := state.List(ctx, schedulerNamespace, prefix)
		if err != nil {
			return nil, false, fmt.Errorf("cloudwatch list scheduler metrics: %w", err)
		}
		var groups []string
		for _, k := range keys {
			group, _, _ := strings.Cut(strings.TrimPrefix(k, prefix), "/")
			if len(groups) == 0 || groups[len(groups)-1] != group {
				groups = append(groups, group)
			}
		}
		return groups, true, nil
	case len(dims) == 1 && dims[0].Name == schedMetricsDimension:
		return []string{dims[0].Value}, true, nil
	}
	return nil, false, nil
}

// cwSchedulerPoints returns the AWS/Scheduler points of one metric in [from, to); see the file
// comment.
func cwSchedulerPoints(ctx context.Context, state StateManager, accountID, region, metricName string,
	dims []cwDimension, from, to time.Time) ([]cwPoint, error) {
	groups, ok, err := cwSchedulerGroups(ctx, state, accountID, region, dims)
	if err != nil || !ok {
		return nil, err
	}
	var out []cwPoint
	for _, g := range groups {
		keys, err := schedGroupMetricKeys(ctx, state, accountID, region, g)
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			hour, ok := schedMetricsHour(k)
			if !ok || !time.Unix(hour, 0).Before(to) || !time.Unix(hour+3600, 0).After(from) {
				continue
			}
			rec, err := loadSchedMetrics(ctx, state, k)
			if err != nil {
				return nil, err
			}
			out = append(out, cwSchedulerRecordPoints(rec, metricName, from, to)...)
		}
	}
	return out, nil
}

// cwSchedulerRecordPoints returns one hour record's points of one metric in [from, to).
func cwSchedulerRecordPoints(rec schedMetricRecord, metricName string, from, to time.Time) []cwPoint {
	var out []cwPoint
	for _, m := range rec.Minutes {
		at := time.Unix(m.T, 0).UTC()
		if n := m.Counts[metricName]; n > 0 && !at.Before(from) && at.Before(to) {
			out = append(out, cwPoint{T: at.UnixMilli(), Values: []float64{1}, Counts: []float64{float64(n)}, Unit: "Count"})
		}
	}
	return out
}

// cwSchedulerMetrics returns the AWS/Scheduler metrics ListMetrics reports: for each metric any
// group has counted, the account metric with no dimensions and one metric per group, ordered by
// name and then dimensions.
func cwSchedulerMetrics(ctx context.Context, state StateManager, accountID, region string) ([]cwListedMetric, error) {
	groups, _, err := cwSchedulerGroups(ctx, state, accountID, region, nil)
	if err != nil {
		return nil, err
	}
	type key struct{ name, group string }
	last := map[key]time.Time{}
	for _, g := range groups {
		keys, err := schedGroupMetricKeys(ctx, state, accountID, region, g)
		if err != nil {
			return nil, err
		}
		for _, rk := range keys {
			rec, err := loadSchedMetrics(ctx, state, rk)
			if err != nil {
				return nil, err
			}
			for _, m := range rec.Minutes {
				at := time.Unix(m.T, 0).UTC()
				for name, n := range m.Counts {
					if n <= 0 {
						continue
					}
					for _, k := range []key{{name, ""}, {name, g}} {
						if at.After(last[k]) {
							last[k] = at
						}
					}
				}
			}
		}
	}
	keys := make([]key, 0, len(last))
	for k := range last {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].name != keys[j].name {
			return keys[i].name < keys[j].name
		}
		return keys[i].group < keys[j].group
	})
	out := make([]cwListedMetric, 0, len(keys))
	for _, k := range keys {
		m := cwListedMetric{namespace: schedMetricsNamespace, name: k.name, last: last[k]}
		if k.group != "" {
			m.dims = []cwDimension{{Name: schedMetricsDimension, Value: k.group}}
		}
		out = append(out, m)
	}
	return out, nil
}
