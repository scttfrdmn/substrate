package substrate

import (
	"context"
	"time"
)

// ValidationStatus is the overall pass/fail/warn outcome of a validation report.
type ValidationStatus string

const (
	// ValidationPassed indicates all checks passed.
	ValidationPassed ValidationStatus = "pass"

	// ValidationWarned indicates checks passed but with warnings.
	ValidationWarned ValidationStatus = "warn"

	// ValidationFailed indicates one or more checks failed.
	ValidationFailed ValidationStatus = "fail"
)

// CostBreakdown holds cost totals for a validation report.
type CostBreakdown struct {
	// Total is the sum of all event costs in USD.
	Total float64 `json:"total"`

	// Monthly is the cost extrapolated to a 30-day period.
	Monthly float64 `json:"monthly_projection"`

	// ByService maps service name to total cost in USD.
	ByService map[string]float64 `json:"by_service"`

	// ByOperation maps "service/operation" to total cost in USD.
	ByOperation map[string]float64 `json:"by_operation"`
}

// QuotaCheck records the quota headroom for one service or operation.
type QuotaCheck struct {
	// Service is the AWS service name.
	Service string `json:"service"`

	// Operation is the API operation name, or empty for a service-level check.
	Operation string `json:"operation,omitempty"`

	// LimitRPS is the configured rate limit in requests per second.
	LimitRPS float64 `json:"limit_rps"`

	// PeakRPS is the observed peak rate from the event stream.
	PeakRPS float64 `json:"peak_rps"`

	// HeadroomPct is the remaining headroom as a percentage of LimitRPS.
	HeadroomPct float64 `json:"headroom_pct"`

	// Status is "pass", "warn" (headroom < 30%), or "fail" (headroom < 10%).
	Status string `json:"status"`
}

// ConsistencyIncident records a single InconsistentStateException event.
type ConsistencyIncident struct {
	// Sequence is the event sequence number.
	Sequence int64 `json:"sequence"`

	// Service is the AWS service name.
	Service string `json:"service"`

	// Operation is the API operation name.
	Operation string `json:"operation"`

	// ErrorCode is the structured AWS error code.
	ErrorCode string `json:"error_code"`
}

// Suggestion is an actionable optimisation recommendation.
type Suggestion struct {
	// Category is "cost", "quota", "consistency", or "reliability".
	Category string `json:"category"`

	// Message is the human-readable recommendation.
	Message string `json:"message"`

	// Impact is "low", "medium", or "high".
	Impact string `json:"impact"`
}

// ValidationReport is a JSON-serialisable analysis of a recorded event stream.
type ValidationReport struct {
	// StreamID identifies the event stream this report covers.
	StreamID string `json:"stream_id"`

	// PassFail is the overall validation outcome.
	PassFail ValidationStatus `json:"pass_fail"`

	// TotalEvents is the number of events analysed.
	TotalEvents int `json:"total_events"`

	// FailedEvents is the number of events that recorded an error.
	FailedEvents int `json:"failed_events"`

	// Cost contains the aggregated cost breakdown.
	Cost CostBreakdown `json:"cost"`

	// QuotaChecks lists quota headroom analysis per service/operation.
	QuotaChecks []QuotaCheck `json:"quota_checks"`

	// ConsistencyIncidents lists InconsistentStateException events.
	ConsistencyIncidents []ConsistencyIncident `json:"consistency_incidents"`

	// Suggestions lists actionable recommendations.
	Suggestions []Suggestion `json:"suggestions"`

	// Violations lists MaxCost or other intent violations.
	Violations []string `json:"violations,omitempty"`
}

// ValidateRecording analyses the event stream identified by streamID and
// returns a ValidationReport. The intent parameter is applied for constraint
// checking such as MaxCost violations.
func ValidateRecording(ctx context.Context, store *EventStore, streamID string, intent Intent) (*ValidationReport, error) {
	events, err := store.GetStream(ctx, streamID)
	if err != nil {
		return nil, err
	}

	report := &ValidationReport{
		StreamID:             streamID,
		QuotaChecks:          []QuotaCheck{},
		ConsistencyIncidents: []ConsistencyIncident{},
		Suggestions:          []Suggestion{},
		Violations:           []string{},
		Cost: CostBreakdown{
			ByService:   make(map[string]float64),
			ByOperation: make(map[string]float64),
		},
	}

	report.TotalEvents = len(events)

	// Collect per-service/operation timestamps for quota analysis.
	svcTimes := make(map[string][]time.Time) // "service" or "service/operation"

	var firstTime, lastTime time.Time

	for _, ev := range events {
		if ev.Error != "" {
			report.FailedEvents++
		}

		report.Cost.Total += ev.Cost
		report.Cost.ByService[ev.Service] += ev.Cost
		opKey := ev.Service + "/" + ev.Operation
		report.Cost.ByOperation[opKey] += ev.Cost

		if ev.ErrorCode == "InconsistentStateException" {
			report.ConsistencyIncidents = append(report.ConsistencyIncidents, ConsistencyIncident{
				Sequence:  ev.Sequence,
				Service:   ev.Service,
				Operation: ev.Operation,
				ErrorCode: ev.ErrorCode,
			})
		}

		svcTimes[ev.Service] = append(svcTimes[ev.Service], ev.Timestamp)
		svcTimes[opKey] = append(svcTimes[opKey], ev.Timestamp)

		if firstTime.IsZero() || ev.Timestamp.Before(firstTime) {
			firstTime = ev.Timestamp
		}
		if lastTime.IsZero() || ev.Timestamp.After(lastTime) {
			lastTime = ev.Timestamp
		}
	}

	// Monthly projection: extrapolate based on observed event rate.
	if len(events) > 1 && !firstTime.IsZero() && !lastTime.IsZero() {
		duration := lastTime.Sub(firstTime).Seconds()
		if duration > 0 {
			rate := float64(len(events)) / duration
			const secondsPerMonth = 30 * 24 * 3600
			report.Cost.Monthly = (report.Cost.Total / float64(len(events))) * rate * secondsPerMonth
		}
	}

	// Quota analysis against default rules.
	rules := defaultQuotaRules()
	for key, rule := range rules {
		times, ok := svcTimes[key]
		if !ok || len(times) == 0 {
			continue
		}
		peakRPS := computePeakRPS(times)
		headroom := 0.0
		if rule.Rate > 0 {
			headroom = (1 - peakRPS/rule.Rate) * 100
			if headroom < 0 {
				headroom = 0
			}
		}
		status := "pass"
		if headroom < 10 {
			status = "fail"
		} else if headroom < 30 {
			status = "warn"
		}

		parts := splitServiceOp(key)
		qc := QuotaCheck{
			Service:     parts[0],
			LimitRPS:    rule.Rate,
			PeakRPS:     peakRPS,
			HeadroomPct: headroom,
			Status:      status,
		}
		if len(parts) > 1 {
			qc.Operation = parts[1]
		}
		report.QuotaChecks = append(report.QuotaChecks, qc)
	}

	// Generate suggestions.
	if report.Cost.Monthly > 10 {
		report.Suggestions = append(report.Suggestions, Suggestion{
			Category: "cost",
			Message:  "Projected monthly cost exceeds $10; consider reducing request frequency or using cheaper storage tiers.",
			Impact:   "medium",
		})
	}
	if len(report.ConsistencyIncidents) > 0 {
		report.Suggestions = append(report.Suggestions, Suggestion{
			Category: "consistency",
			Message:  "Consistency incidents detected; add retry logic with exponential back-off for read-after-write patterns.",
			Impact:   "high",
		})
	}
	if report.FailedEvents > 0 {
		report.Suggestions = append(report.Suggestions, Suggestion{
			Category: "reliability",
			Message:  "Failed requests detected; review error codes and add appropriate error handling.",
			Impact:   "medium",
		})
	}
	if report.Cost.Total > 0 && len(report.Suggestions) == 0 {
		report.Suggestions = append(report.Suggestions, Suggestion{
			Category: "cost",
			Message:  "Monitor costs regularly as request volume grows.",
			Impact:   "low",
		})
	}

	// Intent violations.
	if intent.MaxCost > 0 && report.Cost.Total > intent.MaxCost {
		report.Violations = append(report.Violations,
			"total cost exceeds MaxCost limit",
		)
	}

	// Determine overall PassFail.
	report.PassFail = ValidationPassed
	for _, qc := range report.QuotaChecks {
		if qc.Status == "fail" {
			report.PassFail = ValidationFailed
			break
		}
	}
	if report.PassFail != ValidationFailed {
		for _, qc := range report.QuotaChecks {
			if qc.Status == "warn" {
				report.PassFail = ValidationWarned
				break
			}
		}
	}
	if report.FailedEvents > 0 {
		report.PassFail = ValidationFailed
	}

	return report, nil
}

// computePeakRPS computes the peak requests-per-second using a sliding 1-second window.
func computePeakRPS(times []time.Time) float64 {
	if len(times) == 0 {
		return 0
	}
	peak := 0
	for i := range times {
		window := 0
		for j := i; j < len(times); j++ {
			if times[j].Sub(times[i]) <= time.Second {
				window++
			} else {
				break
			}
		}
		if window > peak {
			peak = window
		}
	}
	return float64(peak)
}

// splitServiceOp splits a "service/operation" key into its parts.
// Returns a single-element slice when there is no slash.
func splitServiceOp(key string) []string {
	for i, ch := range key {
		if ch == '/' {
			return []string{key[:i], key[i+1:]}
		}
	}
	return []string{key}
}
