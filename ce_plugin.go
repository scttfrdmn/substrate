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

// ec2HourlyRates maps EC2 instance types to their on-demand hourly USD price.
// Values are approximate us-east-1 Linux on-demand rates.  Unknown types fall
// back to ec2DefaultHourlyRate.
var ec2HourlyRates = map[string]float64{
	"t3.nano":    0.0052,
	"t3.micro":   0.0104,
	"t3.small":   0.0208,
	"t3.medium":  0.0416,
	"t3.large":   0.0832,
	"t3.xlarge":  0.1664,
	"t3.2xlarge": 0.3328,
	"m5.large":   0.096,
	"m5.xlarge":  0.192,
	"m5.2xlarge": 0.384,
	"m5.4xlarge": 0.768,
	"m7i.large":  0.192,
	"m7i.xlarge": 0.384,
	"c5.large":   0.085,
	"c5.xlarge":  0.170,
	"r5.large":   0.126,
	"r5.xlarge":  0.252,
}

// ec2DefaultHourlyRate is used for instance types not in ec2HourlyRates.
const ec2DefaultHourlyRate = 0.096

// CEPlugin emulates the AWS Cost Explorer service.
// It derives cost data from the EventStore — no persistent state is required.
// When no EventStore is available, all operations return valid empty responses.
type CEPlugin struct {
	store  *EventStore
	state  StateManager
	tc     *TimeController
	logger Logger
}

// Name returns the service name "ce".
func (p *CEPlugin) Name() string { return ceNamespace }

// Initialize configures the CEPlugin with state, logger, and optional EventStore
// and TimeController.
func (p *CEPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if s, ok := cfg.Options["event_store"]; ok {
		if es, ok := s.(*EventStore); ok {
			p.store = es
		}
	}
	if t, ok := cfg.Options["time_controller"]; ok {
		if tc, ok := t.(*TimeController); ok {
			p.tc = tc
		}
	}
	return nil
}

// Shutdown is a no-op for CEPlugin.
func (p *CEPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Cost Explorer JSON-target request to the appropriate handler.
func (p *CEPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "GetCostAndUsage":
		return p.getCostAndUsage(ctx, req)
	case "GetCostForecast":
		return p.getCostForecast(ctx, req)
	case "GetDimensionValues":
		return p.getDimensionValues(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "CEPlugin: unsupported operation " + req.Operation,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

func (p *CEPlugin) getCostAndUsage(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TimePeriod  CEDateInterval `json:"TimePeriod"`
		Granularity string         `json:"Granularity"`
		Metrics     []string       `json:"Metrics"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "MalformedData", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	var results []CECostResultByTime

	start, end, parseErr := parseCEDateRange(input.TimePeriod)
	if parseErr == nil {
		// API-call costs from the event store.
		bySvc := make(map[string]float64)
		var totalCost float64
		if p.store != nil {
			summary, err := p.store.GetCostSummary(context.Background(), reqCtx.AccountID, start, end)
			if err != nil {
				p.logger.Error("ce: GetCostSummary failed", "error", err)
			} else if summary != nil {
				for svc, cost := range summary.ByService {
					bySvc[svc] += cost
				}
				totalCost += summary.TotalCost
			}
		}

		// EC2 compute usage cost derived from instance run-time in state.
		const ec2ServiceKey = "Amazon Elastic Compute Cloud - Compute"
		if ec2Cost := p.computeEC2UsageCost(reqCtx.AccountID, start, end); ec2Cost > 0 {
			bySvc[ec2ServiceKey] += ec2Cost
			totalCost += ec2Cost
		}

		groups := make([]CEGroup, 0, len(bySvc))
		for svc, cost := range bySvc {
			groups = append(groups, CEGroup{
				Keys: []string{svc},
				Metrics: map[string]CEMetric{
					"UnblendedCost": {Amount: fmt.Sprintf("%.6f", cost), Unit: "USD"},
				},
			})
		}
		sort.Slice(groups, func(i, j int) bool { return groups[i].Keys[0] < groups[j].Keys[0] })

		results = []CECostResultByTime{
			{
				TimePeriod: input.TimePeriod,
				Total: map[string]CEMetric{
					"UnblendedCost": {Amount: fmt.Sprintf("%.6f", totalCost), Unit: "USD"},
				},
				Groups:    groups,
				Estimated: false,
			},
		}
	}

	if results == nil {
		results = []CECostResultByTime{}
	}

	out := map[string]interface{}{
		"ResultsByTime":            results,
		"DimensionValueAttributes": []interface{}{},
	}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("ce: marshal GetCostAndUsage: %w", err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
}

func (p *CEPlugin) getCostForecast(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TimePeriod  CEDateInterval `json:"TimePeriod"`
		Metric      string         `json:"Metric"`
		Granularity string         `json:"Granularity"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "MalformedData", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	var totalAmount float64

	if p.store != nil {
		forecast, err := p.store.GetCostForecast(context.Background(), reqCtx.AccountID, "", 30, 7, 2.0)
		if err != nil {
			p.logger.Error("ce: GetCostForecast failed", "error", err)
		} else if forecast != nil {
			totalAmount = forecast.ProjectedCost
		}
	}

	out := map[string]interface{}{
		"Total": CEMetric{
			Amount: fmt.Sprintf("%.6f", totalAmount),
			Unit:   "USD",
		},
		"ForecastResultsByTime": []interface{}{},
	}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("ce: marshal GetCostForecast: %w", err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
}

func (p *CEPlugin) getDimensionValues(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TimePeriod CEDateInterval `json:"TimePeriod"`
		Dimension  string         `json:"Dimension"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "MalformedData", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	// Derive unique service names from the event store.
	seen := make(map[string]struct{})
	if p.store != nil {
		events, err := p.store.GetEvents(context.Background(), EventFilter{AccountID: reqCtx.AccountID})
		if err == nil {
			for _, ev := range events {
				if ev.Service != "" {
					seen[ev.Service] = struct{}{}
				}
			}
		}
	}

	values := make([]map[string]interface{}, 0, len(seen))
	for svc := range seen {
		values = append(values, map[string]interface{}{
			"Value":      svc,
			"Attributes": map[string]string{},
		})
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i]["Value"].(string) < values[j]["Value"].(string)
	})

	out := map[string]interface{}{
		"DimensionValues": values,
		"ReturnSize":      len(values),
		"TotalSize":       len(values),
	}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("ce: marshal GetDimensionValues: %w", err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
}

// computeEC2UsageCost returns the estimated USD cost of all EC2 instances in
// the given account that were running during [queryStart, queryEnd).  It reads
// instance records directly from state so costs accrue even when no API calls
// are recorded in the event store.  The upper bound of each instance's run time
// is capped at now (from the TimeController if set, otherwise wall-clock time)
// so that in-flight instances don't accrue cost beyond the simulated present.
func (p *CEPlugin) computeEC2UsageCost(accountID string, queryStart, queryEnd time.Time) float64 {
	if p.state == nil {
		return 0
	}
	now := time.Now()
	if p.tc != nil {
		now = p.tc.Now()
	}
	// Clamp query end to now so we don't project future cost.
	if queryEnd.After(now) {
		queryEnd = now
	}
	if !queryStart.Before(queryEnd) {
		return 0
	}

	keys, err := p.state.List(context.Background(), ec2Namespace, "instance:"+accountID+"/")
	if err != nil || len(keys) == 0 {
		return 0
	}

	var total float64
	for _, key := range keys {
		data, err := p.state.Get(context.Background(), ec2Namespace, key)
		if err != nil || data == nil {
			continue
		}
		var inst EC2Instance
		if json.Unmarshal(data, &inst) != nil {
			continue
		}
		launchTime, err := time.Parse(time.RFC3339, inst.LaunchTime)
		if err != nil {
			continue
		}
		// Determine when this instance stopped accruing cost.
		runEnd := queryEnd
		if inst.TerminatedTime != "" {
			if t, err := time.Parse(time.RFC3339, inst.TerminatedTime); err == nil && t.Before(runEnd) {
				runEnd = t
			}
		} else if inst.State.Code == 80 { // stopped
			// Stopped instances don't accrue compute cost.
			continue
		}
		// Compute overlap of [launchTime, runEnd) with [queryStart, queryEnd).
		start := launchTime
		if queryStart.After(start) {
			start = queryStart
		}
		end := runEnd
		if queryEnd.Before(end) {
			end = queryEnd
		}
		if !start.Before(end) {
			continue
		}
		hours := end.Sub(start).Hours()
		rate, ok := ec2HourlyRates[inst.InstanceType]
		if !ok {
			rate = ec2DefaultHourlyRate
		}
		total += hours * rate
	}
	return total
}

// parseCEDateRange parses a CEDateInterval into time.Time values.
func parseCEDateRange(interval CEDateInterval) (start, end time.Time, err error) {
	const layout = "2006-01-02"
	if interval.Start != "" {
		start, err = time.Parse(layout, strings.TrimSpace(interval.Start))
		if err != nil {
			return
		}
	}
	if interval.End != "" {
		end, err = time.Parse(layout, strings.TrimSpace(interval.End))
	}
	return
}
