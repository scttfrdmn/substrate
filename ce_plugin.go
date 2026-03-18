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

// ec2ServiceKey is the AWS Cost Explorer service name for EC2 compute.
const ec2ServiceKey = "Amazon Elastic Compute Cloud - Compute"

func (p *CEPlugin) getCostAndUsage(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TimePeriod  CEDateInterval      `json:"TimePeriod"`
		Granularity string              `json:"Granularity"`
		Metrics     []string            `json:"Metrics"`
		GroupBy     []CEGroupDefinition `json:"GroupBy"`
		Filter      *CEFilter           `json:"Filter"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "MalformedData", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	// Use the caller-requested metric names; default to UnblendedCost.
	metricNames := input.Metrics
	if len(metricNames) == 0 {
		metricNames = []string{"UnblendedCost"}
	}
	buildMetrics := func(cost float64) map[string]CEMetric {
		m := make(map[string]CEMetric, len(metricNames))
		for _, name := range metricNames {
			m[name] = CEMetric{Amount: fmt.Sprintf("%.6f", cost), Unit: "USD"}
		}
		return m
	}

	var results []CECostResultByTime

	start, end, parseErr := parseCEDateRange(input.TimePeriod)
	if parseErr == nil {
		// Detect GroupBy TAG — group EC2 instance costs by a tag value.
		if len(input.GroupBy) > 0 && strings.EqualFold(input.GroupBy[0].Type, "TAG") {
			tagKey := ""
			if input.GroupBy[0].Key != nil {
				tagKey = *input.GroupBy[0].Key
			}
			// Only include EC2 when no service filter is present, or when the
			// filter explicitly includes the EC2 service key.
			includeEC2 := true
			if input.Filter != nil && input.Filter.Dimensions != nil &&
				strings.EqualFold(input.Filter.Dimensions.Key, "SERVICE") {
				includeEC2 = false
				for _, v := range input.Filter.Dimensions.Values {
					if v == ec2ServiceKey {
						includeEC2 = true
						break
					}
				}
			}

			byTag := make(map[string]float64)
			if includeEC2 {
				byTag = p.computeEC2UsageCostByTag(reqCtx.AccountID, start, end, tagKey)
			}

			var totalCost float64
			groups := make([]CEGroup, 0, len(byTag))
			for k, cost := range byTag {
				groups = append(groups, CEGroup{Keys: []string{k}, Metrics: buildMetrics(cost)})
				totalCost += cost
			}
			sort.Slice(groups, func(i, j int) bool { return groups[i].Keys[0] < groups[j].Keys[0] })

			results = []CECostResultByTime{{
				TimePeriod: input.TimePeriod,
				Total:      buildMetrics(totalCost),
				Groups:     groups,
				Estimated:  false,
			}}
		} else {
			// Default: group by SERVICE (existing behaviour).
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
			if ec2Cost := p.computeEC2UsageCost(reqCtx.AccountID, start, end); ec2Cost > 0 {
				bySvc[ec2ServiceKey] += ec2Cost
				totalCost += ec2Cost
			}

			groups := make([]CEGroup, 0, len(bySvc))
			for svc, cost := range bySvc {
				groups = append(groups, CEGroup{Keys: []string{svc}, Metrics: buildMetrics(cost)})
			}
			sort.Slice(groups, func(i, j int) bool { return groups[i].Keys[0] < groups[j].Keys[0] })

			results = []CECostResultByTime{{
				TimePeriod: input.TimePeriod,
				Total:      buildMetrics(totalCost),
				Groups:     groups,
				Estimated:  false,
			}}
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

// ec2InstanceCostInWindow returns the USD cost for a single EC2 instance
// for the time it was running within [queryStart, queryEnd).
// queryEnd must already be clamped to the simulated present.
func ec2InstanceCostInWindow(inst EC2Instance, queryStart, queryEnd time.Time) float64 {
	launchTime, err := time.Parse(time.RFC3339, inst.LaunchTime)
	if err != nil {
		return 0
	}
	// Stopped instances don't accrue compute cost.
	if inst.State.Code == 80 {
		return 0
	}
	runEnd := queryEnd
	if inst.TerminatedTime != "" {
		if t, err := time.Parse(time.RFC3339, inst.TerminatedTime); err == nil && t.Before(runEnd) {
			runEnd = t
		}
	}
	start := launchTime
	if queryStart.After(start) {
		start = queryStart
	}
	end := runEnd
	if queryEnd.Before(end) {
		end = queryEnd
	}
	if !start.Before(end) {
		return 0
	}
	hours := end.Sub(start).Hours()
	rate, ok := ec2HourlyRates[inst.InstanceType]
	if !ok {
		rate = ec2DefaultHourlyRate
	}
	return hours * rate
}

// clampedQueryEnd returns queryEnd capped at tc.Now() (or wall time) so
// in-flight instances don't accrue cost beyond the simulated present.
func (p *CEPlugin) clampedQueryEnd(queryEnd time.Time) time.Time {
	now := time.Now()
	if p.tc != nil {
		now = p.tc.Now()
	}
	if queryEnd.After(now) {
		return now
	}
	return queryEnd
}

// computeEC2UsageCost returns the estimated USD cost of all EC2 instances in
// the given account that were running during [queryStart, queryEnd).
func (p *CEPlugin) computeEC2UsageCost(accountID string, queryStart, queryEnd time.Time) float64 {
	if p.state == nil {
		return 0
	}
	queryEnd = p.clampedQueryEnd(queryEnd)
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
		total += ec2InstanceCostInWindow(inst, queryStart, queryEnd)
	}
	return total
}

// computeEC2UsageCostByTag returns per-tag-value EC2 costs grouped by tagKey.
// Map keys use AWS CE tag format: "TagKey$TagValue".  Instances without the
// tag are grouped under "TagKey$" (empty value), matching real AWS behaviour.
func (p *CEPlugin) computeEC2UsageCostByTag(accountID string, queryStart, queryEnd time.Time, tagKey string) map[string]float64 {
	result := make(map[string]float64)
	if p.state == nil {
		return result
	}
	queryEnd = p.clampedQueryEnd(queryEnd)
	if !queryStart.Before(queryEnd) {
		return result
	}
	keys, err := p.state.List(context.Background(), ec2Namespace, "instance:"+accountID+"/")
	if err != nil || len(keys) == 0 {
		return result
	}
	for _, key := range keys {
		data, err := p.state.Get(context.Background(), ec2Namespace, key)
		if err != nil || data == nil {
			continue
		}
		var inst EC2Instance
		if json.Unmarshal(data, &inst) != nil {
			continue
		}
		cost := ec2InstanceCostInWindow(inst, queryStart, queryEnd)
		if cost == 0 {
			continue
		}
		tagValue := ""
		for _, t := range inst.Tags {
			if t.Key == tagKey {
				tagValue = t.Value
				break
			}
		}
		result[tagKey+"$"+tagValue] += cost
	}
	return result
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
