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

// CEPlugin emulates the AWS Cost Explorer service.
// It derives cost data from the EventStore — no persistent state is required.
// When no EventStore is available, all operations return valid empty responses.
type CEPlugin struct {
	store  *EventStore
	state  StateManager
	logger Logger
}

// Name returns the service name "ce".
func (p *CEPlugin) Name() string { return ceNamespace }

// Initialize configures the CEPlugin with state, logger, and optional EventStore.
func (p *CEPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if s, ok := cfg.Options["event_store"]; ok {
		if es, ok := s.(*EventStore); ok {
			p.store = es
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

	if p.store != nil {
		start, end, parseErr := parseCEDateRange(input.TimePeriod)
		if parseErr == nil {
			summary, err := p.store.GetCostSummary(context.Background(), reqCtx.AccountID, start, end)
			if err != nil {
				p.logger.Error("ce: GetCostSummary failed", "error", err)
			} else if summary != nil {
				// Build one bucket per service (groups) and one total bucket.
				totalAmount := fmt.Sprintf("%.6f", summary.TotalCost)
				groups := make([]CEGroup, 0, len(summary.ByService))
				for svc, cost := range summary.ByService {
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
							"UnblendedCost": {Amount: totalAmount, Unit: "USD"},
						},
						Groups:    groups,
						Estimated: false,
					},
				}
			}
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
