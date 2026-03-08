package substrate

import (
	"context"
	"strings"
	"time"
)

// CostConfig holds configuration for the cost controller.
type CostConfig struct {
	// Enabled gates cost recording. When false, CostForRequest always returns
	// 0.0.
	Enabled bool

	// Overrides maps "service/operation" or "service" keys to USD per request,
	// replacing the built-in pricing table entries.
	Overrides map[string]float64
}

// CostController computes per-request estimated AWS cost in USD. It is
// stateless after initialisation: CostForRequest is a pure lookup with no
// side effects, making it fully replay-safe.
type CostController struct {
	table map[string]float64
}

// NewCostController creates a CostController from cfg. When cfg.Enabled is
// false, all lookups return 0.0. Overrides in cfg take precedence over the
// built-in pricing table.
func NewCostController(cfg CostConfig) *CostController {
	if !cfg.Enabled {
		return &CostController{table: make(map[string]float64)}
	}

	table := defaultCostTable()
	for k, v := range cfg.Overrides {
		table[k] = v
	}

	return &CostController{table: table}
}

// CostForRequest returns the estimated USD cost for the given request. It
// first checks for an operation-specific key ("service/operation"), then falls
// back to a service-level key ("service"), and finally returns 0.0 when no
// entry matches.
func (c *CostController) CostForRequest(req *AWSRequest) float64 {
	opKey := strings.ToLower(req.Service) + "/" + req.Operation
	if cost, ok := c.table[opKey]; ok {
		return cost
	}
	svcKey := strings.ToLower(req.Service)
	if cost, ok := c.table[svcKey]; ok {
		return cost
	}
	return 0.0
}

// defaultCostTable returns the built-in per-request pricing table. IAM and
// STS are free. S3, DynamoDB, and Lambda values are pre-populated so those
// services get cost tracking as soon as their plugins land.
func defaultCostTable() map[string]float64 {
	return map[string]float64{
		"iam":              0.0,
		"sts":              0.0,
		"s3/PutObject":     0.000005,
		"s3/GetObject":     0.0000004,
		"dynamodb/GetItem": 0.00000025,
		"lambda/Invoke":    0.0000002,
	}
}

// CostSummary holds aggregated cost information for a set of events.
type CostSummary struct {
	// AccountID is the AWS account this summary covers.
	AccountID string

	// TotalCost is the sum of all event costs in USD.
	TotalCost float64

	// ByService maps service name to total cost in USD.
	ByService map[string]float64

	// ByOperation maps "service/operation" to total cost in USD.
	ByOperation map[string]float64

	// RequestCount is the number of matching events.
	RequestCount int64

	// StartTime is the earliest event timestamp included (zero if unbounded).
	StartTime time.Time

	// EndTime is the latest event timestamp included (zero if unbounded).
	EndTime time.Time
}

// GetCostSummary returns aggregated cost data for accountID in the half-open
// interval [start, end). Zero values for start and end are treated as
// unbounded. Events are fetched from the store using [EventStore.GetEvents].
func (e *EventStore) GetCostSummary(ctx context.Context, accountID string, start, end time.Time) (*CostSummary, error) {
	filter := EventFilter{
		AccountID: accountID,
		StartTime: start,
		EndTime:   end,
	}

	events, err := e.GetEvents(ctx, filter)
	if err != nil {
		return nil, err
	}

	summary := &CostSummary{
		AccountID:   accountID,
		ByService:   make(map[string]float64),
		ByOperation: make(map[string]float64),
		StartTime:   start,
		EndTime:     end,
	}

	for _, ev := range events {
		summary.TotalCost += ev.Cost
		summary.RequestCount++
		summary.ByService[ev.Service] += ev.Cost
		opKey := ev.Service + "/" + ev.Operation
		summary.ByOperation[opKey] += ev.Cost
	}

	return summary, nil
}
