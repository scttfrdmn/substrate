package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// healthNamespace is the service name used by HealthPlugin.
const healthNamespace = "health"

// HealthPlugin provides a stub emulation of the AWS Health API.
// All operations return valid empty responses satisfying the SDK shape.
// No persistent state is required.
type HealthPlugin struct {
	logger Logger
}

// Name returns the service name "health".
func (p *HealthPlugin) Name() string { return healthNamespace }

// Initialize configures the HealthPlugin with the provided configuration.
func (p *HealthPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.logger = cfg.Logger
	return nil
}

// Shutdown is a no-op for HealthPlugin.
func (p *HealthPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Health JSON-target request to the appropriate stub handler.
func (p *HealthPlugin) HandleRequest(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "DescribeEvents":
		return p.describeEvents()
	case "DescribeEventDetails":
		return p.describeEventDetails()
	case "DescribeAffectedEntities":
		return p.describeAffectedEntities()
	case "DescribeEventAggregates":
		return p.describeEventAggregates()
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "HealthPlugin: unsupported operation " + req.Operation,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

func (p *HealthPlugin) describeEvents() (*AWSResponse, error) {
	out := map[string]interface{}{
		"Events":    []interface{}{},
		"NextToken": "",
	}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("health: marshal DescribeEvents: %w", err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
}

func (p *HealthPlugin) describeEventDetails() (*AWSResponse, error) {
	out := map[string]interface{}{
		"SuccessfulSet": []interface{}{},
		"FailedSet":     []interface{}{},
	}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("health: marshal DescribeEventDetails: %w", err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
}

func (p *HealthPlugin) describeAffectedEntities() (*AWSResponse, error) {
	out := map[string]interface{}{
		"Entities":  []interface{}{},
		"NextToken": "",
	}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("health: marshal DescribeAffectedEntities: %w", err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
}

func (p *HealthPlugin) describeEventAggregates() (*AWSResponse, error) {
	out := map[string]interface{}{
		"EventAggregates": []interface{}{},
		"NextToken":       "",
	}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("health: marshal DescribeEventAggregates: %w", err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
}
