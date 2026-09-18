package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// healthNamespace is the service name used by HealthPlugin.
const healthNamespace = "health"

// HealthEvent represents a seedable AWS Health event.
type HealthEvent struct {
	// Arn is the event ARN.
	Arn string `json:"arn"`

	// Service is the affected AWS service.
	Service string `json:"service"`

	// EventTypeCode identifies the event type.
	EventTypeCode string `json:"eventTypeCode"`

	// EventTypeCategory is "issue", "accountNotification", or "scheduledChange".
	EventTypeCategory string `json:"eventTypeCategory"`

	// Region is the affected region.
	Region string `json:"region"`

	// StatusCode is "open", "closed", or "upcoming".
	StatusCode string `json:"statusCode"`

	// StartTime is the event start as a Unix timestamp.
	StartTime float64 `json:"startTime,omitempty"`

	// Description is a human-readable description.
	Description string `json:"description,omitempty"`
}

// HealthPlugin provides emulation of the AWS Health API with seedable events.
// Events are seeded via the POST /v1/health/events control-plane endpoint.
type HealthPlugin struct {
	state  StateManager
	logger Logger
}

// Name returns the service name "health".
func (p *HealthPlugin) Name() string { return healthNamespace }

// Initialize configures the HealthPlugin with the provided configuration.
func (p *HealthPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	return nil
}

// Shutdown is a no-op for HealthPlugin.
func (p *HealthPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Health JSON-target request to the appropriate handler.
func (p *HealthPlugin) HandleRequest(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "DescribeEvents":
		return p.describeEvents()
	case "DescribeEventDetails":
		return p.describeEventDetails(req)
	case "DescribeAffectedEntities":
		return p.describeAffectedEntities()
	case "DescribeEventAggregates":
		return p.describeEventAggregates()
	default:
		return nil, unknownActionError(p.Name(), req.Operation)
	}
}

func (p *HealthPlugin) loadEvents() []HealthEvent {
	if p.state == nil {
		return nil
	}
	data, err := p.state.Get(context.Background(), healthNamespace, "events")
	if err != nil || data == nil {
		return nil
	}
	var events []HealthEvent
	if json.Unmarshal(data, &events) != nil {
		return nil
	}
	return events
}

func (p *HealthPlugin) describeEvents() (*AWSResponse, error) {
	events := p.loadEvents()
	if events == nil {
		events = []HealthEvent{}
	}
	out := map[string]interface{}{
		"Events":    events,
		"NextToken": "",
	}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("health: marshal DescribeEvents: %w", err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
}

// healthInvalidBody returns the refusal for a request body that will not parse (#1007).
//
// `API_DescribeEventDetails` publishes one operation-specific error, `UnsupportedLocale`/400, which
// describes a locale rather than a body. The code therefore comes from AWS Health's own Common Error
// Types page, which lists `ValidationError` at HTTP 400 and glosses it "The input doesn't meet the
// required format or constraints" — #950's step-2 rule for a class of caller error no operation page
// names. `MalformedHttpRequestException`/400 is on the same page and is the nearer-looking miss: it is
// specifically about a body that cannot be *decompressed* under the declared content encoding, which is
// a transport failure rather than malformed JSON, so using it would send a caller to check its
// `Content-Encoding` header over a syntax error in its own request.
func healthInvalidBody() *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    "The input doesn't meet the required format or constraints",
		HTTPStatus: http.StatusBadRequest,
	}
}

func (p *HealthPlugin) describeEventDetails(req *AWSRequest) (*AWSResponse, error) {
	// The body is parsed before the events are loaded, so a body that will not parse is refused without
	// reading state. `eventArns` is Required: Yes with Array Members 1–10, and that check is *not* added
	// here: substrate answers an empty successfulSet for an absent list, which is a separate divergence
	// from this one and is filed rather than folded in.
	var input struct {
		EventArns []string `json:"eventArns"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, healthInvalidBody()
	}

	events := p.loadEvents()

	var successSet []map[string]interface{}
	for _, ev := range events {
		for _, arn := range input.EventArns {
			if ev.Arn == arn {
				successSet = append(successSet, map[string]interface{}{
					"Event": ev,
					"EventDescription": map[string]string{
						"LatestDescription": ev.Description,
					},
				})
			}
		}
	}
	if successSet == nil {
		successSet = []map[string]interface{}{}
	}

	out := map[string]interface{}{
		"SuccessfulSet": successSet,
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
