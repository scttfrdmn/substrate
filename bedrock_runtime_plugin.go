package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// bedrockRuntimeNamespace is the state namespace for Amazon Bedrock Runtime.
const bedrockRuntimeNamespace = "bedrock-runtime"

// BedrockRuntimePlugin emulates the Amazon Bedrock Runtime service.
// It handles ApplyGuardrail for the bedrock-runtime host, supporting
// pass-through (action NONE) and blocklist-based intervention
// (action GUARDRAIL_INTERVENED). Guardrails are auto-created on first use.
type BedrockRuntimePlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "bedrock-runtime".
func (p *BedrockRuntimePlugin) Name() string { return bedrockRuntimeNamespace }

// Initialize sets up the BedrockRuntimePlugin with the provided configuration.
func (p *BedrockRuntimePlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for BedrockRuntimePlugin.
func (p *BedrockRuntimePlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Bedrock Runtime request to the appropriate handler.
func (p *BedrockRuntimePlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, guardrailID, version := parseBedrockRuntimeOperation(req.Operation, req.Path)
	switch op {
	case "ApplyGuardrail":
		return p.applyGuardrail(ctx, req, guardrailID, version)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "BedrockRuntimePlugin: unsupported path " + req.Path,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// parseBedrockRuntimeOperation maps an HTTP method and path to a Bedrock Runtime
// operation name plus guardrailID and version.
func parseBedrockRuntimeOperation(method, path string) (op, guardrailID, version string) {
	// /guardrail/{guardrailIdentifier}/version/{guardrailVersion}/apply
	rest := strings.TrimPrefix(path, "/")
	if !strings.HasPrefix(rest, "guardrail/") {
		return "", "", ""
	}
	rest = strings.TrimPrefix(rest, "guardrail/")
	slashIdx := strings.IndexByte(rest, '/')
	if slashIdx < 0 {
		return "", "", ""
	}
	gID := rest[:slashIdx]
	rest = rest[slashIdx+1:]
	if !strings.HasPrefix(rest, "version/") {
		return "", "", ""
	}
	rest = strings.TrimPrefix(rest, "version/")
	slashIdx = strings.IndexByte(rest, '/')
	if slashIdx < 0 {
		return "", "", ""
	}
	ver := rest[:slashIdx]
	rest = rest[slashIdx+1:]
	if rest == "apply" && method == "POST" {
		return "ApplyGuardrail", gID, ver
	}
	return "", "", ""
}

func (p *BedrockRuntimePlugin) applyGuardrail(ctx *RequestContext, req *AWSRequest, guardrailID, _ string) (*AWSResponse, error) {
	var body struct {
		Source  string `json:"source"`
		Content []struct {
			Text *struct {
				Text string `json:"text"`
			} `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "ValidationException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	// Extract input text from first text content item.
	inputText := ""
	for _, item := range body.Content {
		if item.Text != nil {
			inputText = item.Text.Text
			break
		}
	}

	goCtx := context.Background()
	blocklistKey := "guardrail:" + ctx.AccountID + "/" + guardrailID + "/blocklist"

	// Auto-create the guardrail state entry on first use (empty blocklist).
	data, err := p.state.Get(goCtx, bedrockRuntimeNamespace, blocklistKey)
	if err != nil || data == nil {
		// Auto-register with empty blocklist.
		empty, _ := json.Marshal([]string{})
		if putErr := p.state.Put(goCtx, bedrockRuntimeNamespace, blocklistKey, empty); putErr != nil {
			return nil, fmt.Errorf("applyGuardrail: put blocklist: %w", putErr)
		}
		data = empty
	}

	var blocklist []string
	_ = json.Unmarshal(data, &blocklist)

	usage := map[string]int{
		"topicPolicyUnitsProcessed":                    0,
		"contentPolicyUnitsProcessed":                  1,
		"wordPolicyUnitsProcessed":                     0,
		"sensitiveInformationPolicyUnitsProcessed":     0,
		"sensitiveInformationPolicyFreeUnitsProcessed": 0,
		"contextualGroundingPolicyUnitsProcessed":      0,
	}

	// Check blocklist.
	for _, term := range blocklist {
		if strings.Contains(inputText, term) {
			return bedrockRuntimeJSONResponse(http.StatusOK, map[string]interface{}{
				"action": "GUARDRAIL_INTERVENED",
				"outputs": []map[string]string{
					{"text": "Sorry, I can't help with that."},
				},
				"assessments": []map[string]interface{}{
					{
						"topicPolicy": map[string]interface{}{
							"topics": []map[string]string{
								{"name": "blocked-topic", "type": "DENY", "action": "BLOCKED"},
							},
						},
					},
				},
				"usage": usage,
			})
		}
	}

	// Pass-through: echo input text.
	return bedrockRuntimeJSONResponse(http.StatusOK, map[string]interface{}{
		"action": "NONE",
		"outputs": []map[string]string{
			{"text": inputText},
		},
		"assessments": []interface{}{},
		"usage":       usage,
	})
}

// bedrockRuntimeJSONResponse serializes v to JSON and returns an AWSResponse.
func bedrockRuntimeJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("bedrock-runtime json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
