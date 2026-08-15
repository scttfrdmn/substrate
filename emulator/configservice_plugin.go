package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// ConfigServicePlugin emulates AWS Config — the detective half of the control
// loop (#580).
//
// Every other AWS API substrate models answers "make this so". Config answers a
// different question: *is anything watching, and is anything compliant?* No
// service here could answer it before this plugin, so the one path in a consumer
// that checks whether its own controls are in place was the one path with no
// emulator coverage.
//
// The fidelity that matters is a real misconfiguration rather than a CRUD surface.
// DescribeConfigurationRecorders returning a recorder says **nothing** about
// whether that recorder is recording — that is
// DescribeConfigurationRecorderStatus.recording. Two states look identical to the
// operation most consumers call, and telling them apart is what this plugin is
// for. The same shape recurs in the ordering refusals: a recorder cannot start
// without a delivery channel, a channel cannot be created without a recorder, and
// a channel cannot be deleted while the recorder runs. A consumer's ordering bug
// becomes an observable refusal rather than a silent success.
//
// Rule and conformance-pack compliance is **seeded, never computed**. Evaluating a
// rule against resource state is workload-internal rather than an API observation,
// so per the scope boundary in CLAUDE.md it is exposed as a seedable value read
// from a control-plane endpoint at request time. Computing it would require
// hundreds of AWS-managed rule implementations, and — worse — would make a
// consumer's compliance assertion silently change meaning as unrelated plugins
// gained fidelity. PutEvaluations is the exception that proves the rule: a custom
// rule reporting its own result *is* an API observation, so what a caller submits
// is recorded and reported back.
//
// Requests reach here over the JSON 1.1 target protocol
// (X-Amz-Target: StarlingDoveService.{Op}); see the targetServiceAliases entry
// that maps that prefix, without which the plugin would be registered,
// unit-tested and unreachable.
type ConfigServicePlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// cfgsvcHandler handles one AWS Config operation.
type cfgsvcHandler func(*RequestContext, *AWSRequest) (*AWSResponse, error)

// Name returns the service name "config".
//
// This is the value the request parser resolves a Config request to — from the
// endpoint prefix, the host and the SigV4 signing name alike — and it is
// deliberately not the "configservice" file prefix, which exists only because
// emulator/config.go already owns substrate's own Config type.
func (p *ConfigServicePlugin) Name() string { return configServiceNamespace }

// Initialize configures the ConfigServicePlugin with the provided configuration.
func (p *ConfigServicePlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for ConfigServicePlugin.
func (p *ConfigServicePlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an AWS Config request to the first operation cluster
// that claims it.
//
// Each cluster lives in its own file (configservice_recorder.go and the rest) and
// owns its claim function, following OrganizationsPlugin: adding an operation
// touches one file rather than a switch every cluster shares. AWS Config has 97
// operations and substrate implements the detective-controls subset, so an
// unclaimed operation is answered with InvalidAction naming itself rather than
// with a bare refusal.
func (p *ConfigServicePlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	for _, claim := range []func(string) (cfgsvcHandler, bool){
		p.recorderOperation,
		p.channelOperation,
		p.ruleOperation,
		p.packOperation,
		p.tagOperation,
	} {
		if h, ok := claim(req.Operation); ok {
			return h(ctx, req)
		}
	}
	return nil, cfgsvcInvalidAction(req.Operation)
}

// cfgsvcJSONResponse marshals an AWS Config response body.
func cfgsvcJSONResponse(out interface{}, op string) (*AWSResponse, error) {
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("%s marshal: %w", op, err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
}

// cfgsvcEmptyResponse is the body for an operation the API model gives no output
// shape — PutConfigurationRecorder, Start/Stop, the deletes and the tag
// operations all answer 200 with an empty JSON object.
func cfgsvcEmptyResponse() *AWSResponse {
	return &AWSResponse{Body: []byte(`{}`), StatusCode: http.StatusOK}
}
