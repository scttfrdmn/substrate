package substrate

import (
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// FaultRule describes a single fault injection rule that can match requests
// by service and/or operation and inject either a latency delay or an error.
type FaultRule struct {
	// Service restricts the rule to a specific AWS service name (e.g. "s3").
	// An empty string matches all services.
	Service string

	// Operation restricts the rule to a specific AWS operation (e.g. "PutObject").
	// An empty string matches all operations.
	Operation string

	// FaultType selects the kind of fault: "error" or "latency".
	FaultType string

	// ErrorCode is the AWS error code returned when FaultType is "error"
	// (e.g. "InternalError").
	ErrorCode string

	// HTTPStatus is the HTTP status code returned with an injected error.
	// Defaults to 500 when zero.
	HTTPStatus int

	// ErrorMsg is the human-readable error message returned with an injected error.
	ErrorMsg string

	// LatencyMs is the artificial delay in milliseconds injected when FaultType
	// is "latency".
	LatencyMs int

	// Probability is the fraction of matching requests that actually trigger the
	// fault, in the range [0.0, 1.0]. A value of 1.0 (the default) fires on
	// every matching request.
	Probability float64
}

// FaultConfig holds the configuration for fault injection.
type FaultConfig struct {
	// Enabled gates fault injection. When false, InjectFault is a no-op.
	Enabled bool

	// Rules is the ordered list of fault rules. Rules are evaluated in order;
	// the first matching rule fires.
	Rules []FaultRule
}

// FaultController injects configurable faults (errors and latency) into the
// Substrate request pipeline. It uses a seeded, non-global PRNG for
// deterministic fault firing in tests.
type FaultController struct {
	mu     sync.RWMutex
	config FaultConfig
	rng    *rand.Rand
}

// NewFaultController creates a FaultController with the given configuration.
// seed controls the PRNG used for probabilistic fault firing; use a fixed seed
// in tests for determinism.
func NewFaultController(cfg FaultConfig, seed int64) *FaultController {
	return &FaultController{
		config: cfg,
		rng:    rand.New(rand.NewSource(seed)), //nolint:gosec // not used for cryptography
	}
}

// InjectFault evaluates the fault rules against the request. It returns a
// non-nil [*AWSError] when an error fault fires, and a non-zero [time.Duration]
// when a latency fault fires. Both values are zero when no rule matches or
// fault injection is disabled.
func (f *FaultController) InjectFault(_ *RequestContext, req *AWSRequest) (*AWSError, time.Duration) {
	f.mu.RLock()
	cfg := f.config
	f.mu.RUnlock()

	if !cfg.Enabled {
		return nil, 0
	}

	for _, rule := range cfg.Rules {
		if !ruleMatches(rule, req) {
			continue
		}
		p := rule.Probability
		if p <= 0 {
			p = 1.0
		}
		f.mu.Lock()
		roll := f.rng.Float64()
		f.mu.Unlock()
		if roll >= p {
			continue
		}
		switch rule.FaultType {
		case "latency":
			return nil, time.Duration(rule.LatencyMs) * time.Millisecond
		case "error":
			status := rule.HTTPStatus
			if status == 0 {
				status = http.StatusInternalServerError
			}
			msg := rule.ErrorMsg
			if msg == "" {
				msg = "injected fault"
			}
			code := rule.ErrorCode
			if code == "" {
				code = "InternalError"
			}
			return &AWSError{
				Code:       code,
				Message:    msg,
				HTTPStatus: status,
			}, 0
		}
	}
	return nil, 0
}

// UpdateConfig replaces the fault injection configuration. It is safe to call
// concurrently with InjectFault.
func (f *FaultController) UpdateConfig(cfg FaultConfig) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.config = cfg
}

// ruleMatches reports whether rule applies to req based on Service and Operation
// filters.
func ruleMatches(rule FaultRule, req *AWSRequest) bool {
	if rule.Service != "" && rule.Service != req.Service {
		return false
	}
	if rule.Operation != "" && rule.Operation != req.Operation {
		return false
	}
	return true
}
