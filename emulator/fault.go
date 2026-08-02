package emulator

import (
	"math/rand" // nosemgrep
	"net/http"
	"strings"
	"sync"
	"time"
)

// FaultRule describes a single fault injection rule that can match requests
// by service and/or operation and inject either a latency delay or an error.
type FaultRule struct {
	// Service restricts the rule to a specific AWS service name (e.g. "s3").
	// An empty string matches all services.
	Service string `json:"service,omitempty"`

	// Operation restricts the rule to a specific AWS operation (e.g. "PutObject").
	// An empty string matches all operations.
	//
	// The name is the semantic operation for every service, S3 included: the
	// request parser resolves an S3 REST request to its operation name before
	// faults are evaluated, so a rule naming PutObject fires on PutObject and
	// not on UploadPart, which is also a PUT (#480). A rule naming a bare HTTP
	// method therefore no longer matches an S3 request.
	Operation string `json:"operation,omitempty"`

	// PathSuffix restricts the rule to requests whose path ends with this
	// string (e.g. ".parquet", or "/big.bin"). An empty string matches any path.
	//
	// This and the two matchers below select on the wire request rather than on
	// its resolved operation, which is how a rule expresses a distinction the
	// operation name does not carry: one key rather than every key, or one
	// header family rather than every request.
	PathSuffix string `json:"path_suffix,omitempty"`

	// QueryKey restricts the rule to requests carrying this query parameter
	// (e.g. "uploads", "uploadId", "partNumber"). An empty string matches any
	// request. The parameter's value is not compared, because presence is what
	// S3's sub-resource parameters signal, and they are what separate
	// operations that share a method and a path.
	QueryKey string `json:"query_key,omitempty"`

	// HeaderPrefix restricts the rule to requests carrying at least one header
	// whose name begins with this prefix, compared case-insensitively (e.g.
	// "x-amz-copy-source", "x-amz-server-side-encryption"). An empty string
	// matches any request.
	HeaderPrefix string `json:"header_prefix,omitempty"`

	// FaultType selects the kind of fault: "error" or "latency".
	FaultType string `json:"fault_type"`

	// ErrorCode is the AWS error code returned when FaultType is "error"
	// (e.g. "InternalError"). It reaches the client as sent: an injected error
	// is serialized in the same wire shape the target service's own errors use,
	// so an SDK recovers the code rather than falling back to the HTTP status.
	ErrorCode string `json:"error_code,omitempty"`

	// HTTPStatus is the HTTP status code returned with an injected error.
	// Defaults to 500 when zero.
	HTTPStatus int `json:"http_status,omitempty"`

	// ErrorMsg is the human-readable error message returned with an injected error.
	ErrorMsg string `json:"error_msg,omitempty"`

	// LatencyMs is the artificial delay in milliseconds injected when FaultType
	// is "latency".
	LatencyMs int `json:"latency_ms,omitempty"`

	// Probability is the fraction of matching requests that actually trigger the
	// fault, in the range [0.0, 1.0]. A value of 1.0 (the default) fires on
	// every matching request.
	//
	// The PRNG behind this is shared by every rule in a FaultConfig and is
	// rolled once per matching rule per request, so a fixed seed reproduces a
	// run only while the sequence of requests is unchanged. Adding a request
	// upstream, or a retry, shifts every later roll. Prefer Times for a bounded
	// outcome: it needs no roll at all. See #510.
	Probability float64 `json:"probability,omitempty"`

	// Times bounds how many matching requests the rule fires on. Zero means
	// one, not unlimited — a rule that fires on nothing is never what a caller
	// meant, and reading zero as unlimited turns a mistyped field into a test
	// that consumes the whole retry budget. A negative value means unlimited,
	// which is the behavior before this field existed and now has to be asked
	// for explicitly.
	//
	// The bound is what makes retry assertable: fail twice, then succeed, is
	// the outcome that distinguishes working retry from no retry, and an
	// unbounded rule can only ever produce failure.
	Times int `json:"times,omitempty"`

	// Fired counts the requests this rule has injected a fault into. The
	// controller maintains it; a value supplied when the rule is armed is kept
	// as the starting count, and GET /v1/fault/rules reports the running total.
	//
	// It exists because a rule that matches nothing produces exactly the same
	// passing test as a consumer's retry working. Asserting a non-zero count is
	// what tells those two apart.
	Fired int `json:"fired,omitempty"`
}

// remaining reports whether the rule may still fire. It is only meaningful for
// a rule that has already matched. A negative Times is unlimited; zero means
// one, per the field's documentation.
func (r FaultRule) remaining() bool {
	if r.Times < 0 {
		return true
	}
	limit := r.Times
	if limit == 0 {
		limit = 1
	}
	return r.Fired < limit
}

// FaultConfig holds the configuration for fault injection.
type FaultConfig struct {
	// Enabled gates fault injection. When false, InjectFault is a no-op.
	Enabled bool `json:"enabled"`

	// Rules is the ordered list of fault rules. Rules are evaluated in order;
	// the first matching rule that has not reached its Times bound fires.
	Rules []FaultRule `json:"rules"`
}

// FaultController injects configurable faults (errors and latency) into the
// Substrate request pipeline. It uses a seeded, non-global PRNG for
// deterministic fault firing in tests.
type FaultController struct {
	mu     sync.Mutex
	config FaultConfig
	rng    *rand.Rand
}

// NewFaultController creates a FaultController with the given configuration.
// seed controls the PRNG used for probabilistic fault firing; use a fixed seed
// in tests for determinism, subject to the caveat on [FaultRule.Probability]
// that makes a bounded [FaultRule.Times] the more reproducible choice.
func NewFaultController(cfg FaultConfig, seed int64) *FaultController {
	return &FaultController{
		config: cfg.withOwnedRules(),
		rng:    rand.New(rand.NewSource(seed)), //nolint:gosec // not used for cryptography
	}
}

// withOwnedRules returns cfg with a private copy of its rule slice, so the
// controller's fired counts are written to memory no caller shares.
//
// Without the copy, incrementing a count reaches into the caller's own slice:
// a test that arms the same FaultConfig value twice would find its second arming
// already spent, because the first run had mutated the literal it passed in.
func (c FaultConfig) withOwnedRules() FaultConfig {
	if c.Rules == nil {
		return c
	}
	c.Rules = append([]FaultRule(nil), c.Rules...)
	return c
}

// InjectFault evaluates the fault rules against the request. It returns a
// non-nil [*AWSError] when an error fault fires, and a non-zero [time.Duration]
// when a latency fault fires. Both values are zero when no rule matches or
// fault injection is disabled.
//
// The match, the probability roll and the fired-count increment all happen
// under one lock. That atomicity is the property the mechanism exists for:
// with Times set to 1, N concurrent requests must produce exactly one failure,
// which is not something a counter on the client side can arrange.
func (f *FaultController) InjectFault(_ *RequestContext, req *AWSRequest) (*AWSError, time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.config.Enabled {
		return nil, 0
	}

	for i := range f.config.Rules {
		rule := &f.config.Rules[i]
		if !ruleMatches(*rule, req) || !rule.remaining() {
			continue
		}
		p := rule.Probability
		if p <= 0 {
			p = 1.0
		}
		if f.rng.Float64() >= p {
			continue
		}
		switch rule.FaultType {
		case "latency":
			rule.Fired++
			return nil, time.Duration(rule.LatencyMs) * time.Millisecond
		case "error":
			rule.Fired++
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
// concurrently with InjectFault. Arming a rule again starts its Times bound
// over, because the incoming rules carry their own fired counts.
func (f *FaultController) UpdateConfig(cfg FaultConfig) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.config = cfg.withOwnedRules()
}

// GetConfig returns a snapshot of the current fault injection configuration,
// including each rule's fired count. The rule slice is copied, so a caller
// reading a count cannot race a concurrent InjectFault incrementing it. It is
// safe to call concurrently with InjectFault and UpdateConfig.
func (f *FaultController) GetConfig() FaultConfig {
	f.mu.Lock()
	defer f.mu.Unlock()
	cfg := f.config
	if f.config.Rules != nil {
		cfg.Rules = append([]FaultRule(nil), f.config.Rules...)
	}
	return cfg
}

// FaultsFired returns the total number of faults injected across all rules.
// A test that arms a rule and then observes success has proven nothing unless
// the fault actually fired, and a rule matching no request at all looks exactly
// like a consumer's retry working.
func (f *FaultController) FaultsFired() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	total := 0
	for _, rule := range f.config.Rules {
		total += rule.Fired
	}
	return total
}

// ruleMatches reports whether rule applies to req. Every non-empty matcher must
// match: Service and Operation select the API call, and PathSuffix, QueryKey
// and HeaderPrefix select a wire request within it.
func ruleMatches(rule FaultRule, req *AWSRequest) bool {
	if rule.Service != "" && rule.Service != req.Service {
		return false
	}
	if rule.Operation != "" && rule.Operation != req.Operation {
		return false
	}
	if rule.PathSuffix != "" && !strings.HasSuffix(req.Path, rule.PathSuffix) {
		return false
	}
	if rule.QueryKey != "" {
		if _, ok := req.Params[rule.QueryKey]; !ok {
			return false
		}
	}
	if rule.HeaderPrefix != "" && !hasHeaderPrefix(req.Headers, rule.HeaderPrefix) {
		return false
	}
	return true
}

// hasHeaderPrefix reports whether any header name begins with prefix, compared
// case-insensitively: a header name is case-insensitive on the wire, and the
// SDKs do not agree on a canonical spelling.
func hasHeaderPrefix(headers map[string]string, prefix string) bool {
	lower := strings.ToLower(prefix)
	for name := range headers {
		if strings.HasPrefix(strings.ToLower(name), lower) {
			return true
		}
	}
	return false
}
