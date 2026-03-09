package substrate

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// QuotaConfig holds configuration for the quota controller.
type QuotaConfig struct {
	// Enabled gates quota enforcement. When false, all requests pass regardless
	// of rate.
	Enabled bool

	// Rules maps a service key (e.g. "iam") or operation key (e.g.
	// "iam/CreateUser") to a rate rule. Operation-specific rules take
	// precedence over service-level rules.
	Rules map[string]RateRule
}

// RateRule defines a token-bucket rate limit for a service or operation.
type RateRule struct {
	// Rate is the sustained token replenishment rate in tokens per second.
	Rate float64

	// Burst is the maximum number of tokens that may accumulate (initial
	// capacity and maximum bucket size).
	Burst float64
}

// tokenBucket is a single token-bucket instance.
type tokenBucket struct {
	tokens   float64
	lastFill time.Time
	rate     float64
	burst    float64
}

// take attempts to consume one token. It returns true when the token is
// granted and false when the bucket is exhausted.
func (b *tokenBucket) take(now time.Time) bool {
	elapsed := now.Sub(b.lastFill).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.lastFill = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// QuotaController enforces configurable AWS service rate limits using
// per-key token buckets. Time is sourced from the provided [TimeController]
// so tests can advance the clock deterministically.
type QuotaController struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
	cfg     QuotaConfig
	tc      *TimeController
}

// NewQuotaController creates a QuotaController with the given configuration
// and time source.
func NewQuotaController(cfg QuotaConfig, tc *TimeController) *QuotaController {
	return &QuotaController{
		buckets: make(map[string]*tokenBucket),
		cfg:     cfg,
		tc:      tc,
	}
}

// CheckQuota enforces the rate limit for the request's service/operation.
// It returns nil when the request is allowed or quota is disabled.
// When throttled it returns an [*AWSError] with code ThrottlingException and
// HTTP 429. Requests with reqCtx.Metadata["replaying"]=true are always
// allowed.
func (q *QuotaController) CheckQuota(reqCtx *RequestContext, req *AWSRequest) error {
	if !q.cfg.Enabled {
		return nil
	}
	if isReplaying(reqCtx) {
		return nil
	}

	now := q.tc.Now()
	key := q.resolveKey(req.Service, req.Operation)
	if key == "" {
		// No rule configured for this service — allow.
		return nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	b, ok := q.buckets[key]
	if !ok {
		rule := q.cfg.Rules[key]
		b = &tokenBucket{
			tokens:   rule.Burst,
			lastFill: now,
			rate:     rule.Rate,
			burst:    rule.Burst,
		}
		q.buckets[key] = b
	}

	if b.take(now) {
		return nil
	}

	return &AWSError{
		Code: "ThrottlingException",
		Message: fmt.Sprintf(
			"rate exceeded for %s/%s", req.Service, req.Operation,
		),
		HTTPStatus: http.StatusTooManyRequests,
	}
}

// UpdateConfig replaces the quota rules and discards all existing token buckets.
// It is safe to call concurrently with CheckQuota.
func (q *QuotaController) UpdateConfig(cfg QuotaConfig) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.cfg = cfg
	q.buckets = make(map[string]*tokenBucket)
}

// resolveKey returns the most-specific quota key for the given service and
// operation. Operation-specific keys ("service/operation") take precedence
// over service-level keys ("service"). An empty string is returned when no
// rule matches.
func (q *QuotaController) resolveKey(service, operation string) string {
	opKey := service + "/" + operation
	if _, ok := q.cfg.Rules[opKey]; ok {
		return opKey
	}
	if _, ok := q.cfg.Rules[service]; ok {
		return service
	}
	return ""
}

// defaultQuotaRules returns the built-in rate limits that mirror AWS service
// quotas. S3 prefix-level limits are deferred to TODO(#22).
func defaultQuotaRules() map[string]RateRule {
	return map[string]RateRule{
		"iam":            {Rate: 100, Burst: 100},
		"iam/CreateUser": {Rate: 20, Burst: 20},
		"iam/DeleteUser": {Rate: 20, Burst: 20},
		"iam/CreateRole": {Rate: 20, Burst: 20},
		"iam/DeleteRole": {Rate: 20, Burst: 20},
		"sts":            {Rate: 100, Burst: 100},
		"sts/AssumeRole": {Rate: 50, Burst: 50},
		"s3":             {Rate: 3500, Burst: 5500},
		"s3/GetObject":   {Rate: 5500, Burst: 5500},
	}
}

// isReplaying reports whether the request context carries a replaying flag.
func isReplaying(reqCtx *RequestContext) bool {
	if reqCtx.Metadata == nil {
		return false
	}
	v, ok := reqCtx.Metadata["replaying"]
	if !ok {
		return false
	}
	b, _ := v.(bool)
	return b
}
