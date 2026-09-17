package emulator

import (
	"errors"
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
	//
	// S3 adds two keys between those two levels, "s3/read" and "s3/write", which
	// carry its published per-prefix ceilings; a rule under either governs one
	// token bucket per accounting prefix rather than one for the whole service. A
	// rule under any other S3 key governs a single bucket, like every other
	// service's. See [s3RateRuleKey], [s3IsRateClassRuleKey] and
	// [QuotaController.resolveKey].
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
// When throttled it returns an [*AWSError]: ThrottlingException and HTTP 429 for
// every service but S3, and S3's own SlowDown and HTTP 503 for S3 (see
// [s3SlowDownError]). Requests with reqCtx.Metadata["replaying"]=true are always
// allowed.
//
// One rule can govern many token buckets. For nearly every rule the rule and the
// bucket are one and the same, so a rule's rate is the rate of whatever it names.
// S3's two rate-class rules are the exception: they carry ceilings AWS publishes
// per prefix, so each governs one token bucket per (S3 bucket, accounting prefix)
// pair (#818) — [s3PrefixBucketKey] composes the identity and
// [s3IsRateClassRuleKey] is the test. An S3 rule a caller wrote against an
// operation or against the service is not one of those ceilings and governs a
// single bucket like any other.
func (q *QuotaController) CheckQuota(reqCtx *RequestContext, req *AWSRequest) error {
	if !q.cfg.Enabled {
		return nil
	}
	if isReplaying(reqCtx) {
		return nil
	}

	now := q.tc.Now()

	q.mu.Lock()
	defer q.mu.Unlock()

	// Resolved under the lock, because UpdateConfig replaces the whole rule map
	// and a read outside it is a data race against that write.
	key := q.resolveKey(req)
	if key == "" {
		// No rule configured for this service — allow.
		return nil
	}

	// A class rule carries a ceiling AWS states per prefix, so it governs one token
	// bucket per prefix; every other rule, S3's included, governs one.
	perPrefix := req.Service == "s3" && s3IsRateClassRuleKey(key)
	bucketKey := key
	if perPrefix {
		bucketKey = s3PrefixBucketKey(key, req)
	}

	b, ok := q.buckets[bucketKey]
	if !ok {
		rule := q.cfg.Rules[key]
		b = &tokenBucket{
			tokens:   rule.Burst,
			lastFill: now,
			rate:     rule.Rate,
			burst:    rule.Burst,
		}
		q.buckets[bucketKey] = b
	}

	if b.take(now) {
		return nil
	}

	if req.Service == "s3" {
		if perPrefix {
			bucket, prefix := s3AccountingPrefix(req)
			return s3SlowDownError(s3PrefixExceededDetail(bucket, prefix))
		}
		return s3SlowDownError(s3RuleExceededDetail(key))
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

// resolveKey returns the most-specific quota key configured for the request, and
// an empty string when no rule matches.
//
// Most specific first: an operation key ("service/operation"), then — for S3 only
// — the request's rate class ("s3/read", "s3/write"), then the service key
// ("service"). The class sits between the two because AWS publishes S3's ceilings
// per class of HTTP method (#818), while a caller who wants to rate-limit one S3
// operation must still be able to say so and have it win.
//
// Callers must hold q.mu: the rule map is read here and replaced wholesale by
// [QuotaController.UpdateConfig].
func (q *QuotaController) resolveKey(req *AWSRequest) string {
	if opKey := req.Service + "/" + req.Operation; ruleExists(q.cfg.Rules, opKey) {
		return opKey
	}
	if req.Service == "s3" {
		if classKey := s3RateRuleKey(s3RateClassOf(req)); ruleExists(q.cfg.Rules, classKey) {
			return classKey
		}
	}
	if ruleExists(q.cfg.Rules, req.Service) {
		return req.Service
	}
	return ""
}

// ruleExists reports whether rules configures key.
func ruleExists(rules map[string]RateRule, key string) bool {
	_, ok := rules[key]
	return ok
}

// defaultQuotaRules returns the built-in rate limits that mirror AWS service
// quotas.
//
// The two S3 rules are the published per-prefix ceilings, keyed by rate class
// rather than by service or operation, and each governs one token bucket per
// accounting prefix — see [s3RateRuleKey] and this file's S3 companion for both
// derivations and for the figures' source (#818). They replace a single
// bucket-wide "s3" rule of 3500/5500 that accounted every prefix together, which
// is the accounting AWS's own guidance tells a caller to work around.
func defaultQuotaRules() map[string]RateRule {
	return map[string]RateRule{
		"iam":            {Rate: 100, Burst: 100},
		"iam/CreateUser": {Rate: 20, Burst: 20},
		"iam/DeleteUser": {Rate: 20, Burst: 20},
		"iam/CreateRole": {Rate: 20, Burst: 20},
		"iam/DeleteRole": {Rate: 20, Burst: 20},
		"sts":            {Rate: 100, Burst: 100},
		"sts/AssumeRole": {Rate: 50, Burst: 50},
		s3RateRuleKey(s3RateWrite): {
			Rate:  s3PrefixWriteRequestsPerSecond,
			Burst: s3PrefixWriteRequestsPerSecond,
		},
		s3RateRuleKey(s3RateRead): {
			Rate:  s3PrefixReadRequestsPerSecond,
			Burst: s3PrefixReadRequestsPerSecond,
		},
	}
}

// refusalErrorCode returns the AWS error code a pre-plugin gate refused with, for
// naming a metric after the code the caller actually saw. A refusal that is not
// an [*AWSError] has no code to report, and answers "InternalFailure" — the code
// [Server.writeError] serves such an error under, so the label and the wire agree.
func refusalErrorCode(err error) string {
	var awsErr *AWSError
	if errors.As(err, &awsErr) {
		return awsErr.Code
	}
	return "InternalFailure"
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
