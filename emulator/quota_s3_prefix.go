package emulator

// S3's request-rate ceilings are published per prefix, not per bucket, and they
// differ between reads and writes. This file holds the two derivations that lets
// [QuotaController.CheckQuota] account for them that way: which of S3's two
// published ceilings a request counts against, and which prefix within the bucket
// it counts against.
//
// AWS states both figures in one sentence, in *Best practices design patterns:
// optimizing Amazon S3 performance* (verbatim):
//
//	your application can achieve at least 3,500 PUT/COPY/POST/DELETE or 5,500
//	GET/HEAD requests per second per partitioned Amazon S3 prefix. There are no
//	limits to the number of prefixes in a bucket.
//
// Accounting per bucket, which is what substrate did before #818, gets both
// directions wrong: it throttles a caller who spread writes across prefixes — the
// very thing the page above tells them to do — and it does not throttle a caller
// hammering one prefix, whose real ceiling is that of the prefix alone. A test
// asserting that parallelising across prefixes lifts the ceiling could not observe
// it either way.
//
// # What a prefix is here, and why substrate has to choose
//
// AWS defines a prefix as *"a string of characters at the beginning of the object
// key name"* of any length (*Organizing objects using prefixes*) — so a key belongs
// to arbitrarily many prefixes at once, and the page is explicit that prefixes are
// not directories. Which prefix is a *partition* boundary is decided by S3 itself:
// the performance page says the scaling "happens gradually and is not
// instantaneous", and AWS publishes neither where a partition splits nor when it
// repartitions. There is therefore nothing to copy, and reproducing S3's adaptive
// partitioning is out of scope besides — it is not observable through an API call.
//
// So the accounting prefix is substrate's own rule, stated here rather than left
// implicit: **the object key up to and including its first `/`**, and the empty
// prefix — the bucket root — for a key with no `/` and for an operation that names
// no key at all. Two reasons for that boundary rather than a deeper one:
//
//   - It is the boundary AWS's own parallelisation example uses: *"if you create 10
//     prefixes in an Amazon S3 bucket to parallelize reads, you could scale your
//     read performance to 55,000 read requests per second"*. A rule that split
//     deeper would report that headroom for keys AWS would have kept in one
//     partition.
//   - It is the coarsest choice short of the bucket, so substrate never claims more
//     headroom than S3 would give. A caller whose test passes against substrate's
//     accounting is not relying on a split S3 has not made; the error is in the
//     direction of throttling sooner, which is the safe direction for a consumer
//     whose retry loop is what this models.
//
// The rule is documented for consumers in `docs/services.md` under S3, stated as
// substrate's reading there too.

import (
	"fmt"
	"net/http"
	"strings"
)

// Substrate's per-prefix request-rate ceilings for S3, in requests per second.
//
// Both figures are AWS's, quoted in this file's preamble: 3,500
// PUT/COPY/POST/DELETE and 5,500 GET/HEAD per second per partitioned prefix. AWS
// publishes no burst allowance alongside them, so [defaultQuotaRules] uses one
// second's worth as the bucket depth — substrate's reading, and the smallest one
// that lets a caller who is inside the published rate never be refused.
const (
	// s3PrefixWriteRequestsPerSecond is the PUT/COPY/POST/DELETE ceiling.
	s3PrefixWriteRequestsPerSecond = 3500

	// s3PrefixReadRequestsPerSecond is the GET/HEAD ceiling.
	s3PrefixReadRequestsPerSecond = 5500
)

// s3RateClass names which of S3's two published per-prefix ceilings a request
// counts against. AWS states the ceiling per class of HTTP method rather than per
// operation, so the class — not the operation — is what a rule is keyed on.
type s3RateClass string

const (
	// s3RateRead is the GET/HEAD class.
	s3RateRead s3RateClass = "read"

	// s3RateWrite is the PUT/COPY/POST/DELETE class.
	s3RateWrite s3RateClass = "write"
)

// s3RateRuleKey renders the [QuotaConfig.Rules] key that configures one class's
// ceiling: "s3/read" and "s3/write".
//
// The two keys occupy the operation half of the "service/operation" key space
// without naming an operation, because the quantity AWS publishes is per class and
// there is no S3 operation to hang it on. Nothing collides: no S3 operation is
// named "read" or "write", and an operation-specific rule still wins over its
// class (see [QuotaController.resolveKey]), so a caller who wants to rate-limit
// `s3/PutObject` alone still can.
func s3RateRuleKey(class s3RateClass) string {
	return "s3/" + string(class)
}

// s3IsRateClassRuleKey reports whether key is one of the two class keys, and so
// whether the rule under it is accounted per prefix.
//
// Only a class rule is: it carries a ceiling AWS states per prefix, so accounting it
// any other way would misreport the quantity. A rule a caller wrote against an
// operation or against the service is substrate's own throttle rather than one of
// AWS's published figures, and it governs a single bucket the way every other
// service's rules do — a caller who writes `s3: {rate: 5}` means five requests per
// second, not five per prefix.
func s3IsRateClassRuleKey(key string) bool {
	return key == s3RateRuleKey(s3RateRead) || key == s3RateRuleKey(s3RateWrite)
}

// s3RateClassOf classifies a request into the class whose ceiling it counts
// against.
//
// The verb decides it, because that is how AWS states the two figures: GET and
// HEAD are reads and everything else is a write. A COPY needs no arm of its own —
// CopyObject is a PUT carrying x-amz-copy-source, so it is already in the write
// class where AWS's sentence puts it.
//
// An in-process [AWSRequest] built by hand carries no verb (see [requestMethod]),
// so the operation name is the fallback. Both classifiers have to agree, which is
// what [s3RateClassOfOperation] documents.
func s3RateClassOf(req *AWSRequest) s3RateClass {
	switch requestMethod(req) {
	case http.MethodGet, http.MethodHead:
		return s3RateRead
	case "":
		return s3RateClassOfOperation(req.Operation)
	default:
		return s3RateWrite
	}
}

// s3RateClassOfOperation classifies an S3 operation *name* into the same two
// classes [s3RateClassOf] derives from a verb.
//
// It exists for the two inputs that carry a name and no verb: an in-process
// [AWSRequest], and a recorded [Event], whose only record of what was called is
// [Event.Operation] — the HTTP method is not stored (see [ValidationReport], whose
// quota section is the caller).
//
// The rule is name-shaped rather than a table of every S3 operation: a name
// beginning Get, Head or List is a read and everything else is a write. That
// agrees with the verb classifier for every operation substrate routes, including
// the two that could disagree — SelectObjectContent is a POST and a write in both,
// and DeleteObjects is a POST and a write in both.
func s3RateClassOfOperation(operation string) s3RateClass {
	for _, readPrefix := range []string{"Get", "Head", "List"} {
		if strings.HasPrefix(operation, readPrefix) {
			return s3RateRead
		}
	}
	return s3RateWrite
}

// s3AccountingPrefix returns the bucket and the prefix within it that a request's
// rate counts against, per the rule this file's preamble states: the object key up
// to and including its first "/", and the empty prefix for a key with no "/" or an
// operation naming no key.
//
// Bucket and key come from [parseS3Operation], the same derivation the S3 plugin
// itself uses, so the gate cannot account a request against a different prefix
// than the one the plugin serves it from.
func s3AccountingPrefix(req *AWSRequest) (bucket, prefix string) {
	bucket, key, _ := parseS3Operation(req)
	if key == "" {
		return bucket, ""
	}
	first := strings.IndexByte(key, '/')
	if first < 0 {
		return bucket, ""
	}
	return bucket, key[:first+1]
}

// s3PrefixBucketKey composes the token-bucket identity for an S3 request under an
// already-resolved rule key.
//
// The rule says what the ceiling is; this says what the ceiling applies to. Two
// requests share a bucket only when they are the same class, in the same S3
// bucket, under the same accounting prefix — which is what makes spreading writes
// across prefixes lift the ceiling, as AWS's guidance says it does.
//
// The rule key stays in the identity so that changing a rule cannot silently reuse
// the tokens accumulated under a different one, and the two halves are joined by a
// NUL, which cannot appear in either a rule key or an S3 key name.
func s3PrefixBucketKey(ruleKey string, req *AWSRequest) string {
	bucket, prefix := s3AccountingPrefix(req)
	return ruleKey + "\x00" + bucket + "/" + prefix
}

// s3SlowDownError is the refusal an S3 request over a rate limit gets, whichever
// rule set that limit — S3 has no ThrottlingException to answer instead.
//
// The code and the status are AWS's: the performance page says a caller scaling
// past their current rate "may see some 503 (Slow Down) errors", and substrate's
// fault-injection surface has served that pair for S3 since #480. It goes back
// through [marshalAWSError] as an [AWSError], which renders an S3 error as the
// bare <Error> document S3 uses rather than the wrapped <ErrorResponse> of the
// Query protocol — the difference between an SDK recovering the code and falling
// back to the HTTP status.
//
// The lead sentence is the one AWS's SDKs and its own guidance quote; detail names
// what was throttled, because that is the one thing a caller cannot infer from the
// code. AWS documents no message string for SlowDown, so per docs/fidelity.md
// neither half is dressed up as authoritative. Assert on the code and the status.
func s3SlowDownError(detail string) *AWSError {
	return &AWSError{
		Code:       "SlowDown",
		Message:    "Please reduce your request rate. " + detail,
		HTTPStatus: http.StatusServiceUnavailable,
	}
}

// s3PrefixExceededDetail names the prefix a per-prefix refusal was accounted
// against, in the terms [s3AccountingPrefix] derives it: a named prefix, or the
// bucket's root for a key with no "/" and for an operation naming no key.
func s3PrefixExceededDetail(bucket, prefix string) string {
	if prefix == "" {
		return fmt.Sprintf("The request rate for the root prefix of bucket %q was exceeded.", bucket)
	}
	return fmt.Sprintf("The request rate for prefix %q in bucket %q was exceeded.", prefix, bucket)
}

// s3RuleExceededDetail names the configured rule a refusal came from, for a rule
// that is not one of AWS's per-prefix ceilings and so is not accounted per prefix.
// Naming the rule rather than a prefix keeps the message from claiming an accounting
// unit that did not apply.
func s3RuleExceededDetail(ruleKey string) string {
	return fmt.Sprintf("The request rate configured for %q was exceeded.", ruleKey)
}
