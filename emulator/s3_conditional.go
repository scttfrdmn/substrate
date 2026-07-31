package emulator

import (
	"hash/fnv"
	"net/http"
	"strings"
	"sync"
	"time"
)

// s3KeyLockStripes is the number of mutexes in [S3Plugin]'s per-key stripe set.
// A power of two well above the concurrency any test drives, so distinct keys
// almost never contend.
const s3KeyLockStripes = 64

// s3KeyMutex is a striped mutex set serializing writes per object key.
//
// It exists because [StateManager] offers no compare-and-swap: Get/Put/Delete/List
// only, and [MemoryStateManager] is last-write-wins. A conditional write that
// checked existence and then called Put would leave a window in which every one of
// N concurrent `If-None-Match: *` PUTs observes the key as absent and all succeed —
// precisely inverting the exactly-one-winner property such a write is used for.
// Holding a per-key lock across check→Put closes that window.
//
// The guarantee this provides is deliberately bounded: it is *process-local*. It is
// airtight for substrate's single-process topology, which is how tests use it, and
// it would not hold across two emulator processes sharing one state backend. That
// boundary is a property of the state layer, not of this lock.
type s3KeyMutex struct {
	stripes [s3KeyLockStripes]sync.Mutex
}

// lock acquires the stripe guarding bucket/key and returns its unlock function.
func (m *s3KeyMutex) lock(bucket, key string) func() {
	h := fnv.New32a()
	_, _ = h.Write([]byte(bucket))
	_, _ = h.Write([]byte{'/'})
	_, _ = h.Write([]byte(key))
	mu := &m.stripes[h.Sum32()%s3KeyLockStripes]
	mu.Lock()
	return mu.Unlock
}

// s3PreconditionFailedMessage is the message S3 pairs with PreconditionFailed.
const s3PreconditionFailedMessage = "At least one of the pre-conditions you specified did not hold"

// s3ConditionalHeaders holds the RFC 7232 precondition headers of a request,
// already resolved case-insensitively.
//
// The distinction between an absent header and an empty one matters: an absent
// header imposes no condition, whereas an empty value is a condition that cannot
// be met. present tracks which of the four were sent at all.
type s3ConditionalHeaders struct {
	IfMatch           string
	IfNoneMatch       string
	IfModifiedSince   string
	IfUnmodifiedSince string

	hasIfMatch           bool
	hasIfNoneMatch       bool
	hasIfModifiedSince   bool
	hasIfUnmodifiedSince bool
}

// readConditionalHeaders extracts the four read preconditions from a request.
func readConditionalHeaders(headers map[string]string) s3ConditionalHeaders {
	c := s3ConditionalHeaders{}
	c.IfMatch, c.hasIfMatch = headerLookupFold(headers, "If-Match")
	c.IfNoneMatch, c.hasIfNoneMatch = headerLookupFold(headers, "If-None-Match")
	c.IfModifiedSince, c.hasIfModifiedSince = headerLookupFold(headers, "If-Modified-Since")
	c.IfUnmodifiedSince, c.hasIfUnmodifiedSince = headerLookupFold(headers, "If-Unmodified-Since")
	return c
}

// copySourceConditionalHeaders extracts CopyObject's source preconditions, which
// are the same four conditions under x-amz-copy-source-* names. They gate reading
// the source, whereas the unprefixed headers gate overwriting the destination, so a
// single CopyObject request can carry both sets.
func copySourceConditionalHeaders(headers map[string]string) s3ConditionalHeaders {
	c := s3ConditionalHeaders{}
	c.IfMatch, c.hasIfMatch = headerLookupFold(headers, "x-amz-copy-source-if-match")
	c.IfNoneMatch, c.hasIfNoneMatch = headerLookupFold(headers, "x-amz-copy-source-if-none-match")
	c.IfModifiedSince, c.hasIfModifiedSince = headerLookupFold(headers, "x-amz-copy-source-if-modified-since")
	c.IfUnmodifiedSince, c.hasIfUnmodifiedSince = headerLookupFold(headers, "x-amz-copy-source-if-unmodified-since")
	return c
}

// any reports whether the request carried any precondition at all, letting a
// caller skip the whole evaluation on the common unconditional path.
func (c s3ConditionalHeaders) any() bool {
	return c.hasIfMatch || c.hasIfNoneMatch || c.hasIfModifiedSince || c.hasIfUnmodifiedSince
}

// headerLookupFold returns the value of the named header and whether it was
// present, matching the name case-insensitively. It exists alongside
// [headerValueFold] because a precondition header sent with an empty value is a
// condition that fails, not an absent condition.
func headerLookupFold(headers map[string]string, name string) (string, bool) {
	for k, v := range headers {
		if strings.EqualFold(k, name) {
			return v, true
		}
	}
	return "", false
}

// evaluateReadPreconditions applies the GetObject/HeadObject preconditions to an
// object that has already been found, returning the error response to serve or nil
// to proceed.
//
// The two combination rules are S3's own, and both exist to stop a
// cache-validation header from overriding an explicit ETag assertion:
//
//   - If-Match true and If-Unmodified-Since false → 200, not 412. The caller
//     named the exact entity it wanted and got it; a coarser timestamp condition
//     does not override that.
//   - If-None-Match false and If-Modified-Since true → 304, not 200. The caller
//     already holds this entity, so there is nothing to send regardless of dates.
//
// A precondition on an absent object is not evaluated at all — the caller's
// NoSuchKey comes first, since there is no ETag to compare against.
func evaluateReadPreconditions(c s3ConditionalHeaders, obj *S3Object) *AWSResponse {
	if !c.any() {
		return nil
	}

	// If-Match / If-None-Match are evaluated first: an explicit entity assertion
	// outranks the date conditions, per the two rules above.
	if c.hasIfNoneMatch && s3ETagMatches(c.IfNoneMatch, obj.ETag) {
		// The caller holds this entity. 304 carries no body and no explanation.
		return &AWSResponse{StatusCode: http.StatusNotModified, Headers: map[string]string{"ETag": obj.ETag}}
	}
	if c.hasIfMatch && !s3ETagMatches(c.IfMatch, obj.ETag) {
		return s3PreconditionFailedResponse()
	}

	// If-None-Match evaluated false and did not return above; If-Modified-Since is
	// then redundant with it, so S3 skips it. Likewise a satisfied If-Match
	// suppresses If-Unmodified-Since.
	if c.hasIfModifiedSince && !c.hasIfNoneMatch {
		if since, ok := parseHTTPDate(c.IfModifiedSince); ok && !obj.LastModified.After(since) {
			return &AWSResponse{StatusCode: http.StatusNotModified, Headers: map[string]string{"ETag": obj.ETag}}
		}
	}
	if c.hasIfUnmodifiedSince && !c.hasIfMatch {
		if since, ok := parseHTTPDate(c.IfUnmodifiedSince); ok && obj.LastModified.After(since) {
			return s3PreconditionFailedResponse()
		}
	}

	return nil
}

// evaluateWritePreconditions applies the PutObject/CopyObject/Complete conditional
// write preconditions against the current state of the destination key, returning
// the error response to serve or nil to proceed.
//
// current is the destination object, or nil when the key is absent. A delete
// marker counts as absent: S3 documents both headers as evaluating against the
// current version, and "if the current object version is a delete marker" the
// If-None-Match write succeeds and the If-Match write is a 404.
//
// Unlike a read, an absent key is not an early NoSuchKey — its absence is exactly
// what If-None-Match asserts.
func evaluateWritePreconditions(c s3ConditionalHeaders, current *S3Object) *AWSResponse {
	exists := current != nil && !current.IsDeleteMarker

	if c.hasIfNoneMatch {
		// "The If-None-Match header expects the * (asterisk) value." Any other
		// value is not a supported form on a write; treat it as unmet rather than
		// silently allowing the overwrite it was sent to prevent.
		if strings.TrimSpace(c.IfNoneMatch) != "*" || exists {
			return s3PreconditionFailedResponse()
		}
	}

	if c.hasIfMatch {
		if !exists {
			// "If there's no current object version with the same name, or if the
			// current object version is a delete marker, the operation fails with
			// a 404 Not Found error."
			return s3ErrorResponse("NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
		}
		if !s3ETagMatches(c.IfMatch, current.ETag) {
			return s3PreconditionFailedResponse()
		}
	}

	return nil
}

// s3PreconditionFailedResponse builds the 412 PreconditionFailed response. The
// code string is what an SDK matches on to distinguish a lost race from a real
// failure, so it must be exact.
func s3PreconditionFailedResponse() *AWSResponse {
	return s3ErrorResponse("PreconditionFailed", s3PreconditionFailedMessage, http.StatusPreconditionFailed)
}

// s3ETagMatches reports whether an If-Match or If-None-Match header value matches
// an object's ETag. "*" matches any existing entity. A comma-separated list
// matches if any member does, per RFC 7232 — S3 restricts the write form of
// If-None-Match to "*", but the read form accepts a list.
//
// Comparison ignores quoting, weak-validator prefixes and hex case, since clients
// differ on whether they echo back the quotes S3 sends.
func s3ETagMatches(header, etag string) bool {
	header = strings.TrimSpace(header)
	if header == "" {
		return false
	}
	if header == "*" {
		return true
	}
	for _, candidate := range strings.Split(header, ",") {
		if s3ETagsEqual(candidate, etag) {
			return true
		}
	}
	return false
}

// parseHTTPDate parses an If-Modified-Since / If-Unmodified-Since value, accepting
// the three formats RFC 9110 requires a recipient to handle. An unparseable date
// makes the condition inapplicable rather than failed, which is what RFC 9110
// specifies and what keeps a malformed date from turning into a spurious 412.
func parseHTTPDate(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	for _, layout := range []string{http.TimeFormat, time.RFC850, time.ANSIC} {
		if t, err := time.Parse(layout, value); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}
