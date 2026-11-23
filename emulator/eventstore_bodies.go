package emulator

import (
	"bytes"
	"encoding/json"
	"net/url"
	"strings"
)

// DefaultMaxBodyBytes is the body cap [EventStoreConfig.MaxBodyBytes] means when it
// is zero: 1 MiB.
const DefaultMaxBodyBytes = 1 << 20

// BodyDigest stands in for a body that was too large to record.
type BodyDigest struct {
	// Length is the body's length in bytes.
	Length int `json:"length"`

	// SHA256 is the hex-encoded SHA-256 of the body.
	SHA256 string `json:"sha256"`
}

// newBodyDigest returns the digest of body.
func newBodyDigest(body []byte) *BodyDigest {
	return &BodyDigest{Length: len(body), SHA256: hashBytes(body)}
}

// overBodyCap reports whether body is larger than the store's configured cap.
func (e *EventStore) overBodyCap(body []byte) bool {
	limit := e.config.MaxBodyBytes
	if limit == 0 {
		limit = DefaultMaxBodyBytes
	}
	return limit > 0 && len(body) > limit
}

// recordedRequest returns the copy of req an event stores: credential headers, signing
// parameters and their copies in the body removed, and the body replaced by a digest
// when it is over the cap.
//
// It is a copy because the caller that recorded the request still holds it, and
// redacting in place would change what that caller sees.
func (e *EventStore) recordedRequest(req *AWSRequest) (*AWSRequest, *BodyDigest) {
	if req == nil {
		return nil, nil
	}
	rec := *req
	rec.Headers = redactRecordedHeaders(req.Headers)
	drop := credentialParams(req.Params)
	rec.Params = withoutKeysFold(req.Params, drop)
	rec.MultiValueParams = withoutMultiKeysFold(req.MultiValueParams, drop)
	if len(rec.Params) != len(req.Params) {
		rec.Body = redactedBody(req, rec.Params, drop)
	}
	if e.overBodyCap(rec.Body) {
		digest := newBodyDigest(rec.Body)
		rec.Body = nil
		return &rec, digest
	}
	return &rec, nil
}

// redactedBody returns req's body with the credential parameters drop removed, for a
// request whose parameters carried some. Two bodies can repeat them:
//
//   - a form-encoded Query POST, which is decoded, filtered and re-encoded;
//   - the JSON re-encoding of the parsed form that the server substitutes for the iam,
//     sts, sqs and sns bodies (see [Server.handleAWSRequest] step 1), which is rebuilt
//     from the filtered parameters.
//
// Any other body is returned as it is: its parameters came from the query string. The
// parameters a replay reads are in Params either way.
func redactedBody(req *AWSRequest, params map[string]string, drop []string) []byte {
	if reencoded, err := json.Marshal(req.Params); err == nil && bytes.Equal(reencoded, req.Body) {
		if filtered, err := json.Marshal(params); err == nil {
			return filtered
		}
	}
	if isFormBody(req.Headers) {
		if form, err := url.ParseQuery(string(req.Body)); err == nil {
			for key := range form {
				if containsFold(drop, key) {
					delete(form, key)
				}
			}
			return []byte(form.Encode())
		}
	}
	return req.Body
}

// isFormBody reports whether headers declare a form-encoded body.
func isFormBody(headers map[string]string) bool {
	for name, value := range headers {
		if strings.EqualFold(name, "Content-Type") {
			return strings.HasPrefix(strings.ToLower(value), "application/x-www-form-urlencoded")
		}
	}
	return false
}

// recordedResponse returns the copy of resp an event stores, with the body replaced
// by a digest when it is over the cap.
func (e *EventStore) recordedResponse(resp *AWSResponse) (*AWSResponse, *BodyDigest) {
	if resp == nil {
		return nil, nil
	}
	rec := *resp
	if e.overBodyCap(resp.Body) {
		rec.Body = nil
		return &rec, newBodyDigest(resp.Body)
	}
	return &rec, nil
}

// redactedRequestHeaders are the request headers never written to an event, compared
// case-insensitively. Each carries a credential, a signature or the signing time.
//
// A replay reads none of them: it restores the caller from [Event.Principal] and does
// not verify signatures (see [ReplayEngine.replayEvent]).
var redactedRequestHeaders = []string{
	"Authorization",
	"Proxy-Authorization",
	"X-Amz-Security-Token",
	"X-Amz-Date",
	"X-Amz-Signature",
	"X-Amz-Credential",
}

// redactedRequestParams are the signing parameters never written to an event: SigV4's
// presigned-URL parameters, whose X-Amz- prefix no API parameter shares.
var redactedRequestParams = []string{
	"X-Amz-Security-Token",
	"X-Amz-Date",
	"X-Amz-Signature",
	"X-Amz-Credential",
}

// sigV2Params are the parameters a Signature Version 2 request signs with, in a query
// string or a form body. They are dropped only from a request that carries
// AWSAccessKeyId, because "Signature" alone is also an API member name (KMS Verify).
var sigV2Params = []string{
	"AWSAccessKeyId",
	"Signature",
	"SignatureMethod",
	"SignatureVersion",
	"SecurityToken",
}

// credentialParams returns the parameter names to drop from a request with params.
func credentialParams(params map[string]string) []string {
	for key := range params {
		if strings.EqualFold(key, "AWSAccessKeyId") {
			return append(append([]string{}, redactedRequestParams...), sigV2Params...)
		}
	}
	return redactedRequestParams
}

// redactRecordedHeaders returns a copy of headers without [redactedRequestHeaders].
func redactRecordedHeaders(headers map[string]string) map[string]string {
	return withoutKeysFold(headers, redactedRequestHeaders)
}

// withoutKeysFold copies m, dropping every key that matches an entry of drop
// case-insensitively. A nil map stays nil.
func withoutKeysFold(m map[string]string, drop []string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if !containsFold(drop, k) {
			out[k] = v
		}
	}
	return out
}

// withoutMultiKeysFold is [withoutKeysFold] for repeated parameters.
func withoutMultiKeysFold(m map[string][]string, drop []string) map[string][]string {
	if m == nil {
		return nil
	}
	out := make(map[string][]string, len(m))
	for k, v := range m {
		if !containsFold(drop, k) {
			out[k] = v
		}
	}
	return out
}

// containsFold reports whether list holds s, ignoring case.
func containsFold(list []string, s string) bool {
	for _, item := range list {
		if strings.EqualFold(item, s) {
			return true
		}
	}
	return false
}
