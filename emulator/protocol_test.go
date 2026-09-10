package emulator_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Per-request wire-protocol classification (#757).
//
// These tests drive detectWireProtocol with the requests the three real clients send,
// not with invented ones. The header and path combinations below were captured from
// aws-sdk-go-v2 v1.47.0 (cloudwatch v1.72.0), the AWS CLI, and a hand-rolled Query
// client against a running emulator; the whole reason CloudWatch was unusable is that
// substrate's own tests only ever sent the third.

// requestWith builds a POST to path carrying headers, which is the shape every one of
// these protocols uses — RPC v2 CBOR permits no other method at all.
func requestWith(t *testing.T, path string, headers map[string]string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(""))
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	return r
}

func TestDetectWireProtocol(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		headers map[string]string
		want    emulator.WireProtocol
	}{
		{
			// What aws-sdk-go-v2's cloudwatch client actually sends. Note the absence of
			// X-Amz-Target: substrate routed on that header first, so the SDK's request
			// reached the plugin only because parser.go maps the service shape name in
			// the path.
			name: "aws-sdk-go-v2 CloudWatch",
			path: "/service/GraniteServiceVersion20100801/operation/PutMetricData",
			headers: map[string]string{
				"Smithy-Protocol":   "rpc-v2-cbor",
				"Content-Type":      "application/cbor",
				"Accept":            "application/cbor",
				"X-Amzn-Query-Mode": "true",
			},
			want: emulator.WireRPCV2CBOR,
		},
		{
			// The header alone is enough: it is required on every conformant request, so
			// it outranks the path and the Content-Type both.
			name:    "Smithy-Protocol header with no CBOR path",
			path:    "/",
			headers: map[string]string{"Smithy-Protocol": "rpc-v2-cbor"},
			want:    emulator.WireRPCV2CBOR,
		},
		{
			// A client that omitted the header is still unambiguous, because no other
			// protocol routes by this path shape.
			name:    "CBOR path and Content-Type with no header",
			path:    "/service/GraniteServiceVersion20100801/operation/ListMetrics",
			headers: map[string]string{"Content-Type": "application/cbor"},
			want:    emulator.WireRPCV2CBOR,
		},
		{
			// The specification allows an arbitrary prefix before /service, so the tail
			// of the path is what decides.
			name:    "CBOR path under a prefix",
			path:    "/mounted/here/service/Svc/operation/Op",
			headers: map[string]string{"Content-Type": "application/cbor"},
			want:    emulator.WireRPCV2CBOR,
		},
		{
			// A CBOR body posted somewhere that is not an RPC v2 path is not this
			// protocol. Nothing else in substrate accepts CBOR, so the honest answer is
			// the fallback rather than a guess.
			name:    "CBOR Content-Type on a non-RPC path",
			path:    "/",
			headers: map[string]string{"Content-Type": "application/cbor"},
			want:    emulator.WireQuery,
		},
		{
			name:    "path too short to be an RPC v2 route",
			path:    "/operation/Op",
			headers: map[string]string{"Content-Type": "application/cbor"},
			want:    emulator.WireQuery,
		},
		{
			name:    "empty service name in an RPC v2 path",
			path:    "/service//operation/Op",
			headers: map[string]string{"Content-Type": "application/cbor"},
			want:    emulator.WireQuery,
		},
		{
			// What the AWS CLI and boto3 send for CloudWatch.
			name: "AWS CLI CloudWatch",
			path: "/",
			headers: map[string]string{
				"X-Amz-Target": "GraniteServiceVersion20100801.ListMetrics",
				"Content-Type": "application/x-amz-json-1.0",
			},
			want: emulator.WireJSONRPC,
		},
		{
			name:    "target header alone",
			path:    "/",
			headers: map[string]string{"X-Amz-Target": "DynamoDB_20120810.GetItem"},
			want:    emulator.WireJSONRPC,
		},
		{
			name:    "JSON RPC Content-Type with no target",
			path:    "/",
			headers: map[string]string{"Content-Type": "application/x-amz-json-1.1"},
			want:    emulator.WireJSONRPC,
		},
		{
			// A hand-rolled Query client, which is the only shape substrate's own
			// CloudWatch tests ever sent.
			name:    "query form body",
			path:    "/",
			headers: map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
			want:    emulator.WireQuery,
		},
		{
			// REST-JSON sends a bare application/json, which is indistinguishable from a
			// plain JSON body. Classifying it as JSON RPC on that basis is exactly the
			// sniffing #392 removed, so it must stay the fallback.
			name:    "bare application/json is not JSON RPC",
			path:    "/2015-03-31/functions/f/invocations",
			headers: map[string]string{"Content-Type": "application/json"},
			want:    emulator.WireQuery,
		},
		{
			name:    "no headers at all",
			path:    "/",
			headers: nil,
			want:    emulator.WireQuery,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := emulator.DetectWireProtocolForTest(requestWith(t, tc.path, tc.headers))
			assert.Equal(t, tc.want, got, "path %q headers %v", tc.path, tc.headers)
		})
	}
}

// TestDetectWireProtocol_NilRequest pins the total-function property: the classifier is
// consulted from the error serializer, which an in-process caller reaches with no HTTP
// request at all.
func TestDetectWireProtocol_NilRequest(t *testing.T) {
	assert.Equal(t, emulator.WireQuery, emulator.DetectWireProtocolForTest(nil))
	assert.False(t, emulator.DetectQueryModeForTest(nil))
	assert.False(t, emulator.RPCV2CBORTargetConflictForTest(nil))
}

func TestWireProtocolString(t *testing.T) {
	assert.Equal(t, "query", emulator.WireQuery.String())
	assert.Equal(t, "json-rpc", emulator.WireJSONRPC.String())
	// The CBOR name is the literal Smithy-Protocol header value, so a log line naming
	// it can be pasted straight into a request.
	assert.Equal(t, "rpc-v2-cbor", emulator.WireRPCV2CBOR.String())
}

// TestWireQueryIsZeroValue pins that an AWSRequest built by hand — which is how every
// in-process test and every third-party plugin's own tests construct one — reads as
// Query without setting the field.
func TestWireQueryIsZeroValue(t *testing.T) {
	var req emulator.AWSRequest
	assert.Equal(t, emulator.WireQuery, req.Protocol)
	assert.False(t, req.QueryMode)
}

func TestDetectQueryMode(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "what the SDKs send", value: "true", want: true},
		{name: "case-insensitive", value: "True", want: true},
		{name: "explicitly false", value: "false", want: false},
		{name: "absent", value: "", want: false},
		{name: "not a boolean", value: "1", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			headers := map[string]string{}
			if tc.value != "" {
				headers["X-Amzn-Query-Mode"] = tc.value
			}
			assert.Equal(t, tc.want, emulator.DetectQueryModeForTest(requestWith(t, "/", headers)))
		})
	}
}

// TestRPCV2CBORTargetConflict covers the one combination the specification requires a
// server to reject. Both spellings of the target header count: the JSON RPC protocols
// send X-Amz-Target and the specification's prohibition names X-Amzn-Target, and a
// server that refused only one of them would let the other through.
func TestRPCV2CBORTargetConflict(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		want    bool
	}{
		{
			name:    "CBOR with X-Amz-Target",
			headers: map[string]string{"Smithy-Protocol": "rpc-v2-cbor", "X-Amz-Target": "Svc.Op"},
			want:    true,
		},
		{
			name:    "CBOR with X-Amzn-Target",
			headers: map[string]string{"Smithy-Protocol": "rpc-v2-cbor", "X-Amzn-Target": "Svc.Op"},
			want:    true,
		},
		{
			name:    "CBOR alone",
			headers: map[string]string{"Smithy-Protocol": "rpc-v2-cbor"},
			want:    false,
		},
		{
			name:    "target alone",
			headers: map[string]string{"X-Amz-Target": "Svc.Op"},
			want:    false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := emulator.RPCV2CBORTargetConflictForTest(requestWith(t, "/", tc.headers))
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestParseAWSRequest_RecordsProtocol asserts the fields reach the plugin, which is the
// only reason they exist. Classification itself is pinned above; this is the wiring.
func TestParseAWSRequest_RecordsProtocol(t *testing.T) {
	t.Run("CBOR CloudWatch request", func(t *testing.T) {
		r := requestWith(t, "/service/GraniteServiceVersion20100801/operation/PutMetricData", map[string]string{
			"Smithy-Protocol":   "rpc-v2-cbor",
			"Content-Type":      "application/cbor",
			"X-Amzn-Query-Mode": "true",
		})
		req, _, err := emulator.ParseAWSRequest(r)
		require.NoError(t, err)
		assert.Equal(t, emulator.WireRPCV2CBOR, req.Protocol)
		assert.True(t, req.QueryMode)
		// The routing this release does not change: the service shape name in the path
		// still resolves to "monitoring".
		assert.Equal(t, "monitoring", req.Service)
		assert.Equal(t, "PutMetricData", req.Operation)
	})

	t.Run("query CloudWatch request", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=ListMetrics&Version=2010-08-01"))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/monitoring/aws4_request,")
		req, _, err := emulator.ParseAWSRequest(r)
		require.NoError(t, err)
		assert.Equal(t, emulator.WireQuery, req.Protocol)
		assert.False(t, req.QueryMode)
	})
}
