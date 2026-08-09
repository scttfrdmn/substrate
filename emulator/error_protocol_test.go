package emulator_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"math"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// jsonErrorBody decodes a JSON-protocol error body.
func jsonErrorBody(t *testing.T, r *http.Response) map[string]any {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out), "body was %s", body)
	return out
}

// TestJSONRPCError_CarriesTypeMember covers #392 for the JSON RPC protocol.
// botocore's BaseJSONParser reads the error code from the body's "__type"
// member and falls back to the stringified HTTP status when it is absent — it
// never reads "Code". Emitting only "Code" therefore surfaced as
// Error.Code == "404" and silently defeated every `except ClientError` branch
// that compares against a symbolic code.
func TestJSONRPCError_CarriesTypeMember(t *testing.T) {
	t.Parallel()
	srv := newSSMTestServer(t)
	resp := ssmRequest(t, srv, "GetParameter", map[string]any{"Name": "/nope"})
	defer resp.Body.Close() //nolint:errcheck

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	body := jsonErrorBody(t, resp)

	// The member botocore actually reads.
	assert.Equal(t, "ParameterNotFound", body["__type"])
	// The lowercase message member the SDKs read.
	assert.NotEmpty(t, body["message"])
	// Retained so callers reading "Code" directly keep working.
	assert.Equal(t, "ParameterNotFound", body["Code"])
	// The status must not be the thing the caller has to match on.
	assert.NotEqual(t, "404", body["__type"])
}

// TestJSONRPCError_MirrorsContentType asserts the 1.0/1.1 variant the caller
// sent is echoed back rather than normalized, since the SDK pairs the response
// parser with the protocol version it negotiated.
func TestJSONRPCError_MirrorsContentType(t *testing.T) {
	t.Parallel()
	srv := newSSMTestServer(t)
	resp := ssmRequest(t, srv, "GetParameter", map[string]any{"Name": "/nope"})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, "application/x-amz-json-1.1", resp.Header.Get("Content-Type"))
}

// TestRESTJSONError_CarriesErrorTypeHeader covers #392 for REST-JSON. Lambda is
// REST-JSON but sends "application/json" on the way in, which is
// indistinguishable from a plain JSON body — so a Content-Type sniff sent
// Lambda's errors out as XML, a shape its SDK cannot parse at all. botocore's
// RestJSONParser prefers the x-amzn-errortype header over the body.
func TestRESTJSONError_CarriesErrorTypeHeader(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	resp := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/functions/nope", nil)
	defer resp.Body.Close() //nolint:errcheck

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "ResourceNotFoundException", resp.Header.Get("x-amzn-ErrorType"))
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	body := jsonErrorBody(t, resp)
	assert.Equal(t, "ResourceNotFoundException", body["Code"])
	assert.NotEmpty(t, body["message"])
}

// TestRESTJSONError_IsNotXML pins the regression directly: a REST-JSON error
// must not be an XML document, which is what the Content-Type sniff produced.
func TestRESTJSONError_IsNotXML(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	resp := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/functions/nope", nil)
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.NotContains(t, string(body), "<ErrorResponse")
}

// TestQueryXMLError_UsesErrorDocument asserts query-protocol services still get
// the <ErrorResponse><Error><Code> document their parser expects, including the
// <Type> element real AWS sends. CloudFormation rejects an unknown action, which
// is the simplest error a plugin raises through the server's writeError path.
//
// This used EC2 as its vehicle until #591, on the premise that EC2 was a Query
// service. It is not — EC2 is the sole service on the "ec2" protocol, whose error
// document the SDK parses at a different XPath entirely — so the vehicle moved to
// a genuinely Query service rather than the assertion being loosened to accept
// both shapes, which would have retired the #392 gate this test exists to be.
func TestQueryXMLError_UsesErrorDocument(t *testing.T) {
	t.Parallel()
	ts := newCFNTestServer(t)
	status, body := cfnRequest(t, ts, map[string]string{"Action": "NoSuchActionAtAll"})
	assert.Equal(t, http.StatusBadRequest, status)

	var doc struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Type string `xml:"Type"`
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), "body was %s", body)
	assert.NotEmpty(t, doc.Error.Code)
	assert.Equal(t, "Sender", doc.Error.Type)
}

// TestEC2Error_UsesEC2Document is #591. EC2 is on the "ec2" protocol, not Query:
// its error document wraps the error in a *plural* <Errors> element, and the SDK's
// ec2query.ErrorComponents reads the code at the XPath "Errors>Error>Code". A Query
// document has no <Errors>, so Code decoded empty and the EC2 deserializer kept its
// "UnknownError" default — every consumer branching on a specific EC2 error code got
// UnknownError instead, for organic errors as much as injected faults.
//
// The struct below deliberately uses the SDK's own XPaths so this test fails for
// the same reason a real SDK caller does. The real-SDK gate is in
// test/e2e/journey_ec2_errors_test.go; this is the unit-level half.
//
// Shape verified against the EC2 error reference:
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html
func TestEC2Error_UsesEC2Document(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	resp := ec2Request(t, ts, map[string]string{"Action": "NoSuchActionAtAll"})
	defer resp.Body.Close() //nolint:errcheck

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var doc struct {
		XMLName xml.Name `xml:"Response"`
		Code    string   `xml:"Errors>Error>Code"`
		Message string   `xml:"Errors>Error>Message"`
		// Capital D, which is the "ec2" protocol's spelling and not the Query
		// protocol's <RequestId>.
		RequestID string `xml:"RequestID"`
	}
	require.NoError(t, xml.Unmarshal(body, &doc), "body was %s", body)
	assert.Equal(t, "Response", doc.XMLName.Local)
	assert.NotEmpty(t, doc.Code, "the SDK reads the code at Errors>Error>Code")
	assert.NotEmpty(t, doc.Message)
	assert.Equal(t, "SUBSTRATE", doc.RequestID)
	assert.Equal(t, "text/xml; charset=UTF-8", resp.Header.Get("Content-Type"))

	// The wrapper is what the SDK keys on, so assert it literally: a document with
	// a singular <Error> child of <Response> satisfies every assertion above once
	// encoding/xml's path matching is given a nested struct, but not the SDK.
	assert.Contains(t, string(body), "<Errors>")
	assert.NotContains(t, string(body), "<ErrorResponse>")
}

// TestEC2Error_IsNotTheQueryDocument pins the mutual exclusivity the fix rests on.
// The "ec2" protocol reads Errors>Error>Code and the shared Query parser reads
// Error>Code, so no single document satisfies both — which is why EC2 needed its
// own arm rather than the Query shape being widened, and why the two arms must not
// drift back together.
func TestEC2Error_IsNotTheQueryDocument(t *testing.T) {
	t.Parallel()

	ec2Body, _, _ := emulator.MarshalAWSErrorForTest(
		"InvalidInstanceID.NotFound", "not found",
		emulator.ErrProtoEC2XMLForTest, "", http.StatusBadRequest)
	queryBody, _, _ := emulator.MarshalAWSErrorForTest(
		"InvalidInstanceID.NotFound", "not found",
		emulator.ErrProtoQueryXMLForTest, "", http.StatusBadRequest)

	// The Query parser's XPath, applied to the EC2 document, recovers nothing.
	var asQuery struct {
		Code string `xml:"Error>Code"`
	}
	require.NoError(t, xml.Unmarshal(ec2Body, &asQuery))
	assert.Empty(t, asQuery.Code, "the EC2 document must not parse as a Query one")

	// And symmetrically: the SDK's EC2 XPath recovers nothing from a Query document,
	// which is exactly the bug #591 reported.
	var asEC2 struct {
		Code string `xml:"Errors>Error>Code"`
	}
	require.NoError(t, xml.Unmarshal(queryBody, &asEC2))
	assert.Empty(t, asEC2.Code, "a Query document must not parse as an EC2 one")
}

// TestErrorProtocolFor_EC2 asserts the classification itself, so a map entry moved
// back to the Query arm fails here and not only in a wire-shape assertion.
func TestErrorProtocolFor_EC2(t *testing.T) {
	t.Parallel()
	assert.Equal(t, emulator.ErrProtoEC2XMLForTest, emulator.ErrorProtocolForTest("ec2", ""))
	// The carve-out is EC2 alone: ec2 is the only service on that protocol, so
	// every other XML service must keep the shape it had.
	for _, svc := range []string{"cloudformation", "iam", "sns", "sts", "rds", "route53"} {
		assert.Equal(t, emulator.ErrProtoQueryXMLForTest, emulator.ErrorProtocolForTest(svc, ""),
			"%s must stay on the Query arm", svc)
	}
	assert.Equal(t, emulator.ErrProtoS3XMLForTest, emulator.ErrorProtocolForTest("s3", ""))
}

// TestMarshalAWSError_EC2EscapesMessage is the EC2 arm's half of the escaping
// property: a message carrying XML metacharacters must still produce a document
// the caller can parse.
func TestMarshalAWSError_EC2EscapesMessage(t *testing.T) {
	t.Parallel()
	body, ct, headers := emulator.MarshalAWSErrorForTest(
		"InvalidParameterValue", `a<b&c>"d"`, emulator.ErrProtoEC2XMLForTest, "",
		http.StatusBadRequest)
	assert.Equal(t, "text/xml; charset=UTF-8", ct)
	assert.Empty(t, headers)

	var doc struct {
		Code    string `xml:"Errors>Error>Code"`
		Message string `xml:"Errors>Error>Message"`
	}
	require.NoError(t, xml.Unmarshal(body, &doc), "body was %s", body)
	assert.Equal(t, "InvalidParameterValue", doc.Code)
	assert.Equal(t, `a<b&c>"d"`, doc.Message)
}

// TestMarshalAWSError_RequestIDIsDeterministic guards the reason the request id is a
// fixed string rather than generateRequestID(), which derives from
// time.Now().UnixNano(): two replays of one recorded run must produce byte-identical
// error bodies, and a caller diffing responses must not see a field that moves on its
// own. Both XML arms that carry a request id are checked.
func TestMarshalAWSError_RequestIDIsDeterministic(t *testing.T) {
	t.Parallel()
	for _, proto := range []string{emulator.ErrProtoEC2XMLForTest, emulator.ErrProtoS3XMLForTest} {
		first, _, _ := emulator.MarshalAWSErrorForTest("Throttling", "slow down", proto, "",
			http.StatusServiceUnavailable)
		second, _, _ := emulator.MarshalAWSErrorForTest("Throttling", "slow down", proto, "",
			http.StatusServiceUnavailable)
		assert.Equal(t, string(first), string(second), "%s must be byte-stable", proto)
		assert.Contains(t, string(first), "SUBSTRATE", "%s must carry the fixed request id", proto)
	}
}

// TestIAMError_DropsExceptionSuffix covers the IAM half of #392: the wire error
// code is NoSuchEntity, while NoSuchEntityException is the shape name in the API
// model. Lambda genuinely does use the ...Exception suffix on the wire, so the
// two services legitimately differ and neither can be inferred from the other.
//
// Verified against the IAM API reference:
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_GetRole.html
func TestIAMError_DropsExceptionSuffix(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	resp := iamRequest(t, srv, "GetRole", map[string]any{"RoleName": "nope"})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var doc struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Type string `xml:"Type"`
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(body, &doc))
	assert.Equal(t, "NoSuchEntity", doc.Error.Code)
	assert.Equal(t, "Sender", doc.Error.Type)
}

// TestS3GetObject_MissingBucketIsNoSuchBucket covers the S3 half of #392.
// getObject went straight to the object lookup, so a GET against a bucket that
// was never created reported NoSuchKey. That conflates two distinct conditions:
// a caller cannot tell "someone deleted my bucket" from "this object isn't
// written yet".
func TestS3GetObject_MissingBucketIsNoSuchBucket(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	w := s3Request(t, srv, http.MethodGet, "/no-such-bucket/some-key", nil, nil)
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Body.String(), "<Code>NoSuchBucket</Code>")
	assert.NotContains(t, w.Body.String(), "NoSuchKey")
}

// TestS3HeadObject_MissingBucketIsNoSuchBucket mirrors the GET case. HEAD has no
// body, so the status is the only observable — but the code still has to be
// right for the SDK, which synthesizes the error from it.
func TestS3HeadObject_MissingBucketIsNoSuchBucket(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	w := s3Request(t, srv, http.MethodHead, "/no-such-bucket/some-key", nil, nil)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// TestS3GetObject_MissingKeyInExistingBucketIsNoSuchKey is the other side of the
// distinction: with the bucket present, an absent key must still be NoSuchKey.
// Without this, the fix above could regress into reporting NoSuchBucket for
// everything.
func TestS3GetObject_MissingKeyInExistingBucketIsNoSuchKey(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	w := s3Request(t, srv, http.MethodPut, "/real-bucket", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)

	got := s3Request(t, srv, http.MethodGet, "/real-bucket/absent-key", nil, nil)
	assert.Equal(t, http.StatusNotFound, got.Code)
	assert.Contains(t, got.Body.String(), "<Code>NoSuchKey</Code>")
}

// TestS3GetObject_ExistingObjectStillReadable guards the happy path: adding the
// bucket check must not break a normal read.
func TestS3GetObject_ExistingObjectStillReadable(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	w := s3Request(t, srv, http.MethodPut, "/readable-bucket", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	w = s3Request(t, srv, http.MethodPut, "/readable-bucket/k", []byte("hello"), nil)
	require.Equal(t, http.StatusOK, w.Code)

	got := s3Request(t, srv, http.MethodGet, "/readable-bucket/k", nil, nil)
	assert.Equal(t, http.StatusOK, got.Code)
	assert.Equal(t, "hello", got.Body.String())
}

// TestErrorProtocolFor_Classification pins the protocol chosen per service, and
// the fallback for a service not yet in the table: an x-amz-json request body
// implies JSON RPC, anything else implies XML — the behavior substrate had
// before #392.
func TestErrorProtocolFor_Classification(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		service     string
		contentType string
		want        string
	}{
		// S3 is REST-XML like cloudfront and route53 but has its own arm: its real
		// error document is a bare <Error>, not the <ErrorResponse> wrapper, and its
		// parser recovers no code from the wrapped form (#480).
		{"s3 is s3 xml", "s3", "", emulator.ErrProtoS3XMLForTest},
		{"cloudfront stays query xml", "cloudfront", "", emulator.ErrProtoQueryXMLForTest},
		{"route53 stays query xml", "route53", "", emulator.ErrProtoQueryXMLForTest},
		{"iam is xml", "iam", "application/x-amz-json-1.1", emulator.ErrProtoQueryXMLForTest},
		// CloudFormation is a Query service whose errors are XML. The unknown-service
		// fallback below happens to answer XML for a form-encoded body too, so only a
		// direct assertion pins the table entry rather than the fallback.
		{"cloudformation is xml", "cloudformation", "application/x-amz-json-1.1", emulator.ErrProtoQueryXMLForTest},
		{"ssm is json rpc", "ssm", "application/x-amz-json-1.1", emulator.ErrProtoJSONRPCForTest},
		{"dynamodb is json rpc", "dynamodb", "application/x-amz-json-1.0", emulator.ErrProtoJSONRPCForTest},
		{"lambda is rest json", "lambda", "application/json", emulator.ErrProtoRESTJSONForTest},
		// Services whose ...Gateway/...Service targetPrefix makes them look
		// REST-shaped but which model as json: their errors belong in "__type",
		// not the x-amzn-errortype header. Verified against each service's
		// "protocol" field in botocore's service-2.json.
		{"acm is json rpc", "acm", "application/x-amz-json-1.1", emulator.ErrProtoJSONRPCForTest},
		{"budgets is json rpc", "budgets", "application/x-amz-json-1.1", emulator.ErrProtoJSONRPCForTest},
		{"cost explorer is json rpc", "ce", "application/x-amz-json-1.1", emulator.ErrProtoJSONRPCForTest},
		{"health is json rpc", "health", "application/x-amz-json-1.1", emulator.ErrProtoJSONRPCForTest},
		{"organizations is json rpc", "organizations", "application/x-amz-json-1.1", emulator.ErrProtoJSONRPCForTest},
		{"redshift-data is json rpc", "redshift-data", "application/x-amz-json-1.1", emulator.ErrProtoJSONRPCForTest},
		{"servicequotas is json rpc", "servicequotas", "application/x-amz-json-1.1", emulator.ErrProtoJSONRPCForTest},
		{"tagging is json rpc", "tagging", "application/x-amz-json-1.1", emulator.ErrProtoJSONRPCForTest},
		{"transfer is json rpc", "transfer", "application/x-amz-json-1.1", emulator.ErrProtoJSONRPCForTest},
		// RAM is the one service in that group that really is rest-json.
		{"ram is rest json", "ram", "application/json", emulator.ErrProtoRESTJSONForTest},
		// CloudWatch models as smithy-rpc-v2-cbor but still supports query, and
		// substrate answers its successes in query XML.
		{"monitoring is xml", "monitoring", "application/x-amz-json-1.1", emulator.ErrProtoQueryXMLForTest},
		// The service name wins over the Content-Type: IAM sends x-amz-json on the
		// way in but answers errors in XML, which is exactly why the sniff failed.
		{"unknown falls back to json rpc", "notaservice", "application/x-amz-json-1.1", emulator.ErrProtoJSONRPCForTest},
		{"unknown falls back to xml", "notaservice", "application/json", emulator.ErrProtoQueryXMLForTest},
		{"unknown with no content type", "notaservice", "", emulator.ErrProtoQueryXMLForTest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, emulator.ErrorProtocolForTest(tt.service, tt.contentType))
		})
	}
}

// TestMarshalAWSError_JSONRPCDefaultsContentType asserts a JSON-RPC error falls
// back to the 1.1 variant when the request carried no usable x-amz-json type,
// rather than echoing a Content-Type the SDK's JSON parser would not accept.
func TestMarshalAWSError_JSONRPCDefaultsContentType(t *testing.T) {
	t.Parallel()
	body, ct, headers := emulator.MarshalAWSErrorForTest(
		"ParameterNotFound", "nope", emulator.ErrProtoJSONRPCForTest, "text/plain", http.StatusNotFound)
	assert.Equal(t, "application/x-amz-json-1.1", ct)
	assert.Equal(t, "ParameterNotFound", headers["x-amzn-ErrorType"])

	var out map[string]string
	require.NoError(t, json.Unmarshal(body, &out))
	assert.Equal(t, "ParameterNotFound", out["__type"])
}

// TestMarshalAWSError_XMLEscapesMessage guards against a message with XML
// metacharacters producing a document the caller cannot parse.
func TestMarshalAWSError_XMLEscapesMessage(t *testing.T) {
	t.Parallel()
	body, ct, headers := emulator.MarshalAWSErrorForTest(
		"NoSuchKey", `a<b&c>"d"`, emulator.ErrProtoQueryXMLForTest, "", http.StatusNotFound)
	assert.Equal(t, "text/xml; charset=UTF-8", ct)
	assert.Empty(t, headers)

	var doc struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(body, &doc), "body was %s", body)
	assert.Equal(t, "NoSuchKey", doc.Error.Code)
	assert.Equal(t, `a<b&c>"d"`, doc.Error.Message)
}

// TestMarshalAWSError_S3IsByteIdenticalToThePlugin is the #480 finding-4 gate at
// the unit level: the pipeline's S3 arm must produce the same bytes the S3 plugin
// produces for the same error, because a caller that can tell an injected error
// from a genuine one can tell a fault fixture from production.
func TestMarshalAWSError_S3IsByteIdenticalToThePlugin(t *testing.T) {
	t.Parallel()
	body, ct, headers := emulator.MarshalAWSErrorForTest(
		"SlowDown", "injected fault", emulator.ErrProtoS3XMLForTest, "", http.StatusServiceUnavailable)

	want := emulator.S3ErrorResponseForTest("SlowDown", "injected fault", http.StatusServiceUnavailable)
	assert.Equal(t, string(want), string(body))
	assert.Equal(t, "text/xml; charset=UTF-8", ct)
	assert.Empty(t, headers)

	// The shape itself, asserted independently of the plugin so a change to both
	// at once cannot slip through: a bare <Error>, not the <ErrorResponse> wrapper.
	var doc struct {
		XMLName   xml.Name `xml:"Error"`
		Code      string   `xml:"Code"`
		Message   string   `xml:"Message"`
		RequestID string   `xml:"RequestId"`
	}
	require.NoError(t, xml.Unmarshal(body, &doc), "body was %s", body)
	assert.Equal(t, "SlowDown", doc.Code)
	assert.Equal(t, "injected fault", doc.Message)
	assert.NotEmpty(t, doc.RequestID)
	assert.NotContains(t, string(body), "<ErrorResponse")
}

// errAfterGetsStateManager is a StateManager that serves the first n Get calls
// normally and then fails every one after. Failing on a later call is what lets
// a test reach a specific lookup — a store that fails from the first Get never
// gets past bucket creation.
type errAfterGetsStateManager struct {
	inner  emulator.StateManager
	allow  int
	getErr error
	gets   int
}

func (m *errAfterGetsStateManager) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	m.gets++
	if m.gets > m.allow {
		return nil, m.getErr
	}
	return m.inner.Get(ctx, namespace, key)
}

func (m *errAfterGetsStateManager) Put(ctx context.Context, namespace, key string, value []byte) error {
	return m.inner.Put(ctx, namespace, key, value)
}

func (m *errAfterGetsStateManager) Delete(ctx context.Context, namespace, key string) error {
	return m.inner.Delete(ctx, namespace, key)
}

func (m *errAfterGetsStateManager) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	return m.inner.List(ctx, namespace, prefix)
}

// TestS3_StateFailureIsNotReportedAsNoSuchBucket asserts the error return of
// bucketMissingResponse is propagated rather than collapsed into "the bucket
// does not exist". The two are opposite signals: NoSuchBucket tells a caller
// their bucket is gone and to stop retrying, while a store failure is transient
// and should be retried. Reporting the former for the latter would send a
// consumer down a permanent-failure path over a blip.
func TestS3_StateFailureIsNotReportedAsNoSuchBucket(t *testing.T) {
	t.Parallel()

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			state := &errAfterGetsStateManager{
				inner:  emulator.NewMemoryStateManager(),
				getErr: errors.New("state store unavailable"),
				allow:  math.MaxInt, // permissive until the bucket exists
			}
			srv := newS3TestServerWithState(t, state)

			w := s3Request(t, srv, http.MethodPut, "/blip-bucket", nil, nil)
			require.Equal(t, http.StatusOK, w.Code)

			// Every Get from here on fails, so the bucket lookup errors even
			// though the bucket was written.
			state.allow = state.gets

			got := s3Request(t, srv, method, "/blip-bucket/some-key", nil, nil)
			assert.Equal(t, http.StatusInternalServerError, got.Code,
				"a state-store failure must surface as a server error, not as NoSuchBucket")
			assert.NotContains(t, got.Body.String(), "NoSuchBucket")
		})
	}
}
