package emulator_test

import (
	"encoding/json"
	"encoding/xml"
	"io"
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
// <Type> element real AWS sends. EC2 rejects an unknown action, which is the
// simplest error the plugin raises through the server's writeError path.
func TestQueryXMLError_UsesErrorDocument(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	resp := ec2Request(t, ts, map[string]string{"Action": "NoSuchActionAtAll"})
	defer resp.Body.Close() //nolint:errcheck

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var doc struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Type string `xml:"Type"`
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(body, &doc), "body was %s", body)
	assert.NotEmpty(t, doc.Error.Code)
	assert.Equal(t, "Sender", doc.Error.Type)
	assert.Equal(t, "text/xml; charset=UTF-8", resp.Header.Get("Content-Type"))
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
		{"s3 is xml", "s3", "", emulator.ErrProtoQueryXMLForTest},
		{"iam is xml", "iam", "application/x-amz-json-1.1", emulator.ErrProtoQueryXMLForTest},
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
		"ParameterNotFound", "nope", emulator.ErrProtoJSONRPCForTest, "text/plain")
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
		"NoSuchKey", `a<b&c>"d"`, emulator.ErrProtoQueryXMLForTest, "")
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
