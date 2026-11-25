package emulator_test

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// unemulatedAuth is a SigV4 Authorization header whose credential scope names service,
// which is the routing signal an --endpoint-url client sends for a REST service.
func unemulatedAuth(service string) string {
	return "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20261124/us-east-1/" + service +
		"/aws4_request, SignedHeaders=host;x-amz-date, Signature=0000"
}

// TestUnemulatedService_ErrorEnvelopeFollowsTheRequest drives one raw request per wire
// protocol at a service no plugin emulates and asserts the refusal arrives in the envelope
// that protocol's parser reads (#1614): the status and code stay substrate's own 501
// ServiceNotAvailable throughout, and only the document carrying them changes.
func TestUnemulatedService_ErrorEnvelopeFollowsTheRequest(t *testing.T) {
	ts := emulator.StartTestServer(t)

	type envelope int
	const (
		restJSON envelope = iota
		jsonRPC
		queryXML
		cbor
	)

	tests := []struct {
		name    string
		method  string
		path    string
		host    string
		headers map[string]string
		body    string
		want    envelope
		// wantCT is the response Content-Type the envelope is served with.
		wantCT string
	}{
		{
			name:    "EKS is REST-JSON",
			method:  http.MethodPost,
			path:    "/clusters",
			headers: map[string]string{"Authorization": unemulatedAuth("eks"), "Content-Type": "application/json"},
			body:    `{"name":"c1"}`,
			want:    restJSON,
			wantCT:  "application/json",
		},
		{
			name:    "FIS with no body is REST-JSON",
			method:  http.MethodGet,
			path:    "/experiments",
			headers: map[string]string{"Authorization": unemulatedAuth("fis")},
			want:    restJSON,
			wantCT:  "application/json",
		},
		{
			name:   "a request with no routing signal is REST-JSON",
			method: http.MethodGet,
			path:   "/",
			want:   restJSON,
			wantCT: "application/json",
		},
		{
			name:    "an X-Amz-Target is JSON RPC",
			method:  http.MethodPost,
			path:    "/",
			headers: map[string]string{"X-Amz-Target": "Comprehend_20171127.DetectSentiment", "Content-Type": "application/x-amz-json-1.1"},
			body:    `{}`,
			want:    jsonRPC,
			wantCT:  "application/x-amz-json-1.1",
		},
		{
			name:    "an x-amz-json Content-Type with no target is JSON RPC",
			method:  http.MethodPost,
			path:    "/",
			headers: map[string]string{"Authorization": unemulatedAuth("swf"), "Content-Type": "application/x-amz-json-1.0"},
			body:    `{}`,
			want:    jsonRPC,
			wantCT:  "application/x-amz-json-1.0",
		},
		{
			name:    "a form body naming an Action is Query",
			method:  http.MethodPost,
			path:    "/",
			headers: map[string]string{"Authorization": unemulatedAuth("sdb"), "Content-Type": "application/x-www-form-urlencoded; charset=utf-8"},
			body:    "Action=ListDomains&Version=2009-04-15",
			want:    queryXML,
			wantCT:  "text/xml; charset=UTF-8",
		},
		{
			name:    "an Action in the query string is Query",
			method:  http.MethodGet,
			path:    "/?Action=DescribeDomains&Version=2013-01-01",
			headers: map[string]string{"Authorization": unemulatedAuth("cloudsearch")},
			want:    queryXML,
			wantCT:  "text/xml; charset=UTF-8",
		},
		{
			name:    "S3 Control is REST-XML",
			method:  http.MethodGet,
			path:    "/v20180820/configuration/publicAccessBlock",
			host:    "123456789012.s3-control.us-east-1.amazonaws.com",
			headers: map[string]string{"Authorization": unemulatedAuth("s3")},
			want:    queryXML,
			wantCT:  "text/xml; charset=UTF-8",
		},
		{
			name:    "S3 Control FIPS dualstack is REST-XML",
			method:  http.MethodGet,
			path:    "/v20180820/jobs",
			host:    "123456789012.s3-control-fips.dualstack.us-east-1.amazonaws.com",
			headers: map[string]string{"Authorization": unemulatedAuth("s3")},
			want:    queryXML,
			wantCT:  "text/xml; charset=UTF-8",
		},
		{
			// curl -d sends a form Content-Type whatever the body; only an Action names a
			// Query operation.
			name:    "a JSON body under a form Content-Type is REST-JSON",
			method:  http.MethodPost,
			path:    "/clusters",
			headers: map[string]string{"Authorization": unemulatedAuth("eks"), "Content-Type": "application/x-www-form-urlencoded"},
			body:    `{"name":"c1"}`,
			want:    restJSON,
			wantCT:  "application/json",
		},
		{
			name:    "a Smithy-Protocol header is RPC v2 CBOR",
			method:  http.MethodPost,
			path:    "/service/Textract/operation/DetectDocumentText",
			headers: map[string]string{"Smithy-Protocol": "rpc-v2-cbor", "Content-Type": "application/cbor", "Accept": "application/cbor"},
			body:    "\xa0", // an empty CBOR map
			want:    cbor,
			wantCT:  "application/cbor",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, ts.URL+tc.path, strings.NewReader(tc.body))
			require.NoError(t, err)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			if tc.host != "" {
				req.Host = tc.host
			}
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			assert.Equal(t, http.StatusNotImplemented, resp.StatusCode, "body: %s", body)
			assert.Equal(t, tc.wantCT, resp.Header.Get("Content-Type"))

			switch tc.want {
			case restJSON:
				// botocore's RestJSONParser and aws-sdk-go-v2's restjson both read the
				// code from this header before the body.
				assert.Equal(t, "ServiceNotAvailable", resp.Header.Get("X-Amzn-ErrorType"))
				var doc map[string]string
				require.NoError(t, json.Unmarshal(body, &doc), "body: %s", body)
				assert.Equal(t, "ServiceNotAvailable", doc["Code"])
				assert.Contains(t, doc["message"], "service not emulated")
			case jsonRPC:
				var doc map[string]string
				require.NoError(t, json.Unmarshal(body, &doc), "body: %s", body)
				assert.Equal(t, "ServiceNotAvailable", doc["__type"])
				assert.Contains(t, doc["message"], "service not emulated")
			case queryXML:
				var doc struct {
					XMLName xml.Name `xml:"ErrorResponse"`
					Error   struct {
						Type string `xml:"Type"`
						Code string `xml:"Code"`
					} `xml:"Error"`
					RequestID string `xml:"RequestId"`
				}
				require.NoError(t, xml.Unmarshal(body, &doc), "body: %s", body)
				assert.Equal(t, "ServiceNotAvailable", doc.Error.Code)
				// A 5xx is the service's fault.
				assert.Equal(t, "Receiver", doc.Error.Type)
				assert.NotEmpty(t, doc.RequestID)
			case cbor:
				assert.Equal(t, "rpc-v2-cbor", resp.Header.Get("Smithy-Protocol"))
				decoded, err := emulator.CBORDecodeForTest(body)
				require.NoError(t, err)
				doc, ok := decoded.(map[string]any)
				require.True(t, ok, "a CBOR error is a map, got %T", decoded)
				assert.Equal(t, "ServiceNotAvailable", doc["__type"])
				assert.Contains(t, doc["message"], "service not emulated")
			}
		})
	}
}

// TestUnemulatedService_ProtocolClassification pins unclassifiedErrorProtocol through the
// in-process classifier, including the two inputs the server table above cannot reach: a
// nil request, and a registered service, which is never reclassified by its request.
func TestUnemulatedService_ProtocolClassification(t *testing.T) {
	form := map[string]string{"Content-Type": "application/x-www-form-urlencoded"}
	tests := []struct {
		name    string
		service string
		path    string
		headers map[string]string
		want    string
	}{
		{"a form Content-Type alone is not Query", "sdb", "/", form, emulator.ErrProtoRESTJSONForTest},
		{"unemulated Action in the query string", "sdb", "/?Action=ListDomains", nil, emulator.ErrProtoQueryXMLForTest},
		{"unemulated REST", "eks", "/clusters", map[string]string{"Content-Type": "application/json"}, emulator.ErrProtoRESTJSONForTest},
		{"unemulated rest-xml", "s3-control", "/v20180820/jobs", nil, emulator.ErrProtoQueryXMLForTest},
		{"unemulated JSON RPC", "comprehend", "/", map[string]string{"X-Amz-Target": "Comprehend_20171127.X"}, emulator.ErrProtoJSONRPCForTest},
		{"unemulated CBOR", "textract", "/service/Textract/operation/X", map[string]string{"Smithy-Protocol": "rpc-v2-cbor"}, emulator.ErrProtoRPCV2CBORForTest},
		{"registered REST-JSON keeps its protocol on a form", "lambda", "/", form, emulator.ErrProtoRESTJSONForTest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := requestWith(t, tc.path, tc.headers)
			assert.Equal(t, tc.want, emulator.ErrorProtocolForRequestForTest(tc.service, r))
		})
	}
	t.Run("nil request is the Content-Type-free answer", func(t *testing.T) {
		assert.Equal(t, emulator.ErrProtoRESTJSONForTest, emulator.ErrorProtocolForRequestForTest("eks", nil))
		assert.Equal(t, emulator.ErrProtoQueryXMLForTest, emulator.ErrorProtocolForRequestForTest("s3-control", nil))
	})
}

// TestUnemulatedService_AccessDeniedFollowsTheEnvelope asserts the authorization gate's
// code for an unemulated service agrees with the envelope carrying it: a REST-JSON or
// CBOR refusal is AccessDeniedException, a Query one AccessDenied.
func TestUnemulatedService_AccessDeniedFollowsTheEnvelope(t *testing.T) {
	tests := []struct {
		name string
		req  *emulator.AWSRequest
		want string
	}{
		{"REST-JSON", &emulator.AWSRequest{Service: "eks"}, "AccessDeniedException"},
		{"CBOR", &emulator.AWSRequest{Service: "textract", Protocol: emulator.WireRPCV2CBOR}, "AccessDeniedException"},
		{"JSON RPC", &emulator.AWSRequest{Service: "comprehend", Protocol: emulator.WireJSONRPC}, "AccessDeniedException"},
		{"Query", &emulator.AWSRequest{Service: "sdb", Params: map[string]string{"Action": "ListDomains"}}, "AccessDenied"},
		{"rest-xml", &emulator.AWSRequest{Service: "s3-control"}, "AccessDenied"},
		{"CloudWatch over CBOR keeps its per-service code", &emulator.AWSRequest{Service: "monitoring", Protocol: emulator.WireRPCV2CBOR}, "AccessDenied"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, emulator.AccessDeniedCodeForRequestForTest(tc.req))
		})
	}
}

// TestUnemulatedService_S3ControlHosts pins every S3 Control endpoint shape to the one
// service name restXMLServices keys: plain, FIPS and dualstack, each with and without the
// "{AccountId}." host prefix the s3control model gives its account-scoped operations.
func TestUnemulatedService_S3ControlHosts(t *testing.T) {
	tests := []struct {
		host string
		want string
	}{
		{"s3-control.us-east-1.amazonaws.com", "s3-control"},
		{"123456789012.s3-control.us-east-1.amazonaws.com", "s3-control"},
		{"s3-control-fips.us-east-1.amazonaws.com", "s3-control"},
		{"123456789012.s3-control-fips.us-east-1.amazonaws.com", "s3-control"},
		{"s3-control.dualstack.us-east-1.amazonaws.com", "s3-control"},
		{"123456789012.s3-control.dualstack.us-east-1.amazonaws.com", "s3-control"},
		{"s3-control-fips.dualstack.us-east-1.amazonaws.com", "s3-control"},
		{"123456789012.s3-control-fips.dualstack.us-east-1.amazonaws.com", "s3-control"},
		// A label that is not an account ID ahead of s3-control is not S3 Control's shape.
		{"mybucket.s3-control.us-east-1.amazonaws.com", "mybucket"},
		// The label after s3-control must be a Region or "dualstack", as the endpoint shapes are.
		{"s3-control.example.amazonaws.com", "s3-control"},
		{"123456789012.s3-control.notaregion.amazonaws.com", "123456789012"},
		{"s3-control.amazonaws.com", "s3-control"},
	}
	for _, tc := range tests {
		t.Run(tc.host, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodGet, "http://"+tc.host+"/v20180820/jobs", nil)
			require.NoError(t, err)
			r.Host = tc.host
			req, _, err := emulator.ParseAWSRequest(r)
			require.NoError(t, err)
			assert.Equal(t, tc.want, req.Service)
		})
	}
}

// TestUnemulatedService_MalformedFormStillNamesItsAction asserts the error envelope and the
// authorization gate's code classify a Query form with a malformed escape the same way. The
// form names its Action before the bad pair, so both read it as Query: the envelope from the
// HTTP request's form, the gate from the parsed parameters.
func TestUnemulatedService_MalformedFormStillNamesItsAction(t *testing.T) {
	const body = "Action=ListDomains&Version=2009-04-15&x=%zz"
	newReq := func() *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.Header.Set("Authorization", unemulatedAuth("sdb"))
		return r
	}

	r := newReq()
	parsed, _, err := emulator.ParseAWSRequest(r)
	require.NoError(t, err)
	assert.Equal(t, "ListDomains", parsed.Params["Action"], "the Action survives the malformed pair")
	assert.NotContains(t, parsed.Params, "Version", "the rest of a malformed form is not trusted")
	assert.Equal(t, emulator.ErrProtoQueryXMLForTest, emulator.ErrorProtocolForRequestForTest("sdb", r))
	assert.Equal(t, "AccessDenied", emulator.AccessDeniedCodeForRequestForTest(parsed))

	ts := emulator.StartTestServer(t)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
	require.NoError(t, err)
	req.Header = newReq().Header
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotImplemented, resp.StatusCode)
	assert.Contains(t, string(got), "<ErrorResponse>", "the envelope is Query: %s", got)
}
