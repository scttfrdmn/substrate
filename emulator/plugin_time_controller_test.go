package emulator_test

// Every default plugin registration carries the simulated clock (#904).
//
// A plugin that keeps a clock reads it from Options["time_controller"] and falls back
// to a private NewTimeController(time.Now()) when the key is absent. The fallback is
// silent by design, so fifteen of the sixty-seven registrations in
// RegisterDefaultPlugins could leave the key out and look exactly like the fifty-two
// that supplied it — and eleven services stamped their timestamps from the wall clock
// inside an emulator whose whole claim is a controlled clock.
//
// The eleven are asserted here. The other four of the fifteen — Tagging, Health, Price
// List and the API Gateway proxy — have no clock field at all, so the option is a
// deliberate no-op for them and there is nothing observable to assert; that is recorded
// on the issue rather than tested.
//
// Every case goes through the server rather than through a plugin constructed by the
// test, because the defect was in RegisterDefaultPlugins and a plugin initialized by
// hand with a clock proves nothing about how the server initializes it. Each assertion
// is exact equality against the frozen clock, and each fails against the parent commit.

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// pluginClockFrozen is the instant every case in this file runs at. Any fixed instant
// works; a whole second in UTC keeps the three truncating formats — CloudFront's
// second-granularity RFC3339, Kinesis' epoch seconds and ECS' epoch milliseconds —
// comparable to the same value the RFC3339 cases carry.
var pluginClockFrozen = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// startPluginClockServer starts a server whose simulated clock is stopped at
// pluginClockFrozen.
//
// The scale is set to zero *before* the instant, and the order is load-bearing:
// TimeController.Now adds the wall-clock time elapsed since its baseline scaled by the
// current scale, and SetScale rolls that elapsed time into the baseline before changing
// the factor. Setting the instant first and stopping the clock second therefore banks
// the microseconds spent in between, and an exact-equality assertion fails by that
// much.
func startPluginClockServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	ts := emulator.StartTestServer(t)
	ts.SetScale(0)
	ts.SetTime(pluginClockFrozen)
	return ts
}

// pluginClockDo posts one request to ts and returns the status and the raw body.
//
// The Authorization header is a fake signature. StartTestServer registers credentials
// but does not verify signatures unless asked to, and an unsigned request resolves to
// the same account anyway, so the header exists only to name a credential scope. None
// of the values asserted here depends on the account.
func pluginClockDo(t *testing.T, ts *emulator.TestServer,
	method, host, signingName, path, target, contentType, body string,
) (int, []byte) {
	t.Helper()

	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, rdr)
	require.NoError(t, err)
	req.Host = host
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if target != "" {
		req.Header.Set("X-Amz-Target", target)
	}
	if signingName != "" {
		req.Header.Set("Authorization",
			"AWS4-HMAC-SHA256 Credential=AKIATEST12345678901/20260101/us-east-1/"+
				signingName+"/aws4_request, SignedHeaders=host, Signature=fake")
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, got
}

// pluginClockTarget posts a JSON-target operation and requires a 200.
func pluginClockTarget(t *testing.T, ts *emulator.TestServer, host, signingName, target, body string) []byte {
	t.Helper()
	status, got := pluginClockDo(t, ts, http.MethodPost, host, signingName, "/", target,
		"application/x-amz-json-1.1", body)
	require.Equal(t, http.StatusOK, status, "%s: %s", target, got)
	return got
}

// pluginClockREST posts a REST operation and requires want.
func pluginClockREST(t *testing.T, ts *emulator.TestServer,
	host, signingName, path, contentType, body string, want int,
) []byte {
	t.Helper()
	status, got := pluginClockDo(t, ts, http.MethodPost, host, signingName, path, "", contentType, body)
	require.Equal(t, want, status, "POST %s: %s", path, got)
	return got
}

// pluginClockJSONField decodes body and returns the named member of the named wrapper,
// as the bytes the response carried.
//
// The value is returned raw rather than decoded into a time.Time or a float64 because
// three of the eleven members are numbers and one is a composed string, and the wire
// form is part of what is being asserted: a member that decodes to the right instant
// but renders as a different shape is still a wire difference.
func pluginClockJSONField(t *testing.T, body []byte, wrapper, member string) string {
	t.Helper()
	inner := body
	if wrapper != "" {
		var outer map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(body, &outer), "%s", body)
		raw, ok := outer[wrapper]
		require.True(t, ok, "no %q member in %s", wrapper, body)
		inner = raw
	}
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(inner, &fields), "%s", inner)
	raw, ok := fields[member]
	require.True(t, ok, "no %q member in %s", member, inner)
	return string(raw)
}

// pluginClockQuoted renders s as the JSON string a response carries it as, so a raw
// member can be compared against an expected value without unquoting it first.
func pluginClockQuoted(s string) string {
	return `"` + s + `"`
}

// TestPluginTimeController_EveryDefaultRegistrationCarriesTheSimulatedClock asserts
// that each of the eleven plugins whose registration used to omit the clock now stamps
// the frozen simulated clock, exactly.
//
// One case per plugin rather than one per timestamp: what is under test is the wiring,
// which is per-registration, so a second member of the same response would assert the
// same thing twice. The member chosen for each is the cheapest one to reach — six need
// a single call, and the five that need two are the ones whose create response carries
// no time at all.
func TestPluginTimeController_EveryDefaultRegistrationCarriesTheSimulatedClock(t *testing.T) {
	rfc3339 := pluginClockQuoted(pluginClockFrozen.Format(time.RFC3339))

	for _, tc := range []struct {
		name string
		// observe makes the wire calls and returns the timestamp as the response
		// carried it, including any quotes.
		observe func(t *testing.T, ts *emulator.TestServer) string
		want    string
	}{
		{
			// ACM's create answers only the ARN, so the certificate is described back.
			name: "acm DescribeCertificate reports Certificate.CreatedAt",
			observe: func(t *testing.T, ts *emulator.TestServer) string {
				t.Helper()
				const host, signing = "acm.us-east-1.amazonaws.com", "acm"
				created := pluginClockTarget(t, ts, host, signing,
					"CertificateManager.RequestCertificate", `{"DomainName":"example.com"}`)
				var out struct {
					CertificateArn string `json:"CertificateArn"`
				}
				require.NoError(t, json.Unmarshal(created, &out), "%s", created)
				require.NotEmpty(t, out.CertificateArn)

				body, err := json.Marshal(map[string]string{"CertificateArn": out.CertificateArn})
				require.NoError(t, err)
				described := pluginClockTarget(t, ts, host, signing,
					"CertificateManager.DescribeCertificate", string(body))
				return pluginClockJSONField(t, described, "Certificate", "CreatedAt")
			},
			want: rfc3339,
		},
		{
			name: "apigateway CreateRestApi reports createdDate",
			observe: func(t *testing.T, ts *emulator.TestServer) string {
				t.Helper()
				got := pluginClockREST(t, ts, "apigateway.us-east-1.amazonaws.com", "apigateway",
					"/restapis", "application/json", `{"name":"clock"}`, http.StatusCreated)
				return pluginClockJSONField(t, got, "", "createdDate")
			},
			want: rfc3339,
		},
		{
			// Both API Gateway generations answer on one host; the "/v2/" path prefix is
			// the only thing that tells them apart, so this case also pins that the two
			// registrations are separately wired rather than one of them serving both.
			name: "apigatewayv2 CreateApi reports createdDate",
			observe: func(t *testing.T, ts *emulator.TestServer) string {
				t.Helper()
				got := pluginClockREST(t, ts, "apigateway.us-east-1.amazonaws.com", "apigateway",
					"/v2/apis", "application/json", `{"Name":"clock","ProtocolType":"HTTP"}`,
					http.StatusCreated)
				return pluginClockJSONField(t, got, "", "createdDate")
			},
			want: rfc3339,
		},
		{
			// Epoch seconds to three decimals, like ECS below and for the same reason:
			// ECR speaks application/x-amz-json-1.1, whose timestamps are numbers. It
			// answered a quoted RFC3339 string until #1090, because the persisted record's
			// time.Time reached the wire directly — so this case now also pins the unit.
			name: "ecr CreateRepository reports repository.createdAt",
			observe: func(t *testing.T, ts *emulator.TestServer) string {
				t.Helper()
				got := pluginClockTarget(t, ts, "api.ecr.us-east-1.amazonaws.com", "ecr",
					"AmazonEC2ContainerRegistry_V1_1_0.CreateRepository", `{"repositoryName":"clock"}`)
				return pluginClockJSONField(t, got, "repository", "createdAt")
			},
			want: strconv.FormatFloat(float64(pluginClockFrozen.UnixNano())/1e9, 'f', 3, 64),
		},
		{
			// ECS renders epoch seconds to three decimals, so this case is also the one
			// that would catch a clock read correctly and then rendered in a different
			// unit.
			name: "ecs RegisterTaskDefinition reports taskDefinition.registeredAt",
			observe: func(t *testing.T, ts *emulator.TestServer) string {
				t.Helper()
				got := pluginClockTarget(t, ts, "ecs.us-east-1.amazonaws.com", "ecs",
					"AmazonEC2ContainerServiceV20141113.RegisterTaskDefinition", `{"family":"clock"}`)
				return pluginClockJSONField(t, got, "taskDefinition", "registeredAt")
			},
			want: strconv.FormatFloat(float64(pluginClockFrozen.UnixNano())/1e9, 'f', 3, 64),
		},
		{
			name: "cognito-idp CreateUserPool reports UserPool.CreationDate",
			observe: func(t *testing.T, ts *emulator.TestServer) string {
				t.Helper()
				got := pluginClockTarget(t, ts, "cognito-idp.us-east-1.amazonaws.com", "cognito-idp",
					"AWSCognitoIdentityProviderService.CreateUserPool", `{"PoolName":"clock"}`)
				return pluginClockJSONField(t, got, "UserPool", "CreationDate")
			},
			want: rfc3339,
		},
		{
			// The one member here that is not the clock itself: the credentials expire an
			// hour out, so this case pins that the offset is applied to the simulated
			// clock rather than to a wall-clock one.
			name: "cognito-identity GetCredentialsForIdentity reports Credentials.Expiration an hour out",
			observe: func(t *testing.T, ts *emulator.TestServer) string {
				t.Helper()
				got := pluginClockTarget(t, ts, "cognito-identity.us-east-1.amazonaws.com",
					"cognito-identity", "AWSCognitoIdentityService.GetCredentialsForIdentity", `{}`)
				return pluginClockJSONField(t, got, "Credentials", "Expiration")
			},
			want: pluginClockQuoted(pluginClockFrozen.Add(time.Hour).Format(time.RFC3339)),
		},
		{
			// CreateStream answers an empty body, so the stream is described back. The
			// timestamp is whole epoch seconds.
			name: "kinesis DescribeStream reports StreamCreationTimestamp",
			observe: func(t *testing.T, ts *emulator.TestServer) string {
				t.Helper()
				const host, signing = "kinesis.us-east-1.amazonaws.com", "kinesis"
				pluginClockTarget(t, ts, host, signing, "Kinesis_20131202.CreateStream",
					`{"StreamName":"clock"}`)
				got := pluginClockTarget(t, ts, host, signing, "Kinesis_20131202.DescribeStream",
					`{"StreamName":"clock"}`)
				return pluginClockJSONField(t, got, "StreamDescription", "StreamCreationTimestamp")
			},
			want: strconv.FormatInt(pluginClockFrozen.Unix(), 10),
		},
		{
			// CreateDistribution's response carries no time member at all, so the
			// invalidation is what is asserted — and its CreateTime is stamped at request
			// time rather than at create, which makes this the one case that shows the
			// clock is read per request rather than captured once.
			name: "cloudfront CreateInvalidation reports CreateTime",
			observe: func(t *testing.T, ts *emulator.TestServer) string {
				t.Helper()
				const host, signing = "cloudfront.amazonaws.com", "cloudfront"
				created := pluginClockREST(t, ts, host, signing, "/2020-05-31/distribution",
					"application/xml",
					`<DistributionConfig><Comment>clock</Comment><Enabled>true</Enabled></DistributionConfig>`,
					http.StatusCreated)
				var dist struct {
					ID string `xml:"Id"`
				}
				require.NoError(t, xml.Unmarshal(created, &dist), "%s", created)
				require.NotEmpty(t, dist.ID)

				got := pluginClockREST(t, ts, host, signing,
					"/2020-05-31/distribution/"+dist.ID+"/invalidation", "application/xml",
					`<InvalidationBatch><Paths><Quantity>1</Quantity><Items><Path>/*</Path></Items>`+
						`</Paths><CallerReference>clock</CallerReference></InvalidationBatch>`,
					http.StatusCreated)
				var inv struct {
					CreateTime string `xml:"CreateTime"`
				}
				require.NoError(t, xml.Unmarshal(got, &inv), "%s", got)
				return inv.CreateTime
			},
			want: pluginClockFrozen.Format(time.RFC3339),
		},
		{
			// The identifier #904 was found through. Its counter half starts at one on a
			// fresh plugin instance, which is #886's; the clock half is this issue's.
			name: "sesv2 SendEmail reports a MessageId minted from the clock",
			observe: func(t *testing.T, ts *emulator.TestServer) string {
				t.Helper()
				got := pluginClockREST(t, ts, "email.us-east-1.amazonaws.com", "ses",
					"/v2/email/outbound-emails", "application/json", `{}`, http.StatusOK)
				return pluginClockJSONField(t, got, "", "MessageId")
			},
			want: pluginClockQuoted(fmt.Sprintf("msg-%d-1", pluginClockFrozen.UnixNano())),
		},
		{
			name: "firehose PutRecord reports a RecordId minted from the clock",
			observe: func(t *testing.T, ts *emulator.TestServer) string {
				t.Helper()
				got := pluginClockTarget(t, ts, "firehose.us-east-1.amazonaws.com", "firehose",
					"Firehose_20150804.PutRecord", `{}`)
				return pluginClockJSONField(t, got, "", "RecordId")
			},
			want: pluginClockQuoted(fmt.Sprintf("rec-%d", pluginClockFrozen.UnixNano())),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := startPluginClockServer(t)
			assert.Equal(t, tc.want, tc.observe(t, ts))
		})
	}
}

// TestPluginTimeController_TheControlEndpointReachesAPreviouslyUnwiredPlugin asserts
// the consequence a consumer sees: POST /v1/control/time moves these plugins' clocks.
//
// The endpoint is what the wiring gap broke from the outside — a caller testing an API
// Gateway createdDate or an ECR createdAt against a controlled clock got wall-clock
// values whatever it set. The scale is stopped through the TestServer rather than
// through POST /v1/control/scale because that endpoint refuses a scale of zero, and the
// endpoint under test preserves whatever scale is in force.
func TestPluginTimeController_TheControlEndpointReachesAPreviouslyUnwiredPlugin(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ts.SetScale(0)

	moved := time.Date(2031, 7, 8, 9, 10, 11, 0, time.UTC)
	body, err := json.Marshal(map[string]string{"time": moved.Format(time.RFC3339)})
	require.NoError(t, err)
	status, got := pluginClockDo(t, ts, http.MethodPost, "", "", "/v1/control/time", "",
		"application/json", string(body))
	require.Equal(t, http.StatusOK, status, "%s", got)

	api := pluginClockREST(t, ts, "apigateway.us-east-1.amazonaws.com", "apigateway",
		"/restapis", "application/json", `{"name":"moved"}`, http.StatusCreated)
	assert.Equal(t, pluginClockQuoted(moved.Format(time.RFC3339)),
		pluginClockJSONField(t, api, "", "createdDate"))

	// ECR renders the same instant as epoch seconds rather than as a quoted string, since
	// #1090 gave the repository shape a projection that marshals its timestamp the way the
	// JSON protocol publishes it. The two members are the same moment in two encodings,
	// which is why this assertion cannot share the one above.
	repo := pluginClockTarget(t, ts, "api.ecr.us-east-1.amazonaws.com", "ecr",
		"AmazonEC2ContainerRegistry_V1_1_0.CreateRepository", `{"repositoryName":"moved"}`)
	assert.Equal(t, strconv.FormatFloat(float64(moved.UnixNano())/1e9, 'f', 3, 64),
		pluginClockJSONField(t, repo, "repository", "createdAt"))
}
