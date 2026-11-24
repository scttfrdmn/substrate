package emulator_test

// Request validation against the vendored service models (#1612), with ACM as the service
// it is on for.
//
// Every request here is raw HTTP. An SDK validates the same constraints on the client and
// would refuse each invalid request before it was sent, which would hide whether the server
// refuses it.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// wireACMCall sends one ACM JSON 1.1 request, unsigned, and returns the status and body.
func wireACMCall(t *testing.T, ts *emulator.TestServer, op, body string, header map[string]string) (int, []byte) {
	t.Helper()
	h := map[string]string{
		"Content-Type": "application/x-amz-json-1.1",
		"X-Amz-Target": "CertificateManager." + op,
	}
	for k, v := range header {
		h[k] = v
	}
	return replayDo(t, ts, http.MethodPost, "/", "acm.us-east-1.amazonaws.com", []byte(body), h)
}

// wireACMError decodes a JSON 1.1 error body into its code and message.
func wireACMError(t *testing.T, body []byte) (string, string) {
	t.Helper()
	var e struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(body, &e), string(body))
	return e.Type, e.Message
}

// TestWireValidation_ACMRefusesWhatTheModelForbids is the table of refusals: each constraint
// kind, the path forms, and several failures in one message.
func TestWireValidation_ACMRefusesWhatTheModelForbids(t *testing.T) {
	ts := emulator.StartTestServer(t)
	arn := wireACMCertificate(t, ts, "example.com")

	for _, tc := range []struct {
		name, op, body, message string
	}{
		{
			"a missing required member", "RequestCertificate", `{}`,
			"1 validation error detected: Value null at 'domainName' failed to satisfy constraint: " +
				"Member must not be null",
		},
		{
			"an explicit null is missing", "DescribeCertificate", `{"CertificateArn":null}`,
			"1 validation error detected: Value null at 'certificateArn' failed to satisfy constraint: " +
				"Member must not be null",
		},
		{
			"an over-length string", "RequestCertificate", `{"DomainName":"` + strings.Repeat("a", 254) + `"}`,
			"1 validation error detected: Value of the input at 'domainName' failed to satisfy constraint: " +
				"Member must have length less than or equal to 253",
		},
		{
			"an out-of-range number", "ListCertificates", `{"MaxItems":0}`,
			"1 validation error detected: Value of the input at 'maxItems' failed to satisfy constraint: " +
				"Member must have value greater than or equal to 1",
		},
		{
			"a number over its range", "ListCertificates", `{"MaxItems":1001}`,
			"1 validation error detected: Value of the input at 'maxItems' failed to satisfy constraint: " +
				"Member must have value less than or equal to 1000",
		},
		{
			"a nested member, at its list element's path", "AddTagsToCertificate",
			`{"CertificateArn":"` + arn + `","Tags":[{"Key":"a"},{"Value":"b"}]}`,
			"1 validation error detected: Value null at 'tags.2.member.key' failed to satisfy constraint: " +
				"Member must not be null",
		},
		{
			"a list under its minimum length", "AddTagsToCertificate", `{"CertificateArn":"` + arn + `","Tags":[]}`,
			"1 validation error detected: Value of the input at 'tags' failed to satisfy constraint: " +
				"Member must have length greater than or equal to 1",
		},
		{
			"a pattern, which ACM opts into", "AddTagsToCertificate",
			`{"CertificateArn":"` + arn + `","Tags":[{"Key":"k","Value":"a*b"}]}`,
			"1 validation error detected: Value of the input at 'tags.1.member.value' failed to satisfy constraint: " +
				`Member must satisfy regular expression pattern: ([\p{L}\p{Z}\p{N}_.:/=+\-@]*)`,
		},
		{
			"an enum, which ACM opts into", "RequestCertificate", `{"DomainName":"example.com","KeyAlgorithm":"RSA_9"}`,
			"1 validation error detected: Value of the input at 'keyAlgorithm' failed to satisfy constraint: " +
				"Member must satisfy enum value set: [RSA_1024, RSA_2048, RSA_3072, RSA_4096, EC_prime256v1, " +
				"EC_secp384r1, EC_secp521r1]",
		},
		{
			"a list of scalars, reported once at the list", "ListCertificates",
			`{"CertificateStatuses":["ISSUED","NOPE"]}`,
			"1 validation error detected: Value of the input at 'certificateStatuses' failed to satisfy constraint: " +
				"Member must satisfy constraint: [Member must satisfy enum value set: [PENDING_VALIDATION, ISSUED, " +
				"INACTIVE, EXPIRED, VALIDATION_TIMED_OUT, REVOKED, FAILED]]",
		},
		{
			"several failures in one message", "AddTagsToCertificate", `{}`,
			"2 validation errors detected: Value null at 'certificateArn' failed to satisfy constraint: " +
				"Member must not be null; Value null at 'tags' failed to satisfy constraint: Member must not be null",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := wireACMCall(t, ts, tc.op, tc.body, nil)
			require.Equal(t, http.StatusBadRequest, status, string(body))
			code, message := wireACMError(t, body)
			assert.Equal(t, "ValidationException", code)
			assert.Equal(t, tc.message, message)
		})
	}
}

// TestWireValidation_ThePluginsOwnChecksAnswerInTheGatesWords pins that ACM's handler, which a
// dispatch on a caller's behalf reaches without the gate, refuses what the gate refuses with the
// same code and message.
func TestWireValidation_ThePluginsOwnChecksAnswerInTheGatesWords(t *testing.T) {
	ts := emulator.StartTestServer(t)
	p, reqCtx := setupACMPlugin(t)
	long := "arn:aws:acm:us-east-1:123456789012:certificate/" + strings.Repeat("a", 2048)
	for _, tc := range []struct{ name, op, body string }{
		{"a missing DomainName", "RequestCertificate", `{}`},
		{"a missing CertificateArn", "DescribeCertificate", `{}`},
		{"an over-length CertificateArn", "DescribeCertificate", `{"CertificateArn":"` + long + `"}`},
		{"a CertificateArn off the pattern", "DeleteCertificate", `{"CertificateArn":"not-an-arn-but-long-enough"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := wireACMCall(t, ts, tc.op, tc.body, nil)
			require.Equal(t, http.StatusBadRequest, status)
			gateCode, gateMessage := wireACMError(t, body)

			req := &emulator.AWSRequest{Service: "acm", Operation: tc.op, Body: []byte(tc.body),
				Headers: map[string]string{"X-Amz-Target": "CertificateManager." + tc.op}}
			_, err := p.HandleRequest(reqCtx, req)
			var awsErr *emulator.AWSError
			require.ErrorAs(t, err, &awsErr)
			assert.Equal(t, gateCode, awsErr.Code)
			assert.Equal(t, gateMessage, awsErr.Message)
		})
	}
}

// TestWireValidation_WithItOffThePluginStillRefuses pins that turning the gate off removes no
// refusal the plugin makes: a CertificateArn out of length or off the pattern, and a
// RequestCertificate without a DomainName, are refused by ACM's handler in the gate's words.
func TestWireValidation_WithItOffThePluginStillRefuses(t *testing.T) {
	on := emulator.StartTestServer(t)
	off := emulator.StartTestServer(t, emulator.WithoutWireValidation())
	for _, tc := range []struct{ name, op, body string }{
		{"an over-length CertificateArn", "DescribeCertificate",
			`{"CertificateArn":"arn:aws:acm:us-east-1:123456789012:certificate/` + strings.Repeat("a", 2048) + `"}`},
		{"a CertificateArn off the pattern", "DeleteCertificate", `{"CertificateArn":"not-an-arn-but-long-enough"}`},
		{"a missing DomainName", "RequestCertificate", `{}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			onStatus, onBody := wireACMCall(t, on, tc.op, tc.body, nil)
			offStatus, offBody := wireACMCall(t, off, tc.op, tc.body, nil)
			require.Equal(t, http.StatusBadRequest, offStatus, string(offBody))
			assert.Equal(t, onStatus, offStatus)
			onCode, onMessage := wireACMError(t, onBody)
			offCode, offMessage := wireACMError(t, offBody)
			assert.Equal(t, "ValidationException", offCode)
			assert.Equal(t, onCode, offCode)
			assert.Equal(t, onMessage, offMessage)
		})
	}
}

// TestWireValidation_AWrongKindOfValueIsThePlugins pins that a value of the wrong kind — a string
// where the Tags list belongs — is not reported as a list of missing members: the gate stops and
// the plugin answers its own refusal for a body it cannot decode.
func TestWireValidation_AWrongKindOfValueIsThePlugins(t *testing.T) {
	ts := emulator.StartTestServer(t)
	arn := wireACMCertificate(t, ts, "example.com")
	status, body := wireACMCall(t, ts, "AddTagsToCertificate", `{"CertificateArn":"`+arn+`","Tags":"nope"}`, nil)
	require.Equal(t, http.StatusBadRequest, status, string(body))
	code, message := wireACMError(t, body)
	assert.Equal(t, "ValidationError", code, "ACM's answer for a body it cannot decode")
	assert.NotContains(t, message, "validation error detected")
}

// TestWireValidation_ARefusalIsCounted pins that a request the gate refuses is counted in the
// request metrics, under the code the service answered.
func TestWireValidation_ARefusalIsCounted(t *testing.T) {
	cfg := emulator.DefaultConfig()
	cfg.Metrics.Enabled = true
	registry := emulator.NewPluginRegistry()
	registry.Register(&serverPlugin{serviceName: "acm"})
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	metrics := emulator.NewMetricsCollector()
	srv := emulator.NewServer(*cfg, registry, store, emulator.NewMemoryStateManager(),
		emulator.NewTimeController(time.Now()), emulator.NewDefaultLogger(slog.LevelError, false),
		emulator.ServerOptions{Metrics: metrics})

	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	r.Host = "acm.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-amz-json-1.1")
	r.Header.Set("X-Amz-Target", "CertificateManager.RequestCertificate")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())

	var out bytes.Buffer
	metrics.Render(&out, nil)
	assert.Contains(t, out.String(), `substrate_requests_total{service="acm",operation="RequestCertificate",status="error"} 1`)
	assert.Contains(t, out.String(), `substrate_request_errors_total{service="acm",error_code="ValidationException"} 1`)
}

// TestWireValidation_AnUnreadableBodyIsThePlugins pins that validation leaves a body that will
// not parse to the plugin, whose refusal for it is ACM's published one.
func TestWireValidation_AnUnreadableBodyIsThePlugins(t *testing.T) {
	ts := emulator.StartTestServer(t)
	status, body := wireACMCall(t, ts, "DescribeCertificate", `{`, nil)
	require.Equal(t, http.StatusBadRequest, status, string(body))
	code, message := wireACMError(t, body)
	assert.Equal(t, "ValidationError", code)
	assert.NotContains(t, message, "validation error detected")
}

// wireACMCertificate requests a certificate and returns its ARN.
func wireACMCertificate(t *testing.T, ts *emulator.TestServer, domain string) string {
	t.Helper()
	status, body := wireACMCall(t, ts, "RequestCertificate", `{"DomainName":"`+domain+`"}`, nil)
	require.Equal(t, http.StatusOK, status, string(body))
	var out struct{ CertificateArn string }
	require.NoError(t, json.Unmarshal(body, &out))
	return out.CertificateArn
}

// TestWireValidation_AValidRequestIsAnsweredIdentically pins that the gate adds nothing to a
// request it lets through: the same session against a server with validation off answers
// byte-identical bodies.
func TestWireValidation_AValidRequestIsAnsweredIdentically(t *testing.T) {
	epoch := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	session := func(ts *emulator.TestServer) [][]byte {
		var bodies [][]byte
		call := func(op, body string) {
			status, out := wireACMCall(t, ts, op, body, nil)
			require.Equal(t, http.StatusOK, status, "%s: %s", op, out)
			bodies = append(bodies, out)
		}
		call("RequestCertificate", `{"DomainName":"example.com","SubjectAlternativeNames":["www.example.com"],`+
			`"KeyAlgorithm":"EC_prime256v1","Tags":[{"Key":"env","Value":"prod"}]}`)
		var created struct{ CertificateArn string }
		require.NoError(t, json.Unmarshal(bodies[0], &created))
		call("DescribeCertificate", `{"CertificateArn":"`+created.CertificateArn+`"}`)
		call("ListCertificates", `{"MaxItems":10,"CertificateStatuses":["ISSUED"]}`)
		call("AddTagsToCertificate", `{"CertificateArn":"`+created.CertificateArn+`","Tags":[{"Key":"a","Value":"b"}]}`)
		call("ListTagsForCertificate", `{"CertificateArn":"`+created.CertificateArn+`"}`)
		return bodies
	}
	validated := session(emulator.StartTestServer(t, emulator.WithDeterministicRun(epoch)))
	unvalidated := session(emulator.StartTestServer(t, emulator.WithDeterministicRun(epoch), emulator.WithoutWireValidation()))
	require.Len(t, unvalidated, len(validated))
	for i := range validated {
		assert.Equal(t, string(unvalidated[i]), string(validated[i]), "response %d", i)
	}
}

// TestWireValidation_TheOptOutReachesThePlugin pins WithoutWireValidation: a request the gate
// would refuse reaches ACM's own handler. ListCertificates' handler makes no range check of its
// own, so the out-of-range MaxItems is served; RequestCertificate's handler keeps its DomainName
// check for the dispatches the gate does not see, and answers it in the gate's words.
func TestWireValidation_TheOptOutReachesThePlugin(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithoutWireValidation())
	status, body := wireACMCall(t, ts, "ListCertificates", `{"MaxItems":0}`, nil)
	require.Equal(t, http.StatusOK, status, "the plugin served what the gate would refuse: %s", body)

	status, body = wireACMCall(t, ts, "RequestCertificate", `{}`, nil)
	require.Equal(t, http.StatusBadRequest, status, string(body))
	code, message := wireACMError(t, body)
	assert.Equal(t, "ValidationException", code)
	assert.Equal(t, "1 validation error detected: Value null at 'domainName' failed to satisfy constraint: "+
		"Member must not be null", message)

	events, err := ts.Store().GetEvents(t.Context(), emulator.EventFilter{Service: "acm"})
	require.NoError(t, err)
	require.NotEmpty(t, events)
	assert.Empty(t, events[len(events)-1].ValidationModel, "the gate did not run, so the event does not say it did")
}

// TestWireValidation_TheConfigKeyTurnsItOff pins wire.validate and its environment variable.
func TestWireValidation_TheConfigKeyTurnsItOff(t *testing.T) {
	cfg, err := emulator.LoadConfig("")
	require.NoError(t, err)
	assert.False(t, cfg.Wire.SkipValidation, "validation is on by default")

	t.Setenv("SUBSTRATE_WIRE_VALIDATE", "false")
	cfg, err = emulator.LoadConfig("")
	require.NoError(t, err)
	assert.True(t, cfg.Wire.SkipValidation)
}

// TestWireValidation_AnInternalDispatchIsNotValidated pins that only a client's request is
// validated. CloudFormation requests the certificate on the stack's behalf through the registry,
// with a DomainName the gate would refuse from a client, and the certificate is created.
func TestWireValidation_AnInternalDispatchIsNotValidated(t *testing.T) {
	ts := emulator.StartTestServer(t)
	long := strings.Repeat("a", 254)
	cfnCreateStack(t, ts, "wire-internal", `{"Resources":{"C":{"Type":"AWS::CertificateManager::Certificate",`+
		`"Properties":{"DomainName":"`+long+`"}}}}`)

	status, body := wireACMCall(t, ts, "ListCertificates", `{}`, nil)
	require.Equal(t, http.StatusOK, status, string(body))
	assert.Contains(t, string(body), long, "the stack's certificate was created")

	events, err := ts.Store().GetStream(t.Context(), "wire-internal")
	require.NoError(t, err)
	var dispatched bool
	for _, e := range events {
		if e.Service == "acm" && e.Operation == "RequestCertificate" {
			dispatched = true
			assert.Empty(t, e.ValidationModel, "an internal dispatch is never validated")
			assert.Empty(t, e.Error)
		}
	}
	assert.True(t, dispatched, "the stack dispatched RequestCertificate")

	// The same request from a client is refused.
	status, _ = wireACMCall(t, ts, "RequestCertificate", `{"DomainName":"`+long+`"}`, nil)
	assert.Equal(t, http.StatusBadRequest, status)
}

// TestWireValidation_AuthorizationAndFaultsComeFirst pins the gate's place in the pipeline:
// a caller authorization refuses, or a request an armed fault answers, never reaches validation.
func TestWireValidation_AuthorizationAndFaultsComeFirst(t *testing.T) {
	t.Run("authorization", func(t *testing.T) {
		ts := emulator.StartTestServer(t)
		key := pipelineCreateUserWithKey(t, ts, "no-policies")
		status, body := wireACMCall(t, ts, "RequestCertificate", `{}`, map[string]string{
			"Authorization": "AWS4-HMAC-SHA256 Credential=" + key +
				"/20250101/us-east-1/acm/aws4_request, SignedHeaders=host, Signature=unverified",
		})
		require.Equal(t, http.StatusForbidden, status, string(body))
		code, _ := wireACMError(t, body)
		assert.Equal(t, "AccessDeniedException", code)
	})
	t.Run("fault injection", func(t *testing.T) {
		ts := emulator.StartTestServer(t)
		ts.FaultController().UpdateConfig(emulator.FaultConfig{
			Enabled: true,
			Rules: []emulator.FaultRule{{
				Service: "acm", Operation: "RequestCertificate", FaultType: "error",
				ErrorCode: "ThrottlingException", HTTPStatus: http.StatusBadRequest, ErrorMsg: "Rate exceeded",
			}},
		})
		status, body := wireACMCall(t, ts, "RequestCertificate", `{}`, nil)
		require.Equal(t, http.StatusBadRequest, status, string(body))
		code, _ := wireACMError(t, body)
		assert.Equal(t, "ThrottlingException", code)
	})
	t.Run("a service not listed is not validated", func(t *testing.T) {
		ts := emulator.StartTestServer(t)
		// Secrets Manager's CreateSecret requires Name; the plugin answers its own refusal.
		status, body := replayDo(t, ts, http.MethodPost, "/", "secretsmanager.us-east-1.amazonaws.com", []byte(`{}`),
			map[string]string{"Content-Type": "application/x-amz-json-1.1", "X-Amz-Target": "secretsmanager.CreateSecret"})
		require.Equal(t, http.StatusBadRequest, status, string(body))
		assert.NotContains(t, string(body), "validation error detected")
	})
}

// TestWireValidation_ReplayIsVersioned pins the replay rule: an event the gate ran on carries the
// snapshot version and replays through the gate to the same refusal, and an event without it —
// a recording made before validation existed, or with it off — replays without the gate.
func TestWireValidation_ReplayIsVersioned(t *testing.T) {
	epoch := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

	t.Run("a validated recording replays its refusal", func(t *testing.T) {
		ts := emulator.StartTestServer(t, emulator.WithDeterministicRun(epoch))
		wireACMCertificate(t, ts, "example.com")
		status, _ := wireACMCall(t, ts, "RequestCertificate", `{}`, nil)
		require.Equal(t, http.StatusBadRequest, status)

		events, err := ts.Store().GetEvents(t.Context(), emulator.EventFilter{Service: "acm"})
		require.NoError(t, err)
		require.Len(t, events, 2)
		for _, e := range events {
			assert.True(t, strings.HasPrefix(e.ValidationModel, "botocore/"), "event %d: %q", e.Sequence, e.ValidationModel)
		}
		results := emulator.StartTestServer(t, emulator.WithDeterministicRun(epoch)).ReplayEvents(t, events)
		emulator.AssertReplayMatches(t, results)
		assert.Equal(t, 1, results.FailedEvents, "the refusal replays as the same refusal")
	})

	t.Run("a recording without the version replays unchanged", func(t *testing.T) {
		// Recorded with the gate off, which is what every recording made before the gate
		// existed looks like: the plugin answered the request, and no event names a snapshot.
		ts := emulator.StartTestServer(t, emulator.WithDeterministicRun(epoch), emulator.WithoutWireValidation())
		wireACMCertificate(t, ts, "example.com")
		status, body := wireACMCall(t, ts, "ListCertificates", `{"MaxItems":0}`, nil)
		require.Equal(t, http.StatusOK, status, string(body))

		events, err := ts.Store().GetEvents(t.Context(), emulator.EventFilter{Service: "acm"})
		require.NoError(t, err)
		raw, err := json.Marshal(events)
		require.NoError(t, err)
		assert.NotContains(t, string(raw), "validation_model", "the field is omitted when empty")

		// Replayed into a server that validates: the events carry no version, so the gate does
		// not run and the plugin serves the request it served then, which a client could not get
		// past the gate today.
		results := emulator.StartTestServer(t, emulator.WithDeterministicRun(epoch)).ReplayEvents(t, events)
		emulator.AssertReplayMatches(t, results)
		assert.Zero(t, results.FailedEvents)
	})

	t.Run("an exported fixture carries the version", func(t *testing.T) {
		ts := emulator.StartTestServer(t, emulator.WithDeterministicRun(epoch))
		status, _ := wireACMCall(t, ts, "RequestCertificate", `{}`, nil)
		require.Equal(t, http.StatusBadRequest, status)
		events, err := ts.Store().GetEvents(t.Context(), emulator.EventFilter{Service: "acm"})
		require.NoError(t, err)
		src, err := emulator.GenerateTestFixture(events, "fixtures_test", "TestACMRefusal")
		require.NoError(t, err)
		assert.Contains(t, src, `"validation_model": "botocore/`)
	})
}

// TestWireValidation_QueryRequestsAreReadFromTheirParameters runs the Query adapter on an
// awsQuery service whose request body the server re-encodes as JSON (iam, sts, sqs, sns): the
// gate reads the parsed form parameters, which the re-encoding leaves in place.
func TestWireValidation_QueryRequestsAreReadFromTheirParameters(t *testing.T) {
	for _, tc := range []struct {
		name    string
		service string
		op      string
		params  url.Values
		want    []string
	}{
		{"a missing member", "sns", "CreateTopic", url.Values{}, []string{"name"}},
		{"a list element's member", "sns", "TagResource", url.Values{
			"ResourceArn": {"arn:aws:sns:us-east-1:123456789012:t"}, "Tags.member.1.Value": {"v"},
		}, []string{"tags.1.member.key"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &emulator.AWSRequest{Service: tc.service, Operation: tc.op, Params: map[string]string{}}
			for k, v := range tc.params {
				req.Params[k] = v[0]
			}
			got := emulator.WireFailurePathsForTest(req)
			assert.Equal(t, tc.want, got, fmt.Sprintf("%s %s", tc.service, tc.op))
		})
	}
}
