package emulator_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// fixtureDynamoCall sends one DynamoDB JSON-protocol call, which routes on X-Amz-Target.
func fixtureDynamoCall(t *testing.T, ts *emulator.TestServer, op, body string) []byte {
	t.Helper()
	status, out := replayDo(t, ts, http.MethodPost, "/", "dynamodb.us-east-1.amazonaws.com", []byte(body),
		map[string]string{
			"X-Amz-Target": "DynamoDB_20120810." + op,
			"Content-Type": "application/x-amz-json-1.0",
		})
	require.Equal(t, http.StatusOK, status, "%s: %s", op, out)
	return out
}

// fixtureSQSQueryCall sends one SQS call in the Query protocol, a form-encoded POST.
func fixtureSQSQueryCall(t *testing.T, ts *emulator.TestServer, form url.Values) []byte {
	t.Helper()
	form.Set("Version", "2012-11-05")
	status, out := replayDo(t, ts, http.MethodPost, "/", "sqs.us-east-1.amazonaws.com", []byte(form.Encode()),
		map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
	require.Equal(t, http.StatusOK, status, "%s: %s", form.Get("Action"), out)
	return out
}

// recordDynamoSession records a DynamoDB session and returns its events.
func recordDynamoSession(t *testing.T) []*emulator.Event {
	t.Helper()
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(replayClockStart)
	fixtureDynamoCall(t, ts, "CreateTable", `{"TableName":"orders",`+
		`"AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],`+
		`"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],"BillingMode":"PAY_PER_REQUEST"}`)
	fixtureDynamoCall(t, ts, "PutItem", `{"TableName":"orders","Item":{"id":{"S":"o-1"},"total":{"N":"42"}}}`)
	fixtureDynamoCall(t, ts, "GetItem", `{"TableName":"orders","Key":{"id":{"S":"o-1"}}}`)
	fixtureDynamoCall(t, ts, "DescribeTable", `{"TableName":"orders"}`)
	fixtureDynamoCall(t, ts, "Scan", `{"TableName":"orders"}`)
	events, err := ts.Store().GetStream(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)
	require.Len(t, events, 5)
	return events
}

// recordSQSSession records an SQS session in the Query protocol and returns its events.
func recordSQSSession(t *testing.T) []*emulator.Event {
	t.Helper()
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(replayClockStart)
	body := fixtureSQSQueryCall(t, ts, url.Values{"Action": {"CreateQueue"}, "QueueName": {"jobs"}})
	queueURL := xmlElement(t, body, "QueueUrl")
	fixtureSQSQueryCall(t, ts, url.Values{"Action": {"SendMessage"}, "QueueUrl": {queueURL}, "MessageBody": {"hello"}})
	fixtureSQSQueryCall(t, ts, url.Values{"Action": {"GetQueueAttributes"}, "QueueUrl": {queueURL},
		"AttributeName.1": {"All"}})
	fixtureSQSQueryCall(t, ts, url.Values{"Action": {"ReceiveMessage"}, "QueueUrl": {queueURL}})
	fixtureSQSQueryCall(t, ts, url.Values{"Action": {"ListQueues"}})
	events, err := ts.Store().GetStream(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)
	require.Len(t, events, 5)
	return events
}

// roundTrip passes events through the JSON a fixture embeds them as.
func roundTrip(t *testing.T, events []*emulator.Event) []*emulator.Event {
	t.Helper()
	raw, err := json.Marshal(events)
	require.NoError(t, err)
	var out []*emulator.Event
	require.NoError(t, json.Unmarshal(raw, &out))
	return out
}

// TestFixture_JSONAndQuerySessionsReplay is #1602's acceptance in process: a DynamoDB
// session (JSON protocol, routed by X-Amz-Target) and an SQS session (Query protocol,
// form-encoded) each generate a fixture, and the events the fixture embeds replay into
// a fresh test server with no difference at all.
func TestFixture_JSONAndQuerySessionsReplay(t *testing.T) {
	for name, record := range map[string]func(*testing.T) []*emulator.Event{
		"dynamodb": recordDynamoSession,
		"sqs":      recordSQSSession,
	} {
		t.Run(name, func(t *testing.T) {
			events := record(t)
			src, err := emulator.GenerateTestFixture(events, "fixture_test", "TestRecorded")
			require.NoError(t, err)
			parseFixture(t, src)

			ts := emulator.StartTestServer(t)
			results := ts.ReplayEvents(t, roundTrip(t, events))
			emulator.AssertReplayMatches(t, results)
			assert.Equal(t, len(events), results.SuccessEvents)

			// The server holds the replayed state, so a test can carry on from it.
			if name == "dynamodb" {
				out := fixtureDynamoCall(t, ts, "GetItem", `{"TableName":"orders","Key":{"id":{"S":"o-1"}}}`)
				assert.Contains(t, string(out), `"42"`)
			}
		})
	}
}

// exportFixture calls GET /v1/debug/export for the default stream.
func exportFixture(t *testing.T, ts *emulator.TestServer) (int, string) {
	t.Helper()
	resp, err := http.Get(ts.URL + "/v1/debug/export?stream=default") //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(raw)
}

// TestFixture_ExportCarriesWhatItCanAndRefusesWhatItCannot pins that an export from a
// server without a default VPC starts its fixture's server the same way, and that one
// from a server with fault rules armed is refused.
func TestFixture_ExportCarriesWhatItCanAndRefusesWhatItCannot(t *testing.T) {
	t.Run("without a default VPC", func(t *testing.T) {
		ts := emulator.StartTestServer(t, emulator.WithoutDefaultVPC())
		ts.FreezeTimeAt(replayClockStart)
		ec2ReplayCall(t, ts, "DescribeVpcs", url.Values{})
		status, src := exportFixture(t, ts)
		require.Equal(t, http.StatusOK, status, src)
		assert.Contains(t, src, "emulator.StartTestServer(t, emulator.WithoutDefaultVPC())")
		assert.Contains(t, src, "no fault rules armed")
		parseFixture(t, src)
	})
	t.Run("with CloudFormation's strict mode off", func(t *testing.T) {
		ts := emulator.StartTestServer(t, emulator.WithLenientCloudFormation())
		ts.FreezeTimeAt(replayClockStart)
		ec2ReplayCall(t, ts, "DescribeVpcs", url.Values{})
		status, src := exportFixture(t, ts)
		require.Equal(t, http.StatusOK, status, src)
		assert.Contains(t, src, "emulator.StartTestServer(t, emulator.WithLenientCloudFormation())")
		parseFixture(t, src)
	})
	t.Run("fault rules armed", func(t *testing.T) {
		ts := emulator.StartTestServer(t)
		replayPutBucket(t, ts, "armed")
		ts.FaultController().UpdateConfig(emulator.FaultConfig{Enabled: true, Rules: []emulator.FaultRule{
			{Service: "dynamodb", Operation: "NeverCalled"},
		}})
		status, body := exportFixture(t, ts)
		assert.Equal(t, http.StatusUnprocessableEntity, status)
		assert.Contains(t, body, "fault rules armed")
	})
}

// recordingTB captures the failures AssertReplayMatches reports.
type recordingTB struct {
	testing.TB
	errors []string
}

// Errorf records a failure instead of failing the enclosing test.
func (r *recordingTB) Errorf(format string, args ...any) {
	r.errors = append(r.errors, format)
}

// Helper is a no-op.
func (r *recordingTB) Helper() {}

// TestFixture_AssertReplayMatchesFailsOnADifference pins the assertion itself, and the
// allowlist's path.Match syntax.
func TestFixture_AssertReplayMatchesFailsOnADifference(t *testing.T) {
	results := &emulator.ReplayResults{
		Differences: []*emulator.EventDifference{
			{Sequence: 1, Operation: "DescribeTable", Field: "response_body/Table/TableId", Expected: "a", Actual: "b"},
		},
		SkipReasons: map[emulator.ReplaySkipReason]int{emulator.SkipClockDriven: 2},
	}
	rec := &recordingTB{TB: t}
	emulator.AssertReplayMatches(rec, results)
	assert.Len(t, rec.errors, 1, "one difference; a clock-driven skip is not a failure")

	rec = &recordingTB{TB: t}
	emulator.AssertReplayMatches(rec, results, "response_body/*/TableId")
	assert.Empty(t, rec.errors, "the allowlist covers the field")

	results.SkipReasons[emulator.SkipNoRequest] = 1
	rec = &recordingTB{TB: t}
	emulator.AssertReplayMatches(rec, results, "response_body/*/TableId")
	assert.Len(t, rec.errors, 1, "an unverified skip fails")
}

// TestFixture_ReplayDifferences pins the exported comparison.
func TestFixture_ReplayDifferences(t *testing.T) {
	same := &emulator.AWSResponse{StatusCode: 200, Body: []byte(`{"a":1,"b":[1,2]}`)}
	reordered := &emulator.AWSResponse{StatusCode: 200, Body: []byte(`{"b":[1,2],"a":1}`)}
	assert.Empty(t, emulator.ReplayDifferences(same, reordered), "member order is ignored")

	diffs := emulator.ReplayDifferences(same, &emulator.AWSResponse{StatusCode: 404, Body: []byte(`{"a":2,"b":[1,2]}`)})
	require.Len(t, diffs, 2)
	assert.Equal(t, "status_code", diffs[0].Field)
	assert.Equal(t, "response_body/a", diffs[1].Field)
	assert.Empty(t, emulator.ReplayDifferences(nil, nil))
}

// TestFixture_RegenerateCheckedInFixtures rewrites the generated fixtures this package
// runs in CI, replay_fixture_*_gen_test.go. It runs only when
// SUBSTRATE_REGENERATE_REPLAY_FIXTURES is set, because regenerating is a deliberate act:
// the checked-in files are the regression the suite holds substrate to.
func TestFixture_RegenerateCheckedInFixtures(t *testing.T) {
	if os.Getenv("SUBSTRATE_REGENERATE_REPLAY_FIXTURES") == "" {
		t.Skip("set SUBSTRATE_REGENERATE_REPLAY_FIXTURES=1 to rewrite the generated fixtures")
	}
	for file, gen := range map[string]struct {
		test   string
		record func(*testing.T) []*emulator.Event
	}{
		"replay_fixture_dynamodb_gen_test.go": {"TestGeneratedFixture_DynamoDB", recordDynamoSession},
		"replay_fixture_sqs_gen_test.go":      {"TestGeneratedFixture_SQSQuery", recordSQSSession},
	} {
		src, err := emulator.GenerateTestFixture(gen.record(t), "emulator_test", gen.test)
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(src, "// Code generated"))
		require.NoError(t, os.WriteFile(filepath.Clean(file), []byte(src), 0o600))
	}
}
