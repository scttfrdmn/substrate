package emulator_test

import (
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// parseFixture fails the test unless src is valid Go.
func parseFixture(t *testing.T, src string) {
	t.Helper()
	if _, err := parser.ParseFile(token.NewFileSet(), "", src, 0); err != nil {
		t.Fatalf("generated source is not valid Go: %v\nsource:\n%s", err, src)
	}
}

func TestGenerateTestFixture_Empty(t *testing.T) {
	src, err := emulator.GenerateTestFixture(nil, "mytestpkg", "TestEmpty")
	require.NoError(t, err)
	assert.Contains(t, src, "package mytestpkg")
	assert.Contains(t, src, "func TestEmpty")
	assert.Contains(t, src, "t.Skip(")
	parseFixture(t, src)
}

// TestGenerateTestFixture_EmbedsTheRecordedEvents pins the fixture shape (#1602): the
// events are embedded whole, headers and request ID included, and replayed in process.
func TestGenerateTestFixture_EmbedsTheRecordedEvents(t *testing.T) {
	events := []*emulator.Event{{
		Sequence:  0,
		RequestID: "req-recorded-0001",
		Service:   "dynamodb",
		Operation: "ListTables",
		Request: &emulator.AWSRequest{
			Service:    "dynamodb",
			Operation:  "ListTables",
			HTTPMethod: "POST",
			Path:       "/",
			Headers:    map[string]string{"X-Amz-Target": "DynamoDB_20120810.ListTables"},
			Body:       []byte(`{}`),
		},
		Response: &emulator.AWSResponse{StatusCode: 200, Body: []byte(`{"TableNames":[]}`)},
	}}

	src, err := emulator.GenerateTestFixture(events, "substrate_test", "TestDynamoReplay")
	require.NoError(t, err)
	parseFixture(t, src)

	for _, want := range []string{
		"DynamoDB_20120810.ListTables",
		"req-recorded-0001",
		`"HTTPMethod": "POST"`,
		"const testDynamoReplayEvents =",
		"var testDynamoReplayAllow = []string{}",
		"ts.ReplayEvents(t, events)",
		"emulator.AssertReplayMatches(t, results, testDynamoReplayAllow...)",
	} {
		assert.Contains(t, src, want)
	}
}

// TestGenerateTestFixture_Backtick pins that a backtick in a recorded value cannot end
// the raw string literal the events are embedded in.
func TestGenerateTestFixture_Backtick(t *testing.T) {
	events := []*emulator.Event{{
		Service: "s3", Operation: "PutObject",
		Request: &emulator.AWSRequest{
			Service: "s3", Operation: "PutObject", Path: "/bucket/key",
			Headers: map[string]string{"X-Amz-Meta-Note": "a ` backtick"},
		},
	}}
	src, err := emulator.GenerateTestFixture(events, "substrate_test", "TestBacktick")
	require.NoError(t, err)
	parseFixture(t, src)
}

// TestGenerateTestFixture_RefusesAnUnreplayableEvent pins that an event the fixture could
// only report as unverified is refused when the fixture is generated.
func TestGenerateTestFixture_RefusesAnUnreplayableEvent(t *testing.T) {
	for name, ev := range map[string]*emulator.Event{
		"no request": {Sequence: 3, Service: "iam", Operation: "ListUsers"},
		"over the cap": {
			Sequence: 4, Service: "s3", Operation: "PutObject",
			Request:            &emulator.AWSRequest{Service: "s3", Operation: "PutObject"},
			RequestBodyOmitted: &emulator.BodyDigest{Length: 2 << 20, SHA256: "ab"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := emulator.GenerateTestFixture([]*emulator.Event{ev}, "substrate_test", "TestX")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "seq ")
		})
	}
}

func TestGenerateTestFixture_InvalidTestName(t *testing.T) {
	events := []*emulator.Event{{
		Service: "sqs", Operation: "ListQueues",
		Request: &emulator.AWSRequest{Service: "sqs", Operation: "ListQueues"},
	}}
	_, err := emulator.GenerateTestFixture(events, "substrate_test", "not a name")
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "valid test name"), err.Error())
}
