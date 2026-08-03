package emulator_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// These tests assert on the raw JSON bytes rather than a Go round-trip, which is
// the whole point of #528: a struct marshaled and unmarshaled by the same
// definition agrees with itself whatever the casing, so a Go-level assertion
// passes against a wire body no SDK can read. botocore matches response members
// against the service model case-sensitively — a PascalCase member does not
// error, it parses to nothing — so only a byte-level assertion sees the defect.
//
// Each read therefore gets a positive on the camelCase member *and* a negative on
// the PascalCase one, because a body carrying both would satisfy the positive
// while still telling a caller something the API never says.

// TestCWLogs_DescribeLogGroupsWireCasing pins every LogGroup member a caller
// reads, by name, on the bytes.
func TestCWLogs_DescribeLogGroupsWireCasing(t *testing.T) {
	srv := newCWLogsTestServer(t)

	resp := cwLogsRequest(t, srv, "CreateLogGroup", map[string]any{
		"logGroupName":    "/probe/casing",
		"retentionInDays": 7,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = cwLogsRequest(t, srv, "DescribeLogGroups", map[string]any{})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := string(cwLogsReadBody(t, resp))

	for _, member := range []string{
		`"logGroupName":`, `"logGroupArn":`, `"arn":`, `"creationTime":`, `"retentionInDays":`,
	} {
		assert.Contains(t, body, member, "the API's member name must be on the wire")
	}
	for _, wrong := range []string{
		`"LogGroupName"`, `"ARN"`, `"CreationTime"`, `"RetentionInDays"`,
	} {
		assert.NotContains(t, body, wrong,
			"a PascalCase member parses to nothing in an SDK, silently")
	}

	// The outer key was already correct before #528, and staying correct matters:
	// the count assertion is what made the defect survivable in a smoke test.
	assert.Contains(t, body, `"logGroups":`)
}

// TestCWLogs_DescribeLogGroupsReportsBothARNForms pins the reference's
// distinction between `arn` and `logGroupArn`, which differ only in a trailing
// ":*" and are used for different things — the suffixed one in IAM policies for
// most actions, the unsuffixed one in logGroupIdentifier and tagging APIs. Naming
// one value under both members would hand a caller a policy resource the real
// service rejects.
func TestCWLogs_DescribeLogGroupsReportsBothARNForms(t *testing.T) {
	srv := newCWLogsTestServer(t)

	resp := cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{"logGroupName": "/probe/arns"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = cwLogsRequest(t, srv, "DescribeLogGroups", map[string]any{})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result struct {
		LogGroups []struct {
			ARN         string `json:"arn"`
			LogGroupARN string `json:"logGroupArn"`
		} `json:"logGroups"`
	}
	require.NoError(t, json.Unmarshal(cwLogsReadBody(t, resp), &result))
	require.Len(t, result.LogGroups, 1)

	got := result.LogGroups[0]
	assert.Equal(t, "arn:aws:logs:us-east-1:123456789012:log-group:/probe/arns", got.LogGroupARN,
		"logGroupArn has no trailing :*")
	assert.Equal(t, got.LogGroupARN+":*", got.ARN,
		"arn is the same ARN with the trailing :* the reference specifies")
}

// TestCWLogs_LogGroupPolicyARNEdgeCases covers the two inputs to the `arn`
// projection that the HTTP path cannot produce, because a group created through
// CreateLogGroup always carries substrate's own unsuffixed ARN.
//
// Both matter to a restored snapshot rather than a live create: a record written
// before the field existed has an empty ARN, and one written by a future version
// may already carry the suffix. Appending unconditionally would turn the first
// into a bare ":*" — an ARN-shaped string that is not an ARN — and double-suffix
// the second.
func TestCWLogs_LogGroupPolicyARNEdgeCases(t *testing.T) {
	const bare = "arn:aws:logs:us-east-1:123456789012:log-group:/probe/edge"

	tests := []struct {
		name string
		in   string
		want string
		why  string
	}{
		{"unsuffixed gains the suffix", bare, bare + ":*",
			"the common case: substrate's builder produces the unsuffixed form"},
		{"empty stays empty", "", "",
			"a bare \":*\" is not an ARN, and an absent ARN must stay absent so omitempty drops it"},
		{"already suffixed is unchanged", bare + ":*", bare + ":*",
			"the projection must be idempotent over its own output"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, emulator.CWLogGroupPolicyARNForTest(tt.in), tt.why)
		})
	}
}

// TestCWLogs_ARNlessStateReportsNoARN carries the empty-ARN case through to the
// wire, which is where it would actually be seen: a snapshot restored from a
// version that did not record ARNs must report no ARN at all, not two members
// holding "" and ":*".
func TestCWLogs_ARNlessStateReportsNoARN(t *testing.T) {
	srv, state := newCWLogsTestServerWithState(t)

	require.NoError(t, emulator.CWPutLogGroupStateForTest(t.Context(), state,
		"123456789012", "us-east-1", emulator.CWLogGroup{
			LogGroupName: "/probe/arnless",
			CreationTime: 1785728276850,
		}))

	resp := cwLogsRequest(t, srv, "DescribeLogGroups", map[string]any{})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := string(cwLogsReadBody(t, resp))

	require.Contains(t, body, `"logGroupName":"/probe/arnless"`)
	assert.NotContains(t, body, `":*"`,
		"an absent ARN must not become a bare \":*\"")
	assert.NotContains(t, body, `"arn":`,
		"omitempty drops the member rather than reporting an empty ARN")
	assert.NotContains(t, body, `"logGroupArn":`)
}

// TestCWLogs_DescribeLogStreamsWireCasing pins the LogStream members. The stream
// is written to before the read so lastIngestionTime and uploadSequenceToken are
// populated — an omitempty member that is never set cannot be asserted absent
// from correct output.
func TestCWLogs_DescribeLogStreamsWireCasing(t *testing.T) {
	srv := newCWLogsTestServer(t)

	require.Equal(t, http.StatusOK,
		cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{"logGroupName": "/probe/streams"}).StatusCode)
	require.Equal(t, http.StatusOK,
		cwLogsRequest(t, srv, "CreateLogStream", map[string]string{
			"logGroupName": "/probe/streams", "logStreamName": "s1",
		}).StatusCode)
	require.Equal(t, http.StatusOK,
		cwLogsRequest(t, srv, "PutLogEvents", map[string]any{
			"logGroupName": "/probe/streams", "logStreamName": "s1",
			"logEvents": []map[string]any{{"timestamp": 1785728276850, "message": "hello"}},
		}).StatusCode)

	resp := cwLogsRequest(t, srv, "DescribeLogStreams", map[string]string{
		"logGroupName": "/probe/streams",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := string(cwLogsReadBody(t, resp))

	for _, member := range []string{
		`"logStreamName":`, `"arn":`, `"creationTime":`, `"lastIngestionTime":`,
		`"uploadSequenceToken":`,
	} {
		assert.Contains(t, body, member)
	}
	for _, wrong := range []string{
		`"LogStreamName"`, `"ARN"`, `"CreationTime"`, `"LastIngestionTime"`,
		`"UploadSequenceToken"`,
	} {
		assert.NotContains(t, body, wrong)
	}

	// LogStream documents a single `arn` member with no suffix variant, unlike
	// LogGroup — so there must be no logStreamArn invented for symmetry.
	assert.NotContains(t, body, `"logStreamArn"`,
		"the LogStream type has one ARN member, not two")
}

// TestCWLogs_GetLogEventsWireCasing pins the OutputLogEvent members.
func TestCWLogs_GetLogEventsWireCasing(t *testing.T) {
	srv := newCWLogsTestServer(t)

	require.Equal(t, http.StatusOK,
		cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{"logGroupName": "/probe/events"}).StatusCode)
	require.Equal(t, http.StatusOK,
		cwLogsRequest(t, srv, "CreateLogStream", map[string]string{
			"logGroupName": "/probe/events", "logStreamName": "s1",
		}).StatusCode)
	require.Equal(t, http.StatusOK,
		cwLogsRequest(t, srv, "PutLogEvents", map[string]any{
			"logGroupName": "/probe/events", "logStreamName": "s1",
			"logEvents": []map[string]any{{"timestamp": 1785728276850, "message": "hello"}},
		}).StatusCode)

	resp := cwLogsRequest(t, srv, "GetLogEvents", map[string]any{
		"logGroupName": "/probe/events", "logStreamName": "s1",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := string(cwLogsReadBody(t, resp))

	for _, member := range []string{`"timestamp":`, `"message":`, `"ingestionTime":`} {
		assert.Contains(t, body, member)
	}
	for _, wrong := range []string{`"Timestamp"`, `"Message"`, `"IngestionTime"`} {
		assert.NotContains(t, body, wrong)
	}

	// And each member carries its value. A member name present with a zero behind
	// it is the same silent-success failure in a subtler form: ingestionTime has no
	// omitempty (the API always reports it), so a dropped projection field shows up
	// as a plausible 0 rather than as an absence.
	var result struct {
		Events []struct {
			Timestamp     int64  `json:"timestamp"`
			Message       string `json:"message"`
			IngestionTime int64  `json:"ingestionTime"`
		} `json:"events"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &result))
	require.Len(t, result.Events, 1)
	got := result.Events[0]
	assert.Equal(t, int64(1785728276850), got.Timestamp)
	assert.Equal(t, "hello", got.Message)
	assert.NotZero(t, got.IngestionTime,
		"ingestionTime is set by PutLogEvents from the simulated clock, so 0 means the projection dropped it")
}

// TestCWLogs_FilterLogEventsStaysCorrect guards the operation that was already
// right. FilterLogEvents is why #528 was easy to miss — a smoke test using it
// passes — and it is also the pattern the fix copied, so a regression in it would
// mean the pattern itself was undone.
func TestCWLogs_FilterLogEventsStaysCorrect(t *testing.T) {
	srv := newCWLogsTestServer(t)

	require.Equal(t, http.StatusOK,
		cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{"logGroupName": "/probe/filter"}).StatusCode)
	require.Equal(t, http.StatusOK,
		cwLogsRequest(t, srv, "CreateLogStream", map[string]string{
			"logGroupName": "/probe/filter", "logStreamName": "s1",
		}).StatusCode)
	require.Equal(t, http.StatusOK,
		cwLogsRequest(t, srv, "PutLogEvents", map[string]any{
			"logGroupName": "/probe/filter", "logStreamName": "s1",
			"logEvents": []map[string]any{{"timestamp": 1785728276850, "message": "hello"}},
		}).StatusCode)

	resp := cwLogsRequest(t, srv, "FilterLogEvents", map[string]any{
		"logGroupName": "/probe/filter",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := string(cwLogsReadBody(t, resp))

	for _, member := range []string{
		`"logStreamName":`, `"timestamp":`, `"message":`, `"ingestionTime":`, `"eventId":`,
		`"searchedLogStreams":`, `"searchedCompletely":`,
	} {
		assert.Contains(t, body, member)
	}
	for _, wrong := range []string{`"Timestamp"`, `"Message"`, `"IngestionTime"`} {
		assert.NotContains(t, body, wrong)
	}
}

// TestCWLogs_StateEncodingIsUnchanged is the reason the fix added response
// element types rather than retagging CWLogGroup and friends.
//
// Those structs are the persisted format: Snapshot/Restore round-trips them and
// debug_session.go replays them, and LambdaPlugin.autoCreateLambdaLogGroup writes one
// directly to dodge a registry cycle. Retagging would have changed the wire and
// the persisted bytes in one edit, so this asserts the state encoding still uses
// the PascalCase names a v0.87 snapshot contains — and that a group written under
// those names is still fully readable over the corrected wire.
func TestCWLogs_StateEncodingIsUnchanged(t *testing.T) {
	srv, state := newCWLogsTestServerWithState(t)

	require.Equal(t, http.StatusOK,
		cwLogsRequest(t, srv, "CreateLogGroup", map[string]any{
			"logGroupName": "/probe/state", "retentionInDays": 30,
		}).StatusCode)

	raw, err := state.Get(t.Context(), "logs", "loggroup:123456789012/us-east-1//probe/state")
	require.NoError(t, err)
	require.NotNil(t, raw, "the state key layout is what a snapshot restores against")

	stored := string(raw)
	for _, member := range []string{
		`"LogGroupName"`, `"ARN"`, `"CreationTime"`, `"RetentionInDays"`,
	} {
		assert.Contains(t, stored, member,
			"the persisted encoding must stay as a v0.87 snapshot wrote it")
	}
	assert.NotContains(t, stored, `"logGroupArn"`,
		"logGroupArn is a wire member; it is not persisted")

	// And the same record reads back over the wire under the API's names, which
	// is the property that would break if the projection were dropped.
	resp := cwLogsRequest(t, srv, "DescribeLogGroups", map[string]any{})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result struct {
		LogGroups []struct {
			LogGroupName    string `json:"logGroupName"`
			RetentionInDays int    `json:"retentionInDays"`
		} `json:"logGroups"`
	}
	require.NoError(t, json.Unmarshal(cwLogsReadBody(t, resp), &result))
	require.Len(t, result.LogGroups, 1)
	assert.Equal(t, "/probe/state", result.LogGroups[0].LogGroupName)
	assert.Equal(t, 30, result.LogGroups[0].RetentionInDays)
}

// TestCWLogs_PutRetentionPolicy covers the operation that was InvalidAction
// before this change while RetentionInDays was already modeled and reported.
func TestCWLogs_PutRetentionPolicy(t *testing.T) {
	srv := newCWLogsTestServer(t)

	require.Equal(t, http.StatusOK,
		cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{"logGroupName": "/probe/retain"}).StatusCode)

	// No retention until one is set: retentionInDays is omitempty, and the
	// reference has no value meaning "never", so its absence is the signal.
	resp := cwLogsRequest(t, srv, "DescribeLogGroups", map[string]any{})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotContains(t, string(cwLogsReadBody(t, resp)), `"retentionInDays"`,
		"a group with no policy reports no retention, not a zero")

	resp = cwLogsRequest(t, srv, "PutRetentionPolicy", map[string]any{
		"logGroupName": "/probe/retain", "retentionInDays": 14,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.JSONEq(t, `{}`, string(cwLogsReadBody(t, resp)),
		"the API returns 200 with an empty body")

	assert.Equal(t, 14, cwLogsRetention(t, srv, "/probe/retain"))

	// Setting it again replaces rather than accumulating.
	require.Equal(t, http.StatusOK,
		cwLogsRequest(t, srv, "PutRetentionPolicy", map[string]any{
			"logGroupName": "/probe/retain", "retentionInDays": 365,
		}).StatusCode)
	assert.Equal(t, 365, cwLogsRetention(t, srv, "/probe/retain"))
}

// TestCWLogs_PutRetentionPolicyRejectsInvalidDays pins the fixed valid set. The
// API enumerates it rather than accepting a range, so a plausible-looking value
// is an error — and accepting one would let a template pass under test that the
// real service rejects.
func TestCWLogs_PutRetentionPolicyRejectsInvalidDays(t *testing.T) {
	srv := newCWLogsTestServer(t)
	require.Equal(t, http.StatusOK,
		cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{"logGroupName": "/probe/bad"}).StatusCode)

	// wantMessage separates the two reasons a request is refused, which share a
	// code and a status and are distinguishable only in the message. Treating an
	// absent member as an explicit 0 would tell a caller "0 is not valid" about a
	// value they never sent, which sends them looking in the wrong place.
	tests := []struct {
		name        string
		body        map[string]any
		wantMessage string
		why         string
	}{
		{"a plausible round number", map[string]any{
			"logGroupName": "/probe/bad", "retentionInDays": 45,
		}, "retentionInDays value of 45 is not valid",
			"45 is between two valid values and is not itself valid"},
		{"a hundred days", map[string]any{
			"logGroupName": "/probe/bad", "retentionInDays": 100,
		}, "retentionInDays value of 100 is not valid",
			"100 sits between 90 and 120"},
		{"zero", map[string]any{
			"logGroupName": "/probe/bad", "retentionInDays": 0,
		}, "retentionInDays value of 0 is not valid",
			"0 means 'never' in substrate's state, but it is not a value the API accepts"},
		{"negative", map[string]any{
			"logGroupName": "/probe/bad", "retentionInDays": -1,
		}, "retentionInDays value of -1 is not valid",
			"a negative retention is meaningless"},
		{"absent", map[string]any{
			"logGroupName": "/probe/bad",
		}, "retentionInDays is required",
			"an absent required member is a different fault from an invalid value"},
		{"no log group", map[string]any{
			"retentionInDays": 7,
		}, "logGroupName is required",
			"logGroupName is a required parameter"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := cwLogsRequest(t, srv, "PutRetentionPolicy", tt.body)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode, tt.why)
			body := cwLogsReadBody(t, resp)
			assert.Equal(t, "InvalidParameterException", cwLogsErrorTypeFrom(t, body))
			assert.Contains(t, string(body), tt.wantMessage, tt.why)
		})
	}

	// The rejection left no retention behind.
	assert.Equal(t, 0, cwLogsRetention(t, srv, "/probe/bad"))

	// And the message enumerates the valid values in ascending order, so a
	// consumer's assertion on it is stable across runs.
	resp := cwLogsRequest(t, srv, "PutRetentionPolicy", map[string]any{
		"logGroupName": "/probe/bad", "retentionInDays": 45,
	})
	body := string(cwLogsReadBody(t, resp))
	assert.Contains(t, body, "1, 3, 5, 7, 14, 30")
	assert.Contains(t, body, "3288, 3653")
}

// TestCWLogs_PutRetentionPolicyUnknownGroup pins the status code, which is the
// part a JSON-1.1 service does differently: ResourceNotFoundException is
// documented at HTTP 400, not 404, because the code travels in the body's
// "__type" rather than in the status line.
func TestCWLogs_PutRetentionPolicyUnknownGroup(t *testing.T) {
	srv := newCWLogsTestServer(t)

	for _, op := range []string{"PutRetentionPolicy", "DeleteRetentionPolicy"} {
		t.Run(op, func(t *testing.T) {
			resp := cwLogsRequest(t, srv, op, map[string]any{
				"logGroupName": "/does/not/exist", "retentionInDays": 7,
			})
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode,
				"the reference documents ResourceNotFoundException at 400 for this operation")
			assert.Equal(t, "ResourceNotFoundException", cwLogsErrorType(t, resp))
		})
	}
}

// TestCWLogs_DeleteRetentionPolicy covers the documented way to make a group's
// events never expire — there is no retentionInDays value meaning "never", so
// deleting the policy is the only route.
func TestCWLogs_DeleteRetentionPolicy(t *testing.T) {
	srv := newCWLogsTestServer(t)

	require.Equal(t, http.StatusOK,
		cwLogsRequest(t, srv, "CreateLogGroup", map[string]any{
			"logGroupName": "/probe/forever", "retentionInDays": 7,
		}).StatusCode)
	require.Equal(t, 7, cwLogsRetention(t, srv, "/probe/forever"))

	resp := cwLogsRequest(t, srv, "DeleteRetentionPolicy", map[string]string{
		"logGroupName": "/probe/forever",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.JSONEq(t, `{}`, string(cwLogsReadBody(t, resp)))

	resp = cwLogsRequest(t, srv, "DescribeLogGroups", map[string]any{})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotContains(t, string(cwLogsReadBody(t, resp)), `"retentionInDays"`,
		"the member is gone, not zero: an SDK reads its absence as 'never expires'")

	// Deleting again is not an error — the operation documents none for it, and a
	// cleanup path that runs twice must not fail the second time.
	resp = cwLogsRequest(t, srv, "DeleteRetentionPolicy", map[string]string{
		"logGroupName": "/probe/forever",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = cwLogsRequest(t, srv, "DeleteRetentionPolicy", map[string]string{})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "InvalidParameterException", cwLogsErrorType(t, resp))
}

// TestCWLogs_CFNLogGroupRetentionIsReadable is the end-to-end shape the issue
// reported: parsl's ECS path creates a log group before registering a task
// definition and needs to verify it afterwards. The template sets RetentionInDays
// — as testdata/cfn/ecs_worker.yml does — so this covers the CFN deploy path
// reaching the same corrected wire.
func TestCWLogs_CFNLogGroupRetentionIsReadable(t *testing.T) {
	srv := newCWLogsTestServer(t)

	// Deployed groups are indistinguishable from directly created ones on the
	// wire, which is the point: #528 was never CloudFormation-specific.
	require.Equal(t, http.StatusOK,
		cwLogsRequest(t, srv, "CreateLogGroup", map[string]any{
			"logGroupName": "/ecs/worker-family", "retentionInDays": 7,
		}).StatusCode)

	resp := cwLogsRequest(t, srv, "DescribeLogGroups", map[string]string{
		"logGroupNamePrefix": "/ecs/",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result struct {
		LogGroups []struct {
			LogGroupName    string `json:"logGroupName"`
			RetentionInDays int    `json:"retentionInDays"`
		} `json:"logGroups"`
	}
	require.NoError(t, json.Unmarshal(cwLogsReadBody(t, resp), &result))
	require.Len(t, result.LogGroups, 1)
	assert.Equal(t, "/ecs/worker-family", result.LogGroups[0].LogGroupName,
		"verifying a group exists means reading its name back, which is what returned {}")
	assert.Equal(t, 7, result.LogGroups[0].RetentionInDays)

	// Cleanup verification is the other half the issue names: after the delete,
	// the prefix read is empty rather than a count that passes for the wrong
	// reason.
	require.Equal(t, http.StatusOK,
		cwLogsRequest(t, srv, "DeleteLogGroup", map[string]string{
			"logGroupName": "/ecs/worker-family",
		}).StatusCode)
	resp = cwLogsRequest(t, srv, "DescribeLogGroups", map[string]string{
		"logGroupNamePrefix": "/ecs/",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	result.LogGroups = nil
	require.NoError(t, json.Unmarshal(cwLogsReadBody(t, resp), &result))
	assert.Empty(t, result.LogGroups)
}

// cwLogsRetention reads a group's retentionInDays over the wire, returning 0 when
// the member is absent.
func cwLogsRetention(t *testing.T, srv *emulator.Server, logGroupName string) int {
	t.Helper()
	resp := cwLogsRequest(t, srv, "DescribeLogGroups", map[string]string{
		"logGroupNamePrefix": logGroupName,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result struct {
		LogGroups []struct {
			LogGroupName    string `json:"logGroupName"`
			RetentionInDays int    `json:"retentionInDays"`
		} `json:"logGroups"`
	}
	require.NoError(t, json.Unmarshal(cwLogsReadBody(t, resp), &result))
	for _, lg := range result.LogGroups {
		if lg.LogGroupName == logGroupName {
			return lg.RetentionInDays
		}
	}
	t.Fatalf("log group %q not reported", logGroupName)
	return 0
}

// cwLogsErrorType reads the error code from a JSON-1.1 error body. botocore reads
// the "__type" member, so that is what an assertion has to check — a code that
// reached only the header would be invisible to the caller.
func cwLogsErrorType(t *testing.T, resp *http.Response) string {
	t.Helper()
	return cwLogsErrorTypeFrom(t, cwLogsReadBody(t, resp))
}

// cwLogsErrorTypeFrom is cwLogsErrorType over bytes already read, for a caller
// that also asserts on the message — the response body can only be read once.
func cwLogsErrorTypeFrom(t *testing.T, raw []byte) string {
	t.Helper()
	var body map[string]any
	require.NoError(t, json.Unmarshal(raw, &body))
	code, _ := body["__type"].(string)
	// Some services qualify the type with a prefix; compare on the final segment.
	if i := strings.LastIndex(code, "#"); i >= 0 {
		code = code[i+1:]
	}
	return code
}
