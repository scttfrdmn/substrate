package emulator_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// schedulerCompletionBody builds a CreateSchedule/UpdateSchedule body carrying the required members
// plus extra, a JSON fragment of further members (empty for none).
func schedulerCompletionBody(extra string) string {
	body := `{"ScheduleExpression":"rate(5 minutes)",` +
		`"Target":{"Arn":"arn:aws:lambda:us-east-1:123456789012:function:test-handler",` +
		`"RoleArn":"arn:aws:iam::123456789012:role/r"},` +
		`"FlexibleTimeWindow":{"Mode":"OFF"}`
	if extra != "" {
		body += "," + extra
	}
	return body + "}"
}

// getScheduleRaw reads a schedule back through GetSchedule and decodes it into a raw-member map, so
// an absent member is distinguishable from an empty one.
func getScheduleRaw(t *testing.T, ts *httptest.Server, name string) map[string]json.RawMessage {
	t.Helper()
	resp := schedulerRequest(t, ts, http.MethodGet, "/schedules/"+name, "")
	body := readSchedulerBody(t, resp)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "GetSchedule: %s", body)
	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &out))
	return out
}

// TestScheduler_CompletionMembersRoundTrip is #1442: ActionAfterCompletion, StartDate, EndDate and
// KmsKeyArn were missing from the request decoder, the record and GetSchedule's response, so a
// schedule created with ActionAfterCompletion=DELETE and an EndDate read back with neither.
func TestScheduler_CompletionMembersRoundTrip(t *testing.T) {
	ts := httptest.NewServer(newSchedulerTestServer(t))
	t.Cleanup(ts.Close)

	const extra = `"ActionAfterCompletion":"DELETE","StartDate":1767225600,` +
		`"EndDate":1801785600.5,"KmsKeyArn":"arn:aws:kms:us-east-1:123456789012:key/abc-123"`
	resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/self-deleting", schedulerCompletionBody(extra))
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	got := getScheduleRaw(t, ts, "self-deleting")
	assert.JSONEq(t, `"DELETE"`, string(got["ActionAfterCompletion"]))
	assert.JSONEq(t, `1767225600`, string(got["StartDate"]), "StartDate is epoch seconds, as CreationDate is")
	assert.JSONEq(t, `1801785600.5`, string(got["EndDate"]), "a fractional second survives")
	assert.JSONEq(t, `"arn:aws:kms:us-east-1:123456789012:key/abc-123"`, string(got["KmsKeyArn"]))
	assert.JSONEq(t, `"arn:aws:lambda:us-east-1:123456789012:function:test-handler"`,
		string(mustTargetArn(t, got)))
}

// mustTargetArn returns Target.Arn from a raw GetSchedule response.
func mustTargetArn(t *testing.T, got map[string]json.RawMessage) json.RawMessage {
	t.Helper()
	var target map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(got["Target"], &target))
	return target["Arn"]
}

// TestScheduler_CompletionMembersAbsentWhenUnset asserts a schedule created without the four members
// reports none of them: no page publishes a default for ActionAfterCompletion, and the dates and the
// key have none by nature, so reporting a value would invent one.
func TestScheduler_CompletionMembersAbsentWhenUnset(t *testing.T) {
	ts := httptest.NewServer(newSchedulerTestServer(t))
	t.Cleanup(ts.Close)

	resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/plain", schedulerCompletionBody(""))
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	got := getScheduleRaw(t, ts, "plain")
	for _, member := range []string{"ActionAfterCompletion", "StartDate", "EndDate", "KmsKeyArn"} {
		assert.NotContains(t, got, member)
	}
}

// TestScheduler_UpdateReplacesCompletionMembers asserts UpdateSchedule's documented replace rule holds
// for the four members: an update naming new values replaces them, and one omitting them clears them
// rather than keeping what the create set. The dates are in 2100: since #1475 a DELETE schedule whose
// EndDate has passed deletes itself on the next request, which a 1970 EndDate would.
func TestScheduler_UpdateReplacesCompletionMembers(t *testing.T) {
	ts := httptest.NewServer(newSchedulerTestServer(t))
	t.Cleanup(ts.Close)

	resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/replaced", schedulerCompletionBody(
		`"ActionAfterCompletion":"DELETE","StartDate":4102444800,"EndDate":4102448400,`+
			`"KmsKeyArn":"arn:aws:kms:us-east-1:123456789012:alias/one"`))
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = schedulerRequest(t, ts, http.MethodPut, "/schedules/replaced", schedulerCompletionBody(
		`"ActionAfterCompletion":"NONE","EndDate":4102452000`))
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	got := getScheduleRaw(t, ts, "replaced")
	assert.JSONEq(t, `"NONE"`, string(got["ActionAfterCompletion"]))
	assert.JSONEq(t, `4102452000`, string(got["EndDate"]))
	assert.NotContains(t, got, "StartDate", "an omitted StartDate is cleared, not kept")
	assert.NotContains(t, got, "KmsKeyArn", "an omitted KmsKeyArn is cleared, not kept")

	resp = schedulerRequest(t, ts, http.MethodPut, "/schedules/replaced", schedulerCompletionBody(""))
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)
	got = getScheduleRaw(t, ts, "replaced")
	assert.NotContains(t, got, "ActionAfterCompletion")
	assert.NotContains(t, got, "EndDate")
}

// TestScheduler_CompletionMembersValidated asserts the published constraints on the new members, and
// on ScheduleExpressionTimezone's length, are refused with ValidationException/400 on both create and
// update.
func TestScheduler_CompletionMembersValidated(t *testing.T) {
	cases := []struct {
		name   string
		extra  string
		member string
	}{
		{name: "unknown ActionAfterCompletion", extra: `"ActionAfterCompletion":"ARCHIVE"`, member: "actionAfterCompletion"},
		{name: "lowercase ActionAfterCompletion", extra: `"ActionAfterCompletion":"delete"`, member: "actionAfterCompletion"},
		{name: "KmsKeyArn not a KMS ARN", extra: `"KmsKeyArn":"arn:aws:s3:::bucket"`, member: "kmsKeyArn"},
		{name: "KmsKeyArn short account", extra: `"KmsKeyArn":"arn:aws:kms:us-east-1:1234:key/abc"`, member: "kmsKeyArn"},
		{name: "KmsKeyArn too long", extra: `"KmsKeyArn":"arn:aws:kms:us-east-1:123456789012:key/` + strings.Repeat("a", 2048) + `"`, member: "kmsKeyArn"},
		{name: "timezone too long", extra: `"ScheduleExpressionTimezone":"` + strings.Repeat("z", 51) + `"`, member: "scheduleExpressionTimezone"},
		{name: "EndDate not a number", extra: `"EndDate":"tomorrow"`, member: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(newSchedulerTestServer(t))
			t.Cleanup(ts.Close)

			resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/refused", schedulerCompletionBody(tc.extra))
			body := readSchedulerBody(t, resp)
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "create: %s", body)
			assert.Contains(t, string(body), "ValidationException")
			if tc.member != "" {
				assert.Contains(t, string(body), tc.member)
			}

			resp = schedulerRequest(t, ts, http.MethodPost, "/schedules/kept", schedulerCompletionBody(""))
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusOK, resp.StatusCode)
			resp = schedulerRequest(t, ts, http.MethodPut, "/schedules/kept", schedulerCompletionBody(tc.extra))
			body = readSchedulerBody(t, resp)
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "update: %s", body)
		})
	}
}

// TestScheduler_KmsKeyArnAcceptsKeysAndAliases asserts both forms the published pattern admits.
func TestScheduler_KmsKeyArnAcceptsKeysAndAliases(t *testing.T) {
	for _, arn := range []string{
		"arn:aws:kms:us-east-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab",
		"arn:aws:kms:eu-west-2:123456789012:alias/my_alias-1",
		"arn:aws-us-gov:kms:us-gov-west-1:123456789012:key/k",
	} {
		t.Run(arn, func(t *testing.T) {
			ts := httptest.NewServer(newSchedulerTestServer(t))
			t.Cleanup(ts.Close)
			resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/keyed", schedulerCompletionBody(`"KmsKeyArn":"`+arn+`"`))
			body := readSchedulerBody(t, resp)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
		})
	}
}
