package emulator_test

// EventBridge Scheduler's published request constraints (#1008) and its reachable empty-name guards
// (#1009).
//
// CreateSchedule and UpdateSchedule each publish three Required: Yes body members — ScheduleExpression,
// Target and FlexibleTimeWindow — plus a required Name in the URI, and Scheduler checked none of them:
// a POST with an empty body created a schedule with no expression and no target, which GetSchedule then
// reported as a live resource. Every assertion here names the constraint it comes from, and asserts the
// status alongside the code, per #923.
//
// The bodies are sent over HTTP rather than through HandleRequest, because two of the defects are
// routing and decoding properties that an in-process call cannot see: an empty path parameter used to
// route to ListSchedules, and a caller's `Target.RoleArn` used to be dropped by a decode into a struct
// whose JSON tag is `role_arn`.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// schedulerValidCreateBody is a create AWS would accept, as the baseline each case perturbs by one
// member. It carries a RoleArn and a MaximumWindowInMinutes so the round-trip assertion below has
// something to lose.
const schedulerValidCreateBody = `{
	"ScheduleExpression": "rate(5 minutes)",
	"Target": {
		"Arn": "arn:aws:lambda:us-east-1:123456789012:function:my-fn",
		"RoleArn": "arn:aws:iam::123456789012:role/my-role",
		"RetryPolicy": {"MaximumEventAgeInSeconds": 120, "MaximumRetryAttempts": 3}
	},
	"FlexibleTimeWindow": {"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 15}
}`

// schedulerErrorBody is the code and message of a refusal.
type schedulerErrorBody struct {
	Message string `json:"message"`
	Code    string `json:"Code"`
}

// schedulerRefusal sends a request and reads the refusal off it, failing the test if the status is not
// the 400 ValidationException is published at.
func schedulerRefusal(t *testing.T, ts *httptest.Server, method, path, body string) schedulerErrorBody {
	t.Helper()
	resp := schedulerRequest(t, ts, method, path, body)
	raw := readSchedulerBody(t, resp)
	var out schedulerErrorBody
	require.NoError(t, json.Unmarshal(raw, &out), "refusal body: %s", raw)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode,
		"ValidationException is published at HTTP 400: %s", raw)
	assert.Equal(t, "ValidationException", out.Code,
		"the only refusal Scheduler publishes for a constraint failure")
	return out
}

// TestSchedulerValidation_ARequiredMemberIsRefused walks the required set, one member removed at a time.
//
// Each case is the valid body minus exactly one member, so a failure names the guard that stopped
// working rather than reporting that "an empty body is refused" — which the last case asserts anyway,
// because an empty body is what substrate used to accept.
func TestSchedulerValidation_ARequiredMemberIsRefused(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	for _, tc := range []struct {
		name        string
		body        string
		wantMessage string
	}{
		{"noBodyAtAll", `{}`, "'scheduleExpression'"},
		{"noScheduleExpression", `{
			"Target": {"Arn": "arn:aws:sqs:us-east-1:123456789012:q", "RoleArn": "arn:aws:iam::123456789012:role/r"},
			"FlexibleTimeWindow": {"Mode": "OFF"}
		}`, "'scheduleExpression'"},
		{"noTarget", `{
			"ScheduleExpression": "rate(5 minutes)",
			"FlexibleTimeWindow": {"Mode": "OFF"}
		}`, "'target'"},
		{"noTargetArn", `{
			"ScheduleExpression": "rate(5 minutes)",
			"Target": {"RoleArn": "arn:aws:iam::123456789012:role/r"},
			"FlexibleTimeWindow": {"Mode": "OFF"}
		}`, "'target.arn'"},
		{"noTargetRoleArn", `{
			"ScheduleExpression": "rate(5 minutes)",
			"Target": {"Arn": "arn:aws:sqs:us-east-1:123456789012:q"},
			"FlexibleTimeWindow": {"Mode": "OFF"}
		}`, "'target.roleArn'"},
		{"noFlexibleTimeWindow", `{
			"ScheduleExpression": "rate(5 minutes)",
			"Target": {"Arn": "arn:aws:sqs:us-east-1:123456789012:q", "RoleArn": "arn:aws:iam::123456789012:role/r"}
		}`, "'flexibleTimeWindow'"},
		{"noFlexibleTimeWindowMode", `{
			"ScheduleExpression": "rate(5 minutes)",
			"Target": {"Arn": "arn:aws:sqs:us-east-1:123456789012:q", "RoleArn": "arn:aws:iam::123456789012:role/r"},
			"FlexibleTimeWindow": {}
		}`, "'flexibleTimeWindow.mode'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := schedulerRefusal(t, ts, http.MethodPost, "/schedules/required-"+tc.name, tc.body)
			assert.Contains(t, err.Message, tc.wantMessage, "the message names the member")
			assert.Contains(t, err.Message, "must not be null",
				"a required member absent is a null-member refusal")
		})
	}
}

// TestSchedulerValidation_AValueOutsideAPublishedRangeIsRefused covers the enums and the one numeric
// range, including the case a non-pointer member could not express.
//
// MaximumWindowInMinutes publishes Valid Range 1–1440 and Required: No, so 0 and 1441 are refusals
// while an absent member is accepted — a distinction that only survives because the request member is a
// pointer. AWS does not publish that a FLEXIBLE window requires the bound, so the absent case asserts
// acceptance rather than a rule substrate would have invented.
func TestSchedulerValidation_AValueOutsideAPublishedRangeIsRefused(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	t.Run("modeOutsideTheEnum", func(t *testing.T) {
		err := schedulerRefusal(t, ts, http.MethodPost, "/schedules/bad-mode",
			`{"ScheduleExpression":"rate(5 minutes)","Target":{"Arn":"arn:aws:sqs:us-east-1:123456789012:q","RoleArn":"arn:aws:iam::123456789012:role/r"},"FlexibleTimeWindow":{"Mode":"SOMETIMES"}}`)
		assert.Contains(t, err.Message, "enum value set: [OFF, FLEXIBLE]",
			"the message names the published Valid Values")
	})
	t.Run("stateOutsideTheEnum", func(t *testing.T) {
		err := schedulerRefusal(t, ts, http.MethodPost, "/schedules/bad-state",
			`{"ScheduleExpression":"rate(5 minutes)","State":"PAUSED","Target":{"Arn":"arn:aws:sqs:us-east-1:123456789012:q","RoleArn":"arn:aws:iam::123456789012:role/r"},"FlexibleTimeWindow":{"Mode":"OFF"}}`)
		assert.Contains(t, err.Message, "enum value set: [ENABLED, DISABLED]")
	})
	for _, minutes := range []string{"0", "1441"} {
		t.Run("maximumWindowInMinutes="+minutes, func(t *testing.T) {
			body := `{"ScheduleExpression":"rate(5 minutes)","Target":{"Arn":"arn:aws:sqs:us-east-1:123456789012:q","RoleArn":"arn:aws:iam::123456789012:role/r"},"FlexibleTimeWindow":{"Mode":"FLEXIBLE","MaximumWindowInMinutes":` + minutes + `}}`
			err := schedulerRefusal(t, ts, http.MethodPost, "/schedules/window-"+minutes, body)
			assert.Contains(t, err.Message, "must be between 1 and 1440",
				"the message names the published Valid Range")
		})
	}
	t.Run("anAbsentMaximumWindowIsAccepted", func(t *testing.T) {
		resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/window-absent",
			`{"ScheduleExpression":"rate(5 minutes)","Target":{"Arn":"arn:aws:sqs:us-east-1:123456789012:q","RoleArn":"arn:aws:iam::123456789012:role/r"},"FlexibleTimeWindow":{"Mode":"FLEXIBLE"}}`)
		assert.Equal(t, http.StatusOK, resp.StatusCode,
			"MaximumWindowInMinutes is Required: No, even for a FLEXIBLE window")
	})
}

// TestSchedulerValidation_ANameOutsideThePublishedPatternIsRefused covers the URI parameter's own
// constraints: length 1–64 and pattern [0-9a-zA-Z-_.]+.
func TestSchedulerValidation_ANameOutsideThePublishedPatternIsRefused(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	t.Run("aCharacterOutsideThePattern", func(t *testing.T) {
		err := schedulerRefusal(t, ts, http.MethodPost, "/schedules/has+plus", schedulerValidCreateBody)
		assert.Contains(t, err.Message, "regular expression pattern: [0-9a-zA-Z-_.]+")
	})
	t.Run("longerThanSixtyFour", func(t *testing.T) {
		long := ""
		for range 65 {
			long += "a"
		}
		err := schedulerRefusal(t, ts, http.MethodPost, "/schedules/"+long, schedulerValidCreateBody)
		assert.Contains(t, err.Message, "length less than or equal to 64")
	})
	t.Run("aGroupNameOutsideThePattern", func(t *testing.T) {
		err := schedulerRefusal(t, ts, http.MethodPost, "/schedules/group-check",
			`{"GroupName":"bad group","ScheduleExpression":"rate(5 minutes)","Target":{"Arn":"arn:aws:sqs:us-east-1:123456789012:q","RoleArn":"arn:aws:iam::123456789012:role/r"},"FlexibleTimeWindow":{"Mode":"OFF"}}`)
		assert.Contains(t, err.Message, "'groupName'")
	})
}

// TestSchedulerValidation_AnEmptyNameReachesTheOperationsGuard is #1009 on this service: the four
// empty-name guards were unreachable because parseSchedulerOperation folded "/schedules/" onto
// ListSchedules for a GET and onto no route at all for the other three verbs.
//
// A GET is the case worth pinning hardest: it used to answer 200 with every schedule in the group, so a
// caller that built the path from an empty variable read a full listing as the schedule it asked for.
func TestSchedulerValidation_AnEmptyNameReachesTheOperationsGuard(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			err := schedulerRefusal(t, ts, method, "/schedules/", schedulerValidCreateBody)
			assert.Contains(t, err.Message, "'name'", "the refusal names the URI parameter")
		})
	}

	t.Run("theCollectionRouteStillLists", func(t *testing.T) {
		resp := schedulerRequest(t, ts, http.MethodGet, "/schedules", "")
		assert.Equal(t, http.StatusOK, resp.StatusCode,
			"only the trailing slash changed meaning; /schedules still lists")
	})
}

// TestSchedulerValidation_TheTargetIsDecodedAsAWSPublishesIt is the decode half of the fix.
//
// The handlers used to unmarshal a caller's body into the storage types, whose tags are snake_case, so
// `Target.RoleArn`, `Target.RetryPolicy` and `FlexibleTimeWindow.MaximumWindowInMinutes` never survived
// the request — GetSchedule reported them empty however they were sent. A round trip is the assertion
// because both ends now use the published spelling.
func TestSchedulerValidation_TheTargetIsDecodedAsAWSPublishesIt(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/decode-test", schedulerValidCreateBody)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(readSchedulerBody(t, resp)))

	resp = schedulerRequest(t, ts, http.MethodGet, "/schedules/decode-test", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var got struct {
		Target struct {
			Arn         string `json:"Arn"`
			RoleArn     string `json:"RoleArn"`
			RetryPolicy *struct {
				MaximumEventAgeInSeconds int32 `json:"MaximumEventAgeInSeconds"`
				MaximumRetryAttempts     int32 `json:"MaximumRetryAttempts"`
			} `json:"RetryPolicy"`
		} `json:"Target"`
		FlexibleTimeWindow struct {
			Mode                   string `json:"Mode"`
			MaximumWindowInMinutes int32  `json:"MaximumWindowInMinutes"`
		} `json:"FlexibleTimeWindow"`
	}
	require.NoError(t, json.Unmarshal(readSchedulerBody(t, resp), &got))

	assert.Equal(t, "arn:aws:iam::123456789012:role/my-role", got.Target.RoleArn,
		"RoleArn is Required: Yes and used to be dropped by the decode")
	require.NotNil(t, got.Target.RetryPolicy, "RetryPolicy used to be dropped by the decode")
	assert.Equal(t, int32(120), got.Target.RetryPolicy.MaximumEventAgeInSeconds)
	assert.Equal(t, int32(3), got.Target.RetryPolicy.MaximumRetryAttempts)
	assert.Equal(t, int32(15), got.FlexibleTimeWindow.MaximumWindowInMinutes,
		"MaximumWindowInMinutes used to be dropped by the decode")
}
