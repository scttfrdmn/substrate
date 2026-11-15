package emulator_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Schedule groups and their tags (#1501).

const schedGroupARNPrefix = "arn:aws:scheduler:us-east-1:123456789012:schedule-group/"

type schedGroupOut struct {
	Arn                  string
	Name                 string
	State                string
	CreationDate         float64
	LastModificationDate float64
}

func (e *schedEnv) groupNames(query string) ([]string, string) {
	e.t.Helper()
	var out struct {
		ScheduleGroups []schedGroupOut
		NextToken      string
	}
	require.NoError(e.t, json.Unmarshal(e.mustSched(http.MethodGet, "/schedule-groups"+query, nil), &out))
	names := make([]string, 0, len(out.ScheduleGroups))
	for _, g := range out.ScheduleGroups {
		names = append(names, g.Name)
	}
	return names, out.NextToken
}

func (e *schedEnv) tags(arn string) map[string]string {
	e.t.Helper()
	var out struct{ Tags []struct{ Key, Value string } }
	require.NoError(e.t, json.Unmarshal(e.mustSched(http.MethodGet, "/tags/"+url.PathEscape(arn), nil), &out))
	m := map[string]string{}
	for _, tag := range out.Tags {
		m[tag.Key] = tag.Value
	}
	return m
}

func TestSchedulerGroups_Lifecycle(t *testing.T) {
	t.Parallel()
	e := newSchedEnv(t)

	// The default group exists without a create.
	var def schedGroupOut
	require.NoError(t, json.Unmarshal(e.mustSched(http.MethodGet, "/schedule-groups/default", nil), &def))
	assert.Equal(t, schedGroupOut{
		Arn: schedGroupARNPrefix + "default", Name: "default", State: "ACTIVE",
		CreationDate: float64(schedClockInstant.Unix()), LastModificationDate: float64(schedClockInstant.Unix()),
	}, def)

	out := e.mustSched(http.MethodPost, "/schedule-groups/aegc-dev", map[string]any{
		"ClientToken": "token-1",
		"Tags":        []map[string]string{{"Key": "aegc:managed", "Value": "true"}},
	})
	assert.JSONEq(t, `{"ScheduleGroupArn":"`+schedGroupARNPrefix+`aegc-dev"}`, string(out))
	e.mustSched(http.MethodPost, "/schedule-groups/aegc-prod", nil)
	e.mustSched(http.MethodPost, "/schedule-groups/other", nil)

	names, token := e.groupNames("")
	assert.Equal(t, []string{"aegc-dev", "aegc-prod", "default", "other"}, names)
	assert.Empty(t, token)
	names, _ = e.groupNames("?NamePrefix=aegc-")
	assert.Equal(t, []string{"aegc-dev", "aegc-prod"}, names)
	names, token = e.groupNames("?MaxResults=3")
	assert.Equal(t, []string{"aegc-dev", "aegc-prod", "default"}, names)
	require.NotEmpty(t, token)
	names, token = e.groupNames("?MaxResults=3&NextToken=" + url.QueryEscape(token))
	assert.Equal(t, []string{"other"}, names)
	assert.Empty(t, token)

	// Deleting a group deletes its schedules; no later request sees it DELETING.
	e.mustSched(http.MethodPost, "/schedules/lease-state", leaseSchedule(nil))
	e.mustSched(http.MethodDelete, "/schedule-groups/aegc-dev?clientToken=token-2", nil)
	status, body := e.sched(http.MethodGet, "/schedule-groups/aegc-dev", nil)
	assert.Equal(t, http.StatusNotFound, status)
	assert.Contains(t, string(body), "Schedule group aegc-dev does not exist.")
	status, _ = e.sched(http.MethodGet, "/schedules/lease-state?groupName=aegc-dev", nil)
	assert.Equal(t, http.StatusNotFound, status, "the group's schedules went with it")

	// A recreated group starts empty.
	e.mustSched(http.MethodPost, "/schedule-groups/aegc-dev", nil)
	list := e.mustSched(http.MethodGet, "/schedules?ScheduleGroup=aegc-dev", nil)
	assert.JSONEq(t, `{"Schedules":[]}`, string(list))
}

func TestSchedulerGroups_Refusals(t *testing.T) {
	t.Parallel()
	e := newSchedEnv(t)
	e.mustSched(http.MethodPost, "/schedule-groups/taken", nil)

	for _, tc := range []struct {
		name, method, path string
		body               any
		status             int
		code               string
	}{
		{"a taken name", http.MethodPost, "/schedule-groups/taken", nil, http.StatusConflict, "ConflictException"},
		{"the default name", http.MethodPost, "/schedule-groups/default", nil, http.StatusConflict, "ConflictException"},
		{"a name outside the pattern", http.MethodPost, "/schedule-groups/bad%20name", nil, http.StatusBadRequest, "ValidationException"},
		{"a tag with no value", http.MethodPost, "/schedule-groups/x", map[string]any{"Tags": []map[string]string{{"Key": "k"}}}, http.StatusBadRequest, "ValidationException"},
		{"a tag key over 128", http.MethodPost, "/schedule-groups/x", map[string]any{"Tags": []map[string]string{{"Key": strings.Repeat("k", 129), "Value": "v"}}}, http.StatusBadRequest, "ValidationException"},
		{"get of a missing group", http.MethodGet, "/schedule-groups/missing", nil, http.StatusNotFound, "ResourceNotFoundException"},
		{"delete of a missing group", http.MethodDelete, "/schedule-groups/missing", nil, http.StatusNotFound, "ResourceNotFoundException"},
		{"delete of the default group", http.MethodDelete, "/schedule-groups/default", nil, http.StatusBadRequest, "ValidationException"},
		{"a NextToken no listing issued", http.MethodGet, "/schedule-groups?NextToken=garbage", nil, http.StatusBadRequest, "ValidationException"},
		{"delete of a missing schedule", http.MethodDelete, "/schedules/missing", nil, http.StatusNotFound, "ResourceNotFoundException"},
		{"delete of a schedule in a missing group", http.MethodDelete, "/schedules/s?groupName=missing", nil, http.StatusNotFound, "ResourceNotFoundException"},
		{"a schedule in a missing group", http.MethodPost, "/schedules/s", leaseSchedule(map[string]any{"GroupName": "missing"}), http.StatusNotFound, "ResourceNotFoundException"},
		{"a dead-letter queue that is not an SQS ARN", http.MethodPost, "/schedules/s", leaseSchedule(map[string]any{
			"GroupName": nil, "Target": map[string]any{"Arn": "arn:aws:sqs:us-east-1:123456789012:q", "RoleArn": schedRoleARN,
				"DeadLetterConfig": map[string]any{"Arn": "arn:aws:sns:us-east-1:123456789012:topic"}},
		}), http.StatusBadRequest, "ValidationException"},
		{"EventBridgeParameters with no Source", http.MethodPost, "/schedules/s", leaseSchedule(map[string]any{
			"GroupName": nil, "Target": map[string]any{"Arn": "arn:aws:events:us-east-1:123456789012:event-bus/default", "RoleArn": schedRoleARN,
				"EventBridgeParameters": map[string]any{"DetailType": "t"}},
		}), http.StatusBadRequest, "ValidationException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := e.sched(tc.method, tc.path, tc.body)
			assert.Equal(t, tc.status, status, "%s", body)
			assert.Contains(t, string(body), tc.code)
		})
	}
}

func TestSchedulerGroups_Tags(t *testing.T) {
	t.Parallel()
	e := newSchedEnv(t)
	e.createLeaseGroup()
	arn := schedGroupARNPrefix + "aegc-dev"
	assert.Equal(t, map[string]string{"aegc:managed": "true"}, e.tags(arn))

	e.mustSched(http.MethodPost, "/tags/"+url.PathEscape(arn), map[string]any{
		"Tags": []map[string]string{{"Key": "env", "Value": "dev"}, {"Key": "aegc:managed", "Value": "yes"}, {"Key": "team", "Value": "x"}},
	})
	assert.Equal(t, map[string]string{"aegc:managed": "yes", "env": "dev", "team": "x"}, e.tags(arn), "a tag's value is replaced")

	e.mustSched(http.MethodDelete, "/tags/"+url.PathEscape(arn)+"?TagKeys=env&TagKeys=team&TagKeys=absent", nil)
	assert.Equal(t, map[string]string{"aegc:managed": "yes"}, e.tags(arn))
	e.mustSched(http.MethodDelete, "/tags/"+url.PathEscape(arn)+"?TagKeys=aegc%3Amanaged", nil)
	assert.Empty(t, e.tags(arn))
	assert.Empty(t, e.tags(schedGroupARNPrefix+"default"), "the default group can be read for tags")

	for _, tc := range []struct {
		name, method, path string
		body               any
		status             int
		code               string
	}{
		{"a schedule ARN", http.MethodGet, "/tags/" + url.PathEscape("arn:aws:scheduler:us-east-1:123456789012:schedule/aegc-dev/s"), nil, http.StatusBadRequest, "ValidationException"},
		{"a missing group", http.MethodGet, "/tags/" + url.PathEscape(schedGroupARNPrefix+"missing"), nil, http.StatusNotFound, "ResourceNotFoundException"},
		{"another account's group", http.MethodGet, "/tags/" + url.PathEscape("arn:aws:scheduler:us-east-1:210987654321:schedule-group/aegc-dev"), nil, http.StatusNotFound, "ResourceNotFoundException"},
		{"TagResource with no Tags", http.MethodPost, "/tags/" + url.PathEscape(arn), map[string]any{}, http.StatusBadRequest, "ValidationException"},
		{"UntagResource with no TagKeys", http.MethodDelete, "/tags/" + url.PathEscape(arn), nil, http.StatusBadRequest, "ValidationException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := e.sched(tc.method, tc.path, tc.body)
			assert.Equal(t, tc.status, status, "%s", body)
			assert.Contains(t, string(body), tc.code)
		})
	}
}

// A default-group schedule stored before groups were indexed still lists with no ScheduleGroup:
// the listing materializes the default group before walking the group index (#1501 review).
func TestSchedulerGroups_APreGroupsDefaultScheduleStillLists(t *testing.T) {
	t.Parallel()
	e := newSchedEnv(t)
	state := e.ts.StateManager()
	rec := `{"name":"legacy","group_name":"default","schedule_expression":"rate(1 day)","state":"DISABLED",` +
		`"target":{"arn":"arn:aws:sqs:us-east-1:123456789012:q","role_arn":"` + schedRoleARN + `"},` +
		`"flexible_time_window":{"mode":"OFF"},"arn":"arn:aws:scheduler:us-east-1:123456789012:schedule/default/legacy",` +
		`"creation_date":"2026-01-01T00:00:00Z","last_modification_date":"2026-01-01T00:00:00Z",` +
		`"account_id":"123456789012","region":"us-east-1"}`
	require.NoError(t, state.Put(t.Context(), "scheduler", "sched:123456789012/us-east-1/default/legacy", []byte(rec)))
	require.NoError(t, state.Put(t.Context(), "scheduler", "sched_names:123456789012/us-east-1/default", []byte(`["legacy"]`)))

	var out struct {
		Schedules []struct{ Name, GroupName string }
	}
	require.NoError(t, json.Unmarshal(e.mustSched(http.MethodGet, "/schedules", nil), &out))
	require.Len(t, out.Schedules, 1)
	assert.Equal(t, "legacy", out.Schedules[0].Name)
	assert.Equal(t, "default", out.Schedules[0].GroupName)
}

// The three target objects a due schedule acts on are kept and reported (#1501).
func TestScheduler_TargetParametersRoundTrip(t *testing.T) {
	t.Parallel()
	e := newSchedEnv(t)
	e.mustSched(http.MethodPost, "/schedules/full", map[string]any{
		"ScheduleExpression": "rate(1 day)",
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
		"State":              "DISABLED",
		"Target": map[string]any{
			"Arn": "arn:aws:sqs:us-east-1:123456789012:fifo.fifo", "RoleArn": schedRoleARN,
			"DeadLetterConfig":      map[string]any{"Arn": "arn:aws:sqs:us-east-1:123456789012:dlq"},
			"SqsParameters":         map[string]any{"MessageGroupId": "g1"},
			"EventBridgeParameters": map[string]any{"DetailType": "Lease Expired", "Source": "aegc.lease"},
		},
	})
	var got struct{ Target map[string]json.RawMessage }
	require.NoError(t, json.Unmarshal(e.mustSched(http.MethodGet, "/schedules/full", nil), &got))
	assert.JSONEq(t, `{"Arn":"arn:aws:sqs:us-east-1:123456789012:dlq"}`, string(got.Target["DeadLetterConfig"]))
	assert.JSONEq(t, `{"MessageGroupId":"g1"}`, string(got.Target["SqsParameters"]))
	assert.JSONEq(t, `{"DetailType":"Lease Expired","Source":"aegc.lease"}`, string(got.Target["EventBridgeParameters"]))
}
