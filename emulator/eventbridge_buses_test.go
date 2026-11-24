package emulator_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// EventBridge event buses and their resource policy (#1613): CreateEventBus, DescribeEventBus,
// UpdateEventBus, DeleteEventBus, ListEventBuses, PutPermission and RemovePermission.

// ebOK posts an operation that must succeed and returns its decoded body.
func ebOK(t *testing.T, ts *emulator.TestServer, op string, body map[string]any) map[string]any {
	t.Helper()
	status, raw, code := ebCall(t, ts, ebTarget, op, body)
	require.Empty(t, code, "%s: %s", op, raw)
	require.Equal(t, http.StatusOK, status, "%s: %s", op, raw)
	out := map[string]any{}
	require.NoError(t, json.Unmarshal([]byte(raw), &out), raw)
	return out
}

// ebRefused posts an operation that must be refused with code at 400.
func ebRefused(t *testing.T, ts *emulator.TestServer, op string, body map[string]any, code string) string {
	t.Helper()
	status, raw, got := ebCall(t, ts, ebTarget, op, body)
	require.Equal(t, code, got, "%s: %s", op, raw)
	require.Equal(t, http.StatusBadRequest, status, "%s: %s", op, raw)
	return raw
}

const ebBusARNPrefix = "arn:aws:events:us-east-1:" + taggingTestAccount + ":event-bus/"

func TestEBBuses_Lifecycle(t *testing.T) {
	ts := smTagServer(t)
	created := ebOK(t, ts, "CreateEventBus", map[string]any{
		"Name": "orders", "Description": "order events", "KmsKeyIdentifier": "alias/orders",
		"DeadLetterConfig": map[string]string{"Arn": "arn:aws:sqs:us-east-1:" + taggingTestAccount + ":dlq"},
		"LogConfig":        map[string]string{"IncludeDetail": "FULL", "Level": "INFO"},
	})
	assert.Equal(t, ebBusARNPrefix+"orders", created["EventBusArn"])
	assert.Equal(t, "order events", created["Description"])
	assert.Equal(t, "alias/orders", created["KmsKeyIdentifier"])

	described := ebOK(t, ts, "DescribeEventBus", map[string]any{"Name": "orders"})
	assert.Equal(t, "orders", described["Name"])
	assert.Equal(t, ebBusARNPrefix+"orders", described["Arn"])
	assert.Equal(t, map[string]any{"IncludeDetail": "FULL", "Level": "INFO"}, described["LogConfig"])
	assert.NotNil(t, described["CreationTime"])
	assert.NotNil(t, described["LastModifiedTime"])
	assert.Equal(t, described, ebOK(t, ts, "DescribeEventBus", map[string]any{"Name": ebBusARNPrefix + "orders"}),
		"Name takes the bus ARN as well")

	// An omitted member is kept, a present one replaces, an empty one clears.
	ts.AdvanceTime(time.Hour)
	updated := ebOK(t, ts, "UpdateEventBus", map[string]any{"Name": "orders", "Description": ""})
	assert.Equal(t, "orders", updated["Name"])
	assert.Equal(t, "alias/orders", updated["KmsKeyIdentifier"])
	assert.NotContains(t, updated, "Description")
	ebOK(t, ts, "UpdateEventBus", map[string]any{"Name": "orders", "DeadLetterConfig": map[string]string{}})
	after := ebOK(t, ts, "DescribeEventBus", map[string]any{"Name": "orders"})
	assert.NotContains(t, after, "DeadLetterConfig")
	assert.Equal(t, described["CreationTime"], after["CreationTime"])
	assert.Greater(t, after["LastModifiedTime"], described["LastModifiedTime"])

	ebOK(t, ts, "DeleteEventBus", map[string]any{"Name": "orders"})
	ebRefused(t, ts, "DescribeEventBus", map[string]any{"Name": "orders"}, "ResourceNotFoundException")
	ebRefused(t, ts, "UpdateEventBus", map[string]any{"Name": "orders"}, "ResourceNotFoundException")
	ebOK(t, ts, "DeleteEventBus", map[string]any{"Name": "orders"})
}

func TestEBBuses_TheDefaultBusAlwaysExists(t *testing.T) {
	ts := smTagServer(t)
	described := ebOK(t, ts, "DescribeEventBus", map[string]any{})
	assert.Equal(t, map[string]any{"Name": "default", "Arn": ebBusARNPrefix + "default"}, described,
		"an unwritten default bus has no timestamps or configuration")

	status, raw, code := ebCall(t, ts, ebWest2Target, "ListEventBuses", map[string]any{})
	require.Empty(t, code, raw)
	require.Equal(t, http.StatusOK, status)
	assert.JSONEq(t, `{"EventBuses":[{"Name":"default","Arn":"arn:aws:events:us-west-2:`+taggingTestAccount+`:event-bus/default"}]}`, raw)

	raw = ebRefused(t, ts, "DeleteEventBus", map[string]any{"Name": "default"}, "ValidationException")
	assert.Contains(t, raw, "default event bus cannot be deleted")
	raw = ebRefused(t, ts, "CreateEventBus", map[string]any{"Name": "default"}, "ResourceAlreadyExistsException")
	assert.Contains(t, raw, "Event bus default already exists.")
}

func TestEBBuses_CreateRefusals(t *testing.T) {
	ts := smTagServer(t)
	ebOK(t, ts, "CreateEventBus", map[string]any{"Name": "taken"})
	fifty := make([]map[string]string, 0, 51)
	for i := range 51 {
		fifty = append(fifty, map[string]string{"Key": fmt.Sprintf("k%02d", i), "Value": "v"})
	}
	for _, tc := range []struct {
		name string
		body map[string]any
		code string
	}{
		{"a taken name", map[string]any{"Name": "taken"}, "ResourceAlreadyExistsException"},
		{"no name", map[string]any{}, "ValidationError"},
		{"a name outside the pattern", map[string]any{"Name": "bad name"}, "ValidationError"},
		{"a slash in a custom bus name", map[string]any{"Name": "a/b"}, "ValidationError"},
		{"a partner event source", map[string]any{"Name": "aws.partner/x/y", "EventSourceName": "aws.partner/x/y"}, "ResourceNotFoundException"},
		{"a long description", map[string]any{"Name": "d", "Description": strings.Repeat("d", 513)}, "ValidationError"},
		{"a malformed key identifier", map[string]any{"Name": "k", "KmsKeyIdentifier": "key id"}, "ValidationError"},
		{"a long key identifier", map[string]any{"Name": "k", "KmsKeyIdentifier": strings.Repeat("k", 2049)}, "ValidationError"},
		{"an unknown IncludeDetail", map[string]any{"Name": "l", "LogConfig": map[string]string{"IncludeDetail": "SOME"}}, "ValidationError"},
		{"an unknown Level", map[string]any{"Name": "l", "LogConfig": map[string]string{"Level": "LOUD"}}, "ValidationError"},
		{"an empty tag key", map[string]any{"Name": "t", "Tags": ebTagList("", "v")}, "ValidationError"},
		{"more than fifty tags", map[string]any{"Name": "t", "Tags": fifty}, "LimitExceededException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ebRefused(t, ts, "CreateEventBus", tc.body, tc.code)
		})
	}
	out := ebOK(t, ts, "ListEventBuses", map[string]any{})
	assert.Len(t, out["EventBuses"], 2, "no refused create stored a bus")

	status, raw, code := ebCall(t, ts, ebTarget, "CreateEventBus", nil)
	assert.Equal(t, "ValidationError", code, raw)
	assert.Equal(t, http.StatusBadRequest, status)
}

func TestEBBuses_TheBusQuotaCountsTheDefaultBus(t *testing.T) {
	ts := smTagServer(t)
	for i := range 99 {
		ebOK(t, ts, "CreateEventBus", map[string]any{"Name": fmt.Sprintf("bus-%02d", i)})
	}
	ebRefused(t, ts, "CreateEventBus", map[string]any{"Name": "one-too-many"}, "LimitExceededException")
	// Writing the default bus gives it a record, which is not a custom bus.
	ebOK(t, ts, "PutPermission", map[string]any{"StatementId": "s", "Action": "events:PutEvents", "Principal": "111122223333"})
	ebOK(t, ts, "DeleteEventBus", map[string]any{"Name": "bus-00"})
	ebOK(t, ts, "CreateEventBus", map[string]any{"Name": "one-more"})
}

func TestEBBuses_ABusWithRulesCannotBeDeleted(t *testing.T) {
	ts := smTagServer(t)
	ebOK(t, ts, "CreateEventBus", map[string]any{"Name": "orders"})
	ebPutRule(t, ts, ebTarget, map[string]any{"Name": "r", "EventBusName": "orders", "EventPattern": `{"source":["x"]}`})
	raw := ebRefused(t, ts, "DeleteEventBus", map[string]any{"Name": "orders"}, "ValidationException")
	assert.Contains(t, raw, "EventBus can't be deleted since it has rules.")
	ebOK(t, ts, "DescribeEventBus", map[string]any{"Name": "orders"})

	ebOK(t, ts, "DeleteRule", map[string]any{"Name": "r", "EventBusName": "orders"})
	ebOK(t, ts, "DeleteEventBus", map[string]any{"Name": "orders"})
	ebRefused(t, ts, "DescribeEventBus", map[string]any{"Name": "orders"}, "ResourceNotFoundException")
	ebRefused(t, ts, "DeleteEventBus", map[string]any{}, "ValidationError")
}

// ebBusNames returns the names one ListEventBuses page answers, and its token.
func ebBusNames(t *testing.T, ts *emulator.TestServer, body map[string]any) ([]string, string) {
	t.Helper()
	var out struct {
		EventBuses []struct{ Name string }
		NextToken  string
	}
	raw, err := json.Marshal(ebOK(t, ts, "ListEventBuses", body))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &out))
	names := make([]string, 0, len(out.EventBuses))
	for _, b := range out.EventBuses {
		names = append(names, b.Name)
	}
	return names, out.NextToken
}

func TestEBBuses_ListPagesByName(t *testing.T) {
	ts := smTagServer(t)
	for _, n := range []string{"orders", "audit", "orders-eu"} {
		ebOK(t, ts, "CreateEventBus", map[string]any{"Name": n})
	}
	names, token := ebBusNames(t, ts, map[string]any{})
	assert.Equal(t, []string{"audit", "default", "orders", "orders-eu"}, names)
	assert.Empty(t, token)

	page1, token := ebBusNames(t, ts, map[string]any{"Limit": 3})
	require.NotEmpty(t, token)
	ebOK(t, ts, "CreateEventBus", map[string]any{"Name": "billing"}) // sorts before the cursor
	page2, last := ebBusNames(t, ts, map[string]any{"Limit": 3, "NextToken": token})
	assert.Equal(t, []string{"audit", "default", "orders"}, page1)
	assert.Equal(t, []string{"orders-eu"}, page2, "a create behind the cursor does not repeat a page")
	assert.Empty(t, last)

	prefixed, _ := ebBusNames(t, ts, map[string]any{"NamePrefix": "orders"})
	assert.Equal(t, []string{"orders", "orders-eu"}, prefixed)

	ebRefused(t, ts, "ListEventBuses", map[string]any{"NextToken": "not-a-token"}, "InvalidToken")
	ebRefused(t, ts, "ListEventBuses", map[string]any{"NextToken": token, "NamePrefix": "o"}, "InvalidToken")
	ebRefused(t, ts, "ListEventBuses", map[string]any{"Limit": 101}, "ValidationError")
}

// ebPolicy returns the Policy DescribeEventBus answers for bus, decoded.
func ebPolicy(t *testing.T, ts *emulator.TestServer, bus string) map[string]any {
	t.Helper()
	described := ebOK(t, ts, "DescribeEventBus", map[string]any{"Name": bus})
	policy, ok := described["Policy"].(string)
	if !ok {
		return nil
	}
	out := map[string]any{}
	require.NoError(t, json.Unmarshal([]byte(policy), &out), policy)
	return out
}

func TestEBBuses_PermissionsEditThePolicy(t *testing.T) {
	ts := smTagServer(t)
	busARN := ebBusARNPrefix + "default"
	ebOK(t, ts, "PutPermission", map[string]any{"StatementId": "one", "Action": "events:PutEvents", "Principal": "111122223333"})
	ebOK(t, ts, "PutPermission", map[string]any{
		"StatementId": "org", "Action": "events:PutEvents", "Principal": "*",
		"Condition": map[string]string{"Type": "StringEquals", "Key": "aws:PrincipalOrgID", "Value": "o-1234567890"},
	})
	ebOK(t, ts, "PutPermission", map[string]any{"StatementId": "one", "Action": "events:PutEvents", "Principal": "444455556666"})

	assert.Equal(t, map[string]any{
		"Version": "2012-10-17",
		"Statement": []any{
			map[string]any{"Sid": "one", "Effect": "Allow", "Principal": map[string]any{"AWS": "arn:aws:iam::444455556666:root"},
				"Action": "events:PutEvents", "Resource": busARN},
			map[string]any{"Sid": "org", "Effect": "Allow", "Principal": "*", "Action": "events:PutEvents", "Resource": busARN,
				"Condition": map[string]any{"StringEquals": map[string]any{"aws:PrincipalOrgID": "o-1234567890"}}},
		},
	}, ebPolicy(t, ts, "default"), "a statement with a taken Sid replaces it in place")
	names, _ := ebBusNames(t, ts, map[string]any{})
	assert.Equal(t, []string{"default"}, names, "the default bus is listed once after a write gives it a record")

	ebOK(t, ts, "RemovePermission", map[string]any{"StatementId": "one"})
	assert.Len(t, ebPolicy(t, ts, "default")["Statement"], 1)
	ebRefused(t, ts, "RemovePermission", map[string]any{"StatementId": "one"}, "ResourceNotFoundException")
	ebOK(t, ts, "RemovePermission", map[string]any{"StatementId": "org"})
	assert.Nil(t, ebPolicy(t, ts, "default"), "a bus left with no statements has no Policy")

	ebOK(t, ts, "CreateEventBus", map[string]any{"Name": "orders"})
	doc := `{"Version":"2012-10-17","Statement":{"Sid":"all","Effect":"Allow","Principal":"*","Action":"events:PutEvents","Resource":"*"}}`
	ebOK(t, ts, "PutPermission", map[string]any{"EventBusName": "orders", "Policy": doc})
	described := ebOK(t, ts, "DescribeEventBus", map[string]any{"Name": "orders"})
	assert.Equal(t, doc, described["Policy"], "a Policy is stored as given")
	ebOK(t, ts, "PutPermission", map[string]any{"EventBusName": "orders", "StatementId": "two", "Action": "events:PutEvents", "Principal": "111122223333"})
	assert.Len(t, ebPolicy(t, ts, "orders")["Statement"], 2, "a single-object Statement is extended to a list")
	ebOK(t, ts, "RemovePermission", map[string]any{"EventBusName": "orders", "RemoveAllPermissions": true})
	assert.Nil(t, ebPolicy(t, ts, "orders"))
	ebOK(t, ts, "RemovePermission", map[string]any{"EventBusName": "orders", "RemoveAllPermissions": true})
}

func TestEBBuses_PermissionRefusals(t *testing.T) {
	ts := smTagServer(t)
	ok := map[string]any{"StatementId": "s", "Action": "events:PutEvents", "Principal": "111122223333"}
	with := func(k string, v any) map[string]any {
		out := map[string]any{}
		for kk, vv := range ok {
			out[kk] = vv
		}
		out[k] = v
		return out
	}
	for _, tc := range []struct {
		name, op string
		body     map[string]any
		code     string
	}{
		{"Policy with a statement member", "PutPermission", with("Policy", `{}`), "ValidationError"},
		{"a Policy that is not JSON", "PutPermission", map[string]any{"Policy": "{"}, "ValidationError"},
		{"a Policy that is an array", "PutPermission", map[string]any{"Policy": "[]"}, "ValidationError"},
		{"a Policy that is null", "PutPermission", map[string]any{"Policy": "null"}, "ValidationError"},
		{"a Policy that is a string", "PutPermission", map[string]any{"Policy": `"x"`}, "ValidationError"},
		{"a Statement that is a string", "PutPermission", map[string]any{"Policy": `{"Statement":"x"}`}, "ValidationError"},
		{"a Statement list holding a number", "PutPermission", map[string]any{"Policy": `{"Statement":[{"Sid":"a"},1]}`}, "ValidationError"},
		{"a bus ARN in another account", "PutPermission", with("EventBusName", "arn:aws:events:us-east-1:210987654321:event-bus/default"), "ResourceNotFoundException"},
		{"a bus ARN in another Region", "RemovePermission", map[string]any{"EventBusName": "arn:aws:events:us-west-2:" + taggingTestAccount + ":event-bus/default", "RemoveAllPermissions": true}, "ResourceNotFoundException"},
		{"a Policy over the size quota", "PutPermission", map[string]any{"Policy": `{"x":"` + strings.Repeat("p", 10240) + `"}`}, "PolicyLengthExceededException"},
		{"no StatementId", "PutPermission", map[string]any{"Action": "events:PutEvents", "Principal": "111122223333"}, "ValidationError"},
		{"a malformed StatementId", "PutPermission", with("StatementId", "a b"), "ValidationError"},
		{"a malformed Action", "PutPermission", with("Action", "PutEvents"), "ValidationError"},
		{"a malformed Principal", "PutPermission", with("Principal", "1234"), "ValidationError"},
		{"an incomplete Condition", "PutPermission", with("Condition", map[string]string{"Type": "StringEquals"}), "ValidationError"},
		{"an absent bus", "PutPermission", with("EventBusName", "absent"), "ResourceNotFoundException"},
		{"no StatementId on remove", "RemovePermission", map[string]any{}, "ValidationError"},
		{"an absent bus on remove", "RemovePermission", map[string]any{"EventBusName": "absent", "RemoveAllPermissions": true}, "ResourceNotFoundException"},
		{"an empty policy on remove", "RemovePermission", map[string]any{"StatementId": "s"}, "ResourceNotFoundException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ebRefused(t, ts, tc.op, tc.body, tc.code)
		})
	}
	assert.Nil(t, ebPolicy(t, ts, "default"), "no refused call wrote a policy")
	ebOK(t, ts, "PutPermission", ok)
	ebOK(t, ts, "RemovePermission", map[string]any{"StatementId": "s"})

	// Statements accumulate until the policy passes 10,240 characters.
	var code string
	for i := 0; i < 200 && code == ""; i++ {
		_, _, code = ebCall(t, ts, ebTarget, "PutPermission", with("StatementId", fmt.Sprintf("statement-%03d", i)))
	}
	assert.Equal(t, "PolicyLengthExceededException", code)
}
