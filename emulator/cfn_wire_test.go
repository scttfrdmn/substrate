package emulator_test

import (
	"encoding/json"
	"net/http"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for CloudFormation's two
// records (#756).
//
// CFNStackState declares AccountID and Region as `,omitempty` and CreatedAt and UpdatedAt as
// time.Time, and CFNChangeSet declares CreatedAt, all under wire-visible `json` tags. None reaches a
// body: CloudFormation speaks the query protocol, and every response is marshaled from an XML struct
// declared for its operation (cfnStackItem, the change-set builders), which publish CreationTime and
// LastUpdatedTime — different names — and no account or Region. The members are read back from the
// stored records first, because an omitempty member that was never set is absent for free (#1304).
// A zero time is refused as unset too. CreateStack sets UpdatedAt itself, so the read-back passes before
// the update; UpdateStack is driven because it answers a body of its own.

// cfnBookkeepingMembers are the members the two records declare and no CloudFormation shape the
// routed operations answer publishes. StackSet and StackInstance shapes do publish an account and a
// Region, but no StackSet operation is routed.
var cfnBookkeepingMembers = []string{"AccountID", "Region", "CreatedAt", "UpdatedAt"}

// cfnWire issues one action and returns the raw body, failing on anything but 200.
func cfnWire(t *testing.T, ts *cfnTestServer, action string, params map[string]string) []byte {
	t.Helper()
	status, body := cfnAction(t, ts, action, params)
	require.Equalf(t, http.StatusOK, status, "%s: %s", action, body)
	return []byte(body)
}

// cfnWireScope returns the "<account>/<region>" segment a stack's state keys carry (#1366), read
// from the StackId ARN the create answered, so the test does not restate how an unsigned request
// is attributed.
func cfnWireScope(t *testing.T, created []byte) string {
	t.Helper()
	m := regexp.MustCompile(`<StackId>arn:aws:cloudformation:([^:]+):(\d+):stack/`).FindSubmatch(created)
	require.NotNil(t, m, "CreateStack must answer a StackId ARN: %s", created)
	return string(m[2]) + "/" + string(m[1])
}

// cfnWireRecord returns the record at key in the cfn namespace as raw JSON.
func cfnWireRecord(t *testing.T, ts *cfnTestServer, key string) map[string]json.RawMessage {
	t.Helper()
	data, err := ts.state.Get(t.Context(), "cfn", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	return record
}

// cfnWireRequireSet requires each member to be held, and neither an empty string nor a zero time.
func cfnWireRequireSet(t *testing.T, record map[string]json.RawMessage, key string, members ...string) {
	t.Helper()
	for _, member := range members {
		raw := string(record[member])
		require.NotEmptyf(t, raw, "%s must persist %s before an absence assertion on it means anything", key, member)
		require.NotContainsf(t, []string{`""`, `"0001-01-01T00:00:00Z"`}, raw, "%s persists an unset %s", key, member)
	}
}

// cfnWireCase is one action and the element its response has to render.
type cfnWireCase struct {
	action string
	params map[string]string
	held   []byte
	anchor string
}

// cfnWireRun drives each case as a subtest: the presence anchor, then the walk.
func cfnWireRun(t *testing.T, ts *cfnTestServer, cases []cfnWireCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.action, func(t *testing.T) {
			body := tc.held
			if body == nil {
				body = cfnWire(t, ts, tc.action, tc.params)
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.action, tc.anchor)
			wireAssertNoMemberXML(t, tc.action, body, cfnBookkeepingMembers, "")
		})
	}
}

func TestCFNWire_StackResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	ts := newCFNTestServer(t)

	const name = "wire-stack"
	stack := map[string]string{"StackName": name}
	created := cfnWire(t, ts, "CreateStack", map[string]string{"StackName": name, "TemplateBody": cfnEmptyTemplate})
	updated := cfnWire(t, ts, "UpdateStack", map[string]string{"StackName": name, "TemplateBody": cfnBucketTemplate})
	stackKey := "stack:" + cfnWireScope(t, created) + "/" + name
	cfnWireRequireSet(t, cfnWireRecord(t, ts, stackKey), stackKey, cfnBookkeepingMembers...)

	drift := cfnWire(t, ts, "DetectStackDrift", stack)
	m := regexp.MustCompile(`<StackDriftDetectionId>([^<]+)</StackDriftDetectionId>`).FindSubmatch(drift)
	require.NotNil(t, m, "DetectStackDrift must report a detection id: %s", drift)

	cfnWireRun(t, ts, []cfnWireCase{
		{action: "CreateStack", held: created, anchor: "<StackId>"},
		{action: "UpdateStack", held: updated, anchor: "<StackId>"},
		{action: "DescribeStacks", params: stack, anchor: "<StackName>" + name + "</StackName>"},
		{action: "ListStacks", anchor: "<StackName>" + name + "</StackName>"},
		{action: "GetTemplate", params: stack, anchor: "<TemplateBody>"},
		{action: "DescribeStackResources", params: stack, anchor: "<LogicalResourceId>Data</LogicalResourceId>"},
		{action: "DescribeStackEvents", params: stack, anchor: "<StackName>" + name + "</StackName>"},
		{action: "ListExports", anchor: "<ListExportsResult>"},
		{action: "DetectStackDrift", held: drift, anchor: "<StackDriftDetectionId>"},
		{action: "DescribeStackDriftDetectionStatus", params: map[string]string{"StackDriftDetectionId": string(m[1])}, anchor: "<StackId>"},
		{action: "DescribeStackResourceDrifts", params: stack, anchor: "<DescribeStackResourceDriftsResult>"},
		// Last: it removes the record every case above reads.
		{action: "DeleteStack", params: stack, anchor: "<DeleteStackResponse"},
	})
}

func TestCFNWire_ChangeSetResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	ts := newCFNTestServer(t)

	const stackName, csName = "wire-cs-stack", "wire-change-set"
	stackCreated := cfnWire(t, ts, "CreateStack", map[string]string{"StackName": stackName, "TemplateBody": cfnEmptyTemplate})
	created := cfnWire(t, ts, "CreateChangeSet", map[string]string{
		"StackName": stackName, "ChangeSetName": csName, "ChangeSetType": "UPDATE", "TemplateBody": cfnBucketTemplate,
	})
	key := "changeset:" + cfnWireScope(t, stackCreated) + "/" + stackName + "/" + csName
	cfnWireRequireSet(t, cfnWireRecord(t, ts, key), key, "CreatedAt")

	cs := map[string]string{"StackName": stackName, "ChangeSetName": csName}
	cfnWireRun(t, ts, []cfnWireCase{
		{action: "CreateChangeSet", held: created, anchor: "<Id>"},
		{action: "DescribeChangeSet", params: cs, anchor: "<ChangeSetName>" + csName + "</ChangeSetName>"},
		{action: "ListChangeSets", params: map[string]string{"StackName": stackName}, anchor: "<ChangeSetName>" + csName + "</ChangeSetName>"},
		// Last: it removes the record every case above reads.
		{action: "DeleteChangeSet", params: cs, anchor: "<DeleteChangeSetResponse"},
	})

	// ExecuteChangeSet consumes its change set, so it is driven against a second one.
	const execName = "wire-exec-set"
	cfnWire(t, ts, "CreateChangeSet", map[string]string{
		"StackName": stackName, "ChangeSetName": execName, "ChangeSetType": "UPDATE", "TemplateBody": cfnBucketTemplate,
	})
	cfnWireRun(t, ts, []cfnWireCase{
		{action: "ExecuteChangeSet", params: map[string]string{"StackName": stackName, "ChangeSetName": execName}, anchor: "<ExecuteChangeSetResponse"},
	})
}
