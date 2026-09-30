package emulator_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ecsWire issues op and returns the raw response body, failing the test on anything but 200.
// Raw bytes rather than a decoded struct on purpose: what is under test is which members the
// body has, and a decode into a Go type is exactly the step that hides an extra one.
func ecsWire(t *testing.T, p *emulator.ECSPlugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	resp, err := p.HandleRequest(ctx, ecsRequest(t, op, body))
	require.NoError(t, err, "%s", op)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body
}

// ecsNoBookkeeping asserts that body carries none of substrate's own members.
//
// The three are matched on their rendered names. AccountID and Region carry no omitempty on
// any of the four records, so an unprojected record reports both unconditionally and these two
// assertions are non-vacuous wherever they run. `ever_tagged` is `,omitempty`, so it is absent
// whenever the flag is false whatever the projection does — every caller below therefore tags
// the resource first and anchors on a response proving the tag landed.
func ecsNoBookkeeping(t *testing.T, op string, body []byte) {
	t.Helper()
	for _, member := range []string{`"AccountID"`, `"Region"`, `"ever_tagged"`} {
		assert.NotContains(t, string(body), member,
			"%s: response carries substrate's internal %s: %s", op, member, body)
	}
}

// ecsTagAndAnchor tags arn through ECS's own TagResource and asserts through
// ListTagsForResource that the tag landed, returning nothing but a guarantee.
//
// The tag is what makes every `ever_tagged` assertion non-vacuous: ecsMergeRecordTags stamps
// the flag through taggingStampRecordEverTagged (#938), so a record that has been tagged has
// the flag set and an unprojected response would report it. Without this the absence assertion
// passes on a body that could not have carried the member — the vacuous assertion #1304
// shipped on EFS before it was caught, and the reason
// scripts/wire-bookkeeping-projected.txt's header says what its third column has to prove.
func ecsTagAndAnchor(t *testing.T, p *emulator.ECSPlugin, ctx *emulator.RequestContext, arn string) {
	t.Helper()
	ecsWire(t, p, ctx, "TagResource", map[string]any{
		"resourceArn": arn,
		"tags":        []map[string]string{{"key": "owner", "value": "wire-test"}},
	})
	listed := ecsWire(t, p, ctx, "ListTagsForResource", map[string]any{"resourceArn": arn})
	assert.Contains(t, string(listed), `"owner"`,
		"the anchor: %s must report the tag, or ever_tagged is false and every assertion below is vacuous: %s",
		arn, listed)
}

// TestECSWire_ClusterResponsesCarryNoBookkeepingMember covers the three sites that answer an
// ECSCluster: CreateCluster, DescribeClusters and DeleteCluster.
//
// DeleteCluster is asserted on a cluster tagged while it still existed, because the record it
// answers is the one it just deleted — the flag is read out of state before the Delete, so the
// site can carry the member and the assertion is not vacuous.
func TestECSWire_ClusterResponsesCarryNoBookkeepingMember(t *testing.T) {
	p, ctx := setupECSPlugin(t)

	created := ecsWire(t, p, ctx, "CreateCluster", map[string]any{"clusterName": "wire-cluster"})
	arn := "arn:aws:ecs:us-east-1:123456789012:cluster/wire-cluster"
	ecsTagAndAnchor(t, p, ctx, arn)

	// CreateCluster answers before anything can have tagged the cluster, so its flag is
	// necessarily false and only AccountID and Region are non-vacuous there. Asserted anyway,
	// because those two are the members the site actually leaked.
	ecsNoBookkeeping(t, "CreateCluster", created)
	assert.Contains(t, string(created), `"clusterArn"`, "CreateCluster must answer the cluster: %s", created)

	described := ecsWire(t, p, ctx, "DescribeClusters", map[string]any{"clusters": []string{"wire-cluster"}})
	ecsNoBookkeeping(t, "DescribeClusters", described)
	assert.Contains(t, string(described), `"wire-cluster"`, "DescribeClusters must answer the cluster: %s", described)

	deleted := ecsWire(t, p, ctx, "DeleteCluster", map[string]any{"cluster": "wire-cluster"})
	ecsNoBookkeeping(t, "DeleteCluster", deleted)
	assert.Contains(t, string(deleted), `"INACTIVE"`, "DeleteCluster must answer the deleted cluster: %s", deleted)
}

// TestECSWire_ServiceResponsesCarryNoBookkeepingMember covers the four sites that answer an
// ECSService: CreateService, UpdateService, DescribeServices and DeleteService.
//
// It also pins the second half of the fix. API_Service publishes clusterArn and has no
// clusterName member at all, so `clusterName` inside a service object is a member AWS does not
// have — one the ratchet cannot see, because it is not one of the five bookkeeping names. The
// assertion is anchored: the cluster ARN must be present, so a body that dropped the whole
// record could not pass by answering nothing.
func TestECSWire_ServiceResponsesCarryNoBookkeepingMember(t *testing.T) {
	p, ctx := setupECSPlugin(t)

	ecsWire(t, p, ctx, "CreateCluster", map[string]any{"clusterName": "svc-wire"})
	ecsWire(t, p, ctx, "RegisterTaskDefinition", map[string]any{
		"family":               "svc-wire-td",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})
	created := ecsWire(t, p, ctx, "CreateService", map[string]any{
		"cluster":        "svc-wire",
		"serviceName":    "wire-svc",
		"taskDefinition": "svc-wire-td:1",
		"desiredCount":   2,
	})
	arn := "arn:aws:ecs:us-east-1:123456789012:service/svc-wire/wire-svc"
	ecsTagAndAnchor(t, p, ctx, arn)

	ecsNoBookkeeping(t, "CreateService", created)

	for _, tc := range []struct {
		op   string
		body map[string]any
		want string
	}{
		{"UpdateService", map[string]any{"cluster": "svc-wire", "service": "wire-svc", "desiredCount": 3}, `"desiredCount":3`},
		{"DescribeServices", map[string]any{"cluster": "svc-wire", "services": []string{"wire-svc"}}, `"wire-svc"`},
		{"DeleteService", map[string]any{"cluster": "svc-wire", "service": "wire-svc"}, `"INACTIVE"`},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := ecsWire(t, p, ctx, tc.op, tc.body)
			ecsNoBookkeeping(t, tc.op, body)
			assert.Contains(t, string(body), tc.want, "%s must answer the service: %s", tc.op, body)
			assert.Contains(t, string(body), `"clusterArn"`,
				"%s must answer the cluster ARN, or the clusterName assertion below is vacuous: %s", tc.op, body)
			assert.NotContains(t, string(body), `"clusterName"`,
				"%s: API_Service has no clusterName member: %s", tc.op, body)
		})
	}

	// CreateService too, outside the loop because its record was untagged when it answered.
	assert.Contains(t, string(created), `"clusterArn"`, "CreateService must answer the cluster ARN: %s", created)
	assert.NotContains(t, string(created), `"clusterName"`,
		"CreateService: API_Service has no clusterName member: %s", created)
}

// TestECSWire_TaskResponsesCarryNoBookkeepingMember covers the three sites that answer an
// ECSTask: RunTask, DescribeTasks and StopTask.
//
// It also pins the optional-timestamp omission. The record's StoppedAt is a plain EpochSeconds
// tagged `stoppedAt,omitempty`, and omitempty has no effect on a struct type, so a running task
// reported `"stoppedAt":null` — a member API_Task omits when it has no value. ecsTaskOut makes
// it a pointer, so the member is absent until the task actually stops. The assertion is
// two-sided: absent while running, present once stopped.
func TestECSWire_TaskResponsesCarryNoBookkeepingMember(t *testing.T) {
	p, ctx := setupECSPlugin(t)

	ecsWire(t, p, ctx, "CreateCluster", map[string]any{"clusterName": "task-wire"})
	ecsWire(t, p, ctx, "RegisterTaskDefinition", map[string]any{
		"family":               "task-wire-td",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})
	ran := ecsWire(t, p, ctx, "RunTask", map[string]any{
		"cluster":        "task-wire",
		"taskDefinition": "task-wire-td:1",
	})
	ecsNoBookkeeping(t, "RunTask", ran)
	assert.Contains(t, string(ran), `"startedAt"`, "RunTask must answer a started task: %s", ran)
	assert.NotContains(t, string(ran), `"stoppedAt"`,
		"RunTask: a running task has no stoppedAt, and API_Task omits a member it has no value for: %s", ran)

	var runOut struct {
		Tasks []struct {
			TaskArn string `json:"taskArn"`
		} `json:"tasks"`
	}
	require.NoError(t, json.Unmarshal(ran, &runOut), "decode RunTask: %s", ran)
	require.Len(t, runOut.Tasks, 1, "RunTask must answer one task: %s", ran)
	taskArn := runOut.Tasks[0].TaskArn
	ecsTagAndAnchor(t, p, ctx, taskArn)

	described := ecsWire(t, p, ctx, "DescribeTasks", map[string]any{
		"cluster": "task-wire",
		"tasks":   []string{taskArn},
	})
	ecsNoBookkeeping(t, "DescribeTasks", described)
	assert.Contains(t, string(described), taskArn, "DescribeTasks must answer the task: %s", described)

	stopped := ecsWire(t, p, ctx, "StopTask", map[string]any{
		"cluster": "task-wire",
		"task":    taskArn,
		"reason":  "wire test",
	})
	ecsNoBookkeeping(t, "StopTask", stopped)
	assert.Contains(t, string(stopped), `"stoppedAt"`,
		"StopTask must report stoppedAt, or the RunTask omission above proves nothing: %s", stopped)
	assert.Contains(t, string(stopped), `"STOPPED"`, "StopTask must answer the stopped task: %s", stopped)
}

// TestECSWire_TaskDefinitionResponsesCarryNoBookkeepingMember covers the three sites that
// answer an ECSTaskDefinition: RegisterTaskDefinition, DescribeTaskDefinition and
// DeregisterTaskDefinition.
//
// It also pins where task-definition tags are published. API_TaskDefinition has no tags member;
// RegisterTaskDefinition and DescribeTaskDefinition each publish `tags` as a top-level sibling
// of taskDefinition, and DeregisterTaskDefinition publishes no tags at all. So the member moved
// rather than went away, and the assertions have to distinguish the two — which is why each
// body is decoded into a shape that separates the top-level element from the nested object,
// instead of substring-matching `"tags"` against the whole body.
func TestECSWire_TaskDefinitionResponsesCarryNoBookkeepingMember(t *testing.T) {
	p, ctx := setupECSPlugin(t)

	registered := ecsWire(t, p, ctx, "RegisterTaskDefinition", map[string]any{
		"family":               "td-wire",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
		"tags":                 []map[string]string{{"key": "env", "value": "test"}},
	})
	arn := "arn:aws:ecs:us-east-1:123456789012:task-definition/td-wire:1"
	ecsTagAndAnchor(t, p, ctx, arn)

	// RegisterTaskDefinition: tags at the top level, and no tags member inside the object.
	topLevel, nested := ecsSplitTaskDefinitionTags(t, "RegisterTaskDefinition", registered)
	ecsNoBookkeeping(t, "RegisterTaskDefinition", registered)
	assert.NotNil(t, topLevel,
		"RegisterTaskDefinition publishes tags as a top-level response element: %s", registered)
	assert.Contains(t, string(topLevel), `"env"`,
		"RegisterTaskDefinition must report the tags it was handed: %s", registered)
	assert.Nil(t, nested,
		"API_TaskDefinition has no tags member: %s", registered)

	// DescribeTaskDefinition without include: no tags at all. "If this field is omitted, tags
	// aren't included in the response."
	plain := ecsWire(t, p, ctx, "DescribeTaskDefinition", map[string]any{"taskDefinition": "td-wire:1"})
	ecsNoBookkeeping(t, "DescribeTaskDefinition", plain)
	plainTop, plainNested := ecsSplitTaskDefinitionTags(t, "DescribeTaskDefinition", plain)
	assert.Nil(t, plainTop,
		"DescribeTaskDefinition without include: [TAGS] publishes no tags: %s", plain)
	assert.Nil(t, plainNested, "API_TaskDefinition has no tags member: %s", plain)
	assert.Contains(t, string(plain), `"td-wire"`, "DescribeTaskDefinition must answer the definition: %s", plain)

	// With include: ["TAGS"], at the top level. Anchored against the assertion above: if the
	// include gate did nothing, one of the two would fail.
	withTags := ecsWire(t, p, ctx, "DescribeTaskDefinition", map[string]any{
		"taskDefinition": "td-wire:1",
		"include":        []string{"TAGS"},
	})
	ecsNoBookkeeping(t, "DescribeTaskDefinition+TAGS", withTags)
	inclTop, inclNested := ecsSplitTaskDefinitionTags(t, "DescribeTaskDefinition+TAGS", withTags)
	require.NotNil(t, inclTop,
		"DescribeTaskDefinition with include: [TAGS] publishes tags at the top level: %s", withTags)
	assert.Contains(t, string(inclTop), `"owner"`,
		"the tag TagResource wrote must be reported: %s", withTags)
	assert.Nil(t, inclNested, "API_TaskDefinition has no tags member: %s", withTags)

	// DeregisterTaskDefinition publishes taskDefinition and nothing else.
	deregistered := ecsWire(t, p, ctx, "DeregisterTaskDefinition", map[string]any{"taskDefinition": "td-wire:1"})
	ecsNoBookkeeping(t, "DeregisterTaskDefinition", deregistered)
	deregTop, deregNested := ecsSplitTaskDefinitionTags(t, "DeregisterTaskDefinition", deregistered)
	assert.Nil(t, deregTop,
		"DeregisterTaskDefinition's only response element is taskDefinition: %s", deregistered)
	assert.Nil(t, deregNested, "API_TaskDefinition has no tags member: %s", deregistered)
	assert.Contains(t, string(deregistered), `"INACTIVE"`,
		"DeregisterTaskDefinition must answer the deregistered definition: %s", deregistered)
}

// TestECSWire_ProjectionLeavesTheRecordIntact is the other half of the claim the four tests
// above make. The projection changes the response, not the state encoding a recorded run
// replays from, so all three bookkeeping fields must still be persisted on all four records —
// which is why the twelve baseline lines stay and
// scripts/wire-bookkeeping-projected.txt is what discharges them.
//
// EverTagged is the field with a live reader across a service boundary: the TaggingPlugin's
// ecsNamespace arm writes it and its scans report it, so a projection that had quietly dropped
// it from the record would take a cross-service answer with it.
func TestECSWire_ProjectionLeavesTheRecordIntact(t *testing.T) {
	p, ctx, state := setupECSWirePlugin(t)

	ecsWire(t, p, ctx, "CreateCluster", map[string]any{"clusterName": "rec-wire"})
	ecsWire(t, p, ctx, "RegisterTaskDefinition", map[string]any{
		"family":               "rec-wire-td",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})
	ecsWire(t, p, ctx, "CreateService", map[string]any{
		"cluster":        "rec-wire",
		"serviceName":    "rec-svc",
		"taskDefinition": "rec-wire-td:1",
	})
	ran := ecsWire(t, p, ctx, "RunTask", map[string]any{
		"cluster":        "rec-wire",
		"taskDefinition": "rec-wire-td:1",
	})
	var runOut struct {
		Tasks []struct {
			TaskArn string `json:"taskArn"`
		} `json:"tasks"`
	}
	require.NoError(t, json.Unmarshal(ran, &runOut), "decode RunTask: %s", ran)
	require.Len(t, runOut.Tasks, 1, "RunTask must answer one task: %s", ran)

	const base = "arn:aws:ecs:us-east-1:123456789012:"
	for _, arn := range []string{
		base + "cluster/rec-wire",
		base + "task-definition/rec-wire-td:1",
		base + "service/rec-wire/rec-svc",
		runOut.Tasks[0].TaskArn,
	} {
		ecsTagAndAnchor(t, p, ctx, arn)
	}

	for _, prefix := range []string{"cluster:", "taskdef:", "service:", "task:"} {
		t.Run(prefix, func(t *testing.T) {
			keys, err := state.List(t.Context(), "ecs", prefix)
			require.NoError(t, err)
			require.Len(t, keys, 1, "one %s record was created", prefix)

			raw, err := state.Get(t.Context(), "ecs", keys[0])
			require.NoError(t, err)
			var record struct {
				AccountID  string `json:"AccountID"`
				Region     string `json:"Region"`
				EverTagged bool   `json:"ever_tagged"`
			}
			require.NoError(t, json.Unmarshal(raw, &record), "decode %s: %s", keys[0], raw)

			assert.Equal(t, "123456789012", record.AccountID,
				"the record still scopes itself to an account")
			assert.Equal(t, "us-east-1", record.Region, "the record still scopes itself to a Region")
			assert.True(t, record.EverTagged, "TagResource stamped the flag the tagging plugin reads")
		})
	}
}

// setupECSWirePlugin is setupECSPlugin with the state manager handed back, so the test above
// can read the records the responses are projected from. Separate rather than a change to
// setupECSPlugin's signature, which every other ECS test calls.
func setupECSWirePlugin(t *testing.T) (*emulator.ECSPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.ECSPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(time.Unix(1700000000, 0).UTC())},
	}), "emulator.ECSPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-ecs-wire",
	}, state
}

// ecsSplitTaskDefinitionTags returns the body's top-level `tags` element and the `tags` member
// nested inside its `taskDefinition` object, either as nil when absent.
//
// Separating the two is the whole point: a substring search for `"tags"` cannot tell the
// published top-level element from the unpublished nested member, and the fix moves the member
// from one to the other.
func ecsSplitTaskDefinitionTags(t *testing.T, op string, body []byte) (topLevel, nested json.RawMessage) {
	t.Helper()
	var out struct {
		Tags           json.RawMessage `json:"tags"`
		TaskDefinition json.RawMessage `json:"taskDefinition"`
	}
	require.NoError(t, json.Unmarshal(body, &out), "%s: decode response: %s", op, body)
	require.NotNil(t, out.TaskDefinition, "%s must answer a taskDefinition: %s", op, body)

	var td struct {
		Tags json.RawMessage `json:"tags"`
	}
	require.NoError(t, json.Unmarshal(out.TaskDefinition, &td), "%s: decode taskDefinition: %s", op, body)
	return out.Tags, td.Tags
}
