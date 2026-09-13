package emulator_test

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// PermissionsBoundaryUsageCount on the IAM policy shapes (#815).
//
// The member is derived from a scan of the users and roles on every policy read rather than
// stored on the policy, so what these tests pin is mostly what that choice buys: a count that
// falls to zero when the last boundary goes, a count that follows an entity deleted out from
// under it, and a count a replayed run agrees with. None of those needs a code path of its own,
// which is the point — a stored counter would need one each, and each would be a place to be
// wrong.
//
// Assertions read the raw XML, for the reason #807's shape tests do: a decoded map cannot tell
// an absent member from a zero one, and "reports zero" and "reports nothing" are the two
// answers being distinguished.

// iamBoundaryUsageDoc is a minimal valid policy document, so CreatePolicy has something to
// store; no test here reads it.
const iamBoundaryUsageDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`

// iamBoundaryUsageCount returns the PermissionsBoundaryUsageCount an IAM XML body reports,
// failing when the body carries none or carries more than one — a caller asserting on a count
// means a single policy's, and a missing member must not read as zero.
func iamBoundaryUsageCount(t *testing.T, body string) int {
	t.Helper()
	const open, shut = "<PermissionsBoundaryUsageCount>", "</PermissionsBoundaryUsageCount>"
	start := strings.Index(body, open)
	require.GreaterOrEqual(t, start, 0, "no PermissionsBoundaryUsageCount in %s", body)
	rest := body[start+len(open):]
	end := strings.Index(rest, shut)
	require.GreaterOrEqual(t, end, 0, "unterminated PermissionsBoundaryUsageCount")
	require.NotContains(t, rest[end:], open, "more than one policy reported a count")
	count, err := strconv.Atoi(rest[:end])
	require.NoError(t, err, "PermissionsBoundaryUsageCount is an Integer on the Policy type")
	return count
}

// iamBoundaryUsageFixture creates a policy plus a user and a role, and returns the policy's
// ARN. Neither entity has the boundary set yet.
func iamBoundaryUsageFixture(t *testing.T, srv *emulator.Server) string {
	t.Helper()
	created := iamFormRaw(t, srv, "CreatePolicy", map[string]string{
		"PolicyName": "boundary", "PolicyDocument": iamBoundaryUsageDoc,
	})
	iamFormRaw(t, srv, "CreateUser", map[string]string{"UserName": "alice"})
	iamFormRaw(t, srv, "CreateRole", map[string]string{"RoleName": "deploy"})
	return iamPolicyARNFrom(t, created)
}

// TestIAM_PermissionsBoundaryUsageCount_CountsUsersAndRoles walks a boundary onto a user and a
// role and back off again, asserting the count after every step.
//
// AWS's wording is "the number of entities (users and roles)", so the two are one total rather
// than two — a count of 1 with both set would be the reading where only one kind is scanned.
func TestIAM_PermissionsBoundaryUsageCount_CountsUsersAndRoles(t *testing.T) {
	srv := newIAMTestServer(t)
	arn := iamBoundaryUsageFixture(t, srv)

	steps := []struct {
		name      string
		operation string
		params    map[string]string
		want      int
	}{
		{
			name:      "a user's boundary counts",
			operation: "PutUserPermissionsBoundary",
			params:    map[string]string{"UserName": "alice", "PermissionsBoundary": arn},
			want:      1,
		},
		{
			name:      "a role's boundary counts with it, not separately",
			operation: "PutRolePermissionsBoundary",
			params:    map[string]string{"RoleName": "deploy", "PermissionsBoundary": arn},
			want:      2,
		},
		{
			name:      "removing the user's leaves the role's",
			operation: "DeleteUserPermissionsBoundary",
			params:    map[string]string{"UserName": "alice"},
			want:      1,
		},
		{
			name:      "removing the last one reaches zero",
			operation: "DeleteRolePermissionsBoundary",
			params:    map[string]string{"RoleName": "deploy"},
			want:      0,
		},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			iamFormRaw(t, srv, step.operation, step.params)
			body := iamFormRaw(t, srv, "GetPolicy", map[string]string{"PolicyArn": arn})
			assert.Equal(t, step.want, iamBoundaryUsageCount(t, body))
		})
	}
}

// TestIAM_PermissionsBoundaryUsageCount_FollowsEntityDeletion deletes the entities rather than
// their boundaries.
//
// DeleteUser and DeleteRole drop the whole record, boundary included, and neither mentions
// policies — so this is the criterion a stored counter would most easily miss, and the one
// deriving the count satisfies without a line of code.
func TestIAM_PermissionsBoundaryUsageCount_FollowsEntityDeletion(t *testing.T) {
	srv := newIAMTestServer(t)
	arn := iamBoundaryUsageFixture(t, srv)

	iamFormRaw(t, srv, "PutUserPermissionsBoundary", map[string]string{
		"UserName": "alice", "PermissionsBoundary": arn,
	})
	iamFormRaw(t, srv, "PutRolePermissionsBoundary", map[string]string{
		"RoleName": "deploy", "PermissionsBoundary": arn,
	})

	steps := []struct {
		name      string
		operation string
		params    map[string]string
		want      int
	}{
		{"both entities are counted", "GetPolicy", map[string]string{"PolicyArn": arn}, 2},
		{"deleting the user decrements", "DeleteUser", map[string]string{"UserName": "alice"}, 1},
		{"deleting the role decrements", "DeleteRole", map[string]string{"RoleName": "deploy"}, 0},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			iamFormRaw(t, srv, step.operation, step.params)
			body := iamFormRaw(t, srv, "GetPolicy", map[string]string{"PolicyArn": arn})
			assert.Equal(t, step.want, iamBoundaryUsageCount(t, body))
		})
	}
}

// TestIAM_PermissionsBoundaryUsageCount_OnEveryPolicyShape pins the member onto all three
// operations the `Policy` type's scope note names.
//
// `Description` is the contrast that makes this deliberate: it carries an explicit "not
// included in the response to the ListPolicies operation" carve-out and this member carries
// none, which `ListPolicies`' own sample confirms by rendering a count on every member.
func TestIAM_PermissionsBoundaryUsageCount_OnEveryPolicyShape(t *testing.T) {
	srv := newIAMTestServer(t)

	created := iamFormRaw(t, srv, "CreatePolicy", map[string]string{
		"PolicyName": "boundary", "PolicyDocument": iamBoundaryUsageDoc,
	})
	assert.Equal(t, 0, iamBoundaryUsageCount(t, created),
		"a policy that has just been created is nobody's boundary")

	arn := iamPolicyARNFrom(t, created)
	iamFormRaw(t, srv, "CreateUser", map[string]string{"UserName": "alice"})
	iamFormRaw(t, srv, "PutUserPermissionsBoundary", map[string]string{
		"UserName": "alice", "PermissionsBoundary": arn,
	})

	single := iamFormRaw(t, srv, "GetPolicy", map[string]string{"PolicyArn": arn})
	// Scope=Local is the created policy alone, so the listing has exactly one member and its
	// count is unambiguous — under Scope=All the 52 bundled policies would each report one too.
	listing := iamFormRaw(t, srv, "ListPolicies", map[string]string{"Scope": "Local"})
	require.Contains(t, listing, "<PolicyName>boundary</PolicyName>", "policy missing from listing")

	assert.Equal(t, 1, iamBoundaryUsageCount(t, single))
	assert.Equal(t, iamBoundaryUsageCount(t, single), iamBoundaryUsageCount(t, listing),
		"both shapes derive the count from the same state, so they cannot disagree")
}

// TestIAM_PermissionsBoundaryUsageCount_CountsABundledPolicy sets a boundary to one of the
// bundled AWS managed policies.
//
// Every boundary in a fresh substrate is a bundled ARN, because those are the only policies
// that exist before a caller creates one. The catalog carries no counter and its entries are
// shared pointers, so the count has to come from outside the record — which is why it is
// threaded as an argument rather than assigned to a field.
func TestIAM_PermissionsBoundaryUsageCount_CountsABundledPolicy(t *testing.T) {
	srv := newIAMTestServer(t)

	const bundled = "arn:aws:iam::aws:policy/PowerUserAccess"
	iamFormRaw(t, srv, "CreateRole", map[string]string{"RoleName": "deploy"})
	iamFormRaw(t, srv, "PutRolePermissionsBoundary", map[string]string{
		"RoleName": "deploy", "PermissionsBoundary": bundled,
	})

	body := iamFormRaw(t, srv, "GetPolicy", map[string]string{"PolicyArn": bundled})
	assert.Equal(t, 1, iamBoundaryUsageCount(t, body))

	fresh := newIAMTestServer(t)
	unused := iamFormRaw(t, fresh, "GetPolicy", map[string]string{"PolicyArn": bundled})
	assert.Equal(t, 0, iamBoundaryUsageCount(t, unused),
		"the same bundled policy in another emulator is nobody's boundary")
}

// newIAMRecordedStack returns a server and the plugin registry behind it, both over the
// caller's state and event store.
//
// The registry is returned because a replay runs against one directly, and it has to be a
// registry whose plugin reads the state the replay is rebuilding rather than the state the
// recorded run wrote.
func newIAMRecordedStack(
	t *testing.T,
	state emulator.StateManager,
	store *emulator.EventStore,
) (*emulator.Server, *emulator.PluginRegistry) {
	t.Helper()
	logger := emulator.NewDefaultLogger(slog.LevelInfo, false)
	registry := emulator.NewPluginRegistry()
	plugin := &emulator.IAMPlugin{}
	require.NoError(t, plugin.Initialize(context.TODO(),
		emulator.PluginConfig{State: state, Logger: logger}))
	registry.Register(plugin)

	cfg := emulator.DefaultConfig()
	// A fixed instant, so nothing here reads the wall clock for a value it then asserts on.
	tc := emulator.NewTimeController(time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC))
	return emulator.NewServer(*cfg, registry, store, state, tc, logger), registry
}

// newIAMBodyRecordingStore returns an event store that keeps request bodies, which
// DefaultConfig leaves off: a stream recorded without them replays nothing.
func newIAMBodyRecordingStore() *emulator.EventStore {
	return emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled: true, Backend: "memory", IncludeBodies: true,
	})
}

// TestIAM_PermissionsBoundaryUsageCount_SurvivesReplay records a run that sets two boundaries,
// replays its event stream into empty state, and reads the count back.
//
// This is #815's determinism criterion. It holds because the count is a function of the state
// the replayed events rebuild — there is no accumulator whose live value would have to end up
// agreeing with a replayed one.
func TestIAM_PermissionsBoundaryUsageCount_SurvivesReplay(t *testing.T) {
	ctx := context.Background()

	recorded := newIAMBodyRecordingStore()
	liveState := emulator.NewMemoryStateManager()
	liveSrv, _ := newIAMRecordedStack(t, liveState, recorded)

	arn := iamBoundaryUsageFixture(t, liveSrv)
	iamFormRaw(t, liveSrv, "PutUserPermissionsBoundary", map[string]string{
		"UserName": "alice", "PermissionsBoundary": arn,
	})
	iamFormRaw(t, liveSrv, "PutRolePermissionsBoundary", map[string]string{
		"RoleName": "deploy", "PermissionsBoundary": arn,
	})

	live := iamBoundaryUsageCount(t, iamFormRaw(t, liveSrv, "GetPolicy",
		map[string]string{"PolicyArn": arn}))
	require.Equal(t, 2, live, "the live run has a boundary on one user and one role")

	// The replayed stack records into a store of its own, so the stream being replayed stays
	// exactly the one the live run wrote.
	replayedState := emulator.NewMemoryStateManager()
	replaySrv, registry := newIAMRecordedStack(t, replayedState, newIAMBodyRecordingStore())

	engine := emulator.NewReplayEngine(recorded, replayedState,
		emulator.NewTimeController(time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)),
		registry, emulator.ReplayConfig{}, &testLogger{})
	results, err := engine.Replay(ctx, "default")
	require.NoError(t, err)
	require.Zero(t, results.FailedEvents, "every recorded IAM call must replay")
	require.Zero(t, results.SkippedEvents, "an event with no request body replays nothing")

	replayed := iamBoundaryUsageCount(t, iamFormRaw(t, replaySrv, "GetPolicy",
		map[string]string{"PolicyArn": arn}))
	assert.Equal(t, live, replayed, "a replayed run reports the live run's count")
}
