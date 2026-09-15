package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// roleLastUsedClock is the simulated instant every test in this file freezes at, so an
// assertion may name the rendered LastUsedDate literally (#816).
var roleLastUsedClock = time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)

// roleLastUsedFixture is a server with IAM and STS over one state store, a frozen
// simulated clock, and an event store capturing bodies so its stream can be replayed.
type roleLastUsedFixture struct {
	server   *emulator.Server
	state    emulator.StateManager
	store    *emulator.EventStore
	tc       *emulator.TimeController
	registry *emulator.PluginRegistry
	logger   emulator.Logger
}

// roleLastUsedPutFault is a state store whose Put fails for keys containing a substring, so
// the write AssumeRole makes to the *IAM* record can be broken while the session write it
// makes to the STS namespace still succeeds.
type roleLastUsedPutFault struct {
	emulator.StateManager

	// failKeySubstr, when non-empty, makes Put fail for any key containing it.
	failKeySubstr string
}

func (s *roleLastUsedPutFault) Put(ctx context.Context, namespace, key string, value []byte) error {
	if s.failKeySubstr != "" && strings.Contains(key, s.failKeySubstr) {
		return errors.New("state store unavailable")
	}
	return s.StateManager.Put(ctx, namespace, key, value)
}

// newRoleLastUsedFixture builds a [roleLastUsedFixture] over a fresh in-memory store.
func newRoleLastUsedFixture(t *testing.T) *roleLastUsedFixture {
	t.Helper()
	return newRoleLastUsedFixtureWith(t, emulator.NewMemoryStateManager())
}

// newRoleLastUsedFixtureWith is [newRoleLastUsedFixture] over a caller-supplied store, for
// the test that needs a write to fail.
//
// The clock is frozen — scale 0, so Now() is the baseline and nothing else — rather than
// merely started at a fixed instant. A [emulator.TimeController] at the default scale of
// 1.0 advances with wall time, and RoleLastUsed is rendered to the second, so an assertion
// on the literal value would be a wall-clock dependence: it would pass except when the
// test happened to straddle a second boundary.
func newRoleLastUsedFixtureWith(t *testing.T, state emulator.StateManager) *roleLastUsedFixture {
	t.Helper()

	cfg := emulator.DefaultConfig()
	// The replay test needs the recorded requests themselves, which the store keeps only
	// when told to.
	cfg.EventStore.IncludeBodies = true

	logger := emulator.NewDefaultLogger(slog.LevelInfo, false)

	tc := emulator.NewTimeController(roleLastUsedClock)
	tc.SetScale(0)
	// After SetScale, because SetScale folds the wall time elapsed since construction into
	// the baseline before changing the scale.
	tc.SetTime(roleLastUsedClock)

	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig(), emulator.WithTimeController(tc))
	registry := emulator.NewPluginRegistry()

	iamPlugin := &emulator.IAMPlugin{}
	require.NoError(t, iamPlugin.Initialize(context.TODO(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(iamPlugin)

	stsPlugin := &emulator.STSPlugin{}
	require.NoError(t, stsPlugin.Initialize(context.TODO(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(stsPlugin)

	return &roleLastUsedFixture{
		server:   emulator.NewServer(*cfg, registry, store, state, tc, logger),
		state:    state,
		store:    store,
		tc:       tc,
		registry: registry,
		logger:   logger,
	}
}

// createRole creates a role through the IAM API.
func (f *roleLastUsedFixture) createRole(t *testing.T, roleName string) {
	t.Helper()
	resp := iamRequest(t, f.server, "CreateRole", map[string]string{"RoleName": roleName})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

// assumeRoleInRegion assumes a role through the STS API with a request whose region is the
// one given, which the host header carries the way an SDK's regional endpoint does.
func (f *roleLastUsedFixture) assumeRoleInRegion(t *testing.T, roleName, region string) {
	t.Helper()
	url := "/?Action=AssumeRole&RoleArn=arn:aws:iam::123456789012:role/" + roleName +
		"&RoleSessionName=session-" + region
	r := httptest.NewRequest(http.MethodPost, url, nil)
	r.Host = "sts." + region + ".amazonaws.com"

	w := httptest.NewRecorder()
	f.server.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code, "AssumeRole in %s: %s", region, w.Body.String())
}

// getRoleXML returns the raw GetRole response body. Raw, not decoded: a decoded map cannot
// tell an absent member from an empty one, which is the whole question here.
func (f *roleLastUsedFixture) getRoleXML(t *testing.T, roleName string) string {
	t.Helper()
	resp := iamRequest(t, f.server, "GetRole", map[string]string{"RoleName": roleName})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return string(body)
}

// listRolesXML returns the raw ListRoles response body, for the same reason.
func (f *roleLastUsedFixture) listRolesXML(t *testing.T) string {
	t.Helper()
	resp := iamRequest(t, f.server, "ListRoles", map[string]string{})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return string(body)
}

// roleLastUsedElement returns the <RoleLastUsed> element of an IAM XML body, or "" when the
// body carries none.
func roleLastUsedElement(t *testing.T, body string) string {
	t.Helper()
	const openTag, closeTag = "<RoleLastUsed>", "</RoleLastUsed>"
	start := strings.Index(body, openTag)
	if start < 0 {
		return ""
	}
	end := strings.Index(body[start:], closeTag)
	require.GreaterOrEqual(t, end, 0, "unterminated RoleLastUsed element in %s", body)
	return body[start : start+end+len(closeTag)]
}

// TestIAMRoleLastUsed_GetRole asserts that GetRole omits the member entirely until the role
// is assumed and then renders both of its children (#816).
//
// AWS's `RoleLastUsed` type is "returned as a response element in the GetRole and
// GetAccountAuthorizationDetails operations"; substrate answers both since #848, and
// TestIAMAccountAuthorizationDetails_RoleLastUsedInsideInstanceProfile covers the other. The
// omission before the first assume is substrate's choice, not AWS's: AWS documents
// LastUsedDate as "null if the role has not been used within the IAM tracking period" and
// says nothing about a role never assumed at all.
func TestIAMRoleLastUsed_GetRole(t *testing.T) {
	f := newRoleLastUsedFixture(t)
	f.createRole(t, "lastused-role")

	before := f.getRoleXML(t, "lastused-role")
	require.Contains(t, before, "<RoleName>lastused-role</RoleName>",
		"the role must be in the response for the absence assertion to mean anything")
	assert.NotContains(t, before, "RoleLastUsed",
		"GetRole must omit RoleLastUsed entirely before the role is assumed")

	f.assumeRoleInRegion(t, "lastused-role", "us-east-1")

	after := f.getRoleXML(t, "lastused-role")
	assert.Equal(t,
		"<RoleLastUsed><LastUsedDate>2025-03-04T05:06:07Z</LastUsedDate>"+
			"<Region>us-east-1</Region></RoleLastUsed>",
		roleLastUsedElement(t, after),
		"GetRole must render both children after the role is assumed")
}

// TestIAMRoleLastUsed_CreateRole asserts that CreateRole, which shares the single-role
// wrapper, renders no RoleLastUsed: a role created a moment ago has not been assumed.
func TestIAMRoleLastUsed_CreateRole(t *testing.T) {
	f := newRoleLastUsedFixture(t)

	resp := iamRequest(t, f.server, "CreateRole", map[string]string{"RoleName": "fresh-role"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	require.Contains(t, string(body), "<RoleName>fresh-role</RoleName>")
	assert.NotContains(t, string(body), "RoleLastUsed")
}

// TestIAMRoleLastUsed_ListRolesNeverRenders asserts that ListRoles reports no RoleLastUsed
// even for a role that has been assumed, per AWS's exclusion note on the operation:
//
//	IAM resource-listing operations return a subset of the available attributes for the
//	resource. This operation does not return the following attributes, even though they are
//	an attribute of the returned object: PermissionsBoundary, RoleLastUsed, Tags. To view
//	all of the information for a role, see GetRole.
//
// The assertion is against the raw XML rather than a decoded response, because a decoded
// map cannot distinguish an absent member from an empty one.
func TestIAMRoleLastUsed_ListRolesNeverRenders(t *testing.T) {
	f := newRoleLastUsedFixture(t)
	f.createRole(t, "listed-role")
	f.assumeRoleInRegion(t, "listed-role", "us-east-1")

	// The same run's GetRole does report it, so the ListRoles assertion below is about the
	// shape and not about an unassumed role.
	require.NotEmpty(t, roleLastUsedElement(t, f.getRoleXML(t, "listed-role")))

	listed := f.listRolesXML(t)
	require.Contains(t, listed, "<RoleName>listed-role</RoleName>")
	assert.NotContains(t, listed, "RoleLastUsed",
		"ListRoles must not render RoleLastUsed, per AWS's resource-listing note")
}

// TestIAMRoleLastUsed_RegionIsTheRequests asserts that Region is the assuming request's,
// not the emulator's configured one — AWS documents it as "the name of the AWS Region in
// which the role was last used".
//
// Proven by assuming the same role from two different regions in turn: each assume must
// leave its own region behind, so a value taken from configuration could not pass both
// halves.
func TestIAMRoleLastUsed_RegionIsTheRequests(t *testing.T) {
	f := newRoleLastUsedFixture(t)
	f.createRole(t, "roaming-role")

	for _, region := range []string{"us-east-1", "eu-west-2", "ap-southeast-1"} {
		f.assumeRoleInRegion(t, "roaming-role", region)

		assert.Equal(t,
			"<RoleLastUsed><LastUsedDate>2025-03-04T05:06:07Z</LastUsedDate>"+
				"<Region>"+region+"</Region></RoleLastUsed>",
			roleLastUsedElement(t, f.getRoleXML(t, "roaming-role")),
			"an assume from %s must report %s", region, region)
	}
}

// TestIAMRoleLastUsed_SurvivesReplay asserts that replaying the recorded stream rebuilds
// the identical RoleLastUsed — the acceptance criterion that decided against a projection
// over the event log.
//
// It holds because [emulator.ReplayEngine] re-executes each recorded request with the
// simulated clock set to the event's timestamp and the request context's region taken from
// the event, so the re-executed AssumeRole derives the same date from the same simulated
// clock and the same region from the same recorded request, and writes the same value.
func TestIAMRoleLastUsed_SurvivesReplay(t *testing.T) {
	f := newRoleLastUsedFixture(t)
	f.createRole(t, "replayed-role")
	f.assumeRoleInRegion(t, "replayed-role", "eu-central-1")

	live := roleLastUsedElement(t, f.getRoleXML(t, "replayed-role"))
	require.NotEmpty(t, live)

	engine := emulator.NewReplayEngine(f.store, f.state, f.tc, f.registry,
		emulator.ReplayConfig{}, f.logger)
	results, err := engine.Replay(context.Background(), "default")
	require.NoError(t, err)
	require.Zero(t, results.FailedEvents, "replay must not fail an event")

	assert.Equal(t, live, roleLastUsedElement(t, f.getRoleXML(t, "replayed-role")),
		"a replayed run must report the same RoleLastUsed as the live one")
}

// TestIAMRoleLastUsed_StoreFailureFailsTheAssume asserts that a state store that refuses the
// IAM write fails the whole AssumeRole rather than answering a session and silently losing the
// stamp.
//
// The fault is keyed on the IAM record's key alone: the session write AssumeRole makes lands
// under `session:` in the STS namespace and still succeeds, so this reaches the new write and
// nothing before it.
func TestIAMRoleLastUsed_StoreFailureFailsTheAssume(t *testing.T) {
	fault := &roleLastUsedPutFault{StateManager: emulator.NewMemoryStateManager()}
	f := newRoleLastUsedFixtureWith(t, fault)
	// Armed only after the role exists, so CreateRole's own Put is not the one that fails.
	f.createRole(t, "unwritable-role")
	fault.failKeySubstr = "role:"

	r := httptest.NewRequest(http.MethodPost,
		"/?Action=AssumeRole&RoleArn=arn:aws:iam::123456789012:role/unwritable-role"+
			"&RoleSessionName=doomed", nil)
	r.Host = "sts.us-east-1.amazonaws.com"

	w := httptest.NewRecorder()
	f.server.ServeHTTP(w, r)

	assert.Equal(t, http.StatusInternalServerError, w.Code,
		"a refused IAM write must fail the assume, not be swallowed")
	assert.Contains(t, w.Body.String(), "store role last used",
		"the error must name the write that failed")
}

// TestIAMRoleLastUsed_SeededRecord renders the member from a record seeded into state, which
// is what a consumer that writes an [emulator.IAMRole] directly observes, and covers the
// values an assume cannot produce: a date carrying a non-UTC offset, and a stored value
// whose region needs XML escaping.
func TestIAMRoleLastUsed_SeededRecord(t *testing.T) {
	tests := []struct {
		name     string
		lastUsed any
		want     string
	}{
		{
			name:     "never assumed",
			lastUsed: nil,
			want:     "",
		},
		{
			name: "assumed",
			lastUsed: map[string]any{
				"LastUsedDate": "2025-03-04T05:06:07Z",
				"Region":       "us-west-2",
			},
			want: "<RoleLastUsed><LastUsedDate>2025-03-04T05:06:07Z</LastUsedDate>" +
				"<Region>us-west-2</Region></RoleLastUsed>",
		},
		{
			name: "a date with an offset is reported in UTC",
			lastUsed: map[string]any{
				"LastUsedDate": "2025-03-04T07:06:07+02:00",
				"Region":       "eu-west-1",
			},
			want: "<RoleLastUsed><LastUsedDate>2025-03-04T05:06:07Z</LastUsedDate>" +
				"<Region>eu-west-1</Region></RoleLastUsed>",
		},
		{
			name: "the region is XML-escaped",
			lastUsed: map[string]any{
				"LastUsedDate": "2025-03-04T05:06:07Z",
				"Region":       "us-east-1&more",
			},
			want: "<RoleLastUsed><LastUsedDate>2025-03-04T05:06:07Z</LastUsedDate>" +
				"<Region>us-east-1&amp;more</Region></RoleLastUsed>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			f := newRoleLastUsedFixture(t)
			f.createRole(t, "seeded-role")

			keys, err := f.state.List(ctx, "iam", "role:")
			require.NoError(t, err)
			require.Len(t, keys, 1, "exactly one role was created")

			raw, err := f.state.Get(ctx, "iam", keys[0])
			require.NoError(t, err)
			var record map[string]any
			require.NoError(t, json.Unmarshal(raw, &record))
			if tt.lastUsed != nil {
				record["RoleLastUsed"] = tt.lastUsed
			}
			updated, err := json.Marshal(record)
			require.NoError(t, err)
			require.NoError(t, f.state.Put(ctx, "iam", keys[0], updated))

			assert.Equal(t, tt.want, roleLastUsedElement(t, f.getRoleXML(t, "seeded-role")))
		})
	}
}
