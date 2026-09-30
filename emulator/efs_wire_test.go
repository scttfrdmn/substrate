package emulator_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// efsWireEpoch is the instant the clock in this file is frozen at: 2024-03-01T00:00:00Z,
// which is 1709251200 in epoch seconds. Frozen rather than time.Now(), because the members
// under test are timestamps and an assertion on one of them is otherwise an assertion about
// when the suite ran.
const efsWireEpoch = 1709251200

// newEFSWireTestServer is newEFSTestServer with the clock stopped, so CreationTime and
// SizeInBytes.Timestamp have a value this file can name. The state manager comes back
// alongside the server because the claim this file makes is about the difference between
// the two: the projection changes the response and leaves the record alone.
func newEFSWireTestServer(t *testing.T) (*httptest.Server, emulator.StateManager) {
	t.Helper()
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Unix(efsWireEpoch, 0).UTC())
	// Frozen, not merely started at a known instant: an unfrozen controller advances with
	// real elapsed time, and EpochSeconds renders three decimals, so half a millisecond on
	// a loaded machine would move the value this file asserts on. Freeze then SetTime, in
	// that order — per TimeController.Freeze, the reverse stops the clock tens of
	// nanoseconds past the chosen instant.
	tc.Freeze()
	tc.SetTime(time.Unix(efsWireEpoch, 0).UTC())
	logger := emulator.NewDefaultLogger(0, false)

	p := &emulator.EFSPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(p)

	cfg := emulator.DefaultConfig()
	ts := httptest.NewServer(emulator.NewServer(*cfg, registry, store, state, tc, logger))
	t.Cleanup(ts.Close)
	return ts, state
}

// efsBookkeepingMembers are the four members substrate's EFS records carry that no EFS page
// publishes, spelled as the json tags the records use — those are the names that reached the
// caller. CreatedAt is the sharp one: API_FileSystemDescription publishes CreationTime, so
// the record was answering a near-miss of a Required: Yes member.
var efsBookkeepingMembers = []string{`"AccountID"`, `"Region"`, `"CreatedAt"`, `"ever_tagged"`}

// efsWireOK runs one request, requires a 2xx, and returns the raw body. The raw bytes are
// the point of this file: decoding into a struct is exactly what hides the members it is
// about.
func efsWireOK(t *testing.T, ts *httptest.Server, method, path string, body any) string {
	t.Helper()
	resp := efsRequest(t, ts, method, path, body)
	raw := string(efsBody(t, resp))
	require.Lessf(t, resp.StatusCode, 300, "%s %s answered %d: %s", method, path, resp.StatusCode, raw)
	return raw
}

// TestEFS_ResourceResponsesCarryNoBookkeepingMember is the raw-JSON assertion
// scripts/wire-bookkeeping-projected.txt cites for all three EFS records. It runs over every
// site that answers one, because the projection is only as good as its least projected
// caller and a site added later has to be added here too.
//
// `ever_tagged` is the one member of the four that carries omitempty, so an assertion
// against it passes for free on a record whose flag is false. Only TagResource sets it —
// creating a resource with tags does not — so the file system and the access point are
// tagged through the API before they are described, and every describe below reads a
// record whose flag is true. A mount target cannot be tagged and has no such field.
func TestEFS_ResourceResponsesCarryNoBookkeepingMember(t *testing.T) {
	ts, _ := newEFSWireTestServer(t)

	created := efsWireOK(t, ts, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "wire-token",
		"Tags":          []map[string]string{{"Key": "Name", "Value": "wire-fs"}},
	})
	fsID := efsWireID(t, created, "FileSystemId")

	apCreated := efsWireOK(t, ts, http.MethodPost, "/2015-02-01/access-points", map[string]any{
		"FileSystemId": fsID,
		"Tags":         []map[string]string{{"Key": "Name", "Value": "wire-ap"}},
	})
	apID := efsWireID(t, apCreated, "AccessPointId")

	mtCreated := efsWireOK(t, ts, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsID,
		"SubnetId":     "subnet-0123456789abcdef0",
		"IpAddress":    "10.0.1.42",
	})
	mtID := efsWireID(t, mtCreated, "MountTargetId")

	for _, id := range []string{fsID, apID} {
		efsWireOK(t, ts, http.MethodPost, "/2015-02-01/resource-tags/"+id,
			map[string]any{"Tags": []map[string]string{{"Key": "Env", "Value": "test"}}})
	}

	updated := efsWireOK(t, ts, http.MethodPut, "/2015-02-01/file-systems/"+fsID,
		map[string]any{"ThroughputMode": "elastic"})
	describedOne := efsWireOK(t, ts, http.MethodGet, "/2015-02-01/file-systems/"+fsID, nil)
	describedAll := efsWireOK(t, ts, http.MethodGet, "/2015-02-01/file-systems", nil)
	apDescribedOne := efsWireOK(t, ts, http.MethodGet, "/2015-02-01/access-points/"+apID, nil)
	apDescribed := efsWireOK(t, ts, http.MethodGet, "/2015-02-01/access-points", nil)
	mtDescribedOne := efsWireOK(t, ts, http.MethodGet, "/2015-02-01/mount-targets/"+mtID, nil)
	mtDescribed := efsWireOK(t, ts, http.MethodGet, "/2015-02-01/mount-targets", nil)

	for _, tc := range []struct {
		site   string
		body   string
		anchor string
	}{
		{"CreateFileSystem", created, `"CreationToken":"wire-token"`},
		{"UpdateFileSystem", updated, `"ThroughputMode":"elastic"`},
		{"DescribeFileSystems/{id}", describedOne, `"FileSystemId":"` + fsID + `"`},
		{"DescribeFileSystems", describedAll, `"FileSystemId":"` + fsID + `"`},
		{"CreateAccessPoint", apCreated, `"AccessPointId":"` + apID + `"`},
		{"DescribeAccessPoints/{id}", apDescribedOne, `"AccessPointId":"` + apID + `"`},
		{"DescribeAccessPoints", apDescribed, `"AccessPointId":"` + apID + `"`},
		{"CreateMountTarget", mtCreated, `"MountTargetId":"` + mtID + `"`},
		{"DescribeMountTargets/{id}", mtDescribedOne, `"MountTargetId":"` + mtID + `"`},
		{"DescribeMountTargets", mtDescribed, `"MountTargetId":"` + mtID + `"`},
	} {
		t.Run(tc.site, func(t *testing.T) {
			require.Contains(t, tc.body, tc.anchor,
				"presence anchor: the resource must be in the body for an absence to mean anything")
			for _, member := range efsBookkeepingMembers {
				assert.NotContains(t, tc.body, member,
					"%s answered %s, which no EFS page publishes", tc.site, member)
			}
		})
	}
}

// TestEFS_FileSystemPublishesCreationTimeAndSizeInBytes pins the two Required: Yes members a
// file-system response did not have. Both are timestamps or carry one, so both are read off
// a stopped clock.
func TestEFS_FileSystemPublishesCreationTimeAndSizeInBytes(t *testing.T) {
	ts, _ := newEFSWireTestServer(t)

	created := efsWireOK(t, ts, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "sized",
	})

	var fs struct {
		CreationTime json.RawMessage `json:"CreationTime"`
		SizeInBytes  struct {
			Value           *int64          `json:"Value"`
			Timestamp       json.RawMessage `json:"Timestamp"`
			ValueInIA       *int64          `json:"ValueInIA"`
			ValueInStandard *int64          `json:"ValueInStandard"`
			ValueInArchive  *int64          `json:"ValueInArchive"`
		} `json:"SizeInBytes"`
	}
	require.NoError(t, json.Unmarshal([]byte(created), &fs))

	// "The time that the file system was created, in seconds (since 1970-01-01T00:00:00Z)" —
	// a JSON number, not the RFC3339 string a time.Time marshals to.
	assert.Equal(t, "1709251200.000", string(fs.CreationTime),
		"CreationTime is Required: Yes and published in epoch seconds")
	assert.NotContains(t, string(fs.CreationTime), `"`,
		"a quoted timestamp is what the record's time.Time produced; EpochSeconds is the fix")

	require.NotNil(t, fs.SizeInBytes.Value, "SizeInBytes is Required: Yes on FileSystemDescription")
	assert.Equal(t, int64(0), *fs.SizeInBytes.Value,
		"substrate stores no file data, so the metered size is the published minimum")
	assert.Equal(t, "1709251200.000", string(fs.SizeInBytes.Timestamp),
		"the size was determined when the empty file system was created and nothing has written since")

	// The three storage-class breakdowns are Required: No, and substrate models no storage
	// classes — absent, not zero, so no caller reads a measurement substrate never made.
	assert.Nil(t, fs.SizeInBytes.ValueInIA)
	assert.Nil(t, fs.SizeInBytes.ValueInStandard)
	assert.Nil(t, fs.SizeInBytes.ValueInArchive)
}

// TestEFS_AccessPointAndMountTargetPublishOwnerId pins the published member each of those two
// records was missing. The value is the account the leaked AccountID held, which is what
// makes this the same fix rather than a second one: the field stays in state and reaches the
// caller under the name AWS publishes.
func TestEFS_AccessPointAndMountTargetPublishOwnerId(t *testing.T) {
	ts, _ := newEFSWireTestServer(t)

	created := efsWireOK(t, ts, http.MethodPost, "/2015-02-01/file-systems",
		map[string]any{"CreationToken": "owned"})
	fsID := efsWireID(t, created, "FileSystemId")
	assert.Contains(t, created, `"OwnerId":"123456789012"`,
		"the file system already published OwnerId, from its own OwnerID field")

	ap := efsWireOK(t, ts, http.MethodPost, "/2015-02-01/access-points",
		map[string]any{"FileSystemId": fsID})
	assert.Contains(t, ap, `"OwnerId":"123456789012"`,
		"API_AccessPointDescription publishes OwnerId; substrate published none")

	mt := efsWireOK(t, ts, http.MethodPost, "/2015-02-01/mount-targets",
		map[string]any{"FileSystemId": fsID, "SubnetId": "subnet-0123456789abcdef0"})
	assert.Contains(t, mt, `"OwnerId":"123456789012"`,
		"API_MountTargetDescription publishes OwnerId; substrate published none")
}

// TestEFS_UntaggedFileSystemReportsAnEmptyTagArray keeps Tags a `[]` rather than a `null`.
//
// Tags is Required: Yes on API_FileSystemDescription, so a null is a value AWS never
// answers. This also stands in for the three nil-slice guards the projection replaced: the
// wire slices are built with make, so an empty listing renders `[]` for the same reason.
func TestEFS_UntaggedFileSystemReportsAnEmptyTagArray(t *testing.T) {
	ts, _ := newEFSWireTestServer(t)

	empty := efsWireOK(t, ts, http.MethodGet, "/2015-02-01/file-systems", nil)
	assert.Contains(t, empty, `"FileSystems":[]`, "an empty listing is an array, not a null")

	created := efsWireOK(t, ts, http.MethodPost, "/2015-02-01/file-systems",
		map[string]any{"CreationToken": "untagged"})
	assert.Contains(t, created, `"Tags":[]`, "Tags is Required: Yes, so an untagged file system answers []")
	assert.NotContains(t, created, `"Tags":null`)

	listed := efsWireOK(t, ts, http.MethodGet, "/2015-02-01/file-systems", nil)
	assert.Contains(t, listed, `"Tags":[]`)
}

// TestEFS_ProjectionLeavesTheRecordIntact is the other half of the claim. The three
// bookkeeping fields stay declared and stay persisted: the projection changes the response,
// not the state encoding a recorded run replays from, which is why the baseline lines
// remain and scripts/wire-bookkeeping-projected.txt is what discharges them.
//
// EverTagged is the field with a live reader — TaggingPlugin.scanEFSFileSystems reports it
// on GetResources — so a projection that had quietly dropped it from the record would take
// a cross-service answer with it.
func TestEFS_ProjectionLeavesTheRecordIntact(t *testing.T) {
	ts, state := newEFSWireTestServer(t)

	created := efsWireOK(t, ts, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "persisted",
		"Tags":          []map[string]string{{"Key": "Name", "Value": "persisted"}},
	})
	fsID := efsWireID(t, created, "FileSystemId")
	efsWireOK(t, ts, http.MethodPost, "/2015-02-01/resource-tags/"+fsID,
		map[string]any{"Tags": []map[string]string{{"Key": "Env", "Value": "test"}}})

	keys, err := state.List(t.Context(), "efs", "filesystem:")
	require.NoError(t, err)
	require.Len(t, keys, 1, "one file system was created")

	raw, err := state.Get(t.Context(), "efs", keys[0])
	require.NoError(t, err)
	var record struct {
		AccountID  string    `json:"AccountID"`
		Region     string    `json:"Region"`
		CreatedAt  time.Time `json:"CreatedAt"`
		EverTagged bool      `json:"ever_tagged"`
	}
	require.NoError(t, json.Unmarshal(raw, &record))

	assert.NotEmpty(t, record.AccountID, "the record still scopes itself to an account")
	assert.NotEmpty(t, record.Region, "the record still scopes itself to a Region")
	assert.Equal(t, time.Unix(efsWireEpoch, 0).UTC(), record.CreatedAt.UTC(),
		"CreatedAt stays a time.Time in the record and is converted on projection")
	assert.True(t, record.EverTagged, "TagResource stamped the flag the tagging plugin reads")
}

// efsWireID reads one string member off a create response, so a test can address the
// resource it just made by the id the projection reported.
func efsWireID(t *testing.T, body, member string) string {
	t.Helper()
	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(body), &out))
	raw, ok := out[member]
	require.Truef(t, ok, "no %s in %s", member, body)
	var id string
	require.NoError(t, json.Unmarshal(raw, &id))
	require.NotEmptyf(t, id, "%s is empty in %s", member, body)
	return id
}
