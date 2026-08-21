package emulator_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// describedSnapshot is the subset of a DescribeSnapshots item these tests read.
type describedSnapshot struct {
	SnapshotID  string `xml:"snapshotId"`
	VolumeID    string `xml:"volumeId"`
	VolumeSize  int64  `xml:"volumeSize"`
	State       string `xml:"status"`
	StartTime   string `xml:"startTime"`
	Encrypted   bool   `xml:"encrypted"`
	Description string `xml:"description"`
	OwnerID     string `xml:"ownerId"`
	Tags        []struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	} `xml:"tagSet>item"`
}

// ec2DescribeSnapshots sends DescribeSnapshots with params and returns the snapshots.
func ec2DescribeSnapshots(t *testing.T, ts *httptest.Server, params map[string]string) []describedSnapshot {
	t.Helper()
	full := map[string]string{"Action": "DescribeSnapshots"}
	for k, v := range params {
		full[k] = v
	}
	var doc struct {
		Snapshots []describedSnapshot `xml:"snapshotSet>item"`
	}
	ec2FleetXML(t, ts, full, &doc)
	return doc.Snapshots
}

// ec2SnapshotIDsFor is [ec2DescribeSnapshots] reduced to the IDs, which is what a filter
// test asserts on: the identity of the answer, not its contents.
func ec2SnapshotIDsFor(t *testing.T, ts *httptest.Server, params map[string]string) []string {
	t.Helper()
	snaps := ec2DescribeSnapshots(t, ts, params)
	ids := make([]string, 0, len(snaps))
	for _, s := range snaps {
		ids = append(ids, s.SnapshotID)
	}
	return ids
}

// ec2Filter builds the Filter.1 parameters for one name and its values.
func ec2Filter(name string, values ...string) map[string]string {
	params := map[string]string{"Filter.1.Name": name}
	for i, v := range values {
		params["Filter.1.Value."+strconv.Itoa(i+1)] = v
	}
	return params
}

// ec2SeedTaggedSnapshots creates one AMI per name — each of which materializes a backing
// EBS snapshot, tagged Scope=<name> and described "Created by CreateImage for <name>" — and
// returns the snapshot IDs keyed by name.
//
// Both snapshots come from one instance's root volume, so both report that volume's ID and
// its 8 GiB size — since #689 those are read from the volume record rather than being the
// literal 8 CreateImage used to write. The volume-id and volume-size rows below therefore
// select on a real value; TestEC2_CreateSnapshot_ReportsTheSourceVolume is where a size
// other than the default is asserted, so neither passes by coinciding with a constant.
func ec2SeedTaggedSnapshots(t *testing.T, ts *httptest.Server, names ...string) map[string]string {
	t.Helper()
	instID := ec2TagTestInstance(t, ts)
	for _, n := range names {
		resp := ec2Request(t, ts, map[string]string{
			"Action":                          "CreateImage",
			"InstanceId":                      instID,
			"Name":                            n,
			"TagSpecification.1.ResourceType": "snapshot",
			"TagSpecification.1.Tag.1.Key":    "Scope",
			"TagSpecification.1.Tag.1.Value":  n,
		})
		require.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close() //nolint:errcheck
	}
	out := make(map[string]string, len(names))
	for _, s := range ec2DescribeSnapshots(t, ts, nil) {
		for _, tag := range s.Tags {
			if tag.Key == "Scope" {
				out[tag.Value] = s.SnapshotID
			}
		}
	}
	require.Len(t, out, len(names), "each CreateImage should leave one tagged snapshot")
	return out
}

// TestEC2_DescribeSnapshots_EvaluatedFiltersNarrowTheAnswer covers the ten filter names
// #685 taught describeSnapshots to evaluate. Before it, snapshot-id was the only one that
// did anything and the other thirteen were silently ignored — so every case below whose
// want is narrower than "both" returned both snapshots, and a caller had no way to tell an
// unfiltered answer from a filtered one.
func TestEC2_DescribeSnapshots_EvaluatedFiltersNarrowTheAnswer(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	snaps := ec2SeedTaggedSnapshots(t, ts, "alpha", "beta")
	alpha, beta := snaps["alpha"], snaps["beta"]

	all := ec2DescribeSnapshots(t, ts, nil)
	require.Len(t, all, 2)
	owner := all[0].OwnerID
	require.NotEmpty(t, owner, "ownerId is what the owner-id filter compares against")
	require.NotEmpty(t, all[0].VolumeID,
		"since #689 a CreateImage snapshot records the root volume it was taken from")

	tests := []struct {
		name   string
		filter map[string]string
		want   []string
	}{
		{"description selects one", ec2Filter("description", "Created by CreateImage for alpha"), []string{alpha}},
		{"description values OR", ec2Filter("description",
			"Created by CreateImage for alpha", "Created by CreateImage for beta"), []string{alpha, beta}},
		{"description miss", ec2Filter("description", "no such description"), nil},
		{"encrypted false", ec2Filter("encrypted", "false"), []string{alpha, beta}},
		{"encrypted true", ec2Filter("encrypted", "true"), nil},
		{"owner-id self", ec2Filter("owner-id", owner), []string{alpha, beta}},
		{"owner-id another account", ec2Filter("owner-id", "999999999999"), nil},
		{"snapshot-id selects one", ec2Filter("snapshot-id", alpha), []string{alpha}},
		{"start-time miss", ec2Filter("start-time", "2000-01-01T00:00:00Z"), nil},
		{"status completed", ec2Filter("status", "completed"), []string{alpha, beta}},
		{"status pending", ec2Filter("status", "pending"), nil},
		{"tag-key present", ec2Filter("tag-key", "Scope"), []string{alpha, beta}},
		{"tag-key absent", ec2Filter("tag-key", "Absent"), nil},
		{"tag value selects one", ec2Filter("tag:Scope", "alpha"), []string{alpha}},
		{"tag values OR", ec2Filter("tag:Scope", "alpha", "beta"), []string{alpha, beta}},
		{"tag key absent", ec2Filter("tag:Absent", "alpha"), nil},
		{"volume-size matches", ec2Filter("volume-size", "8"), []string{alpha, beta}},
		{"volume-size miss", ec2Filter("volume-size", "100"), nil},
		{"volume-id selects the shared source volume",
			ec2Filter("volume-id", all[0].VolumeID), []string{alpha, beta}},
		{"volume-id another volume", ec2Filter("volume-id", "vol-0123456789abcdef0"), nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.ElementsMatch(t, tc.want, ec2SnapshotIDsFor(t, ts, tc.filter))
		})
	}
}

// TestEC2_DescribeSnapshots_StartTimeMatchesItsOwnSnapshot pins the start-time filter
// against a value taken from the response, since the timestamp is minted by the simulated
// clock and cannot be written into a table. The negative case lives in the table above.
func TestEC2_DescribeSnapshots_StartTimeMatchesItsOwnSnapshot(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ec2SeedTaggedSnapshots(t, ts, "alpha")

	all := ec2DescribeSnapshots(t, ts, nil)
	require.Len(t, all, 1)
	require.NotEmpty(t, all[0].StartTime)

	assert.Equal(t, []string{all[0].SnapshotID},
		ec2SnapshotIDsFor(t, ts, ec2Filter("start-time", all[0].StartTime)),
		"a snapshot must match the startTime the same response reported")
}

// TestEC2_DescribeSnapshots_FiltersAND pins AWS's documented composition rule: separate
// filters AND while values within one OR.
func TestEC2_DescribeSnapshots_FiltersAND(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	snaps := ec2SeedTaggedSnapshots(t, ts, "alpha", "beta")

	both := map[string]string{
		"Filter.1.Name": "tag:Scope", "Filter.1.Value.1": "alpha",
		"Filter.2.Name": "status", "Filter.2.Value.1": "completed",
	}
	assert.Equal(t, []string{snaps["alpha"]}, ec2SnapshotIDsFor(t, ts, both))

	conflicting := map[string]string{
		"Filter.1.Name": "tag:Scope", "Filter.1.Value.1": "alpha",
		"Filter.2.Name": "status", "Filter.2.Value.1": "pending",
	}
	assert.Empty(t, ec2SnapshotIDsFor(t, ts, conflicting),
		"two filters AND, so a snapshot satisfying only one is excluded")
}

// TestEC2_DescribeSnapshots_InertFiltersReturnEverything pins the four names AWS documents
// that substrate accepts and cannot answer. They constrain nothing rather than being
// refused, because refusing a filter real EC2 accepts would be a false deny — and rather
// than matching nothing, because an empty answer is indistinguishable from "no such
// snapshot" and would hang a caller's wait loop. docs/services.md lists them by name.
func TestEC2_DescribeSnapshots_InertFiltersReturnEverything(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	snaps := ec2SeedTaggedSnapshots(t, ts, "alpha", "beta")
	want := []string{snaps["alpha"], snaps["beta"]}

	for name, value := range map[string]string{
		"owner-alias":   "amazon",
		"progress":      "100%",
		"storage-tier":  "standard",
		"transfer-type": "standard",
	} {
		t.Run(name, func(t *testing.T) {
			assert.ElementsMatch(t, want, ec2SnapshotIDsFor(t, ts, ec2Filter(name, value)),
				"an inert filter constrains nothing, so every snapshot is still returned")
		})
	}
}

// TestEC2_DescribeSnapshots_AccountScopes covers Owner.N and RestorableBy.N, which sit
// outside Filter.N and outside the ID list and were both ignored before #685.
//
// Substrate is single-account, so the honest answer to "snapshots owned by amazon" is none:
// answering with the account's own snapshots would claim they are public.
func TestEC2_DescribeSnapshots_AccountScopes(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	snaps := ec2SeedTaggedSnapshots(t, ts, "alpha", "beta")
	both := []string{snaps["alpha"], snaps["beta"]}

	all := ec2DescribeSnapshots(t, ts, nil)
	require.Len(t, all, 2)
	owner := all[0].OwnerID

	for _, param := range []string{"Owner", "RestorableBy"} {
		t.Run(param, func(t *testing.T) {
			tests := []struct {
				name   string
				params map[string]string
				want   []string
			}{
				{"absent matches everything", nil, both},
				{"self", map[string]string{param + ".1": "self"}, both},
				{"own account ID", map[string]string{param + ".1": owner}, both},
				{"another account", map[string]string{param + ".1": "999999999999"}, nil},
				{"amazon", map[string]string{param + ".1": "amazon"}, nil},
				{"values OR", map[string]string{param + ".1": "amazon", param + ".2": "self"}, both},
			}
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					assert.ElementsMatch(t, tc.want, ec2SnapshotIDsFor(t, ts, tc.params))
				})
			}
		})
	}
}

// TestEC2_DescribeSnapshots_AFilterDoesNotUnresolveANamedID pins the ordering
// [ec2IDFilter.unresolved] documents: a snapshot excluded by a filter still counts as
// resolved, so naming an existing snapshot and filtering it out is an empty 200 rather than
// InvalidSnapshot.NotFound. The two are very different answers to a consumer — one means
// "not yet", the other means "never".
func TestEC2_DescribeSnapshots_AFilterDoesNotUnresolveANamedID(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	snaps := ec2SeedTaggedSnapshots(t, ts, "alpha")

	assert.Empty(t, ec2SnapshotIDsFor(t, ts, map[string]string{
		"SnapshotId.1":  snaps["alpha"],
		"Filter.1.Name": "status", "Filter.1.Value.1": "pending",
	}), "an existing snapshot a filter excluded is an empty answer, not a not-found error")

	status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action":        "DescribeSnapshots",
		"SnapshotId.1":  "snap-0000000000000dead",
		"Filter.1.Name": "status", "Filter.1.Value.1": "completed",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidSnapshot.NotFound", code,
		"a snapshot that does not exist is still not found, filter or no filter")
}
