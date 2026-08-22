package emulator_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// What DeleteSnapshot refuses, now that it refuses anything (#710).
//
// The operation deleted the record correctly — it has since #325, whatever the doc comment
// #710 quotes said — and validated nothing at all. A well-formed ID naming no snapshot got
// HTTP 200 and `return true`, which is the answer a caller reads as "deleted": a cleanup
// loop deleting a typoed ID, or the same ID twice, or an ID from another region was told
// each time that it had removed a snapshot.
//
// Three refusals now, in the order real EC2 reports them — malformed, absent, in use.
// The order is what makes the in-use message trustworthy: reporting InUse for a string that
// cannot be a snapshot ID would claim an AMI holds something no AMI could hold.
//
// Provenance: API_DeleteSnapshot.html's Errors section is empty, so none of the three codes
// comes from the operation's own page. InvalidSnapshotID.Malformed and
// InvalidSnapshot.NotFound come from EC2's client-error table via [ec2SnapshotIDKind], which
// predates this change; InvalidSnapshot.InUse comes from the same table, where the entry
// describes the condition ("The snapshot that you are trying to delete is in use by one or
// more AMIs") rather than quoting a wire message. The assertions below check the code, the
// status and that the message names the two resources — not AWS's exact wording, which AWS
// does not publish.

// ec2DeleteSnapshot sends a DeleteSnapshot and reports (status, code, message).
func ec2DeleteSnapshot(t *testing.T, ts *httptest.Server, snapshotID string) (int, string, string) {
	t.Helper()
	params := map[string]string{"Action": "DeleteSnapshot"}
	if snapshotID != "" {
		params["SnapshotId"] = snapshotID
	}
	return ec2ErrorDetail(t, ts, params)
}

// ec2ImageWithSnapshot creates an AMI from an instance and returns it with the snapshot it
// references, read from the AMI's own block device mapping rather than from DescribeSnapshots
// — the in-use rule is about that reference, so the test has to assert on the same link.
func ec2ImageWithSnapshot(t *testing.T, ts *httptest.Server) (imageID, snapshotID string) {
	t.Helper()
	imageID = ec2CreateImageID(t, ts, map[string]string{
		"Action":     "CreateImage",
		"InstanceId": ec2TagTestInstance(t, ts),
		"Name":       "in-use-ami",
	})
	var doc struct {
		Snapshots []string `xml:"imagesSet>item>blockDeviceMapping>item>ebs>snapshotId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "DescribeImages", "ImageId.1": imageID,
	}, &doc)
	require.Len(t, doc.Snapshots, 1)
	require.NotEmpty(t, doc.Snapshots[0])
	return imageID, doc.Snapshots[0]
}

// ec2SnapshotExists reports whether DescribeSnapshots still lists snapshotID.
func ec2SnapshotExists(t *testing.T, ts *httptest.Server, snapshotID string) bool {
	t.Helper()
	var doc struct {
		IDs []string `xml:"snapshotSet>item>snapshotId"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeSnapshots"}, &doc)
	for _, id := range doc.IDs {
		if id == snapshotID {
			return true
		}
	}
	return false
}

// TestEC2_DeleteSnapshot_DeletesAndIsNotIdempotent pins the happy path and the one that
// used to share it.
//
// A second delete of the same ID is a refusal, not a second success. That is the whole
// behavioral change for a consumer whose cleanup is written to be re-runnable: it has to
// tolerate InvalidSnapshot.NotFound now, which is what it has to tolerate against AWS.
func TestEC2_DeleteSnapshot_DeletesAndIsNotIdempotent(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, snapID := ec2SnapshotOfSize(t, ts, 20)
	require.True(t, ec2SnapshotExists(t, ts, snapID))

	status, code, _ := ec2DeleteSnapshot(t, ts, snapID)
	require.Equal(t, http.StatusOK, status, "code=%s", code)
	assert.False(t, ec2SnapshotExists(t, ts, snapID), "the record is gone from DescribeSnapshots")

	status, code, _ = ec2DeleteSnapshot(t, ts, snapID)
	assert.Equal(t, http.StatusBadRequest, status, "the second delete answered 200 before #710")
	assert.Equal(t, "InvalidSnapshot.NotFound", code)
}

// TestEC2_DeleteSnapshot_RefusesAnIDItCannotResolve covers the two ID failures, plus the
// omitted parameter AWS marks Required: Yes.
func TestEC2_DeleteSnapshot_RefusesAnIDItCannotResolve(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	for name, tc := range map[string]struct {
		snapshotID string
		wantCode   string
	}{
		// Well-formed and names nothing — the case the stub answered 200 for.
		"absent":                 {"snap-0123456789abcdef0", "InvalidSnapshot.NotFound"},
		"wrong prefix":           {"vol-0123456789abcdef0", "InvalidSnapshotID.Malformed"},
		"no prefix":              {"0123456789abcdef0", "InvalidSnapshotID.Malformed"},
		"prefix only":            {"snap-", "InvalidSnapshotID.Malformed"},
		"non-hex body":           {"snap-0123456789abcdefg", "InvalidSnapshotID.Malformed"},
		"omitted altogether":     {"", "MissingParameter"},
		"an AMI ID, not a snap":  {"ami-0123456789abcdef0", "InvalidSnapshotID.Malformed"},
		"uppercase is malformed": {"snap-0123456789ABCDEF0", "InvalidSnapshotID.Malformed"},
	} {
		t.Run(name, func(t *testing.T) {
			status, code, _ := ec2DeleteSnapshot(t, ts, tc.snapshotID)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.wantCode, code)
		})
	}
}

// TestEC2_DeleteSnapshot_RefusesOneAnAMIStillUses pins AWS's documented sequence: deregister
// the AMI, then delete the snapshot.
//
// Before #710 the delete succeeded and left the AMI referencing a snapshot that no longer
// existed — a state real EC2 cannot be put into, and one substrate had to grow a fallback
// for in describeImages to keep rendering a volume size at all.
func TestEC2_DeleteSnapshot_RefusesOneAnAMIStillUses(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	imageID, snapID := ec2ImageWithSnapshot(t, ts)

	status, code, message := ec2DeleteSnapshot(t, ts, snapID)
	require.Equal(t, http.StatusBadRequest, status, "this answered 200 before #710")
	assert.Equal(t, "InvalidSnapshot.InUse", code)
	assert.Contains(t, message, snapID, "the message must name the snapshot")
	assert.Contains(t, message, imageID,
		"and the AMI, since deregistering it is the caller's next step")
	assert.True(t, ec2SnapshotExists(t, ts, snapID), "a refused delete removes nothing")

	// AWS: "You must first deregister the AMI before you can delete the snapshot."
	dereg := ec2Request(t, ts, map[string]string{"Action": "DeregisterImage", "ImageId": imageID})
	require.Equal(t, http.StatusOK, dereg.StatusCode)
	dereg.Body.Close() //nolint:errcheck

	status, code, _ = ec2DeleteSnapshot(t, ts, snapID)
	require.Equal(t, http.StatusOK, status, "code=%s", code)
	assert.False(t, ec2SnapshotExists(t, ts, snapID))
}

// TestEC2_DeleteSnapshot_SharedSnapshotNamesTheSameAMIEveryTime pins the tie-break when two
// AMIs reference one snapshot, which #328 made reachable: RegisterImage against an existing
// snapshot shares it with the AMI that minted it.
//
// The refusal message names an image, and [StateManager.List] returns keys in Go map order,
// so without a tie-break the message would vary run to run for identical inputs — the one
// thing an event-sourced emulator cannot do, since replay would produce a different body
// from the recorded one. The rule is the lowest image ID, and this asserts the property
// rather than a fixed string, because the IDs are freshly generated.
//
// Ten deletes rather than one: a map with two entries yields either order often enough that
// a single call would pass a broken implementation about half the time.
func TestEC2_DeleteSnapshot_SharedSnapshotNamesTheSameAMIEveryTime(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	amiA, snapID := ec2ImageWithSnapshot(t, ts)
	amiB := ec2CreateImageID(t, ts, map[string]string{
		"Action": "RegisterImage", "Name": "shares-the-snapshot",
		"BlockDeviceMapping.1.DeviceName":     "/dev/sda1",
		"BlockDeviceMapping.1.Ebs.SnapshotId": snapID,
	})
	require.NotEqual(t, amiA, amiB)
	// Not the min builtin: apigateway_proxy_test.go:144 declares a package-level min(int,
	// int) that shadows it for the whole test package.
	want := amiA
	if amiB < want {
		want = amiB
	}

	for range 10 {
		status, code, message := ec2DeleteSnapshot(t, ts, snapID)
		require.Equal(t, http.StatusBadRequest, status)
		require.Equal(t, "InvalidSnapshot.InUse", code)
		require.Contains(t, message, want,
			"the lowest image ID, so the same inputs give the same body on replay")
	}
}

// TestEC2_DeleteSnapshot_AnUnrelatedAMIDoesNotBlockIt is the negative half of the in-use
// rule: the check must match on the snapshot, not on the existence of any AMI.
//
// Two AMIs exist here and each has its own backing snapshot, so a check that answered
// "some image exists" rather than "this image names this snapshot" would refuse both
// deletions and pass every assertion in the test above.
func TestEC2_DeleteSnapshot_AnUnrelatedAMIDoesNotBlockIt(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, otherSnap := ec2ImageWithSnapshot(t, ts)
	_, freeSnap := ec2SnapshotOfSize(t, ts, 10)
	require.NotEqual(t, otherSnap, freeSnap)

	status, code, _ := ec2DeleteSnapshot(t, ts, freeSnap)
	require.Equal(t, http.StatusOK, status, "code=%s", code)
	assert.False(t, ec2SnapshotExists(t, ts, freeSnap))
	assert.True(t, ec2SnapshotExists(t, ts, otherSnap), "the AMI's own snapshot is untouched")
}
