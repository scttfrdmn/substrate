package emulator_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CreateSnapshots and CopySnapshot (#709).
//
// Both reached the dispatcher's default arm through v0.106.0, so every assertion in this file
// is a fail-before: the request answered InvalidAction / HTTP 400, which is the answer a
// consumer reads as "this endpoint does not speak EC2" rather than "substrate has not
// implemented this yet".
//
// Provenance: neither operation publishes an operation-specific error — both Errors sections
// point only at the common types, and CreateSnapshots has no Examples section at all — so
// every refused row below is either a code from EC2's client-error table
// (SnapshotCopyUnsupported.InterRegion, InvalidSnapshot.NotFound) or substrate's reading of a
// prose rule AWS states without naming a code. The code comments say which for each.

// snapshotInfo is the CreateSnapshots snapshotSet item as a caller receives it.
//
// The state member is read from `state`, **not** `status`: AWS's SnapshotInfo — which is what
// CreateSnapshots returns — names it `state`, while CreateSnapshot's and DescribeSnapshots'
// own responses name the same thing `status`. A test reading `status` here would pass against
// a handler that had reused CreateSnapshot's struct and reported nothing at all.
type snapshotInfo struct {
	SnapshotID  string `xml:"snapshotId"`
	VolumeID    string `xml:"volumeId"`
	VolumeSize  int64  `xml:"volumeSize"`
	State       string `xml:"state"`
	Status      string `xml:"status"`
	StartTime   string `xml:"startTime"`
	Progress    string `xml:"progress"`
	OwnerID     string `xml:"ownerId"`
	Description string `xml:"description"`
	Encrypted   bool   `xml:"encrypted"`
	Tags        []struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	} `xml:"tagSet>item"`
}

// ec2CreateSnapshots sends CreateSnapshots and returns the snapshotSet it answers.
func ec2CreateSnapshots(t *testing.T, ts *httptest.Server, params map[string]string) []snapshotInfo {
	t.Helper()
	full := map[string]string{"Action": "CreateSnapshots"}
	for k, v := range params {
		full[k] = v
	}
	var doc struct {
		Snapshots []snapshotInfo `xml:"snapshotSet>item"`
	}
	ec2FleetXML(t, ts, full, &doc)
	return doc.Snapshots
}

// ec2InstanceWithDataVolume launches an instance and attaches a data volume of dataGiB at
// /dev/sdb, returning the instance ID, the root volume's ID and the data volume's.
//
// The volume is created and attached explicitly rather than declared in the launch's block
// device mapping, so the test knows the data volume's ID without having to find it. The root
// volume's ID is read back from DescribeVolumes, since the launch mints it.
//
// The two sizes are deliberately different from each other and from
// ec2DefaultVolumeSizeGiB where the caller passes anything but 8: a snapshotSet in which
// every volumeSize agreed would not distinguish a handler that snapshotted each volume from
// one that snapshotted the root twice.
func ec2InstanceWithDataVolume(t *testing.T, ts *httptest.Server, dataGiB int) (instanceID, rootVolumeID, dataVolumeID string) {
	t.Helper()
	instanceID = ec2RunInstanceIDs(t, ts, map[string]string{
		"Action":   "RunInstances",
		"ImageId":  "ami-0123456789abcdef0",
		"MinCount": "1",
		"MaxCount": "1",
	})[0]

	dataVolumeID = ec2CreateSizedVolume(t, ts, dataGiB, nil)
	ec2FleetXML(t, ts, map[string]string{
		"Action":     "AttachVolume",
		"VolumeId":   dataVolumeID,
		"InstanceId": instanceID,
		"Device":     "/dev/sdb",
	}, nil)

	for device, volumeID := range ec2InstanceVolumesByDevice(t, ts, instanceID) {
		if device == "/dev/sda1" {
			rootVolumeID = volumeID
		}
	}
	require.NotEmpty(t, rootVolumeID, "the launch must have materialized a root volume")
	return instanceID, rootVolumeID, dataVolumeID
}

// ec2InstanceVolumesByDevice maps each device name on instanceID to the volume attached at
// it, as DescribeVolumes reports them.
func ec2InstanceVolumesByDevice(t *testing.T, ts *httptest.Server, instanceID string) map[string]string {
	t.Helper()
	var doc struct {
		Volumes []struct {
			VolumeID    string `xml:"volumeId"`
			Attachments []struct {
				InstanceID string `xml:"instanceId"`
				Device     string `xml:"device"`
			} `xml:"attachmentSet>item"`
		} `xml:"volumeSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeVolumes"}, &doc)
	byDevice := map[string]string{}
	for _, vol := range doc.Volumes {
		for _, att := range vol.Attachments {
			if att.InstanceID == instanceID {
				byDevice[att.Device] = vol.VolumeID
			}
		}
	}
	return byDevice
}

// TestEC2_CreateSnapshots_SnapshotsEveryAttachedVolume is the operation's fail-before, and
// pins the member name that makes it impossible to reuse CreateSnapshot's response.
//
// The order is asserted, not just the membership. RunInstances appends the root volume to
// state *after* any declared mapping (see ec2LaunchVolumesFor), and StateManager.List returns
// keys in Go map order, so a handler that rendered the scan's order would answer differently
// on different runs and a consumer reading snapshotSet[0] would pass or fail by iteration
// order. Sorting on the device name is what makes the answer a fact about the request.
func TestEC2_CreateSnapshots_SnapshotsEveryAttachedVolume(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	instanceID, rootVolumeID, dataVolumeID := ec2InstanceWithDataVolume(t, ts, 20)

	got := ec2CreateSnapshots(t, ts, map[string]string{
		"InstanceSpecification.InstanceId": instanceID,
		"Description":                      "nightly",
	})
	require.Len(t, got, 2, "one snapshot per attached volume")

	assert.Equal(t, rootVolumeID, got[0].VolumeID, "/dev/sda1 sorts before /dev/sdb")
	assert.Equal(t, int64(8), got[0].VolumeSize, "the root volume's own size")
	assert.Equal(t, dataVolumeID, got[1].VolumeID)
	assert.Equal(t, int64(20), got[1].VolumeSize, "the data volume's, not the root's")

	for i, snap := range got {
		assert.Contains(t, snap.SnapshotID, "snap-", "item %d", i)
		assert.Equal(t, "completed", snap.State,
			"SnapshotInfo names the state member `state`, not `status`")
		assert.Empty(t, snap.Status, "a `status` element here would be the wrong member name")
		assert.Equal(t, "100%", snap.Progress, "item %d is complete, so progress says so (#715)", i)
		assert.NotEmpty(t, snap.StartTime, "item %d", i)
		assert.NotEmpty(t, snap.OwnerID, "item %d", i)
		assert.Equal(t, "nightly", snap.Description, "the description applies to every snapshot")
	}

	// And DescribeSnapshots reports both, so the two operations cannot drift about what was
	// written — which is the whole point of writing through one state key helper.
	described := ec2DescribeSnapshots(t, ts, map[string]string{
		"SnapshotId.1": got[0].SnapshotID, "SnapshotId.2": got[1].SnapshotID,
	})
	require.Len(t, described, 2)
	for _, snap := range described {
		assert.Equal(t, "completed", snap.State, "DescribeSnapshots names it `status`")
	}
}

// TestEC2_CreateSnapshots_ExcludesWhatTheRequestExcludes covers the two exclusion parameters
// separately, because they are not interchangeable: AWS refuses excluding the root volume by
// ID and directs a caller to ExcludeBootVolume instead, so a handler that treated them as one
// list would accept a request AWS rejects.
func TestEC2_CreateSnapshots_ExcludesWhatTheRequestExcludes(t *testing.T) {
	t.Parallel()

	t.Run("ExcludeBootVolume leaves the data volume", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		instanceID, _, dataVolumeID := ec2InstanceWithDataVolume(t, ts, 30)

		got := ec2CreateSnapshots(t, ts, map[string]string{
			"InstanceSpecification.InstanceId":        instanceID,
			"InstanceSpecification.ExcludeBootVolume": "true",
		})
		require.Len(t, got, 1)
		assert.Equal(t, dataVolumeID, got[0].VolumeID)
		assert.Equal(t, int64(30), got[0].VolumeSize)
	})

	t.Run("ExcludeDataVolumeId leaves the root volume", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		instanceID, rootVolumeID, dataVolumeID := ec2InstanceWithDataVolume(t, ts, 30)

		got := ec2CreateSnapshots(t, ts, map[string]string{
			"InstanceSpecification.InstanceId":            instanceID,
			"InstanceSpecification.ExcludeDataVolumeId.1": dataVolumeID,
		})
		require.Len(t, got, 1)
		assert.Equal(t, rootVolumeID, got[0].VolumeID)
	})

	t.Run("a volume ID that is not attached excludes nothing", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		instanceID, _, _ := ec2InstanceWithDataVolume(t, ts, 30)

		// Not a refusal: the parameter asks for volumes to be left out, and a volume that
		// was never in is left out. AWS documents no error for it, and refusing would
		// break a caller excluding a volume they had already detached.
		got := ec2CreateSnapshots(t, ts, map[string]string{
			"InstanceSpecification.InstanceId":            instanceID,
			"InstanceSpecification.ExcludeDataVolumeId.1": "vol-0123456789abcdef0",
		})
		assert.Len(t, got, 2)
	})
}

// TestEC2_CreateSnapshots_RefusesTheRootVolumeByID pins AWS's one explicit refusal on this
// operation: "If you specify the ID of the root volume, the request fails. To exclude the
// root volume, use ExcludeBootVolume."
//
// The message names ExcludeBootVolume for the same reason AWS's sentence does — it is the
// only thing that tells the caller what to send instead. The code is substrate's; AWS says
// only that the request fails.
func TestEC2_CreateSnapshots_RefusesTheRootVolumeByID(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	instanceID, rootVolumeID, _ := ec2InstanceWithDataVolume(t, ts, 30)

	before := ec2DescribeSnapshots(t, ts, map[string]string{})
	status, code, message := ec2ErrorDetail(t, ts, map[string]string{
		"Action":                           "CreateSnapshots",
		"InstanceSpecification.InstanceId": instanceID,
		"InstanceSpecification.ExcludeDataVolumeId.1": rootVolumeID,
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValue", code)
	assert.Contains(t, message, rootVolumeID)
	assert.Contains(t, message, "ExcludeBootVolume", "the message must name the parameter to use")
	assert.Len(t, ec2DescribeSnapshots(t, ts, map[string]string{}), len(before),
		"a refused request snapshots nothing")
}

// TestEC2_CreateSnapshots_RefusesABadRequest is the parameter table.
//
// The instance is resolved against state rather than assumed, so an absent one is
// InvalidInstanceID.NotFound and not an empty snapshotSet — the distinction matters because
// an empty set is a real answer for an instance with no volumes left to snapshot, and a
// consumer's wait loop turns on "not yet" versus "never".
func TestEC2_CreateSnapshots_RefusesABadRequest(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	instanceID, _, _ := ec2InstanceWithDataVolume(t, ts, 30)

	manyExclusions := map[string]string{"InstanceSpecification.InstanceId": instanceID}
	for i := 1; i <= 41; i++ {
		manyExclusions[fmt.Sprintf("InstanceSpecification.ExcludeDataVolumeId.%d", i)] =
			fmt.Sprintf("vol-%017d", i)
	}

	for name, tc := range map[string]struct {
		params   map[string]string
		wantCode string
	}{
		"no instance": {
			map[string]string{}, "MissingParameter",
		},
		"malformed instance": {
			map[string]string{"InstanceSpecification.InstanceId": "not-an-instance"},
			"InvalidInstanceID.Malformed",
		},
		"absent instance": {
			map[string]string{"InstanceSpecification.InstanceId": "i-0123456789abcdef0"},
			"InvalidInstanceID.NotFound",
		},
		// AWS lists exactly one valid value for CopyTagsFromSource, so a request naming
		// another is refused rather than treated as "do not copy": a caller who meant to
		// copy tags and misspelled the source would otherwise get a silent no-op.
		"unknown CopyTagsFromSource": {
			map[string]string{
				"InstanceSpecification.InstanceId": instanceID,
				"CopyTagsFromSource":               "instance",
			},
			"InvalidParameterValue",
		},
		// "You can specify up to 40 volume IDs per request."
		"forty-one exclusions": {manyExclusions, "InvalidParameterValue"},
	} {
		t.Run(name, func(t *testing.T) {
			params := map[string]string{"Action": "CreateSnapshots"}
			for k, v := range tc.params {
				params[k] = v
			}
			status, code, _ := ec2ErrorDetail(t, ts, params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.wantCode, code)
		})
	}
}

// TestEC2_CreateSnapshots_CopyTagsFromSourceCopiesEachVolumesOwnTags pins the scope of the
// copy: each snapshot inherits *its own* source volume's tags, not the union of every
// volume's, which a handler collecting tags once before the loop would produce.
//
// The colliding key is the other half. AWS settles neither precedence nor ordering when a
// TagSpecification names a key a copied volume tag already carries, so substrate's choice —
// the request wins, and the result is ordered volume-tags-then-new-keys — is pinned here
// rather than left to whichever the implementation happened to do.
func TestEC2_CreateSnapshots_CopyTagsFromSourceCopiesEachVolumesOwnTags(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	instanceID, rootVolumeID, dataVolumeID := ec2InstanceWithDataVolume(t, ts, 40)

	for volumeID, role := range map[string]string{rootVolumeID: "root", dataVolumeID: "data"} {
		ec2FleetXML(t, ts, map[string]string{
			"Action":       "CreateTags",
			"ResourceId.1": volumeID,
			"Tag.1.Key":    "Role",
			"Tag.1.Value":  role,
			"Tag.2.Key":    "Keep",
			"Tag.2.Value":  "yes",
		}, nil)
	}

	got := ec2CreateSnapshots(t, ts, map[string]string{
		"InstanceSpecification.InstanceId": instanceID,
		"CopyTagsFromSource":               "volume",
		// Collides with the volumes' own Keep tag, and introduces one they do not have.
		"TagSpecification.1.ResourceType": "snapshot",
		"TagSpecification.1.Tag.1.Key":    "Keep",
		"TagSpecification.1.Tag.1.Value":  "no",
		"TagSpecification.1.Tag.2.Key":    "Batch",
		"TagSpecification.1.Tag.2.Value":  "b1",
	})
	require.Len(t, got, 2)

	byRole := map[string]map[string]string{}
	for _, snap := range got {
		tags := map[string]string{}
		for _, tag := range snap.Tags {
			tags[tag.Key] = tag.Value
		}
		require.Contains(t, tags, "Role", "each snapshot carries its own volume's Role tag")
		byRole[tags["Role"]] = tags
	}
	require.Len(t, byRole, 2, "the two snapshots must not share one Role value")
	for role, tags := range byRole {
		assert.Equal(t, "no", tags["Keep"], "the request's value wins the collision (%s)", role)
		assert.Equal(t, "b1", tags["Batch"], "a key only the request names is added (%s)", role)
		assert.Len(t, tags, 3, "no duplicate Keep entry (%s)", role)
	}
}

// TestEC2_CopySnapshot_CopiesWithinTheRegion is CopySnapshot's fail-before, and pins the
// three answers a caller can only get from a real copy.
//
// A same-region copy is the case AWS's Example 1 documents — "you can copy it within that
// Region" — and it is the only case a single-region emulator can serve, which is what makes
// the operation coherent here rather than a stub.
func TestEC2_CopySnapshot_CopiesWithinTheRegion(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	sourceVolume, sourceSnap := ec2SnapshotOfSize(t, ts, 55)

	var copied struct {
		SnapshotID string `xml:"snapshotId"`
		Tags       []struct {
			Key   string `xml:"key"`
			Value string `xml:"value"`
		} `xml:"tagSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action":                          "CopySnapshot",
		"SourceRegion":                    "us-east-1",
		"SourceSnapshotId":                sourceSnap,
		"Description":                     "My_snapshot",
		"TagSpecification.1.ResourceType": "snapshot",
		"TagSpecification.1.Tag.1.Key":    "Origin",
		"TagSpecification.1.Tag.1.Value":  "copy",
	}, &copied)

	require.Contains(t, copied.SnapshotID, "snap-")
	assert.NotEqual(t, sourceSnap, copied.SnapshotID, "a copy is a new snapshot")
	require.Len(t, copied.Tags, 1, "tagSet is one of the three documented response elements")
	assert.Equal(t, "Origin", copied.Tags[0].Key)

	described := ec2DescribeSnapshots(t, ts, map[string]string{"SnapshotId.1": copied.SnapshotID})
	require.Len(t, described, 1)
	assert.Equal(t, int64(55), described[0].VolumeSize, "the copy is the source's size")
	assert.Equal(t, "completed", described[0].State)
	assert.Equal(t, "My_snapshot", described[0].Description)

	// AWS: "Snapshots copies have an arbitrary source volume ID. Do not use this volume ID
	// for any purpose." So the copy must not point at the source's volume — a caller given a
	// reference that resolves would be invited to do exactly what that sentence forbids.
	assert.NotEqual(t, sourceVolume, described[0].VolumeID,
		"the copy's volumeId is arbitrary, not the source's")
	assert.NotEmpty(t, described[0].VolumeID, "AWS always renders the member")

	// And it names no volume, which is what makes AWS's warning self-enforcing.
	//
	// The assertion is an empty volumeSet rather than InvalidVolume.NotFound because
	// substrate's DescribeVolumes selects by ID without resolving one — an unknown VolumeId.N
	// narrows the answer to nothing instead of being refused. That is a separate gap from
	// #709 and is left standing rather than widened into this PR; it is recorded so a future
	// reader does not mistake the weaker assertion for the stronger claim.
	var volumes struct {
		Volumes []struct {
			VolumeID string `xml:"volumeId"`
		} `xml:"volumeSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "DescribeVolumes", "VolumeId.1": described[0].VolumeID,
	}, &volumes)
	assert.Empty(t, volumes.Volumes, "the copy's arbitrary volume ID resolves to nothing")
}

// TestEC2_CopySnapshot_EncryptionFollowsTheDocumentedRules covers the three sentences AWS
// writes about encryption on this operation, none of which is published with an error code.
func TestEC2_CopySnapshot_EncryptionFollowsTheDocumentedRules(t *testing.T) {
	t.Parallel()

	t.Run("a copy of an encrypted snapshot is encrypted", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		volID := ec2CreateSizedVolume(t, ts, 25, map[string]string{"Encrypted": "true"})
		source := ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volID})
		require.True(t, source.Encrypted)

		// "Copies of encrypted snapshots are encrypted, even if you omit this parameter."
		var copied struct {
			SnapshotID string `xml:"snapshotId"`
		}
		ec2FleetXML(t, ts, map[string]string{
			"Action": "CopySnapshot", "SourceRegion": "us-east-1",
			"SourceSnapshotId": source.SnapshotID,
		}, &copied)
		described := ec2DescribeSnapshots(t, ts,
			map[string]string{"SnapshotId.1": copied.SnapshotID})
		require.Len(t, described, 1)
		assert.True(t, described[0].Encrypted)
	})

	t.Run("Encrypted=true encrypts a copy of an unencrypted snapshot", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		_, source := ec2SnapshotOfSize(t, ts, 25)

		var copied struct {
			SnapshotID string `xml:"snapshotId"`
		}
		ec2FleetXML(t, ts, map[string]string{
			"Action": "CopySnapshot", "SourceRegion": "us-east-1",
			"SourceSnapshotId": source, "Encrypted": "true",
		}, &copied)
		described := ec2DescribeSnapshots(t, ts,
			map[string]string{"SnapshotId.1": copied.SnapshotID})
		require.Len(t, described, 1)
		assert.True(t, described[0].Encrypted, "the parameter is what encrypts it")
	})

	t.Run("Encrypted=false is refused", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		_, source := ec2SnapshotOfSize(t, ts, 25)

		// "You cannot set this parameter to false." Refusing rather than ignoring is
		// substrate's reading: a caller who wrote false meant an unencrypted copy, and
		// silently producing an encrypted one would be an observation they did not ask for.
		status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
			"Action": "CopySnapshot", "SourceRegion": "us-east-1",
			"SourceSnapshotId": source, "Encrypted": "false",
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValue", code)
	})

	t.Run("KmsKeyId without encryption is refused", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		_, source := ec2SnapshotOfSize(t, ts, 25)

		// "If KmsKeyId is specified, the encrypted state must be true."
		status, code, message := ec2ErrorDetail(t, ts, map[string]string{
			"Action": "CopySnapshot", "SourceRegion": "us-east-1",
			"SourceSnapshotId": source,
			"KmsKeyId":         "alias/ExampleAlias",
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValue", code)
		assert.Contains(t, message, "KmsKeyId")
	})

	t.Run("KmsKeyId with an encrypted source is accepted", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		volID := ec2CreateSizedVolume(t, ts, 25, map[string]string{"Encrypted": "true"})
		source := ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volID})

		// The source's own encryption satisfies "the encrypted state must be true", so this
		// must not be refused — the rule is about the resulting state, not the parameter.
		ec2FleetXML(t, ts, map[string]string{
			"Action": "CopySnapshot", "SourceRegion": "us-east-1",
			"SourceSnapshotId": source.SnapshotID,
			"KmsKeyId":         "alias/ExampleAlias",
		}, nil)
	})
}

// TestEC2_CopySnapshot_RefusesABadRequest is the parameter table.
//
// The cross-region row is the one that decides what kind of emulator this is.
// SnapshotCopyUnsupported.InterRegion is a documented code whose published description —
// "Inter-region snapshot copy is not supported for this AWS Region" — is exactly substrate's
// position, so a consumer whose IaC copies across regions reads a refusal real EC2 also
// issues in regions where the feature is unavailable, rather than a fabricated success.
func TestEC2_CopySnapshot_RefusesABadRequest(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, source := ec2SnapshotOfSize(t, ts, 25)

	for name, tc := range map[string]struct {
		params   map[string]string
		wantCode string
	}{
		"no source region": {
			map[string]string{"SourceSnapshotId": source}, "MissingParameter",
		},
		"no source snapshot": {
			map[string]string{"SourceRegion": "us-east-1"}, "MissingParameter",
		},
		"another region": {
			map[string]string{"SourceRegion": "us-west-2", "SourceSnapshotId": source},
			"SnapshotCopyUnsupported.InterRegion",
		},
		"malformed source": {
			map[string]string{"SourceRegion": "us-east-1", "SourceSnapshotId": "vol-0123456789abcdef0"},
			"InvalidSnapshotID.Malformed",
		},
		"absent source": {
			map[string]string{"SourceRegion": "us-east-1", "SourceSnapshotId": "snap-0123456789abcdef0"},
			"InvalidSnapshot.NotFound",
		},
	} {
		t.Run(name, func(t *testing.T) {
			params := map[string]string{"Action": "CopySnapshot"}
			for k, v := range tc.params {
				params[k] = v
			}
			before := ec2DescribeSnapshots(t, ts, map[string]string{})
			status, code, _ := ec2ErrorDetail(t, ts, params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.wantCode, code)
			assert.Len(t, ec2DescribeSnapshots(t, ts, map[string]string{}), len(before),
				"a refused copy writes no snapshot")
		})
	}
}
