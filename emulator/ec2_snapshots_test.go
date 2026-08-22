package emulator_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #689's surface: a snapshot's size is the source volume's rather than the literal 8, and a
// block device mapping naming a snapshot that does not exist is refused.
//
// Every assertion below is at HTTP level, because both halves of the defect were observable
// only through a response: EC2Snapshot.VolumeSize was 8 at its single producer, so
// DescribeSnapshots reported 8 for a snapshot of a 100 GiB volume, and a mapping naming
// snap-0123456789abcdef0 — a snapshot no account holds — produced an 8 GiB volume a consumer
// then asserted against.

// createdSnapshot is the CreateSnapshot response as a caller receives it.
type createdSnapshot struct {
	SnapshotID  string `xml:"snapshotId"`
	VolumeID    string `xml:"volumeId"`
	Status      string `xml:"status"`
	StartTime   string `xml:"startTime"`
	Progress    string `xml:"progress"`
	OwnerID     string `xml:"ownerId"`
	VolumeSize  int64  `xml:"volumeSize"`
	Description string `xml:"description"`
	Encrypted   bool   `xml:"encrypted"`
	Tags        []struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	} `xml:"tagSet>item"`
}

// ec2CreateSizedVolume creates a volume of sizeGiB and returns its ID.
func ec2CreateSizedVolume(t *testing.T, ts *httptest.Server, sizeGiB int, extra map[string]string) string {
	t.Helper()
	params := map[string]string{
		"Action":           "CreateVolume",
		"AvailabilityZone": "us-east-1a",
		"Size":             strconv.Itoa(sizeGiB),
	}
	for k, v := range extra {
		params[k] = v
	}
	var doc struct {
		VolumeID string `xml:"volumeId"`
		Size     int    `xml:"size"`
	}
	ec2FleetXML(t, ts, params, &doc)
	require.NotEmpty(t, doc.VolumeID)
	require.Equal(t, sizeGiB, doc.Size)
	return doc.VolumeID
}

// ec2CreateSnapshot sends CreateSnapshot and returns the response it answers.
func ec2CreateSnapshot(t *testing.T, ts *httptest.Server, params map[string]string) createdSnapshot {
	t.Helper()
	full := map[string]string{"Action": "CreateSnapshot"}
	for k, v := range params {
		full[k] = v
	}
	var snap createdSnapshot
	ec2FleetXML(t, ts, full, &snap)
	return snap
}

// ec2SnapshotOfSize creates a volume of sizeGiB, snapshots it, and returns both IDs.
//
// It is what every mapping test below needs: a snapshot whose size is *not* 8, so that a
// volume reported as 8 GiB is a failure rather than a coincidence.
func ec2SnapshotOfSize(t *testing.T, ts *httptest.Server, sizeGiB int) (volumeID, snapshotID string) {
	t.Helper()
	volumeID = ec2CreateSizedVolume(t, ts, sizeGiB, nil)
	snap := ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volumeID})
	require.Equal(t, int64(sizeGiB), snap.VolumeSize)
	return volumeID, snap.SnapshotID
}

// TestEC2_CreateSnapshot_ReportsTheSourceVolume is the operation's own fail-before: before
// #689 CreateSnapshot reached the dispatcher's default arm and answered InvalidAction/400,
// so the only snapshots substrate held were CreateImage's, all of them 8 GiB.
//
// The response members are asserted individually rather than as a body substring, because
// the one that matters is volumeSize and 8 appears in an unrelated element of a launch
// response. Element names come from the CreateSnapshot reference: the state member is
// `status`, not `state`.
func TestEC2_CreateSnapshot_ReportsTheSourceVolume(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	volID := ec2CreateSizedVolume(t, ts, 100, map[string]string{"Encrypted": "true"})

	snap := ec2CreateSnapshot(t, ts, map[string]string{
		"VolumeId":                        volID,
		"Description":                     "Daily Backup",
		"TagSpecification.1.ResourceType": "snapshot",
		"TagSpecification.1.Tag.1.Key":    "Name",
		"TagSpecification.1.Tag.1.Value":  "nightly",
	})

	assert.Contains(t, snap.SnapshotID, "snap-")
	assert.Equal(t, volID, snap.VolumeID)
	assert.Equal(t, int64(100), snap.VolumeSize, "the size is the source volume's, not 8")
	assert.Equal(t, "completed", snap.Status)
	assert.Equal(t, "100%", snap.Progress,
		"with no seed in place a snapshot is born complete, and progress says so (#715)")
	assert.NotEmpty(t, snap.StartTime)
	assert.NotEmpty(t, snap.OwnerID)
	assert.Equal(t, "Daily Backup", snap.Description)
	assert.True(t, snap.Encrypted, "a snapshot of an encrypted volume is encrypted")
	require.Len(t, snap.Tags, 1)
	assert.Equal(t, "Name", snap.Tags[0].Key)
	assert.Equal(t, "nightly", snap.Tags[0].Value)

	// And DescribeSnapshots reports the same record, so the two operations cannot drift.
	described := ec2DescribeSnapshots(t, ts, map[string]string{"SnapshotId.1": snap.SnapshotID})
	require.Len(t, described, 1)
	assert.Equal(t, snap.VolumeID, described[0].VolumeID)
	assert.Equal(t, int64(100), described[0].VolumeSize)
	assert.Equal(t, "Daily Backup", described[0].Description)
	assert.True(t, described[0].Encrypted)
	require.Len(t, described[0].Tags, 1)
	assert.Equal(t, "nightly", described[0].Tags[0].Value)

	// The volume-size filter #685 added now has something other than 8 to select on.
	assert.Equal(t, []string{snap.SnapshotID},
		ec2SnapshotIDsFor(t, ts, ec2Filter("volume-size", "100")))
	assert.Equal(t, []string{snap.SnapshotID},
		ec2SnapshotIDsFor(t, ts, ec2Filter("volume-id", volID)))
}

// TestEC2_CreateSnapshot_RefusesAVolumeItCannotFind pins that VolumeId is required and
// checked against state, which is what makes the mapping-level check below coherent: a
// snapshot cannot exist for a volume that does not.
//
// Both codes are documented. EC2's client-error table gives InvalidVolume.NotFound as "The
// specified volume does not exist" and InvalidVolumeID.Malformed as "The specified volume ID
// is not valid"; the messages are substrate's own, from the shared [ec2IDKind] machinery, and
// MissingParameter is the code substrate already raises for an absent required parameter.
func TestEC2_CreateSnapshot_RefusesAVolumeItCannotFind(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		volumeID string
		code     string
		msg      string
	}{
		{
			name: "absent",
			code: "MissingParameter",
			msg:  "The request must contain the parameter VolumeId",
		},
		{
			name:     "malformed",
			volumeID: "vol-not-hex",
			code:     "InvalidVolumeID.Malformed",
			msg:      `Invalid id: "vol-not-hex"`,
		},
		{
			name:     "well-formed but unknown",
			volumeID: "vol-0123456789abcdef0",
			code:     "InvalidVolume.NotFound",
			msg:      "The volume ID 'vol-0123456789abcdef0' does not exist",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			params := map[string]string{"Action": "CreateSnapshot"}
			if tc.volumeID != "" {
				params["VolumeId"] = tc.volumeID
			}
			status, code, msg := ec2ErrorDetail(t, ts, params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.code, code)
			assert.Equal(t, tc.msg, msg)
			assert.Empty(t, ec2DescribeSnapshots(t, ts, nil), "a refused request writes no snapshot")
		})
	}
}

// TestEC2_CreateTags_TagsASnapshot is the other half of #688's
// TestEC2_DescribeTags_ScanIsWiderThanCreateTags: a snap- ID had no arm in
// ec2TaggableResource, so CreateTags on a snapshot answered <return>true</return> having
// written nothing — the same shape #670 fixed for vol-, and immediately relevant now that
// CreateSnapshot's TagSpecification gives a caller a snapshot of their own to tag.
func TestEC2_CreateTags_TagsASnapshot(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, snapID := ec2SnapshotOfSize(t, ts, 30)

	ec2FleetXML(t, ts, map[string]string{
		"Action":       "CreateTags",
		"ResourceId.1": snapID,
		"Tag.1.Key":    "Added",
		"Tag.1.Value":  "later",
	}, nil)

	described := ec2DescribeSnapshots(t, ts, map[string]string{"SnapshotId.1": snapID})
	require.Len(t, described, 1)
	require.Len(t, described[0].Tags, 1)
	assert.Equal(t, "Added", described[0].Tags[0].Key)
	assert.Equal(t, "later", described[0].Tags[0].Value)

	assert.Contains(t, ec2TagKeysFor(t, ts, ec2Filter("resource-type", "snapshot")),
		snapID+"/Added=later", "DescribeTags reports a tag CreateTags wrote to a snapshot")

	// DeleteTags reaches it too — both directions go through the same arm.
	ec2FleetXML(t, ts, map[string]string{
		"Action":       "DeleteTags",
		"ResourceId.1": snapID,
		"Tag.1.Key":    "Added",
	}, nil)
	assert.Empty(t, ec2DescribeSnapshots(t, ts,
		map[string]string{"SnapshotId.1": snapID})[0].Tags)
}

// TestEC2_BlockDeviceMapping_RefusesAnUnknownSnapshot is #689's launch-side fail-before: a
// mapping naming a snapshot no account holds was accepted and materialized an 8 GiB volume.
//
// InvalidSnapshot.NotFound is documented — "The specified snapshot does not exist" — and the
// refusal leaves no state behind, for the ordering reason
// TestEC2_BlockDeviceMapping_RefusalWritesNothing pins.
func TestEC2_BlockDeviceMapping_RefusesAnUnknownSnapshot(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	status, code, msg := ec2InvalidBDM(t, ts, map[string]string{
		"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"BlockDeviceMapping.1.Ebs.SnapshotId": "snap-0123456789abcdef0",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidSnapshot.NotFound", code)
	assert.Equal(t, "The snapshot ID 'snap-0123456789abcdef0' does not exist", msg)

	assert.Empty(t, bdmDescribeVolumes(t, ts, nil), "a refused launch materializes no volume")

	// A malformed snapshot ID is refused too, with the code EC2 publishes for the syntax.
	_, code, _ = ec2InvalidBDM(t, ts, map[string]string{
		"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"BlockDeviceMapping.1.Ebs.SnapshotId": "snap-NOTHEX",
	})
	assert.Equal(t, "InvalidSnapshotID.Malformed", code)
}

// TestEC2_BlockDeviceMapping_InheritsTheSnapshotSize covers the mirror of the same defect:
// ec2VolumeFromMapping took nothing from the snapshot, so a mapping naming a snapshot and no
// size fell through to the 8 GiB default. EC2BlockDeviceMapping.VolumeSize's own field doc
// already stated the correct rule — the doc was ahead of the code.
func TestEC2_BlockDeviceMapping_InheritsTheSnapshotSize(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, snapID := ec2SnapshotOfSize(t, ts, 30)

	ec2FleetXML(t, ts, map[string]string{
		"Action":   "RunInstances",
		"ImageId":  "ami-0abcdef1234567890",
		"MinCount": "1", "MaxCount": "1",
		"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"BlockDeviceMapping.1.Ebs.SnapshotId": snapID,
	}, nil)

	var found bool
	for _, vol := range bdmDescribeVolumes(t, ts, nil) {
		if vol.SnapshotID != snapID {
			continue
		}
		found = true
		assert.Equal(t, 30, vol.Size, "the volume is the snapshot's size, not the 8 GiB default")
		assert.Equal(t, "/dev/sdf", vol.bdmDevice())
	}
	assert.True(t, found, "the launch materialized a volume from the snapshot")
}

// TestEC2_BlockDeviceMapping_SizeBelowTheSnapshotIsRefused is the refusal v0.105.0 deferred
// for exactly the reason #689 removes: with EC2Snapshot.VolumeSize a literal 8 at its single
// producer, the comparison would have tested a constant rather than the caller's request.
//
// Equal and larger are accepted, which is what makes this a rule about the snapshot rather
// than about the size: AWS's sentence on EbsBlockDevice.VolumeSize is "must be equal to or
// larger than the snapshot size".
func TestEC2_BlockDeviceMapping_SizeBelowTheSnapshotIsRefused(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		size    string
		refused bool
	}{
		{name: "smaller", size: "10", refused: true},
		{name: "equal", size: "30"},
		{name: "larger", size: "60"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			_, snapID := ec2SnapshotOfSize(t, ts, 30)

			status, code, msg := ec2InvalidBDM(t, ts, map[string]string{
				"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
				"BlockDeviceMapping.1.Ebs.SnapshotId": snapID,
				"BlockDeviceMapping.1.Ebs.VolumeSize": tc.size,
			})
			if !tc.refused {
				assert.Equal(t, http.StatusOK, status)
				assert.Empty(t, code)
				return
			}
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidBlockDeviceMapping", code)
			assert.Equal(t,
				"Invalid block device mapping for /dev/sdf: Ebs.VolumeSize 10 is smaller than "+
					"the size of snapshot "+snapID+" (30 GiB)", msg)
		})
	}
}

// TestEC2_CreateVolume_InheritsTheSnapshotSize covers CreateVolume's independent copy of the
// same gap: Size absent with SnapshotId present resolved to 8 there too, so the two doors
// onto the same defect are closed in one change.
func TestEC2_CreateVolume_InheritsTheSnapshotSize(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, snapID := ec2SnapshotOfSize(t, ts, 30)

	var doc struct {
		VolumeID string `xml:"volumeId"`
		Size     int    `xml:"size"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action":           "CreateVolume",
		"AvailabilityZone": "us-east-1a",
		"SnapshotId":       snapID,
	}, &doc)
	assert.Equal(t, 30, doc.Size, "a volume restored from a snapshot is at least its size")

	// An explicit size still wins, and one below the snapshot's is refused — the same rule
	// the mapping path applies, through the same comparison.
	ec2FleetXML(t, ts, map[string]string{
		"Action":           "CreateVolume",
		"AvailabilityZone": "us-east-1a",
		"SnapshotId":       snapID,
		"Size":             "60",
	}, &doc)
	assert.Equal(t, 60, doc.Size)

	status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action":           "CreateVolume",
		"AvailabilityZone": "us-east-1a",
		"SnapshotId":       snapID,
		"Size":             "10",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValue", code)
}

// TestEC2_CreateImage_SnapshotSizeIsTheRootVolume pins the second half of the literal 8:
// CreateImage minted a backing snapshot whose size was the constant, and DescribeImages
// rendered the constant again in its own ebs member — so an AMI made from a 40 GiB root
// volume reported 8 GiB twice, through two different operations, and the two would have
// disagreed the moment either one learned a real size.
func TestEC2_CreateImage_SnapshotSizeIsTheRootVolume(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	var run struct {
		InstanceID string `xml:"instancesSet>item>instanceId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action":   "RunInstances",
		"ImageId":  "ami-0abcdef1234567890",
		"MinCount": "1", "MaxCount": "1",
		"BlockDeviceMapping.1.DeviceName":     "/dev/sda1",
		"BlockDeviceMapping.1.Ebs.VolumeSize": "40",
	}, &run)
	require.NotEmpty(t, run.InstanceID)

	var img struct {
		ImageID string `xml:"imageId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateImage", "InstanceId": run.InstanceID, "Name": "forty",
	}, &img)
	require.NotEmpty(t, img.ImageID)

	snaps := ec2DescribeSnapshots(t, ts, nil)
	require.Len(t, snaps, 1)
	assert.Equal(t, int64(40), snaps[0].VolumeSize, "the AMI's snapshot is the root volume's size")

	var described struct {
		Images []struct {
			ImageID string `xml:"imageId"`
			BDM     []struct {
				EBS struct {
					SnapshotID string `xml:"snapshotId"`
					VolumeSize int64  `xml:"volumeSize"`
				} `xml:"ebs"`
			} `xml:"blockDeviceMapping>item"`
		} `xml:"imagesSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeImages"}, &described)
	require.Len(t, described.Images, 1)
	require.Len(t, described.Images[0].BDM, 1)
	assert.Equal(t, snaps[0].SnapshotID, described.Images[0].BDM[0].EBS.SnapshotID)
	assert.Equal(t, int64(40), described.Images[0].BDM[0].EBS.VolumeSize,
		"DescribeImages reads the snapshot's size rather than rendering its own constant")
}
