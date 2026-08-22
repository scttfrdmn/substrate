package emulator_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// What RegisterImage does with the block device mapping it is sent (#711).
//
// Through v0.106.0 it read one thing out of the mapping — the first
// BlockDeviceMapping.N.Ebs.SnapshotId, found by a hand walk of indexes 1..32 — and discarded
// everything else: device names, sizes, volume types, and every mapping after the first that
// named a snapshot. AWS's own third example registers three volumes, so a caller sending
// AWS's documented request got back one, and DescribeImages rendered a /dev/sda1 the request
// may never have mentioned.
//
// The mapping is now parsed by the walk RunInstances and launch templates share, validated
// for the snapshots it names, stored entry for entry, and rendered back in request order.
//
// Provenance: API_RegisterImage.html's Errors section is empty, so no refusal below is
// quoted from the operation's own page. The snapshot refusals come from EC2's client-error
// table via ec2SnapshotIDKind, which predates this change; the ResourceType refusal is
// substrate's code for AWS's "If you specify another value for ResourceType, the request
// fails", which names no code.

// ec2ImageMappingEBS is the ebs element of a rendered block device mapping. It is a pointer
// in [ec2ImageMapping] so that "no ebs element" and "an ebs element reporting 0 GiB" stay
// distinguishable, which is the whole subject of the instance-store and NoDevice tests.
type ec2ImageMappingEBS struct {
	SnapshotID string `xml:"snapshotId"`
	VolumeSize int64  `xml:"volumeSize"`
}

// ec2ImageMapping is one item of a DescribeImages blockDeviceMapping.
type ec2ImageMapping struct {
	DeviceName  string              `xml:"deviceName"`
	VirtualName string              `xml:"virtualName"`
	NoDevice    *string             `xml:"noDevice"`
	EBS         *ec2ImageMappingEBS `xml:"ebs"`
}

// ec2ImageMappings returns the block device mapping DescribeImages renders for imageID.
func ec2ImageMappings(t *testing.T, ts *httptest.Server, imageID string) []ec2ImageMapping {
	t.Helper()
	var doc struct {
		Images []struct {
			Mappings []ec2ImageMapping `xml:"blockDeviceMapping>item"`
		} `xml:"imagesSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeImages", "ImageId.1": imageID}, &doc)
	require.Len(t, doc.Images, 1)
	return doc.Images[0].Mappings
}

// ec2RegisterImage sends a RegisterImage with params on top of Action and Name.
func ec2RegisterImage(t *testing.T, ts *httptest.Server, name string, params map[string]string) map[string]string {
	t.Helper()
	req := map[string]string{"Action": "RegisterImage", "Name": name}
	for k, v := range params {
		req[k] = v
	}
	return req
}

// TestEC2_RegisterImage_StoresEveryMappingAWSSends registers AWS's Example 3 verbatim in
// shape: two volumes from two distinct snapshots and a third that is an empty 100 GiB volume.
//
// This is the operation's fail-before. Before #711 the response to this request described a
// single /dev/sda1 volume, so two of the three volumes the caller registered were
// unobservable through any API call — and the one that survived reported a device name that
// happened to match only because AWS's example uses /dev/sda1 for the root.
//
// The two snapshots have different sizes on purpose: a renderer that resolved every mapping
// against the same snapshot, or against the AMI's root, would report one size twice.
func TestEC2_RegisterImage_StoresEveryMappingAWSSends(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, rootSnap := ec2SnapshotOfSize(t, ts, 30)
	_, dataSnap := ec2SnapshotOfSize(t, ts, 50)

	imageID := ec2CreateImageID(t, ts, ec2RegisterImage(t, ts, "MyImage", map[string]string{
		"RootDeviceName":                      "/dev/sda1",
		"BlockDeviceMapping.1.DeviceName":     "/dev/sda1",
		"BlockDeviceMapping.1.Ebs.SnapshotId": rootSnap,
		"BlockDeviceMapping.2.DeviceName":     "/dev/sdb",
		"BlockDeviceMapping.2.Ebs.SnapshotId": dataSnap,
		"BlockDeviceMapping.3.DeviceName":     "/dev/sdc",
		"BlockDeviceMapping.3.Ebs.VolumeSize": "100",
	}))

	got := ec2ImageMappings(t, ts, imageID)
	require.Len(t, got, 3, "all three of AWS's example volumes, in request order")

	assert.Equal(t, "/dev/sda1", got[0].DeviceName)
	require.NotNil(t, got[0].EBS)
	assert.Equal(t, rootSnap, got[0].EBS.SnapshotID)
	assert.Equal(t, int64(30), got[0].EBS.VolumeSize, "the root snapshot's own size")

	assert.Equal(t, "/dev/sdb", got[1].DeviceName)
	require.NotNil(t, got[1].EBS)
	assert.Equal(t, dataSnap, got[1].EBS.SnapshotID)
	assert.Equal(t, int64(50), got[1].EBS.VolumeSize, "the second snapshot's, not the root's")

	// AWS: "The third volume is an empty 100 GiB Amazon EBS volume." A mapping naming a
	// size and no snapshot is a request AWS documents, so registering it must succeed and
	// it must render no snapshotId at all.
	assert.Equal(t, "/dev/sdc", got[2].DeviceName)
	require.NotNil(t, got[2].EBS)
	assert.Empty(t, got[2].EBS.SnapshotID)
	assert.Equal(t, int64(100), got[2].EBS.VolumeSize)
}

// TestEC2_RegisterImage_RefusesASnapshotItCannotResolve pins the one validation rule this
// path borrows, on a mapping that is not the first — the index the old walk stopped at.
//
// A stored dangling reference is worse than a refusal here, because DescribeImages renders it
// as a real volume of substrate's default size: the caller reads a plausible answer about a
// snapshot that does not exist. Nothing is registered either way, so a refused request leaves
// no half-built AMI.
func TestEC2_RegisterImage_RefusesASnapshotItCannotResolve(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, rootSnap := ec2SnapshotOfSize(t, ts, 20)

	for name, tc := range map[string]struct {
		snapshotID string
		wantCode   string
	}{
		"absent":       {"snap-0123456789abcdef0", "InvalidSnapshot.NotFound"},
		"wrong prefix": {"vol-0123456789abcdef0", "InvalidSnapshotID.Malformed"},
		"no prefix":    {"0123456789abcdef0", "InvalidSnapshotID.Malformed"},
		"non-hex body": {"snap-0123456789abcdefg", "InvalidSnapshotID.Malformed"},
	} {
		t.Run(name, func(t *testing.T) {
			before := ec2FilteredImageIDs(t, ts, map[string]string{})
			status, code, _ := ec2ErrorDetail(t, ts, ec2RegisterImage(t, ts, "refused-"+name,
				map[string]string{
					"BlockDeviceMapping.1.DeviceName":     "/dev/sda1",
					"BlockDeviceMapping.1.Ebs.SnapshotId": rootSnap,
					"BlockDeviceMapping.2.DeviceName":     "/dev/sdb",
					"BlockDeviceMapping.2.Ebs.SnapshotId": tc.snapshotID,
				}))
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.wantCode, code)
			assert.Len(t, ec2FilteredImageIDs(t, ts, map[string]string{}), len(before),
				"a refused request registers no AMI")
		})
	}
}

// TestEC2_RegisterImage_RefusesASizeBelowTheSnapshot is the other half of the borrowed rule,
// and the reason it is worth borrowing rather than writing: the two refusals carry different
// codes because they are different mistakes, and the shared checker already knows that.
func TestEC2_RegisterImage_RefusesASizeBelowTheSnapshot(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, snapID := ec2SnapshotOfSize(t, ts, 40)

	status, code, message := ec2ErrorDetail(t, ts, ec2RegisterImage(t, ts, "too-small",
		map[string]string{
			"BlockDeviceMapping.1.DeviceName":     "/dev/sda1",
			"BlockDeviceMapping.1.Ebs.SnapshotId": snapID,
			"BlockDeviceMapping.1.Ebs.VolumeSize": "20",
		}))
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidBlockDeviceMapping", code)
	assert.Contains(t, message, "/dev/sda1", "the message must name the offending device")
	assert.Contains(t, message, snapID)
}

// TestEC2_RegisterImage_WalksPastIndexThirtyTwoAndStopsAtAGap pins both halves of the parser
// change, in the two directions a caller can notice.
//
// Unbounded: the old walk read indexes 1..32, so a request with more mappings than that had
// the rest silently dropped. Contiguous: the old walk tolerated a gap, and the shared parser
// stops at the first absent index. AWS's query protocol indexes contiguously and every other
// indexed walk in the plugin assumes it, so a sparse request is not one an SDK produces — but
// a hand-built request that skipped an index used to have its later mappings read, and no
// longer does.
func TestEC2_RegisterImage_WalksPastIndexThirtyTwoAndStopsAtAGap(t *testing.T) {
	t.Parallel()

	t.Run("index 33 is read", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		params := map[string]string{}
		for i := 1; i <= 33; i++ {
			params[fmt.Sprintf("BlockDeviceMapping.%d.DeviceName", i)] = fmt.Sprintf("/dev/sd%d", i)
			params[fmt.Sprintf("BlockDeviceMapping.%d.Ebs.VolumeSize", i)] = "10"
		}
		imageID := ec2CreateImageID(t, ts, ec2RegisterImage(t, ts, "thirty-three", params))
		assert.Len(t, ec2ImageMappings(t, ts, imageID), 33,
			"the walk no longer stops at 32")
	})

	t.Run("a gap ends the list", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		imageID := ec2CreateImageID(t, ts, ec2RegisterImage(t, ts, "sparse", map[string]string{
			"BlockDeviceMapping.1.DeviceName":     "/dev/sda1",
			"BlockDeviceMapping.1.Ebs.VolumeSize": "10",
			"BlockDeviceMapping.3.DeviceName":     "/dev/sdc",
			"BlockDeviceMapping.3.Ebs.VolumeSize": "20",
		}))
		got := ec2ImageMappings(t, ts, imageID)
		require.Len(t, got, 1, "index 2 is absent, so index 3 is not read")
		assert.Equal(t, "/dev/sda1", got[0].DeviceName)
	})
}

// TestEC2_RegisterImage_RootDeviceNameChoosesTheProtectedSnapshot pins which of several
// snapshots becomes the AMI's root — through the one behavior that depends on the answer.
//
// [EC2Image.SnapshotID] is not directly observable, so asserting on it would mean reading
// state. DeleteSnapshot's in-use rule is observable and is scoped to the root device
// specifically (#710, following AWS's "You cannot delete a snapshot of the root device of an
// EBS volume used by a registered AMI"), so the root snapshot is the one that cannot be
// deleted and every other one can. The mapping order here is deliberately the reverse of
// AWS's example: the root device is the *second* mapping, so an implementation taking the
// first would protect the data volume's snapshot and leave the root's deletable.
func TestEC2_RegisterImage_RootDeviceNameChoosesTheProtectedSnapshot(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, dataSnap := ec2SnapshotOfSize(t, ts, 25)
	_, rootSnap := ec2SnapshotOfSize(t, ts, 35)

	imageID := ec2CreateImageID(t, ts, ec2RegisterImage(t, ts, "root-is-second", map[string]string{
		"RootDeviceName":                      "/dev/xvda",
		"BlockDeviceMapping.1.DeviceName":     "/dev/sdb",
		"BlockDeviceMapping.1.Ebs.SnapshotId": dataSnap,
		"BlockDeviceMapping.2.DeviceName":     "/dev/xvda",
		"BlockDeviceMapping.2.Ebs.SnapshotId": rootSnap,
	}))

	status, code, message := ec2DeleteSnapshot(t, ts, rootSnap)
	require.Equal(t, http.StatusBadRequest, status, "the root snapshot is protected")
	assert.Equal(t, "InvalidSnapshot.InUse", code)
	assert.Contains(t, message, imageID)

	// And the non-root one is not: AWS's sentence names the root device, and #671 settled
	// that substrate models what the API model states rather than a wider guess.
	status, code, _ = ec2DeleteSnapshot(t, ts, dataSnap)
	require.Equal(t, http.StatusOK, status, "code=%s", code)
}

// TestEC2_RegisterImage_FilterFindsANonRootSnapshot keeps two answers about one AMI
// consistent.
//
// The block-device-mapping.snapshot-id filter read only the root snapshot, so an AMI whose
// second volume DescribeImages had just rendered was invisible to a filter naming that
// volume's snapshot — one operation contradicting the other about the same record.
func TestEC2_RegisterImage_FilterFindsANonRootSnapshot(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, rootSnap := ec2SnapshotOfSize(t, ts, 15)
	_, dataSnap := ec2SnapshotOfSize(t, ts, 45)

	imageID := ec2CreateImageID(t, ts, ec2RegisterImage(t, ts, "two-volumes", map[string]string{
		"BlockDeviceMapping.1.DeviceName":     "/dev/sda1",
		"BlockDeviceMapping.1.Ebs.SnapshotId": rootSnap,
		"BlockDeviceMapping.2.DeviceName":     "/dev/sdb",
		"BlockDeviceMapping.2.Ebs.SnapshotId": dataSnap,
	}))

	for _, snapID := range []string{rootSnap, dataSnap} {
		assert.Equal(t, []string{imageID}, ec2FilteredImageIDs(t, ts, map[string]string{
			"Filter.1.Name": "block-device-mapping.snapshot-id", "Filter.1.Value.1": snapID,
		}), "the AMI must match on every snapshot its mapping names")
	}
}

// TestEC2_RegisterImage_TagsTheAMIAndRefusesAnotherResourceType covers the parameter this
// operation never read at all: a caller could tag an AMI at registration and the tags went
// nowhere, which DescribeTags and every tag-based filter then agreed about.
//
// AWS: "To tag the AMI, the value for ResourceType must be image. If you specify another
// value for ResourceType, the request fails." Both halves are asserted, because the silent
// skip the shared collector performs would satisfy the first alone while dropping the tags of
// a caller who spelled the type wrong.
func TestEC2_RegisterImage_TagsTheAMIAndRefusesAnotherResourceType(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, snapID := ec2SnapshotOfSize(t, ts, 10)
	mapping := map[string]string{
		"BlockDeviceMapping.1.DeviceName":     "/dev/sda1",
		"BlockDeviceMapping.1.Ebs.SnapshotId": snapID,
	}

	tagged := map[string]string{
		"TagSpecification.1.ResourceType": "image",
		"TagSpecification.1.Tag.1.Key":    "Env",
		"TagSpecification.1.Tag.1.Value":  "prod",
	}
	for k, v := range mapping {
		tagged[k] = v
	}
	imageID := ec2CreateImageID(t, ts, ec2RegisterImage(t, ts, "tagged", tagged))
	assert.Equal(t, []string{imageID}, ec2FilteredImageIDs(t, ts, map[string]string{
		"Filter.1.Name": "tag:Env", "Filter.1.Value.1": "prod",
	}), "the tag must be on the AMI, not dropped")

	refused := map[string]string{
		"TagSpecification.1.ResourceType": "volume",
		"TagSpecification.1.Tag.1.Key":    "Env",
		"TagSpecification.1.Tag.1.Value":  "prod",
	}
	for k, v := range mapping {
		refused[k] = v
	}
	before := ec2FilteredImageIDs(t, ts, map[string]string{})
	status, code, message := ec2ErrorDetail(t, ts, ec2RegisterImage(t, ts, "wrong-type", refused))
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValue", code)
	assert.Contains(t, message, "volume", "the message must name the type that was refused")
	assert.Len(t, ec2FilteredImageIDs(t, ts, map[string]string{}), len(before),
		"a refused request registers no AMI")
}

// TestEC2_DescribeImages_RendersInstanceStoreAndSuppressedDevices pins the two mapping shapes
// that are not EBS volumes.
//
// An instance store device has a virtualName and no ebs element, and a suppressed one has a
// noDevice element whose value is empty. Both were unreachable before #711 — the operation
// stored neither — and both would be reported as EBS volumes of substrate's default size by a
// renderer that gave every mapping an ebs element, which is an observation nothing backs.
func TestEC2_DescribeImages_RendersInstanceStoreAndSuppressedDevices(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, snapID := ec2SnapshotOfSize(t, ts, 12)

	imageID := ec2CreateImageID(t, ts, ec2RegisterImage(t, ts, "mixed", map[string]string{
		"BlockDeviceMapping.1.DeviceName":     "/dev/sda1",
		"BlockDeviceMapping.1.Ebs.SnapshotId": snapID,
		"BlockDeviceMapping.2.VirtualName":    "ephemeral0",
		"BlockDeviceMapping.2.DeviceName":     "/dev/sdb",
		"BlockDeviceMapping.3.DeviceName":     "/dev/sdc",
		"BlockDeviceMapping.3.NoDevice":       "",
	}))

	got := ec2ImageMappings(t, ts, imageID)
	require.Len(t, got, 3)

	require.NotNil(t, got[0].EBS, "an EBS mapping has an ebs element")
	assert.Equal(t, int64(12), got[0].EBS.VolumeSize)

	assert.Equal(t, "ephemeral0", got[1].VirtualName)
	assert.Nil(t, got[1].EBS, "an instance store device is not an EBS volume")

	assert.Nil(t, got[2].EBS, "a suppressed device has no volume at all")
	require.NotNil(t, got[2].NoDevice, "noDevice is rendered as a present, empty element")
	assert.Empty(t, *got[2].NoDevice)
}

// TestEC2_DescribeImages_PhantomSnapshotReportsTheDefaultSize is the test the fallback at
// ec2DefaultVolumeSizeGiB has never had, on both records that can reach it.
//
// It exists so a caller sizing a volume off this member reads something it can act on rather
// than 0. Registering a phantom snapshot is no longer a way in — the mapping is validated now
// — so the two remaining routes are asserted directly: a non-root mapping whose snapshot was
// deleted afterwards (which DeleteSnapshot permits, since AWS scopes its refusal to the root
// device), and an AMI record written before #711, which has a root snapshot, no stored
// mapping, and nothing in state to resolve it against.
func TestEC2_DescribeImages_PhantomSnapshotReportsTheDefaultSize(t *testing.T) {
	t.Parallel()

	t.Run("a non-root snapshot deleted after registration", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		_, rootSnap := ec2SnapshotOfSize(t, ts, 60)
		_, dataSnap := ec2SnapshotOfSize(t, ts, 70)
		imageID := ec2CreateImageID(t, ts, ec2RegisterImage(t, ts, "loses-a-volume",
			map[string]string{
				"RootDeviceName":                      "/dev/sda1",
				"BlockDeviceMapping.1.DeviceName":     "/dev/sda1",
				"BlockDeviceMapping.1.Ebs.SnapshotId": rootSnap,
				"BlockDeviceMapping.2.DeviceName":     "/dev/sdb",
				"BlockDeviceMapping.2.Ebs.SnapshotId": dataSnap,
			}))

		status, code, _ := ec2DeleteSnapshot(t, ts, dataSnap)
		require.Equal(t, http.StatusOK, status, "code=%s", code)

		got := ec2ImageMappings(t, ts, imageID)
		require.Len(t, got, 2, "the mapping still describes two volumes")
		require.NotNil(t, got[0].EBS)
		assert.Equal(t, int64(60), got[0].EBS.VolumeSize, "the root volume is unaffected")
		require.NotNil(t, got[1].EBS)
		assert.Equal(t, dataSnap, got[1].EBS.SnapshotID, "the reference is kept, as AWS keeps it")
		assert.Equal(t, int64(8), got[1].EBS.VolumeSize, "not 0, which a caller cannot size from")
	})

	t.Run("an AMI record that predates the mappings field", func(t *testing.T) {
		t.Parallel()
		state := emulator.NewMemoryStateManager()
		ts := newEC2TestServerWithState(t, state)
		// The shape RegisterImage wrote through v0.106.0: a root snapshot and no mapping.
		// Written directly because no request produces it any more, which is the reason
		// newEC2TestServerWithState exists.
		record, err := json.Marshal(emulator.EC2Image{
			ImageID:    "ami-0123456789abcdef0",
			Name:       "pre-711",
			State:      "available",
			SnapshotID: "snap-0123456789abcdef0",
			AccountID:  "123456789012",
			Region:     "us-east-1",
		})
		require.NoError(t, err)
		require.NoError(t, state.Put(context.Background(), "ec2",
			"image:123456789012/us-east-1/ami-0123456789abcdef0", record))

		got := ec2ImageMappings(t, ts, "ami-0123456789abcdef0")
		require.Len(t, got, 1, "the root device entry substrate fabricates for such a record")
		assert.Equal(t, "/dev/sda1", got[0].DeviceName)
		require.NotNil(t, got[0].EBS)
		assert.Equal(t, "snap-0123456789abcdef0", got[0].EBS.SnapshotID)
		assert.Equal(t, int64(8), got[0].EBS.VolumeSize)
	})
}
