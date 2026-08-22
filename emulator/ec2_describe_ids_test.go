package emulator_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ec2CreateVolume creates a volume and returns its ID.
func ec2CreateVolume(t *testing.T, ts *httptest.Server, size string) string {
	t.Helper()
	var created struct {
		VolumeID string `xml:"volumeId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateVolume", "AvailabilityZone": "us-east-1a", "Size": size,
	}, &created)
	require.NotEmpty(t, created.VolumeID)
	return created.VolumeID
}

// ec2RegisterImageID registers an AMI and returns its ID.
func ec2RegisterImageID(t *testing.T, ts *httptest.Server, name string) string {
	t.Helper()
	var created struct {
		ImageID string `xml:"imageId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "RegisterImage", "Name": name, "RootDeviceName": "/dev/sda1",
	}, &created)
	require.NotEmpty(t, created.ImageID)
	return created.ImageID
}

// ec2DescribedVolumeIDs returns the volume IDs a DescribeVolumes call answered with.
func ec2DescribedVolumeIDs(t *testing.T, ts *httptest.Server, params map[string]string) []string {
	t.Helper()
	var described struct {
		Volumes []struct {
			VolumeID string `xml:"volumeId"`
		} `xml:"volumeSet>item"`
	}
	full := map[string]string{"Action": "DescribeVolumes"}
	for k, v := range params {
		full[k] = v
	}
	ec2FleetXML(t, ts, full, &described)
	ids := make([]string, 0, len(described.Volumes))
	for _, v := range described.Volumes {
		ids = append(ids, v.VolumeID)
	}
	return ids
}

// ec2DescribedImageIDs returns the AMI IDs a DescribeImages call answered with.
func ec2DescribedImageIDs(t *testing.T, ts *httptest.Server, params map[string]string) []string {
	t.Helper()
	var described struct {
		Images []struct {
			ImageID string `xml:"imageId"`
		} `xml:"imagesSet>item"`
	}
	full := map[string]string{"Action": "DescribeImages"}
	for k, v := range params {
		full[k] = v
	}
	ec2FleetXML(t, ts, full, &described)
	ids := make([]string, 0, len(described.Images))
	for _, img := range described.Images {
		ids = append(ids, img.ImageID)
	}
	return ids
}

// TestEC2_DescribeImages_NamedIDNarrowsTheAnswer covers the defect #731's survey turned up,
// which is worse than the one it was filed for: DescribeImages read no ImageId.N at all, so
// a caller naming one AMI was answered with every AMI the account owned.
//
// A superset is worse than an error. An error is visible; a superset looks like a successful
// narrowing, so a consumer's "the query returns only my AMI" assertion passed with two AMIs
// in the answer and one in the assertion's blind spot.
func TestEC2_DescribeImages_NamedIDNarrowsTheAnswer(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	first := ec2RegisterImageID(t, ts, "first-ami")
	second := ec2RegisterImageID(t, ts, "second-ami")
	require.NotEqual(t, first, second)

	assert.ElementsMatch(t, []string{first}, ec2DescribedImageIDs(t, ts, map[string]string{"ImageId.1": first}))
	assert.ElementsMatch(t, []string{second}, ec2DescribedImageIDs(t, ts, map[string]string{"ImageId.1": second}))
	assert.ElementsMatch(t, []string{first, second},
		ec2DescribedImageIDs(t, ts, map[string]string{"ImageId.1": first, "ImageId.2": second}))

	// With no ID named, both — which is the answer the ID list was silently giving before.
	assert.ElementsMatch(t, []string{first, second}, ec2DescribedImageIDs(t, ts, nil))

	// One present and one absent fails the whole call: EC2 does not return the partial set.
	status, code := ec2ErrorCode(t, ts, map[string]string{
		"Action": "DescribeImages", "ImageId.1": first, "ImageId.2": "ami-0000000000000dead",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidAMIID.NotFound", code)
}

// TestEC2_DescribeImages_BundledAMIIsNameableButNotEnumerated pins both halves of
// substrate's reading of the bundled catalog (#733), now that this operation resolves an
// explicit ID rather than ignoring it.
//
// Nameable, because a launch accepts a bundled AMI and a describe that called the same AMI
// absent would contradict it — a consumer who resolves an AMI through SSM, describes it to
// read its members, and launches it must get one consistent answer. Not enumerated, because
// DescribeImages lists what an account owns and a public AWS-owned image is not that; real
// AWS would answer an unqualified describe with tens of thousands of public images.
func TestEC2_DescribeImages_BundledAMIIsNameableButNotEnumerated(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	own := ec2RegisterImageID(t, ts, "my-own-ami")

	// Named explicitly, the bundled AMI is answered.
	assert.ElementsMatch(t, []string{ec2TestImage},
		ec2DescribedImageIDs(t, ts, map[string]string{"ImageId.1": ec2TestImage}))

	// Named alongside the account's own AMI, both are answered.
	assert.ElementsMatch(t, []string{own, ec2TestImage},
		ec2DescribedImageIDs(t, ts, map[string]string{"ImageId.1": own, "ImageId.2": ec2TestImage}))

	// Unqualified, only the account's own.
	assert.ElementsMatch(t, []string{own}, ec2DescribedImageIDs(t, ts, nil))

	// And the ID is Region-scoped, as on AWS: the same parameter's eu-west-1 image does not
	// resolve against a us-east-1 request.
	status, code := ec2ErrorCode(t, ts, map[string]string{
		"Action":    "DescribeImages",
		"ImageId.1": emulator.BundledImageID("eu-west-1", ec2BundledParameters[0]),
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidAMIID.NotFound", code)
}

// TestEC2_DescribeImages_IDPlusNonMatchingFilterIsEmpty guards the distinction the ID
// assertion turns on: an AMI a *filter* excluded resolved, so the answer is an empty set,
// not NotFound. Only absence is an error.
func TestEC2_DescribeImages_IDPlusNonMatchingFilterIsEmpty(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	own := ec2RegisterImageID(t, ts, "filtered-ami")

	assert.Empty(t, ec2DescribedImageIDs(t, ts, map[string]string{
		"ImageId.1":        own,
		"Filter.1.Name":    "image-id",
		"Filter.1.Value.1": "ami-0000000000000dead",
	}))

	// The same holds for a bundled AMI reached through the catalog rather than state, so
	// the two passes cannot disagree about what "resolved" means.
	assert.Empty(t, ec2DescribedImageIDs(t, ts, map[string]string{
		"ImageId.1":        ec2TestImage,
		"Filter.1.Name":    "image-id",
		"Filter.1.Value.1": "ami-0000000000000dead",
	}))
}

// TestEC2_DescribeVolumes_ExplicitIDAssertsExistence is #731's own subject: naming a volume
// ID asserts the volume exists, so an ID that resolves to nothing is refused rather than
// narrowing the answer to an empty set.
//
// The empty set was the silent shape — a consumer's error branch was unreachable, and the
// test that covered it passed while asserting the opposite of AWS's behavior.
func TestEC2_DescribeVolumes_ExplicitIDAssertsExistence(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	first := ec2CreateVolume(t, ts, "8")
	second := ec2CreateVolume(t, ts, "16")

	assert.ElementsMatch(t, []string{first}, ec2DescribedVolumeIDs(t, ts, map[string]string{"VolumeId.1": first}))
	assert.ElementsMatch(t, []string{first, second}, ec2DescribedVolumeIDs(t, ts, nil))

	status, code := ec2ErrorCode(t, ts, map[string]string{
		"Action": "DescribeVolumes", "VolumeId.1": first, "VolumeId.2": "vol-0000000000000dead",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidVolume.NotFound", code)

	// A filter that excludes a volume the ID named is still a resolution, so an empty set.
	assert.Empty(t, ec2DescribedVolumeIDs(t, ts, map[string]string{
		"VolumeId.1":       first,
		"Filter.1.Name":    "size",
		"Filter.1.Value.1": "99",
	}))
}

// TestEC2_DescribeVolumes_ReadsEveryIDForm covers the two silent bugs in the hand-rolled ID
// loop #731 replaced, neither of which was the one the issue was filed for.
//
// It read only the indexed form, so the un-indexed VolumeId that hand-built requests and
// some older SDK shapes send was ignored — a request naming one volume was answered about
// every volume. And it broke on the first empty *value* rather than the first missing *key*,
// so an explicitly empty VolumeId.1 discarded VolumeId.2 and everything after it, again
// widening the answer instead of failing.
func TestEC2_DescribeVolumes_ReadsEveryIDForm(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	first := ec2CreateVolume(t, ts, "8")
	second := ec2CreateVolume(t, ts, "16")

	t.Run("the un-indexed form narrows", func(t *testing.T) {
		assert.ElementsMatch(t, []string{first}, ec2DescribedVolumeIDs(t, ts, map[string]string{"VolumeId": first}))
	})

	t.Run("the un-indexed form asserts existence too", func(t *testing.T) {
		status, code := ec2ErrorCode(t, ts, map[string]string{
			"Action": "DescribeVolumes", "VolumeId": "vol-0000000000000dead",
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidVolume.NotFound", code)
	})

	t.Run("an empty value does not truncate the list", func(t *testing.T) {
		// The empty string is now a malformed volume ID — an ID the caller sent and
		// substrate reports on. Truncating instead answered about every volume, which is
		// the failure mode a caller cannot see.
		status, code := ec2ErrorCode(t, ts, map[string]string{
			"Action": "DescribeVolumes", "VolumeId.1": "", "VolumeId.2": second,
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidVolumeID.Malformed", code)
	})

	t.Run("the same ID twice resolves once", func(t *testing.T) {
		assert.ElementsMatch(t, []string{first},
			ec2DescribedVolumeIDs(t, ts, map[string]string{"VolumeId.1": first, "VolumeId.2": first}))
	})
}
