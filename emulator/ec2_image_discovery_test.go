package emulator_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Finding a launchable AMI on a fresh server (#1432).
//
// A fresh server holds no image the account owns, RunInstances refuses an AMI it cannot resolve,
// and through v0.125.0 neither DescribeImages nor the refusal pointed at the bundled catalog. The
// describe now lists it for Owner.N=amazon, and the refusal names where an AMI comes from.

type ec2DiscoveredImage struct {
	ImageID      string `xml:"imageId"`
	Name         string `xml:"name"`
	Architecture string `xml:"architecture"`
	OwnerAlias   string `xml:"imageOwnerAlias"`
	OwnerID      string `xml:"imageOwnerId"`
	Public       bool   `xml:"isPublic"`
}

func ec2DiscoverImages(t *testing.T, ts *httptest.Server, params map[string]string) []ec2DiscoveredImage {
	t.Helper()
	full := map[string]string{"Action": "DescribeImages"}
	for k, v := range params {
		full[k] = v
	}
	var doc struct {
		Images []ec2DiscoveredImage `xml:"imagesSet>item"`
	}
	ec2FleetXML(t, ts, full, &doc)
	return doc.Images
}

func ec2DiscoveredIDs(images []ec2DiscoveredImage) []string {
	out := make([]string, 0, len(images))
	for _, img := range images {
		out = append(out, img.ImageID)
	}
	return out
}

// TestEC2_DescribeImages_OwnerAmazonListsTheBundledCatalog is the issue's first observation turned
// round: a fresh server answers an unqualified describe with nothing, as before, and Owners=amazon
// with the catalog's AWS-published AMIs.
func TestEC2_DescribeImages_OwnerAmazonListsTheBundledCatalog(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	assert.Empty(t, ec2DiscoverImages(t, ts, nil), "an unqualified describe still reports what the account owns")

	amazon := ec2DiscoverImages(t, ts, map[string]string{"Owner.1": "amazon"})
	assert.ElementsMatch(t, []string{
		ec2TestImage, ec2TestImageArm, ec2TestImageMinimal, ec2TestImageMinimalArm,
		ec2TestImageWindows, ec2TestImageECS, ec2TestImageECS2,
	}, ec2DiscoveredIDs(amazon), "every AWS-published bundled AMI, and not the Canonical ones")
	for _, img := range amazon {
		assert.Equal(t, "amazon", img.OwnerAlias)
		assert.True(t, img.Public)
		assert.Empty(t, img.OwnerID, "a bundled AMI names no owning account")
	}
	assert.Equal(t, ec2DiscoveredIDs(amazon),
		ec2DiscoveredIDs(ec2DiscoverImages(t, ts, map[string]string{"Owner": "amazon"})),
		"the un-indexed spelling is the same list")
}

// TestEC2_DescribeImages_OwnerScopes is the table over Owner.N's published forms.
func TestEC2_DescribeImages_OwnerScopes(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	own := ec2RegisterImageID(t, ts, "my-ami")
	const account = "123456789012"

	for _, tc := range []struct {
		name   string
		params map[string]string
		want   []string
	}{
		{"self", map[string]string{"Owner.1": "self"}, []string{own}},
		{"the caller's account ID", map[string]string{"Owner.1": account}, []string{own}},
		{"another account", map[string]string{"Owner.1": "999999999999"}, nil},
		{"aws-marketplace", map[string]string{"Owner.1": "aws-marketplace"}, nil},
		{"amazon and a name filter", map[string]string{
			"Owner.1": "amazon", "Filter.1.Name": "name", "Filter.1.Value.1": "al2023-ami-kernel-default-*",
		}, []string{ec2TestImage, ec2TestImageArm}},
		{"amazon and an architecture filter", map[string]string{
			"Owner.1": "amazon", "Filter.1.Name": "name", "Filter.1.Value.1": "al2023-ami-kernel-default-*",
			"Filter.2.Name": "architecture", "Filter.2.Value.1": "arm64",
		}, []string{ec2TestImageArm}},
		{"self and amazon union", map[string]string{
			"Owner.1": "self", "Owner.2": "amazon", "Filter.1.Name": "name", "Filter.1.Value.1": "my-ami",
		}, []string{own}},
		{"an owned ID scoped to amazon is resolved but not selected", map[string]string{
			"Owner.1": "amazon", "ImageId.1": own,
		}, nil},
		{"a bundled ID scoped to amazon", map[string]string{
			"Owner.1": "amazon", "ImageId.1": ec2TestImageWindows,
		}, []string{ec2TestImageWindows}},
		{"a bundled ID with no owner, as before", map[string]string{"ImageId.1": ec2TestImageUbuntu},
			[]string{ec2TestImageUbuntu}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.ElementsMatch(t, tc.want, ec2DiscoveredIDs(ec2DiscoverImages(t, ts, tc.params)))
		})
	}
}

// TestEC2_DescribeImages_DiscoveredAMILaunches closes the loop the issue asks for: the ID the
// describe answers is one RunInstances accepts.
func TestEC2_DescribeImages_DiscoveredAMILaunches(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	images := ec2DiscoverImages(t, ts, map[string]string{
		"Owner.1": "amazon", "Filter.1.Name": "name", "Filter.1.Value.1": "al2023-ami-kernel-default-x86_64",
	})
	require.Len(t, images, 1)
	var run struct {
		InstanceID string `xml:"instancesSet>item>instanceId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": images[0].ImageID, "MinCount": "1", "MaxCount": "1",
	}, &run)
	assert.NotEmpty(t, run.InstanceID)
}

// TestEC2_RunInstances_UnknownAMIErrorSaysWhereAnAMIComesFrom pins the refusal's wording: the
// code is unchanged, and the message names RegisterImage and the SSM public parameter path.
func TestEC2_RunInstances_UnknownAMIErrorSaysWhereAnAMIComesFrom(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	status, code, msg := ec2ErrorDetail(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": "ami-12345678", "MinCount": "1", "MaxCount": "1",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidAMIID.NotFound", code)
	assert.Contains(t, msg, "The image ID 'ami-12345678' does not exist")
	assert.Contains(t, msg, "RegisterImage")
	assert.Contains(t, msg, "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64")
	assert.Contains(t, msg, "Owner=amazon")

	_, code, msg = ec2ErrorDetail(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": "not-an-ami", "MinCount": "1", "MaxCount": "1",
	})
	assert.Equal(t, "InvalidAMIID.Malformed", code)
	assert.NotContains(t, msg, "RegisterImage", "a malformed ID is a different mistake and keeps its own message")
}
