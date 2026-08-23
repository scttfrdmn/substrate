package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The Image members DescribeImages renders, and where each one's value comes from (#750).
//
// Through v0.108.0 the operation rendered identity, name, description, state, owner and
// creation date and nothing else, so a caller that branched on architecture, platform or the
// root device — which CDK and userdata-selection logic routinely do — read an empty string
// from an AMI substrate knew the answer for. Ten filters were accepted and never evaluated
// on top of that, so filtering on architecture returned every AMI and looked like a match.
//
// Provenance, because two AWS pages disagree with themselves and this file takes a side:
//
//   - platform's casing. API_Image.html lists the valid value as "Windows" and in the same
//     entry says the member "is set to windows for Windows AMIs"; DescribeImages' Example 2
//     response renders <platform>windows</platform> and its platform filter says "The only
//     supported value is windows". Substrate renders lowercase — three statements against
//     one.
//   - the element names. DescribeImages' Example 1 uses <ownerId> and <public> while
//     Examples 2 and 3 and API_Image.html use <imageOwnerId> and <isPublic>. Substrate
//     follows the type page, which is the spelling it already used for imageOwnerId.
//
// And two values are substrate's reading rather than AWS's published text: each bundled
// image's root device name (AWS's device naming reference says only "Differs by AMI —
// /dev/sda1 or /dev/xvda"), and RegisterImage's i386/paravirtual defaults, which are AWS's
// own documented defaults and will look wrong to a modern caller — see
// TestEC2_RegisterImage_UsesAWSDocumentedDefaults for why substrate keeps them anyway.

// ec2DescribedImage is one item of a DescribeImages imagesSet, with every member this file
// asserts on. It is deliberately a separate shape from the plugin's own imageItem: a test
// that reused the production struct would pass on a member whose XML name was wrong in both
// places, which is exactly the class of bug #738 found.
type ec2DescribedImage struct {
	ImageID                string `xml:"imageId"`
	Name                   string `xml:"name"`
	Description            string `xml:"description"`
	State                  string `xml:"imageState"`
	OwnerID                string `xml:"imageOwnerId"`
	OwnerAlias             string `xml:"imageOwnerAlias"`
	Public                 bool   `xml:"isPublic"`
	Architecture           string `xml:"architecture"`
	Platform               string `xml:"platform"`
	PlatformDetails        string `xml:"platformDetails"`
	UsageOperation         string `xml:"usageOperation"`
	ImageType              string `xml:"imageType"`
	RootDeviceType         string `xml:"rootDeviceType"`
	RootDeviceName         string `xml:"rootDeviceName"`
	VirtualizationType     string `xml:"virtualizationType"`
	Hypervisor             string `xml:"hypervisor"`
	PublicSsmParameterName string `xml:"publicSsmParameterName"`
}

// ec2DescribeImagesRaw returns the DescribeImages response body verbatim, so a test can
// assert an element is *absent* rather than decoded-to-empty. Those are different answers
// and unmarshalling cannot tell them apart, which is the whole point of platform's
// optionality.
func ec2DescribeImagesRaw(t *testing.T, ts *httptest.Server, params map[string]string) string {
	t.Helper()
	req := map[string]string{"Action": "DescribeImages"}
	for k, v := range params {
		req[k] = v
	}
	resp := ec2Request(t, ts, req)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "body was %s", body)
	return string(body)
}

// ec2DescribeImages decodes the imagesSet a DescribeImages request answers with.
func ec2DescribeImages(t *testing.T, ts *httptest.Server, params map[string]string) []ec2DescribedImage {
	t.Helper()
	var doc struct {
		Images []ec2DescribedImage `xml:"imagesSet>item"`
	}
	require.NoError(t, xml.Unmarshal([]byte(ec2DescribeImagesRaw(t, ts, params)), &doc))
	return doc.Images
}

// ec2DescribeOneImage is the single-AMI form, which is how a bundled image is reachable at
// all: they are deliberately absent from state (#733), so an unqualified describe does not
// list them and naming the ID is the only route.
func ec2DescribeOneImage(t *testing.T, ts *httptest.Server, imageID string) ec2DescribedImage {
	t.Helper()
	images := ec2DescribeImages(t, ts, map[string]string{"ImageId.1": imageID})
	require.Len(t, images, 1, "DescribeImages %s", imageID)
	return images[0]
}

// TestEC2_DescribeImages_BundledMembers asserts every member of every bundled AMI, so the
// catalog and the renderer cannot drift apart one entry at a time.
//
// The interesting rows are the ones that differ: arm64 against x86_64, the Windows entry's
// platform and billing pair against the Linux ones, Amazon Linux's /dev/xvda against
// Windows' and Ubuntu's /dev/sda1, and the two Canonical entries carrying no owner alias
// where the seven AWS-published ones carry "amazon".
func TestEC2_DescribeImages_BundledMembers(t *testing.T) {
	ts := newEC2TestServer(t)

	tests := []struct {
		param string
		want  ec2DescribedImage
	}{
		{
			param: "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64",
			want: ec2DescribedImage{
				Name: "al2023-ami-kernel-default-x86_64", Architecture: "x86_64",
				PlatformDetails: "Linux/UNIX", UsageOperation: "RunInstances",
				RootDeviceName: "/dev/xvda", OwnerAlias: "amazon",
			},
		},
		{
			param: "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64",
			want: ec2DescribedImage{
				Name: "al2023-ami-kernel-default-arm64", Architecture: "arm64",
				PlatformDetails: "Linux/UNIX", UsageOperation: "RunInstances",
				RootDeviceName: "/dev/xvda", OwnerAlias: "amazon",
			},
		},
		{
			param: "/aws/service/ami-amazon-linux-latest/al2023-ami-minimal-kernel-default-x86_64",
			want: ec2DescribedImage{
				Name: "al2023-ami-minimal-kernel-default-x86_64", Architecture: "x86_64",
				PlatformDetails: "Linux/UNIX", UsageOperation: "RunInstances",
				RootDeviceName: "/dev/xvda", OwnerAlias: "amazon",
			},
		},
		{
			param: "/aws/service/ami-amazon-linux-latest/al2023-ami-minimal-kernel-default-arm64",
			want: ec2DescribedImage{
				Name: "al2023-ami-minimal-kernel-default-arm64", Architecture: "arm64",
				PlatformDetails: "Linux/UNIX", UsageOperation: "RunInstances",
				RootDeviceName: "/dev/xvda", OwnerAlias: "amazon",
			},
		},
		{
			param: "/aws/service/ami-windows-latest/Windows_Server-2022-English-Full-Base",
			want: ec2DescribedImage{
				Name: "Windows_Server-2022-English-Full-Base", Architecture: "x86_64",
				Platform: "windows", PlatformDetails: "Windows",
				UsageOperation: "RunInstances:0002",
				RootDeviceName: "/dev/sda1", OwnerAlias: "amazon",
			},
		},
		{
			param: "/aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id",
			want: ec2DescribedImage{
				Name: "amazon-linux-2023-ecs-optimized", Architecture: "x86_64",
				PlatformDetails: "Linux/UNIX", UsageOperation: "RunInstances",
				RootDeviceName: "/dev/xvda", OwnerAlias: "amazon",
			},
		},
		{
			param: "/aws/service/ecs/optimized-ami/amazon-linux-2/recommended/image_id",
			want: ec2DescribedImage{
				Name: "amazon-linux-2-ecs-optimized", Architecture: "x86_64",
				PlatformDetails: "Linux/UNIX", UsageOperation: "RunInstances",
				RootDeviceName: "/dev/xvda", OwnerAlias: "amazon",
			},
		},
		{
			param: "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id",
			want: ec2DescribedImage{
				Name: "ubuntu-noble-24.04-amd64-server", Architecture: "x86_64",
				PlatformDetails: "Linux/UNIX", UsageOperation: "RunInstances",
				RootDeviceName: "/dev/sda1",
			},
		},
		{
			param: "/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id",
			want: ec2DescribedImage{
				Name: "ubuntu-jammy-22.04-amd64-server", Architecture: "x86_64",
				PlatformDetails: "Linux/UNIX", UsageOperation: "RunInstances",
				RootDeviceName: "/dev/sda1",
			},
		},
	}
	require.Len(t, tests, len(ec2BundledParameters),
		"every bundled parameter needs a row here, or a new catalog entry ships unasserted")

	for _, tc := range tests {
		t.Run(tc.param, func(t *testing.T) {
			id := emulator.BundledImageID(ec2TestRegion, tc.param)
			require.NotEmpty(t, id)
			got := ec2DescribeOneImage(t, ts, id)

			assert.Equal(t, tc.want.Name, got.Name)
			assert.Equal(t, tc.want.Architecture, got.Architecture)
			assert.Equal(t, tc.want.Platform, got.Platform)
			assert.Equal(t, tc.want.PlatformDetails, got.PlatformDetails)
			assert.Equal(t, tc.want.UsageOperation, got.UsageOperation)
			assert.Equal(t, tc.want.RootDeviceName, got.RootDeviceName)
			assert.Equal(t, tc.want.OwnerAlias, got.OwnerAlias)

			// The members every bundled entry shares. They are asserted per row rather
			// than once, because a value that is invariant in the catalog is not
			// invariant in the renderer — the whole point of rendering both passes
			// through one closure is that these hold for a registered AMI too.
			assert.Equal(t, id, got.ImageID)
			assert.Equal(t, "available", got.State)
			assert.Equal(t, "ebs", got.RootDeviceType)
			assert.Equal(t, "hvm", got.VirtualizationType)
			assert.Equal(t, "xen", got.Hypervisor)
			assert.Equal(t, "machine", got.ImageType)
			assert.True(t, got.Public, "a bundled AMI resolved from a public parameter is public")
			assert.Equal(t, tc.param, got.PublicSsmParameterName)

			// No owner. A public AWS-owned AMI is not owned by the caller and substrate
			// holds no account for the "amazon" alias, so claiming the requesting
			// account would make an Owners=self describe match an image it does not own.
			assert.Empty(t, got.OwnerID)
		})
	}
}

// TestEC2_DescribeImages_OmitsRatherThanEmpties covers the three members whose absence is
// the answer, which unmarshalling cannot distinguish from an empty string.
//
// platform is AWS's own rule — "set to windows for Windows AMIs; otherwise, it is blank" —
// and imageOwnerId is substrate's, for a bundled image with no owner to name. Through
// v0.108.0 the owner element was rendered empty, because the member had no omitempty and
// [bundledImage.image]'s own comment claimed nothing rendered it at all.
func TestEC2_DescribeImages_OmitsRatherThanEmpties(t *testing.T) {
	ts := newEC2TestServer(t)

	linux := ec2DescribeImagesRaw(t, ts, map[string]string{"ImageId.1": ec2TestImage})
	assert.NotContains(t, linux, "<platform>",
		"a Linux AMI must omit platform, not render it empty")
	assert.NotContains(t, linux, "<imageOwnerId>",
		"a bundled AMI has no owner substrate can name")
	assert.Contains(t, linux, "<isPublic>true</isPublic>",
		"isPublic is a boolean AWS always renders")
	assert.NotContains(t, linux, "<ownerId>",
		"ownerId is Example 1's spelling; the Image type page says imageOwnerId")
	assert.NotContains(t, linux, "<public>",
		"public is Example 1's spelling; the Image type page says isPublic")

	windows := ec2DescribeImagesRaw(t, ts, map[string]string{"ImageId.1": ec2TestImageWindows})
	assert.Contains(t, windows, "<platform>windows</platform>",
		"lowercase, per the member's own prose, Example 2 and the platform filter")
	assert.NotContains(t, windows, "<platform>Windows</platform>",
		"the Valid Values line's casing is the one AWS's own examples contradict")
}

// TestEC2_DescribeImages_SSMParameterNameRoundTrips is the round trip AWS added
// publicSsmParameterName for: from a parameter to an AMI through GetParameter, and back to
// the same parameter name through DescribeImages.
//
// Substrate can answer it exactly, because the parameter name is the catalog's own key — so
// this asserts two plugins agree about one string rather than that either formats it
// plausibly.
func TestEC2_DescribeImages_SSMParameterNameRoundTrips(t *testing.T) {
	ssm := newSSMTestServer(t)
	ec2 := newEC2TestServer(t)

	for _, param := range ec2BundledParameters {
		t.Run(param, func(t *testing.T) {
			resp := ssmRequest(t, ssm, "GetParameter", map[string]interface{}{"Name": param})
			require.Equal(t, http.StatusOK, resp.StatusCode)
			parameter, ok := readSSMBody(t, resp)["Parameter"].(map[string]interface{})
			require.True(t, ok, "GetParameter %s returned no Parameter member", param)
			imageID, ok := parameter["Value"].(string)
			require.True(t, ok, "GetParameter %s returned no Value", param)

			got := ec2DescribeOneImage(t, ec2, imageID)
			assert.Equal(t, param, got.PublicSsmParameterName,
				"the parameter that resolved the AMI must be the one the AMI reports")
		})
	}
}

// TestEC2_RegisterImage_MembersComeFromTheRequest is #750's other half: a caller-registered
// AMI reports what the caller sent, and reports nothing where a bundled default would have
// been wrong.
//
// The bundled defaults are the hazard the shared renderer creates — both passes go through
// one closure, so a value written there for the catalog's benefit would attach to a
// registered image too. publicSsmParameterName and imageOwnerAlias are the two that would
// be actively false: no public parameter names a caller's own AMI and Amazon has not
// aliased it.
func TestEC2_RegisterImage_MembersComeFromTheRequest(t *testing.T) {
	ts := newEC2TestServer(t)

	ec2FleetXML(t, ts, map[string]string{
		"Action":                              "RegisterImage",
		"Name":                                "members-from-the-request",
		"Description":                         "registered by the caller",
		"Architecture":                        "arm64",
		"VirtualizationType":                  "hvm",
		"RootDeviceName":                      "/dev/xvda",
		"BlockDeviceMapping.1.DeviceName":     "/dev/xvda",
		"BlockDeviceMapping.1.Ebs.VolumeSize": "30",
	}, nil)

	images := ec2DescribeImages(t, ts, nil)
	require.Len(t, images, 1)
	got := images[0]

	assert.Equal(t, "arm64", got.Architecture, "not the i386 default, and not a bundled x86_64")
	assert.Equal(t, "hvm", got.VirtualizationType, "not the paravirtual default")
	assert.Equal(t, "/dev/xvda", got.RootDeviceName)
	assert.Equal(t, "ebs", got.RootDeviceType, "no ImageLocation means the EBS registration form")
	assert.Equal(t, "machine", got.ImageType)
	assert.Equal(t, "xen", got.Hypervisor)
	assert.Equal(t, "available", got.State)
	assert.NotEmpty(t, got.OwnerID, "a registered AMI is owned by the account that registered it")

	assert.Empty(t, got.PublicSsmParameterName, "no public parameter names a caller's own AMI")
	assert.Empty(t, got.OwnerAlias, "Amazon has not aliased a caller's own AMI")
	assert.Empty(t, got.Platform, "RegisterImage takes no Platform parameter")
	assert.Empty(t, got.PlatformDetails, "billing details substrate has no product code to derive")
	assert.Empty(t, got.UsageOperation, "the billing code substrate has no product code to derive")
	assert.False(t, got.Public, "a newly registered AMI has no public launch permissions")

	raw := ec2DescribeImagesRaw(t, ts, nil)
	assert.NotContains(t, raw, "<publicSsmParameterName>")
	assert.NotContains(t, raw, "<imageOwnerAlias>")
	assert.Contains(t, raw, "<isPublic>false</isPublic>")
}

// TestEC2_RegisterImage_UsesAWSDocumentedDefaults pins the two defaults that will look wrong
// to anyone reading them, so nobody "fixes" them without reading this.
//
// AWS documents Architecture as "Default: For Amazon EBS-backed AMIs, i386" and
// VirtualizationType as "Default: paravirtual". No AMI a caller would actually register in
// 2026 is either. Substrate uses them anyway: a caller that omits the parameter here gets
// the answer AWS gives, and inventing x86_64/hvm would mean a template that passes in
// substrate and reports something else on AWS — the divergence this project exists not to
// make.
func TestEC2_RegisterImage_UsesAWSDocumentedDefaults(t *testing.T) {
	ts := newEC2TestServer(t)

	ec2FleetXML(t, ts, map[string]string{
		"Action": "RegisterImage",
		"Name":   "defaults-are-aws-defaults",
	}, nil)

	images := ec2DescribeImages(t, ts, nil)
	require.Len(t, images, 1)
	assert.Equal(t, "i386", images[0].Architecture)
	assert.Equal(t, "paravirtual", images[0].VirtualizationType)
}

// TestEC2_RegisterImage_ImageLocationIsTheInstanceStoreForm covers the one signal AWS uses
// to distinguish the two registration forms: ImageLocation is "the full path to your AMI
// manifest in Amazon S3 storage", which is how an instance-store-backed AMI is registered,
// while the EBS form sends a root device and a mapping instead.
func TestEC2_RegisterImage_ImageLocationIsTheInstanceStoreForm(t *testing.T) {
	ts := newEC2TestServer(t)

	ec2FleetXML(t, ts, map[string]string{
		"Action":        "RegisterImage",
		"Name":          "from-a-manifest",
		"ImageLocation": "my-bucket/my-ami.manifest.xml",
	}, nil)

	images := ec2DescribeImages(t, ts, nil)
	require.Len(t, images, 1)
	assert.Equal(t, "instance-store", images[0].RootDeviceType)
}

// TestEC2_CreateImage_InheritsFromItsSourceAMI asserts an AMI made from an instance reports
// the operating system's own members rather than nothing.
//
// An AMI created from an arm64 Amazon Linux instance runs arm64 Amazon Linux, so reporting
// no architecture while its parent reported arm64 would be two of substrate's own answers
// disagreeing about one lineage. What is *not* inherited matters as much: the new AMI belongs
// to the caller, so it carries no owner alias, no public parameter and no public launch
// permission.
func TestEC2_CreateImage_InheritsFromItsSourceAMI(t *testing.T) {
	ts := newEC2TestServer(t)

	var launched struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action":  "RunInstances",
		"ImageId": ec2TestImageArm,
	}, &launched)
	require.Len(t, launched.Instances, 1)

	var created struct {
		ImageID string `xml:"imageId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action":     "CreateImage",
		"InstanceId": launched.Instances[0].InstanceID,
		"Name":       "imaged-from-arm64",
	}, &created)
	require.NotEmpty(t, created.ImageID)

	got := ec2DescribeOneImage(t, ts, created.ImageID)
	assert.Equal(t, "arm64", got.Architecture, "inherited from the AMI the instance runs")
	assert.Equal(t, "hvm", got.VirtualizationType)
	assert.Equal(t, "Linux/UNIX", got.PlatformDetails)
	assert.Equal(t, "RunInstances", got.UsageOperation)
	assert.Equal(t, "/dev/xvda", got.RootDeviceName)
	assert.Equal(t, "ebs", got.RootDeviceType, "CreateImage always materializes a snapshot")

	assert.Empty(t, got.OwnerAlias, "the new AMI is the caller's, not Amazon's")
	assert.Empty(t, got.PublicSsmParameterName, "no public parameter names it")
	assert.False(t, got.Public)
	assert.NotEmpty(t, got.OwnerID)

	// The fabricated root mapping follows the inherited device name, so the AMI's
	// rootDeviceName member and its blockDeviceMapping cannot contradict each other.
	mappings := ec2ImageMappings(t, ts, created.ImageID)
	require.Len(t, mappings, 1)
	assert.Equal(t, "/dev/xvda", mappings[0].DeviceName)
}

// TestEC2_CreateImage_WindowsInheritsItsPlatform is the Windows half of the inheritance,
// separate because platform is the one member whose *absence* is meaningful — a test that
// only imaged a Linux instance would pass with the field never assigned at all.
func TestEC2_CreateImage_WindowsInheritsItsPlatform(t *testing.T) {
	ts := newEC2TestServer(t)

	var launched struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action":  "RunInstances",
		"ImageId": ec2TestImageWindows,
	}, &launched)
	require.Len(t, launched.Instances, 1)

	var created struct {
		ImageID string `xml:"imageId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action":     "CreateImage",
		"InstanceId": launched.Instances[0].InstanceID,
		"Name":       "imaged-from-windows",
	}, &created)

	got := ec2DescribeOneImage(t, ts, created.ImageID)
	assert.Equal(t, "windows", got.Platform)
	assert.Equal(t, "Windows", got.PlatformDetails)
	assert.Equal(t, "RunInstances:0002", got.UsageOperation)
	assert.Equal(t, "x86_64", got.Architecture)
}

// TestEC2_DescribeImages_FiltersNarrowOnTheNewMembers is the reason the members and the
// filters are one change.
//
// Each name below was in [ec2ImageFilterSpec]'s accepted list — named without narrowing —
// so before #750 every one of these requests answered with the whole catalog and a caller
// read that as a match. That is worse than a refusal, which is the argument #731 made about
// ImageId.N and the same one applies here.
//
// Each row filters over four bundled AMIs the request names explicitly, because a bundled
// image is reachable only by ID (#733); the filter then narrows within that set.
func TestEC2_DescribeImages_FiltersNarrowOnTheNewMembers(t *testing.T) {
	ts := newEC2TestServer(t)

	named := map[string]string{
		"ImageId.1": ec2TestImage,        // AL2023 x86_64, /dev/xvda, amazon
		"ImageId.2": ec2TestImageArm,     // AL2023 arm64,  /dev/xvda, amazon
		"ImageId.3": ec2TestImageWindows, // Windows,       /dev/sda1, amazon
		"ImageId.4": ec2TestImageUbuntu,  // Ubuntu 24.04,  /dev/sda1, no alias
	}

	tests := []struct {
		name   string
		filter string
		value  string
		want   []string
	}{
		{"architecture selects one", "architecture", "arm64", []string{ec2TestImageArm}},
		{"architecture selects the rest", "architecture", "x86_64", []string{ec2TestImage, ec2TestImageWindows, ec2TestImageUbuntu}},
		{"platform selects the Windows AMI", "platform", "windows", []string{ec2TestImageWindows}},
		{"root-device-name splits the four", "root-device-name", "/dev/xvda", []string{ec2TestImage, ec2TestImageArm}},
		{"root-device-type matches all four", "root-device-type", "ebs", []string{ec2TestImage, ec2TestImageArm, ec2TestImageWindows, ec2TestImageUbuntu}},
		{"virtualization-type matches all four", "virtualization-type", "hvm", []string{ec2TestImage, ec2TestImageArm, ec2TestImageWindows, ec2TestImageUbuntu}},
		{"a virtualization type none of them has", "virtualization-type", "paravirtual", nil},
		{"hypervisor", "hypervisor", "xen", []string{ec2TestImage, ec2TestImageArm, ec2TestImageWindows, ec2TestImageUbuntu}},
		{"image-type", "image-type", "machine", []string{ec2TestImage, ec2TestImageArm, ec2TestImageWindows, ec2TestImageUbuntu}},
		{"is-public true", "is-public", "true", []string{ec2TestImage, ec2TestImageArm, ec2TestImageWindows, ec2TestImageUbuntu}},
		{"is-public false excludes every public AMI", "is-public", "false", nil},
		{"owner-alias excludes Canonical", "owner-alias", "amazon", []string{ec2TestImage, ec2TestImageArm, ec2TestImageWindows}},
		{"name", "name", "al2023-ami-kernel-default-arm64", []string{ec2TestImageArm}},
		{"name with a wildcard", "name", "ubuntu-*", []string{ec2TestImageUbuntu}},
		{"description", "description", "Amazon Linux 2023 AMI, kernel default, arm64", []string{ec2TestImageArm}},
		{"state", "state", "available", []string{ec2TestImage, ec2TestImageArm, ec2TestImageWindows, ec2TestImageUbuntu}},
		{"state matches nothing", "state", "pending", nil},
		{
			"public-ssm-parameter-name", "public-ssm-parameter-name",
			"/aws/service/ami-windows-latest/Windows_Server-2022-English-Full-Base",
			[]string{ec2TestImageWindows},
		},
		{
			"owner-id excludes every bundled AMI, which has no owner",
			"owner-id", "123456789012", nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]string{"Filter.1.Name": tc.filter, "Filter.1.Value.1": tc.value}
			for k, v := range named {
				params[k] = v
			}
			var ids []string
			for _, img := range ec2DescribeImages(t, ts, params) {
				ids = append(ids, img.ImageID)
			}
			assert.ElementsMatch(t, tc.want, ids)
		})
	}
}

// TestEC2_DescribeImages_FilterOnARegisteredImage covers the members only a registered AMI
// has, which the bundled table above cannot reach: an owner ID, and the i386/paravirtual
// defaults.
func TestEC2_DescribeImages_FilterOnARegisteredImage(t *testing.T) {
	ts := newEC2TestServer(t)

	ec2FleetXML(t, ts, map[string]string{"Action": "RegisterImage", "Name": "filterable"}, nil)
	all := ec2DescribeImages(t, ts, nil)
	require.Len(t, all, 1)
	owner := all[0].OwnerID
	require.NotEmpty(t, owner)

	tests := []struct {
		name   string
		filter string
		value  string
		want   int
	}{
		{"the account's own ID matches", "owner-id", owner, 1},
		{"another account's does not", "owner-id", "999999999999", 0},
		{"the documented architecture default", "architecture", "i386", 1},
		{"not the architecture a modern caller assumes", "architecture", "x86_64", 0},
		{"the documented virtualization default", "virtualization-type", "paravirtual", 1},
		{"is-public false", "is-public", "false", 1},
		{"is-public true", "is-public", "true", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ec2DescribeImages(t, ts, map[string]string{
				"Filter.1.Name":    tc.filter,
				"Filter.1.Value.1": tc.value,
			})
			assert.Len(t, got, tc.want)
		})
	}
}
