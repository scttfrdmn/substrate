package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ec2TestRegion is the Region every request in this package's EC2 tests is addressed to —
// [ec2Request] sets Host to ec2.us-east-1.amazonaws.com — and therefore the Region the
// bundled AMI IDs below are derived for. An AMI ID names an image in exactly one Region on
// AWS, and substrate's derived IDs are Region-scoped for the same reason, so a test that
// addresses another Region must derive its own.
const ec2TestRegion = "us-east-1"

// The bundled AMIs this package's launches name.
//
// RunInstances refuses an AMI that names nothing (#733), so a test that launches has to
// name one that exists. Almost none of them are *about* the AMI, and for those any of
// these will do; the several that assert one AMI beat another — a request's over a
// template's, a fleet override's over both — need two or more that differ, which is why
// there is a set rather than one constant.
//
// Each is derived from an SSM public parameter, so a test naming one of these names exactly
// what a consumer discovers through GetParameter. Deliberately absent: AWS's own example
// IDs, ami-0abcdef1234567890 and ami-1234567890abcdef0. Substrate refuses those, real AWS
// refuses those, and TestEC2_BundledImages_DocumentationExamplesAreNotBundled pins it.
var (
	ec2TestImage           = emulator.BundledImageID(ec2TestRegion, "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64")
	ec2TestImageArm        = emulator.BundledImageID(ec2TestRegion, "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64")
	ec2TestImageMinimal    = emulator.BundledImageID(ec2TestRegion, "/aws/service/ami-amazon-linux-latest/al2023-ami-minimal-kernel-default-x86_64")
	ec2TestImageMinimalArm = emulator.BundledImageID(ec2TestRegion, "/aws/service/ami-amazon-linux-latest/al2023-ami-minimal-kernel-default-arm64")
	ec2TestImageWindows    = emulator.BundledImageID(ec2TestRegion, "/aws/service/ami-windows-latest/Windows_Server-2022-English-Full-Base")
	ec2TestImageECS        = emulator.BundledImageID(ec2TestRegion, "/aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id")
	ec2TestImageECS2       = emulator.BundledImageID(ec2TestRegion, "/aws/service/ecs/optimized-ami/amazon-linux-2/recommended/image_id")
	ec2TestImageUbuntu     = emulator.BundledImageID(ec2TestRegion, "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id")
	ec2TestImageUbuntu22   = emulator.BundledImageID(ec2TestRegion, "/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id")
)

// ec2BundledParameters is every parameter the vars above name, in the same order, so a test
// can assert a property of the whole catalog without restating the paths.
var ec2BundledParameters = []string{
	"/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64",
	"/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64",
	"/aws/service/ami-amazon-linux-latest/al2023-ami-minimal-kernel-default-x86_64",
	"/aws/service/ami-amazon-linux-latest/al2023-ami-minimal-kernel-default-arm64",
	"/aws/service/ami-windows-latest/Windows_Server-2022-English-Full-Base",
	"/aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id",
	"/aws/service/ecs/optimized-ami/amazon-linux-2/recommended/image_id",
	"/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id",
	"/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id",
}

// TestEC2_BundledImages_IDsAreWellFormedAndDistinct pins the two properties every other
// test in this file leans on: a bundled ID passes substrate's own AMI-ID syntax rule, and
// no two catalog entries collide.
//
// A collision would be silent and would make a precedence test pass for the wrong reason —
// "the request's AMI won" is unfalsifiable when both AMIs are the same string.
func TestEC2_BundledImages_IDsAreWellFormedAndDistinct(t *testing.T) {
	seen := map[string]string{}
	for _, param := range ec2BundledParameters {
		id := emulator.BundledImageID(ec2TestRegion, param)
		require.NotEmpty(t, id, "no bundled image for %s", param)
		assert.True(t, strings.HasPrefix(id, "ami-"), "%s: %q has no ami- prefix", param, id)
		assert.Len(t, id, len("ami-")+17, "%s: %q is not AWS's 17-hex-character form", param, id)
		for i := len("ami-"); i < len(id); i++ {
			c := id[i]
			require.True(t, (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'),
				"%s: %q is not lowercase hex at index %d", param, id, i)
		}
		if other, dup := seen[id]; dup {
			t.Errorf("%s and %s both derive %s", param, other, id)
		}
		seen[id] = param
	}
}

// TestEC2_BundledImages_RegionScoped asserts an AMI ID means one Region, as on AWS, and
// that it is stable across calls.
//
// Stability is what lets a fixture hardcode the value a previous run printed; Region
// scoping is what stops a template that launches in eu-west-1 from succeeding with an ID
// substrate resolves only in us-east-1, which is the divergence #733 exists to remove.
func TestEC2_BundledImages_RegionScoped(t *testing.T) {
	const param = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"

	east := emulator.BundledImageID("us-east-1", param)
	west := emulator.BundledImageID("eu-west-1", param)
	require.NotEmpty(t, east)
	require.NotEmpty(t, west)
	assert.NotEqual(t, east, west, "one AMI ID cannot name an image in two Regions")
	assert.Equal(t, east, emulator.BundledImageID("us-east-1", param), "resolution is not stable")
}

// TestEC2_BundledImages_UnlistedPathResolvesToItsFamily covers the fallback that keeps
// GetParameter and RunInstances from contradicting each other.
//
// Substrate answers every /aws/service/ path that looks like an AMI, and always has, so a
// path the catalog does not list must still resolve to an image that launches — otherwise
// substrate hands out an AMI and then refuses it. A path that is not an AMI path at all
// resolves to nothing, which is what makes this a fallback rather than a catch-all.
func TestEC2_BundledImages_UnlistedPathResolvesToItsFamily(t *testing.T) {
	tests := []struct {
		name  string
		param string
		want  string
	}{
		{"an unlisted Windows release", "/aws/service/ami-windows-latest/Windows_Server-2019-English-Full-Base", ec2TestImageWindows},
		{"an unlisted Ubuntu release", "/aws/service/canonical/ubuntu/server/20.04/stable/current/amd64/hvm/ebs-gp2/ami-id", ec2TestImageUbuntu},
		{"an unlisted ECS variant", "/aws/service/ecs/optimized-ami/amazon-linux-2023/gpu/recommended/image_id", ec2TestImageECS},
		{"an unlisted Amazon Linux kernel", "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-6.1-x86_64", ec2TestImage},
		{"a listed path is itself, not its family's default", "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64", ec2TestImageArm},
		{"a listed release is itself, not its family's newest", "/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id", ec2TestImageUbuntu22},
		{"not an AMI path", "/aws/service/datasync/agent", ""},
		{"not a managed path at all", "/my/own/parameter", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, emulator.BundledImageID(ec2TestRegion, tc.param))
		})
	}
}

// TestEC2_BundledImages_LaunchAndRefusal is the operation #733 was filed against.
//
// A bundled AMI launches; a well-formed AMI ID that names nothing is InvalidAMIID.NotFound;
// something that is not an AMI ID is InvalidAMIID.Malformed, and syntax is reported before
// absence. All four codes and the order come from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html.
func TestEC2_BundledImages_LaunchAndRefusal(t *testing.T) {
	ts := newEC2TestServer(t)

	tests := []struct {
		name     string
		imageID  string
		wantCode string
	}{
		{"a bundled AMI launches", ec2TestImage, ""},
		{"a bundled AMI from another family launches", ec2TestImageWindows, ""},
		{"a well-formed AMI that names nothing", "ami-0dddddddddddddddd", "InvalidAMIID.NotFound"},
		{"an AMI ID from another Region", emulator.BundledImageID("eu-west-1", ec2BundledParameters[0]), "InvalidAMIID.NotFound"},
		{"not an AMI ID at all", "not-an-ami", "InvalidAMIID.Malformed"},
		{"the right prefix, the wrong alphabet", "ami-zzzzzzzz", "InvalidAMIID.Malformed"},
		{"a placeholder an AI left behind", "ami-EXAMPLE", "InvalidAMIID.Malformed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := ec2Request(t, ts, map[string]string{
				"Action":  "RunInstances",
				"ImageId": tc.imageID,
			})
			defer resp.Body.Close() //nolint:errcheck
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			if tc.wantCode == "" {
				assert.Equal(t, http.StatusOK, resp.StatusCode, "body was %s", body)
				return
			}
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "body was %s", body)
			assert.Contains(t, string(body), tc.wantCode)
		})
	}
}

// TestEC2_BundledImages_MalformedBeatsNotFound pins the ordering separately, because both
// refusals are 400s and a test that only checked the status would not notice the two codes
// swapping.
func TestEC2_BundledImages_MalformedBeatsNotFound(t *testing.T) {
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{
		"Action":  "RunInstances",
		"ImageId": "ami-nope",
	})
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "InvalidAMIID.Malformed")
	assert.NotContains(t, string(body), "InvalidAMIID.NotFound")
}

// TestEC2_BundledImages_DocumentationExamplesAreNotBundled is the membership tripwire.
//
// AWS's reference pages use ami-0abcdef1234567890 and ami-1234567890abcdef0 as example IDs,
// and generated IaC copies them. Both must be refused: the reason to refuse an unknown AMI
// at all is that a launch which works in substrate should work on real AWS, and bundling a
// doc example — however convenient for the fixtures in this package — would reintroduce
// exactly the divergence #733 was filed to remove.
func TestEC2_BundledImages_DocumentationExamplesAreNotBundled(t *testing.T) {
	ts := newEC2TestServer(t)

	for _, imageID := range []string{"ami-0abcdef1234567890", "ami-1234567890abcdef0"} {
		t.Run(imageID, func(t *testing.T) {
			resp := ec2Request(t, ts, map[string]string{"Action": "RunInstances", "ImageId": imageID})
			defer resp.Body.Close() //nolint:errcheck
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "body was %s", body)
			assert.Contains(t, string(body), "InvalidAMIID.NotFound")
		})
	}
}

// TestEC2_BundledImages_SSMDiscoveryThenLaunch is the coherence check between two plugins:
// the AMI GetParameter hands out is the AMI RunInstances accepts.
//
// Those were independent computations through v0.107.0 — the SSM resolver hashed the
// parameter name, and nothing on the EC2 side had any notion of an AMI existing — so nothing
// could have caught them diverging. Once one refuses, the other's answer has to be
// resolvable, and this asserts that over the whole catalog rather than trusting the shared
// helper both now call. Both servers are addressed to [ec2TestRegion], which is what makes
// the two IDs comparable at all.
func TestEC2_BundledImages_SSMDiscoveryThenLaunch(t *testing.T) {
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
			assert.Equal(t, emulator.BundledImageID(ec2TestRegion, param), imageID,
				"GetParameter and BundledImageID disagree about %s", param)

			launch := ec2Request(t, ec2, map[string]string{
				"Action":  "RunInstances",
				"ImageId": imageID,
			})
			defer launch.Body.Close() //nolint:errcheck
			body, err := io.ReadAll(launch.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, launch.StatusCode,
				"RunInstances from the discovered AMI: %s", body)

			var launched struct {
				Instances []struct {
					ImageID string `xml:"imageId"`
				} `xml:"instancesSet>item"`
			}
			require.NoError(t, xml.Unmarshal(body, &launched))
			require.Len(t, launched.Instances, 1)
			assert.Equal(t, imageID, launched.Instances[0].ImageID)
		})
	}
}
