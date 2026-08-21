package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #693: CreateLaunchTemplate and CreateLaunchTemplateVersion render the `warning`
// member AWS documents for "parameters or parameter combinations that are not valid".
//
// v0.105.0 taught RunInstances to refuse an invalid block device mapping and left both
// create operations accepting one, because neither documents an error for it — so the
// mapping was swallowed in silence and a caller learned about it only at launch, from a
// template it had already shipped. These tests pin that the same diagnosis now arrives
// through both doors, and that a valid template's response carries no warning element at
// all.

// ltWarningError is one ValidationError within a warning.
type ltWarningError struct {
	Code    string `xml:"code"`
	Message string `xml:"message"`
}

// ltWarningResponse decodes just enough of either create operation's response to read
// the template it made and the warning it attached.
//
// The warning is a pointer so an absent element is distinguishable from a present-but-
// empty one — which is the whole of what the pointer field on the response struct is
// for, and would be untestable through a value here.
type ltWarningResponse struct {
	LaunchTemplateID string `xml:"launchTemplate>launchTemplateId"`
	VersionNumber    int64  `xml:"launchTemplateVersion>versionNumber"`
	Warning          *struct {
		Errors []ltWarningError `xml:"errorSet>item"`
	} `xml:"warning"`
}

// ltWarningRequest sends one launch-template request and returns the decoded response
// beside the raw body, which is what an assertion about an *absent* element has to read.
func ltWarningRequest(t *testing.T, ts *httptest.Server, params map[string]string) (ltWarningResponse, string) {
	t.Helper()
	resp := ec2Request(t, ts, params)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	var out ltWarningResponse
	require.NoError(t, xml.Unmarshal(body, &out), string(body))
	return out, string(body)
}

// TestEC2_CreateLaunchTemplate_WarnsAboutAnInvalidMapping pins that a template carrying
// a mapping RunInstances refuses is created and reported through `warning`, with the
// same code and message the refusal carries.
//
// The message equality is the point of the assertion, not decoration: the warning and
// the refusal are built by the same collector, so a caller diagnosing a template through
// CreateLaunchTemplate reads exactly what it would have read from the launch. Two
// independent message strings would drift the moment one of them was reworded.
func TestEC2_CreateLaunchTemplate_WarnsAboutAnInvalidMapping(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	out, body := ltWarningRequest(t, ts, map[string]string{
		"Action":                     "CreateLaunchTemplate",
		"LaunchTemplateName":         "warned",
		"LaunchTemplateData.ImageId": "ami-0abcdef1234567890",
		"LaunchTemplateData.BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"LaunchTemplateData.BlockDeviceMapping.1.Ebs.VolumeType": "gp3",
	})
	require.NotEmpty(t, out.LaunchTemplateID, "the template is created, not refused")
	require.NotNil(t, out.Warning, "an invalid mapping is reported through warning: %s", body)
	require.Len(t, out.Warning.Errors, 1)
	assert.Equal(t, "InvalidBlockDeviceMapping", out.Warning.Errors[0].Code)

	// The same mapping, reached through a launch, produces the identical diagnosis.
	_, code, msg := ec2ErrorDetail(t, ts, map[string]string{
		"Action":                            "RunInstances",
		"MinCount":                          "1",
		"MaxCount":                          "1",
		"LaunchTemplate.LaunchTemplateName": "warned",
	})
	assert.Equal(t, code, out.Warning.Errors[0].Code)
	assert.Equal(t, msg, out.Warning.Errors[0].Message,
		"the warning and the refusal are the same diagnosis")
}

// TestEC2_CreateLaunchTemplate_WarnsAboutEveryProblem pins that the warning reports
// every problem rather than the first.
//
// This is why the collector had to become the primitive and the first-error function its
// wrapper: ValidationWarning is documented as carrying "an error code and an error
// message […] for each issue that's found", and a list built by calling a first-error
// function once can only ever hold one entry.
func TestEC2_CreateLaunchTemplate_WarnsAboutEveryProblem(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	out, body := ltWarningRequest(t, ts, map[string]string{
		"Action":                     "CreateLaunchTemplate",
		"LaunchTemplateName":         "two-problems",
		"LaunchTemplateData.ImageId": "ami-0abcdef1234567890",
		// Names an Ebs member and neither a size nor a snapshot.
		"LaunchTemplateData.BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"LaunchTemplateData.BlockDeviceMapping.1.Ebs.VolumeType": "gp3",
		// Iops on a type that does not support it.
		"LaunchTemplateData.BlockDeviceMapping.2.DeviceName":     "/dev/sdg",
		"LaunchTemplateData.BlockDeviceMapping.2.Ebs.VolumeSize": "10",
		"LaunchTemplateData.BlockDeviceMapping.2.Ebs.VolumeType": "standard",
		"LaunchTemplateData.BlockDeviceMapping.2.Ebs.Iops":       "100",
	})
	require.NotNil(t, out.Warning, body)
	require.Len(t, out.Warning.Errors, 2, "both mappings are reported: %s", body)
	assert.Contains(t, out.Warning.Errors[0].Message, "/dev/sdf")
	assert.Contains(t, out.Warning.Errors[0].Message, "you must specify either a snapshot ID or a volume size")
	assert.Contains(t, out.Warning.Errors[1].Message, "/dev/sdg")
	assert.Contains(t, out.Warning.Errors[1].Message, "Ebs.Iops is not supported for standard volumes")
}

// TestEC2_CreateLaunchTemplate_ValidTemplateCarriesNoWarning pins the absence of the
// element, which is what makes its presence meaningful.
//
// It reads the raw body because that is the only place the distinction survives: a
// non-pointer struct field would emit `<warning></warning>` on every response —
// encoding/xml ignores omitempty on a struct — and every decoded assertion about a
// present-but-empty warning would still pass.
func TestEC2_CreateLaunchTemplate_ValidTemplateCarriesNoWarning(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	for _, tc := range []struct {
		name   string
		params map[string]string
	}{
		{
			name: "a valid mapping",
			params: map[string]string{
				"LaunchTemplateName": "clean-mapping",
				"LaunchTemplateData.BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
				"LaunchTemplateData.BlockDeviceMapping.1.Ebs.VolumeSize": "20",
			},
		},
		{
			name:   "no mapping at all",
			params: map[string]string{"LaunchTemplateName": "no-mapping"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]string{
				"Action":                     "CreateLaunchTemplate",
				"LaunchTemplateData.ImageId": "ami-0abcdef1234567890",
			}
			for k, v := range tc.params {
				params[k] = v
			}
			out, body := ltWarningRequest(t, ts, params)
			assert.Nil(t, out.Warning)
			assert.NotContains(t, body, "<warning",
				"a valid template's response carries no warning element")
		})
	}
}

// TestEC2_CreateLaunchTemplateVersion_WarnsAboutAnInvalidMapping pins that the second
// door reports too — and that a version inheriting a mapping from its source version is
// warned about it, which is the case a caller cannot see any other way.
//
// The inherited case is also what makes the overlay fix observable: before #693,
// SourceVersion dropped BlockDeviceMappings entirely, so version 2 below carried no
// mapping to warn about and launched an instance with none.
func TestEC2_CreateLaunchTemplateVersion_WarnsAboutAnInvalidMapping(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	v1, _ := ltWarningRequest(t, ts, map[string]string{
		"Action":                     "CreateLaunchTemplate",
		"LaunchTemplateName":         "versioned",
		"LaunchTemplateData.ImageId": "ami-0abcdef1234567890",
		"LaunchTemplateData.BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"LaunchTemplateData.BlockDeviceMapping.1.Ebs.VolumeType": "gp3",
	})
	require.NotNil(t, v1.Warning, "version 1 carries the mapping and the warning")

	for _, tc := range []struct {
		name   string
		params map[string]string
	}{
		{
			name: "the version names the mapping itself",
			params: map[string]string{
				"LaunchTemplateData.ImageId":                             "ami-0abcdef1234567890",
				"LaunchTemplateData.BlockDeviceMapping.1.DeviceName":     "/dev/sdg",
				"LaunchTemplateData.BlockDeviceMapping.1.Ebs.VolumeType": "gp3",
			},
		},
		{
			name: "the version inherits it from SourceVersion",
			params: map[string]string{
				"SourceVersion":              "1",
				"LaunchTemplateData.ImageId": "ami-11112222333344445",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]string{
				"Action":             "CreateLaunchTemplateVersion",
				"LaunchTemplateName": "versioned",
			}
			for k, v := range tc.params {
				params[k] = v
			}
			out, body := ltWarningRequest(t, ts, params)
			require.NotZero(t, out.VersionNumber, "the version is created, not refused")
			require.NotNil(t, out.Warning, body)
			require.Len(t, out.Warning.Errors, 1)
			assert.Equal(t, "InvalidBlockDeviceMapping", out.Warning.Errors[0].Code)
			assert.Contains(t, out.Warning.Errors[0].Message,
				"you must specify either a snapshot ID or a volume size")
		})
	}
}

// TestEC2_CreateLaunchTemplate_WarnsAboutAnUnknownSnapshot pins that the warning reports
// the snapshot rules #689 added, not just the shape rules — the two arrived one PR apart
// and share one collector, so the second door has to see both.
func TestEC2_CreateLaunchTemplate_WarnsAboutAnUnknownSnapshot(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	out, body := ltWarningRequest(t, ts, map[string]string{
		"Action":                     "CreateLaunchTemplate",
		"LaunchTemplateName":         "unknown-snapshot",
		"LaunchTemplateData.ImageId": "ami-0abcdef1234567890",
		"LaunchTemplateData.BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"LaunchTemplateData.BlockDeviceMapping.1.Ebs.SnapshotId": "snap-0123456789abcdef0",
	})
	require.NotNil(t, out.Warning, body)
	require.Len(t, out.Warning.Errors, 1)
	assert.Equal(t, "InvalidSnapshot.NotFound", out.Warning.Errors[0].Code)
	assert.Equal(t, "The snapshot ID 'snap-0123456789abcdef0' does not exist",
		out.Warning.Errors[0].Message)
}

// ltDataMapping is one blockDeviceMappingSet>item of a version's launchTemplateData.
type ltDataMapping struct {
	DeviceName  string  `xml:"deviceName"`
	VirtualName string  `xml:"virtualName"`
	NoDevice    *string `xml:"noDevice"`
	Ebs         *struct {
		SnapshotID          string `xml:"snapshotId"`
		VolumeSize          int    `xml:"volumeSize"`
		VolumeType          string `xml:"volumeType"`
		IOPS                int    `xml:"iops"`
		Throughput          int    `xml:"throughput"`
		Encrypted           *bool  `xml:"encrypted"`
		DeleteOnTermination *bool  `xml:"deleteOnTermination"`
	} `xml:"ebs"`
}

// ltDescribeMappings returns the mappings one version of a template reads back.
func ltDescribeMappings(t *testing.T, ts *httptest.Server, name, version string) ([]ltDataMapping, string) {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{
		"Action":                  "DescribeLaunchTemplateVersions",
		"LaunchTemplateName":      name,
		"LaunchTemplateVersion.1": version,
	})
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	var out struct {
		Versions []struct {
			Mappings []ltDataMapping `xml:"launchTemplateData>blockDeviceMappingSet>item"`
		} `xml:"launchTemplateVersionSet>item"`
	}
	require.NoError(t, xml.Unmarshal(body, &out), string(body))
	require.Len(t, out.Versions, 1, string(body))
	return out.Versions[0].Mappings, string(body)
}

// TestEC2_LaunchTemplateVersion_ReadsBackItsBlockDeviceMappings pins the
// blockDeviceMappingSet member, without which a warned caller cannot read back the
// mapping the warning is about.
//
// DescribeLaunchTemplateVersions is the only operation that returns a template's data at
// all, so an unrendered member is an unreadable parameter: v0.104.0 taught the template
// to parse and store mappings and nothing rendered them, which made the round trip
// silently lossy.
func TestEC2_LaunchTemplateVersion_ReadsBackItsBlockDeviceMappings(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	_, snapID := ec2SnapshotOfSize(t, ts, 30)
	_, body := ltWarningRequest(t, ts, map[string]string{
		"Action":                     "CreateLaunchTemplate",
		"LaunchTemplateName":         "readback",
		"LaunchTemplateData.ImageId": "ami-0abcdef1234567890",
		// A fully specified EBS mapping.
		"LaunchTemplateData.BlockDeviceMapping.1.DeviceName":              "/dev/sdf",
		"LaunchTemplateData.BlockDeviceMapping.1.Ebs.VolumeSize":          "40",
		"LaunchTemplateData.BlockDeviceMapping.1.Ebs.VolumeType":          "gp3",
		"LaunchTemplateData.BlockDeviceMapping.1.Ebs.Iops":                "4000",
		"LaunchTemplateData.BlockDeviceMapping.1.Ebs.Throughput":          "200",
		"LaunchTemplateData.BlockDeviceMapping.1.Ebs.Encrypted":           "true",
		"LaunchTemplateData.BlockDeviceMapping.1.Ebs.DeleteOnTermination": "false",
		"LaunchTemplateData.BlockDeviceMapping.1.Ebs.SnapshotId":          snapID,
		// An instance store device.
		"LaunchTemplateData.BlockDeviceMapping.2.DeviceName":  "/dev/sdb",
		"LaunchTemplateData.BlockDeviceMapping.2.VirtualName": "ephemeral0",
		// A suppression.
		"LaunchTemplateData.BlockDeviceMapping.3.DeviceName": "/dev/sdc",
		"LaunchTemplateData.BlockDeviceMapping.3.NoDevice":   "",
	})
	require.NotContains(t, body, "<warning", "every mapping above is valid")

	mappings, describeBody := ltDescribeMappings(t, ts, "readback", "1")
	require.Len(t, mappings, 3, describeBody)

	ebs := mappings[0]
	assert.Equal(t, "/dev/sdf", ebs.DeviceName)
	require.NotNil(t, ebs.Ebs, describeBody)
	assert.Equal(t, snapID, ebs.Ebs.SnapshotID)
	assert.Equal(t, 40, ebs.Ebs.VolumeSize)
	assert.Equal(t, "gp3", ebs.Ebs.VolumeType)
	assert.Equal(t, 4000, ebs.Ebs.IOPS)
	assert.Equal(t, 200, ebs.Ebs.Throughput)
	require.NotNil(t, ebs.Ebs.Encrypted)
	assert.True(t, *ebs.Ebs.Encrypted)
	// The three-state DeleteOnTermination reads back as the false it was given, rather
	// than as absent — which is the distinction the stored raw string exists for.
	require.NotNil(t, ebs.Ebs.DeleteOnTermination)
	assert.False(t, *ebs.Ebs.DeleteOnTermination)

	store := mappings[1]
	assert.Equal(t, "ephemeral0", store.VirtualName)
	assert.Nil(t, store.Ebs, "an instance store device carries no ebs member")
	assert.Nil(t, store.NoDevice)

	suppressed := mappings[2]
	require.NotNil(t, suppressed.NoDevice, "noDevice is present-and-empty, per AWS")
	assert.Empty(t, *suppressed.NoDevice)
	assert.Nil(t, suppressed.Ebs)
}

// TestEC2_LaunchTemplateVersion_SourceVersionCarriesMappingsAndVolumeTags pins the two
// members SourceVersion inheritance silently dropped.
//
// The drop was invisible in both directions: nothing rendered blockDeviceMappingSet, and
// nothing renders volume tag specifications at all — so the only way to see either was
// to launch from the derived version and look at the volumes it made. That is what this
// test does, which is also the observation a consumer would make.
func TestEC2_LaunchTemplateVersion_SourceVersionCarriesMappingsAndVolumeTags(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	_, body := ltWarningRequest(t, ts, map[string]string{
		"Action":                     "CreateLaunchTemplate",
		"LaunchTemplateName":         "inheriting",
		"LaunchTemplateData.ImageId": "ami-0abcdef1234567890",
		"LaunchTemplateData.BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"LaunchTemplateData.BlockDeviceMapping.1.Ebs.VolumeSize": "25",
		"LaunchTemplateData.TagSpecification.1.ResourceType":     "volume",
		"LaunchTemplateData.TagSpecification.1.Tag.1.Key":        "Backup",
		"LaunchTemplateData.TagSpecification.1.Tag.1.Value":      "nightly",
	})
	require.NotContains(t, body, "<warning", body)

	v2, _ := ltWarningRequest(t, ts, map[string]string{
		"Action":                     "CreateLaunchTemplateVersion",
		"LaunchTemplateName":         "inheriting",
		"SourceVersion":              "1",
		"LaunchTemplateData.ImageId": "ami-11112222333344445",
	})
	require.Equal(t, int64(2), v2.VersionNumber)

	mappings, describeBody := ltDescribeMappings(t, ts, "inheriting", "2")
	require.Len(t, mappings, 1, "version 2 inherits version 1's mapping: %s", describeBody)
	require.NotNil(t, mappings[0].Ebs, describeBody)
	assert.Equal(t, 25, mappings[0].Ebs.VolumeSize)

	// And a launch from version 2 materializes that volume, carrying the inherited
	// volume tags — the only surface either member is otherwise observable through.
	ids := bdmRunInstances(t, ts, map[string]string{
		"LaunchTemplate.LaunchTemplateName": "inheriting",
		"LaunchTemplate.Version":            "2",
	})
	require.Len(t, ids, 1)

	volumes := bdmDescribeVolumes(t, ts, map[string]string{
		"Filter.1.Name":    "attachment.instance-id",
		"Filter.1.Value.1": ids[0],
		"Filter.2.Name":    "attachment.device",
		"Filter.2.Value.1": "/dev/sdf",
	})
	require.Len(t, volumes, 1)
	assert.Equal(t, 25, volumes[0].Size, "the inherited mapping's size, not the 8 GiB default")

	tagged := bdmDescribeVolumes(t, ts, map[string]string{
		"Filter.1.Name":    "attachment.instance-id",
		"Filter.1.Value.1": ids[0],
		"Filter.2.Name":    "tag:Backup",
		"Filter.2.Value.1": "nightly",
	})
	require.NotEmpty(t, tagged, "the inherited volume tag specification is applied")
	for _, v := range tagged {
		assert.NotEqual(t, "", v.VolumeID)
	}
}

// TestEC2_LaunchTemplateVersion_NoSourceVersionInheritsNothing guards the asymmetry the
// overlay fix could have broken: an omitted SourceVersion inherits nothing, so adding
// mappings to the overlay must not make version 2 pick up version 1's.
func TestEC2_LaunchTemplateVersion_NoSourceVersionInheritsNothing(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	_, _ = ltWarningRequest(t, ts, map[string]string{
		"Action":                     "CreateLaunchTemplate",
		"LaunchTemplateName":         "no-inherit",
		"LaunchTemplateData.ImageId": "ami-0abcdef1234567890",
		"LaunchTemplateData.BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"LaunchTemplateData.BlockDeviceMapping.1.Ebs.VolumeSize": "25",
	})
	_, _ = ltWarningRequest(t, ts, map[string]string{
		"Action":                     "CreateLaunchTemplateVersion",
		"LaunchTemplateName":         "no-inherit",
		"LaunchTemplateData.ImageId": "ami-11112222333344445",
	})

	mappings, body := ltDescribeMappings(t, ts, "no-inherit", "2")
	assert.Empty(t, mappings,
		"a version naming no SourceVersion holds only what its own request said: %s", body)
	// The container element itself is still emitted, empty — encoding/xml writes the
	// parent of an `a>b` path unconditionally, so omitempty on the slice cannot suppress
	// it. That is what securityGroupIdSet, networkInterfaceSet and tagSpecificationSet
	// have always done in this same struct, so the new member matches its siblings rather
	// than being the one that disappears.
	assert.Contains(t, body, "<blockDeviceMappingSet></blockDeviceMappingSet>")
}
