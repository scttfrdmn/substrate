package emulator_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Launch-template MetadataOptions and the read-back of only what a template carries (#1471),
// and the launch-template not-found refusals (#1469).

// ltMetadataXML is LaunchTemplateInstanceMetadataOptions or InstanceMetadataOptionsResponse as a
// caller decodes it. Pointers, so an absent member is told from a zero one.
type ltMetadataXML struct {
	State                   *string `xml:"state"`
	HTTPTokens              *string `xml:"httpTokens"`
	HTTPPutResponseHopLimit *int    `xml:"httpPutResponseHopLimit"`
	HTTPEndpoint            *string `xml:"httpEndpoint"`
	HTTPProtocolIPv6        *string `xml:"httpProtocolIpv6"`
	InstanceMetadataTags    *string `xml:"instanceMetadataTags"`
}

func strp(s string) *string { return &s }
func intp(n int) *int       { return &n }

// ltCreate creates a launch template from params and returns its ID.
func ltCreate(t *testing.T, ts *httptest.Server, name string, params map[string]string) string {
	t.Helper()
	full := map[string]string{"Action": "CreateLaunchTemplate", "LaunchTemplateName": name}
	for k, v := range params {
		full[k] = v
	}
	var out struct {
		ID string `xml:"launchTemplate>launchTemplateId"`
	}
	ec2DescribeXML(t, ts, full, &out)
	require.NotEmpty(t, out.ID)
	return out.ID
}

// ltVersionBody returns the raw DescribeLaunchTemplateVersions body for one version.
func ltVersionBody(t *testing.T, ts *httptest.Server, name, version string) string {
	t.Helper()
	return ec2DescribeBody(t, ts, map[string]string{
		"Action": "DescribeLaunchTemplateVersions", "LaunchTemplateName": name, "LaunchTemplateVersion.1": version,
	})
}

// ltVersionMetadata decodes one version's metadataOptions, nil when absent.
func ltVersionMetadata(t *testing.T, body string) *ltMetadataXML {
	t.Helper()
	var doc struct {
		Metadata *ltMetadataXML `xml:"launchTemplateVersionSet>item>launchTemplateData>metadataOptions"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), body)
	return doc.Metadata
}

// ltInstanceMetadata launches from params and returns the instance's ID and metadataOptions
// as DescribeInstances reports them.
func ltInstanceMetadata(t *testing.T, ts *httptest.Server, params map[string]string) (string, *ltMetadataXML) {
	t.Helper()
	full := map[string]string{"Action": "RunInstances", "MinCount": "1", "MaxCount": "1"}
	for k, v := range params {
		full[k] = v
	}
	var run struct {
		ID    string `xml:"instancesSet>item>instanceId"`
		State string `xml:"instancesSet>item>metadataOptions>state"`
	}
	ec2DescribeXML(t, ts, full, &run)
	require.NotEmpty(t, run.ID)
	// The CLI's run-instances example output reports "State": "pending" for every launch; the
	// describe below reports applied.
	assert.Equal(t, "pending", run.State, "RunInstances reports the options pending")
	return run.ID, ltDescribeInstanceMetadata(t, ts, run.ID)
}

func ltDescribeInstanceMetadata(t *testing.T, ts *httptest.Server, id string) *ltMetadataXML {
	t.Helper()
	var doc struct {
		Metadata *ltMetadataXML `xml:"reservationSet>item>instancesSet>item>metadataOptions"`
	}
	ec2DescribeXML(t, ts, map[string]string{"Action": "DescribeInstances", "InstanceId.1": id}, &doc)
	return doc.Metadata
}

// TestEC2_LaunchTemplate_IssueRepro is #1471 as filed: aws-elastic-globus-compute's template
// data, which reads back with MetadataOptions and without the NetworkInterfaces key it never sent.
func TestEC2_LaunchTemplate_IssueRepro(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ltCreate(t, ts, "ltx", map[string]string{
		"LaunchTemplateData.ImageId":                                 ec2TestImageECSArm,
		"LaunchTemplateData.IamInstanceProfile.Arn":                  "arn:aws:iam::123456789012:instance-profile/aegc/x",
		"LaunchTemplateData.SecurityGroupId.1":                       "sg-0123456789abcdef0",
		"LaunchTemplateData.MetadataOptions.HttpTokens":              "required",
		"LaunchTemplateData.MetadataOptions.HttpPutResponseHopLimit": "2",
		"LaunchTemplateData.MetadataOptions.HttpEndpoint":            "enabled",
		"LaunchTemplateData.MetadataOptions.InstanceMetadataTags":    "enabled",
		"LaunchTemplateData.BlockDeviceMapping.1.DeviceName":         "/dev/xvda",
		"LaunchTemplateData.BlockDeviceMapping.1.Ebs.VolumeSize":     "30",
		"LaunchTemplateData.UserData":                                "IyEvYmluL3NoCg==",
		"LaunchTemplateData.TagSpecification.1.ResourceType":         "instance",
		"LaunchTemplateData.TagSpecification.1.Tag.1.Key":            "Name",
		"LaunchTemplateData.TagSpecification.1.Tag.1.Value":          "worker",
		"LaunchTemplateData.TagSpecification.2.ResourceType":         "volume",
		"LaunchTemplateData.TagSpecification.2.Tag.1.Key":            "Name",
		"LaunchTemplateData.TagSpecification.2.Tag.1.Value":          "worker",
	})
	body := ltVersionBody(t, ts, "ltx", "$Default")

	assert.Equal(t, &ltMetadataXML{
		HTTPTokens: strp("required"), HTTPPutResponseHopLimit: intp(2),
		HTTPEndpoint: strp("enabled"), InstanceMetadataTags: strp("enabled"),
	}, ltVersionMetadata(t, body), "exactly the four options sent, no state and no httpProtocolIpv6")
	assert.NotContains(t, body, "networkInterfaceSet", "no interface was sent, so none reads back, not even empty")
	assert.Contains(t, body, "<resourceType>volume</resourceType>", "the volume tag scope reads back as well as the instance one")

	var doc struct {
		Data struct {
			Elements []struct {
				XMLName xml.Name
			} `xml:",any"`
		} `xml:"launchTemplateVersionSet>item>launchTemplateData"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc))
	var keys []string
	for _, el := range doc.Data.Elements {
		keys = append(keys, el.XMLName.Local)
	}
	assert.ElementsMatch(t, []string{
		"blockDeviceMappingSet", "iamInstanceProfile", "imageId", "metadataOptions",
		"securityGroupIdSet", "tagSpecificationSet", "userData",
	}, keys, "the members the request carried, and only those")
}

// TestEC2_LaunchTemplate_EmptyListsAreOmitted pins the encoding fix behind the phantom key: a
// template carrying no list members reads back with no list elements at all, and an interface
// that names no groups or deleteOnTermination reads back without them.
func TestEC2_LaunchTemplate_EmptyListsAreOmitted(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ltCreate(t, ts, "bare", map[string]string{"LaunchTemplateData.InstanceType": "c7g.large"})
	body := ltVersionBody(t, ts, "bare", "1")
	for _, el := range []string{"blockDeviceMappingSet", "securityGroupIdSet", "networkInterfaceSet", "tagSpecificationSet", "metadataOptions", "disableApiTermination"} {
		assert.NotContains(t, body, el)
	}

	ltCreate(t, ts, "ifc", map[string]string{
		"LaunchTemplateData.NetworkInterface.1.DeviceIndex":         "0",
		"LaunchTemplateData.NetworkInterface.1.SubnetId":            "subnet-0123456789abcdef0",
		"LaunchTemplateData.NetworkInterface.2.DeviceIndex":         "1",
		"LaunchTemplateData.NetworkInterface.2.SubnetId":            "subnet-0123456789abcdef0",
		"LaunchTemplateData.NetworkInterface.2.DeleteOnTermination": "false",
	})
	var doc struct {
		Interfaces []struct {
			DeviceIndex int       `xml:"deviceIndex"`
			Groups      *struct{} `xml:"groupSet"`
			DOT         *bool     `xml:"deleteOnTermination"`
		} `xml:"launchTemplateVersionSet>item>launchTemplateData>networkInterfaceSet>item"`
	}
	require.NoError(t, xml.Unmarshal([]byte(ltVersionBody(t, ts, "ifc", "1")), &doc))
	require.Len(t, doc.Interfaces, 2)
	assert.Nil(t, doc.Interfaces[0].Groups, "no groups were named")
	assert.Nil(t, doc.Interfaces[0].DOT, "deleteOnTermination was not stated, so the launch default does not read back")
	require.NotNil(t, doc.Interfaces[1].DOT)
	assert.False(t, *doc.Interfaces[1].DOT, "a stated false reads back as false")
}

// TestEC2_LaunchTemplate_MetadataOptionsValidation pins the published value sets, which
// CreateLaunchTemplate, CreateLaunchTemplateVersion and RunInstances share.
func TestEC2_LaunchTemplate_MetadataOptionsValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		member, value string
		ok            bool
	}{
		{"HttpTokens", "required", true},
		{"HttpTokens", "optional", true},
		{"HttpTokens", "Required", false},
		{"HttpTokens", "v2", false},
		{"HttpEndpoint", "disabled", true},
		{"HttpEndpoint", "on", false},
		{"HttpProtocolIpv6", "enabled", true},
		{"HttpProtocolIpv6", "true", false},
		{"InstanceMetadataTags", "enabled", true},
		{"InstanceMetadataTags", "yes", false},
		{"HttpPutResponseHopLimit", "1", true},
		{"HttpPutResponseHopLimit", "64", true},
		{"HttpPutResponseHopLimit", "0", false},
		{"HttpPutResponseHopLimit", "65", false},
		{"HttpPutResponseHopLimit", "two", false},
	}
	ts := newEC2TestServer(t)
	for i, tc := range tests {
		for _, op := range []struct {
			name   string
			params map[string]string
		}{
			{"CreateLaunchTemplate", map[string]string{
				"Action": "CreateLaunchTemplate", "LaunchTemplateName": "v" + tc.member + tc.value,
				"LaunchTemplateData.MetadataOptions." + tc.member: tc.value,
			}},
			{"RunInstances", map[string]string{
				"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
				"MetadataOptions." + tc.member: tc.value,
			}},
		} {
			status, code, _ := ec2ErrorDetail(t, ts, op.params)
			if tc.ok {
				assert.Equal(t, http.StatusOK, status, "%d %s %s=%s", i, op.name, tc.member, tc.value)
				continue
			}
			assert.Equal(t, http.StatusBadRequest, status, "%s %s=%s", op.name, tc.member, tc.value)
			assert.Equal(t, "InvalidParameterValue", code, "%s %s=%s", op.name, tc.member, tc.value)
		}
	}
}

// TestEC2_LaunchTemplate_MetadataOptionsReachTheInstance covers the launch half: a template's
// options reach the instance, a launch's own options win per option, and the members AWS
// defaults unconditionally are rendered while the AMI-dependent two are not invented.
func TestEC2_LaunchTemplate_MetadataOptionsReachTheInstance(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ltID := ltCreate(t, ts, "imds", map[string]string{
		"LaunchTemplateData.ImageId":                                 ec2TestImageECS2,
		"LaunchTemplateData.MetadataOptions.HttpTokens":              "required",
		"LaunchTemplateData.MetadataOptions.HttpPutResponseHopLimit": "2",
		"LaunchTemplateData.MetadataOptions.InstanceMetadataTags":    "enabled",
	})

	tests := []struct {
		name   string
		params map[string]string
		want   ltMetadataXML
	}{
		{"from the template", map[string]string{"LaunchTemplate.LaunchTemplateId": ltID}, ltMetadataXML{
			State: strp("applied"), HTTPTokens: strp("required"), HTTPPutResponseHopLimit: intp(2),
			HTTPEndpoint: strp("enabled"), HTTPProtocolIPv6: strp("disabled"), InstanceMetadataTags: strp("enabled"),
		}},
		{"the launch overrides one option and keeps the rest", map[string]string{
			"LaunchTemplate.LaunchTemplateName":       "imds",
			"MetadataOptions.HttpPutResponseHopLimit": "1",
			"MetadataOptions.HttpProtocolIpv6":        "enabled",
		}, ltMetadataXML{
			State: strp("applied"), HTTPTokens: strp("required"), HTTPPutResponseHopLimit: intp(1),
			HTTPEndpoint: strp("enabled"), HTTPProtocolIPv6: strp("enabled"), InstanceMetadataTags: strp("enabled"),
		}},
		{"no template and no options", map[string]string{"ImageId": ec2TestImage}, ltMetadataXML{
			State: strp("applied"), HTTPEndpoint: strp("enabled"), HTTPProtocolIPv6: strp("disabled"),
			InstanceMetadataTags: strp("disabled"),
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, got := ltInstanceMetadata(t, ts, tc.params)
			require.NotNil(t, got)
			assert.Equal(t, tc.want, *got)
		})
	}
}

// TestEC2_ModifyInstanceMetadataOptions pins the operation's merge, its pending-then-applied
// state, and its refusals.
func TestEC2_ModifyInstanceMetadataOptions(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	id, _ := ltInstanceMetadata(t, ts, map[string]string{
		"ImageId": ec2TestImage, "MetadataOptions.HttpTokens": "optional", "MetadataOptions.HttpPutResponseHopLimit": "3",
	})

	var resp struct {
		InstanceID string        `xml:"instanceId"`
		Options    ltMetadataXML `xml:"instanceMetadataOptions"`
	}
	ec2DescribeXML(t, ts, map[string]string{
		"Action": "ModifyInstanceMetadataOptions", "InstanceId": id, "HttpTokens": "required",
	}, &resp)
	assert.Equal(t, id, resp.InstanceID)
	assert.Equal(t, ltMetadataXML{
		State: strp("pending"), HTTPTokens: strp("required"), HTTPPutResponseHopLimit: intp(3),
		HTTPEndpoint: strp("enabled"), HTTPProtocolIPv6: strp("disabled"), InstanceMetadataTags: strp("disabled"),
	}, resp.Options, "the named option changes, the existing state is maintained for the rest")

	got := ltDescribeInstanceMetadata(t, ts, id)
	require.NotNil(t, got)
	assert.Equal(t, "applied", *got.State, "a later describe reports the change applied")
	assert.Equal(t, "required", *got.HTTPTokens)
	assert.Equal(t, 3, *got.HTTPPutResponseHopLimit)

	refusals := []struct {
		name     string
		params   map[string]string
		wantCode string
	}{
		{"no instance", map[string]string{"HttpTokens": "required"}, "MissingParameter"},
		{"an instance that does not exist", map[string]string{"InstanceId": "i-0123456789abcdef0", "HttpTokens": "required"}, "InvalidInstanceID.NotFound"},
		{"a malformed instance ID", map[string]string{"InstanceId": "i-nope", "HttpTokens": "required"}, "InvalidInstanceID.Malformed"},
		{"a value outside the set", map[string]string{"InstanceId": id, "HttpEndpoint": "off"}, "InvalidParameterValue"},
	}
	for _, tc := range refusals {
		t.Run(tc.name, func(t *testing.T) {
			tc.params["Action"] = "ModifyInstanceMetadataOptions"
			status, code, _ := ec2ErrorDetail(t, ts, tc.params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.wantCode, code)
		})
	}

	ec2DescribeXML(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": id}, &struct{}{})
	status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action": "ModifyInstanceMetadataOptions", "InstanceId": id, "HttpTokens": "required",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "IncorrectInstanceState", code, "a terminated instance has no metadata options to change")
}

// TestEC2_LaunchTemplateVersion_MetadataOptionsInheritance pins SourceVersion: a version that
// names no MetadataOptions inherits the source's, and one that names any replaces them whole.
func TestEC2_LaunchTemplateVersion_MetadataOptionsInheritance(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ltCreate(t, ts, "inherit", map[string]string{
		"LaunchTemplateData.ImageId":                                 ec2TestImage,
		"LaunchTemplateData.MetadataOptions.HttpTokens":              "required",
		"LaunchTemplateData.MetadataOptions.HttpPutResponseHopLimit": "2",
	})
	for _, v := range []map[string]string{
		{"SourceVersion": "1", "LaunchTemplateData.InstanceType": "m7g.large"},
		{"SourceVersion": "1", "LaunchTemplateData.MetadataOptions.HttpEndpoint": "disabled"},
		{"LaunchTemplateData.InstanceType": "m7g.large"},
	} {
		v["Action"] = "CreateLaunchTemplateVersion"
		v["LaunchTemplateName"] = "inherit"
		ec2DescribeXML(t, ts, v, &struct{}{})
	}
	assert.Equal(t, &ltMetadataXML{HTTPTokens: strp("required"), HTTPPutResponseHopLimit: intp(2)},
		ltVersionMetadata(t, ltVersionBody(t, ts, "inherit", "2")), "inherited from the source")
	assert.Equal(t, &ltMetadataXML{HTTPEndpoint: strp("disabled")},
		ltVersionMetadata(t, ltVersionBody(t, ts, "inherit", "3")), "replaced whole, as one parameter")
	assert.Nil(t, ltVersionMetadata(t, ltVersionBody(t, ts, "inherit", "4")), "no SourceVersion inherits nothing")
}

// TestEC2_LaunchTemplate_DisableApiTerminationAndSecurityGroups covers the two other members
// #1471's sweep found dropped: DisableApiTermination reaches the instance, unless the launch
// states its own, and every SecurityGroupId.N is kept rather than only the first.
func TestEC2_LaunchTemplate_DisableApiTerminationAndSecurityGroups(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ltCreate(t, ts, "protected", map[string]string{
		"LaunchTemplateData.ImageId":               ec2TestImage,
		"LaunchTemplateData.DisableApiTermination": "true",
	})
	body := ltVersionBody(t, ts, "protected", "1")
	assert.Contains(t, body, "<disableApiTermination>true</disableApiTermination>")

	terminate := func(id string) string {
		_, code, _ := ec2ErrorDetail(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": id})
		return code
	}
	id, _ := ltInstanceMetadata(t, ts, map[string]string{"LaunchTemplate.LaunchTemplateName": "protected"})
	assert.Equal(t, "OperationNotPermitted", terminate(id), "the template's protection reached the instance")
	id, _ = ltInstanceMetadata(t, ts, map[string]string{"LaunchTemplate.LaunchTemplateName": "protected", "DisableApiTermination": "false"})
	assert.Empty(t, terminate(id), "a launch stating false wins over the template")

	status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action": "CreateLaunchTemplate", "LaunchTemplateName": "badbool", "LaunchTemplateData.DisableApiTermination": "maybe",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValue", code)

	ltCreate(t, ts, "twogroups", map[string]string{
		"LaunchTemplateData.SecurityGroupId.1": "sg-0123456789abcdef0",
		"LaunchTemplateData.SecurityGroupId.2": "sg-0fedcba9876543210",
	})
	var doc struct {
		Groups []string `xml:"launchTemplateVersionSet>item>launchTemplateData>securityGroupIdSet>item"`
	}
	require.NoError(t, xml.Unmarshal([]byte(ltVersionBody(t, ts, "twogroups", "1")), &doc))
	assert.Equal(t, []string{"sg-0123456789abcdef0", "sg-0fedcba9876543210"}, doc.Groups)
}

// TestEC2_DescribeLaunchTemplates_UnknownIsRefused is #1469 as filed, and its ID twin.
func TestEC2_DescribeLaunchTemplates_UnknownIsRefused(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ltID := ltCreate(t, ts, "real", map[string]string{"LaunchTemplateData.ImageId": ec2TestImage})

	const msg = "At least one of the launch templates specified in the request does not exist."
	tests := []struct {
		name     string
		params   map[string]string
		wantCode string
	}{
		{"an unknown name", map[string]string{"LaunchTemplateName.1": "nope"}, "InvalidLaunchTemplateName.NotFoundException"},
		{"an unknown ID", map[string]string{"LaunchTemplateId.1": "lt-0123456789abcdef0"}, "InvalidLaunchTemplateId.NotFound"},
		{"one known and one unknown name", map[string]string{"LaunchTemplateName.1": "real", "LaunchTemplateName.2": "nope"}, "InvalidLaunchTemplateName.NotFoundException"},
		{"a known ID and an unknown name", map[string]string{"LaunchTemplateId.1": ltID, "LaunchTemplateName.1": "nope"}, "InvalidLaunchTemplateName.NotFoundException"},
		{"an unknown ID is reported before an unknown name", map[string]string{"LaunchTemplateId.1": "lt-0123456789abcdef0", "LaunchTemplateName.1": "nope"}, "InvalidLaunchTemplateId.NotFound"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.params["Action"] = "DescribeLaunchTemplates"
			status, code, message := ec2ErrorDetail(t, ts, tc.params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.wantCode, code)
			assert.Equal(t, msg, message)
		})
	}

	// A template that exists and that a filter excludes is an empty answer, not a refusal.
	var doc struct {
		IDs []string `xml:"launchTemplates>item>launchTemplateId"`
	}
	ec2DescribeXML(t, ts, map[string]string{
		"Action": "DescribeLaunchTemplates", "LaunchTemplateName.1": "real",
		"Filter.1.Name": "launch-template-name", "Filter.1.Value.1": "other",
	}, &doc)
	assert.Empty(t, doc.IDs)
	ec2DescribeXML(t, ts, map[string]string{"Action": "DescribeLaunchTemplates", "LaunchTemplateName.1": "real"}, &doc)
	assert.Equal(t, []string{ltID}, doc.IDs)
}

// TestEC2_LaunchTemplateNotFound_ByTheIdentifierNamed pins every other operation that names one
// template: by ID it is InvalidLaunchTemplateId.NotFound, by name
// InvalidLaunchTemplateName.NotFoundException — where all of them answered the ID code before
// #1469, and RunInstances skipped an unknown template altogether.
func TestEC2_LaunchTemplateNotFound_ByTheIdentifierNamed(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	const missingID = "lt-0123456789abcdef0"
	ops := []struct {
		action string
		id     map[string]string
		name   map[string]string
	}{
		{"DescribeLaunchTemplateVersions",
			map[string]string{"LaunchTemplateId": missingID},
			map[string]string{"LaunchTemplateName": "nope"}},
		{"DeleteLaunchTemplate",
			map[string]string{"LaunchTemplateId": missingID},
			map[string]string{"LaunchTemplateName": "nope"}},
		{"CreateLaunchTemplateVersion",
			map[string]string{"LaunchTemplateId": missingID, "LaunchTemplateData.InstanceType": "c7g.large"},
			map[string]string{"LaunchTemplateName": "nope", "LaunchTemplateData.InstanceType": "c7g.large"}},
		{"ModifyLaunchTemplate",
			map[string]string{"LaunchTemplateId": missingID, "SetDefaultVersion": "1"},
			map[string]string{"LaunchTemplateName": "nope", "SetDefaultVersion": "1"}},
		{"DeleteLaunchTemplateVersions",
			map[string]string{"LaunchTemplateId": missingID, "LaunchTemplateVersion.1": "1"},
			map[string]string{"LaunchTemplateName": "nope", "LaunchTemplateVersion.1": "1"}},
		{"RunInstances",
			map[string]string{"LaunchTemplate.LaunchTemplateId": missingID, "MinCount": "1", "MaxCount": "1"},
			map[string]string{"LaunchTemplate.LaunchTemplateName": "nope", "MinCount": "1", "MaxCount": "1"}},
	}
	for _, op := range ops {
		t.Run(op.action, func(t *testing.T) {
			op.id["Action"] = op.action
			status, code, message := ec2ErrorDetail(t, ts, op.id)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidLaunchTemplateId.NotFound", code)
			assert.Equal(t, "The specified launch template, with template ID "+missingID+", does not exist.", message)

			op.name["Action"] = op.action
			status, code, message = ec2ErrorDetail(t, ts, op.name)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidLaunchTemplateName.NotFoundException", code)
			assert.Equal(t, "The specified launch template, with template name nope, does not exist.", message)
		})
	}
}
